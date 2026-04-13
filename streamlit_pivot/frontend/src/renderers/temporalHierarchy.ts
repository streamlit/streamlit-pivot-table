/**
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Temporal hierarchy helpers for hierarchical date column headers.
 *
 * These helpers compute header-row-level mappings, parent bucket groupings,
 * and collapse state for temporal column fields with hierarchy levels > 1.
 * All logic is rendering-only: column keys in PivotData stay single-segment.
 */

import { makeKeyString } from "../engine/PivotData";
import {
  extractParentBuckets,
  buildModifiedColKey,
  formatTemporalParentLabel,
  isTemporalColumnType,
} from "../engine/dateGrouping";
import {
  type DateGrain,
  type PivotConfigV1,
  type ColumnTypeMap,
  getEffectiveDateGrain,
  getTemporalHierarchyLevels,
} from "../engine/types";
import type { ColSlot } from "./TableRenderer";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Metadata for a single column field's temporal hierarchy. */
export interface TemporalColInfo {
  dimIndex: number;
  field: string;
  grain: DateGrain;
  hierarchyLevels: DateGrain[];
}

/** Maps a physical header row to its source column dimension and hierarchy. */
export interface HeaderLevelMapping {
  /** Index into config.columns */
  dimIndex: number;
  /** The column field name */
  field: string;
  /** Grain at this hierarchy level */
  grain: DateGrain;
  /** Position within the temporal hierarchy (0 = outermost) */
  hierarchyOffset: number;
  /** Whether this is the leaf (finest) level for this field */
  isLeaf: boolean;
  /** Whether this level is part of a temporal hierarchy */
  isTemporal: boolean;
}

/** A group of consecutive column slots sharing the same parent bucket. */
export interface ParentGroup {
  parentBucket: string;
  label: string;
  /** Indices into the colSlots array (start inclusive, end exclusive). */
  startIdx: number;
  endIdx: number;
  /** The stringified modified column key for collapse state. */
  collapseKey: string;
  isCollapsed: boolean;
}

// ---------------------------------------------------------------------------
// Core computations
// ---------------------------------------------------------------------------

/**
 * Compute temporal hierarchy info for all column fields.
 * Returns only fields that have hierarchy levels > 1.
 */
export function computeTemporalColInfos(
  config: PivotConfigV1,
  columnTypes?: ColumnTypeMap,
  adaptiveDateGrains?: Record<string, DateGrain>,
): TemporalColInfo[] {
  const result: TemporalColInfo[] = [];
  for (let i = 0; i < config.columns.length; i++) {
    const field = config.columns[i]!;
    const ct = columnTypes?.get(field);
    if (!ct || !isTemporalColumnType(ct)) continue;
    const grain = getEffectiveDateGrain(
      config,
      field,
      ct,
      adaptiveDateGrains?.[field],
    );
    if (!grain || grain === "year") continue;
    const levels = getTemporalHierarchyLevels(grain);
    if (levels.length <= 1) continue;
    result.push({ dimIndex: i, field, grain, hierarchyLevels: levels });
  }
  return result;
}

/**
 * Build the array of header level mappings.
 * Each entry describes what a physical header row represents.
 */
export function computeHeaderLevels(
  config: PivotConfigV1,
  temporalInfos: TemporalColInfo[],
): HeaderLevelMapping[] {
  const temporalByIndex = new Map(temporalInfos.map((t) => [t.dimIndex, t]));
  const levels: HeaderLevelMapping[] = [];

  for (let i = 0; i < config.columns.length; i++) {
    const field = config.columns[i]!;
    const tInfo = temporalByIndex.get(i);
    if (tInfo) {
      for (let h = 0; h < tInfo.hierarchyLevels.length; h++) {
        levels.push({
          dimIndex: i,
          field,
          grain: tInfo.hierarchyLevels[h]!,
          hierarchyOffset: h,
          isLeaf: h === tInfo.hierarchyLevels.length - 1,
          isTemporal: true,
        });
      }
    } else {
      const grain = (getEffectiveDateGrain(config, field) ??
        field) as DateGrain;
      levels.push({
        dimIndex: i,
        field,
        grain,
        hierarchyOffset: 0,
        isLeaf: true,
        isTemporal: false,
      });
    }
  }

  return levels;
}

/**
 * Compute the total number of header rows including temporal hierarchy.
 */
export function computeNumHeaderLevels(
  config: PivotConfigV1,
  temporalInfos: TemporalColInfo[],
): number {
  if (config.columns.length === 0) return 1;
  const temporalByIndex = new Map(temporalInfos.map((t) => [t.dimIndex, t]));
  let total = 0;
  for (let i = 0; i < config.columns.length; i++) {
    const tInfo = temporalByIndex.get(i);
    total += tInfo ? tInfo.hierarchyLevels.length : 1;
  }
  return total;
}

/**
 * Group consecutive column slots by their parent bucket at a given temporal
 * hierarchy level. Returns parent groups with their collapse state.
 */
export function computeParentGroups(
  colSlots: ColSlot[],
  slotOffset: number,
  slotCount: number,
  temporalInfo: TemporalColInfo,
  parentGrain: DateGrain,
  parentLevelIdx: number,
  config: PivotConfigV1,
  dimensionFormatPattern?: string,
): ParentGroup[] {
  const groups: ParentGroup[] = [];
  const collapsedKeys = new Set(
    config.collapsed_temporal_groups?.[temporalInfo.field] ?? [],
  );

  let i = 0;
  while (i < slotCount) {
    const slot = colSlots[slotOffset + i]!;
    if (slot.collapsedLevel !== undefined) {
      i++;
      continue;
    }
    // Skip temporal-collapsed slots only when the collapse happened at a
    // HIGHER (outer) level in the hierarchy.  If the collapse is at the
    // current level, we need to emit a collapsed group header.  If the
    // collapse is at a LOWER (inner) level, the slot still belongs to
    // the current level's parent group.
    const tCollapse = (slot as TemporalColSlot).temporalCollapse;
    if (tCollapse) {
      const collapseIdx = temporalInfo.hierarchyLevels.indexOf(
        tCollapse.parentGrain,
      );
      if (collapseIdx >= 0 && collapseIdx < parentLevelIdx) {
        i++;
        continue;
      }
    }

    const leafKey = slot.key[temporalInfo.dimIndex] ?? "";
    const parentBuckets = extractParentBuckets(leafKey, temporalInfo.grain);
    const parentBucket = parentBuckets[parentLevelIdx] ?? leafKey;

    const startIdx = i;
    let endIdx = i + 1;

    // Group consecutive slots with the same parent bucket AND same sibling context
    while (endIdx < slotCount) {
      const nextSlot = colSlots[slotOffset + endIdx]!;
      if (nextSlot.collapsedLevel !== undefined) break;
      const nextLeafKey = nextSlot.key[temporalInfo.dimIndex] ?? "";
      const nextParentBuckets = extractParentBuckets(
        nextLeafKey,
        temporalInfo.grain,
      );
      const nextParentBucket = nextParentBuckets[parentLevelIdx] ?? nextLeafKey;
      if (nextParentBucket !== parentBucket) break;

      // Only check dimensions BEFORE the temporal field — those are
      // outer siblings whose context must match.  Dimensions AFTER the
      // temporal field are children that vary within the same parent span.
      let siblingMatch = true;
      for (let idx = 0; idx < temporalInfo.dimIndex; idx++) {
        if (slot.key[idx] !== nextSlot.key[idx]) {
          siblingMatch = false;
          break;
        }
      }
      if (!siblingMatch) break;
      endIdx++;
    }

    // Build collapse key from the prefix up to and including the temporal
    // field — child dimensions after the temporal field are NOT included
    // so that all children under the same parent share one collapse key.
    const modifiedColKey = buildModifiedColKey(
      slot.key,
      temporalInfo.dimIndex,
      temporalInfo.field,
      parentBucket,
    );
    const collapsePrefix = modifiedColKey.slice(0, temporalInfo.dimIndex + 1);
    const collapseKey = makeKeyString(collapsePrefix);

    groups.push({
      parentBucket,
      label: formatTemporalParentLabel(
        parentBucket,
        parentGrain,
        dimensionFormatPattern,
      ),
      startIdx,
      endIdx,
      collapseKey,
      isCollapsed: collapsedKeys.has(collapseKey),
    });

    i = endIdx;
  }

  return groups;
}

/**
 * Filter colSlots to hide leaf columns under collapsed temporal parents.
 * Returns a new array of ColSlots with collapsed groups replaced by a
 * single representative slot, plus metadata about which slots are
 * temporal-collapsed.
 */
export interface TemporalColSlot extends ColSlot {
  /** If set, this slot represents a collapsed temporal parent. */
  temporalCollapse?: {
    field: string;
    parentBucket: string;
    parentGrain: DateGrain;
    modifiedColKey: string[];
  };
}

export function computeTemporalColSlots(
  colSlots: ColSlot[],
  temporalInfos: TemporalColInfo[],
  config: PivotConfigV1,
): TemporalColSlot[] {
  if (temporalInfos.length === 0) return colSlots;

  // Pre-build collapsed-key sets per field (avoids per-slot reconstruction).
  const collapsedSets = new Map<string, Set<string>>();
  for (const tInfo of temporalInfos) {
    const keys = config.collapsed_temporal_groups?.[tInfo.field];
    if (keys && keys.length > 0) {
      collapsedSets.set(tInfo.field, new Set(keys));
    }
  }
  if (collapsedSets.size === 0) return colSlots;

  // Tracks which collapsed parent groups already have a representative slot.
  // Key: "field\x01parentBucket\x01outerPrefix"
  const emittedCollapse = new Set<string>();

  const result: TemporalColSlot[] = [];

  for (let i = 0; i < colSlots.length; i++) {
    const slot = colSlots[i]!;

    if (slot.collapsedLevel !== undefined) {
      result.push(slot);
      continue;
    }

    let collapsed = false;
    for (const tInfo of temporalInfos) {
      const collapsedKeys = collapsedSets.get(tInfo.field);
      if (!collapsedKeys) continue;

      const leafKey = slot.key[tInfo.dimIndex] ?? "";
      const parentBuckets = extractParentBuckets(leafKey, tInfo.grain);

      // When parentBuckets is empty (null/empty leaf key), fall back to
      // the leaf key itself at every hierarchy level — matching the
      // fallback behaviour in computeParentGroups.
      const effectiveBuckets =
        parentBuckets.length > 0
          ? parentBuckets
          : tInfo.hierarchyLevels.slice(0, -1).map(() => leafKey);

      for (let lvl = 0; lvl < effectiveBuckets.length; lvl++) {
        const parentBucket = effectiveBuckets[lvl]!;
        const modifiedColKey = buildModifiedColKey(
          slot.key,
          tInfo.dimIndex,
          tInfo.field,
          parentBucket,
        );
        const collapsePrefix = modifiedColKey.slice(0, tInfo.dimIndex + 1);
        const collapseKey = makeKeyString(collapsePrefix);

        if (collapsedKeys.has(collapseKey)) {
          // Emit a representative slot only once per parent + outer-sibling context.
          const outerPrefixStr = makeKeyString(
            slot.key.slice(0, tInfo.dimIndex),
          );
          const dedupeKey = `${tInfo.field}\x01${parentBucket}\x01${outerPrefixStr}`;
          if (!emittedCollapse.has(dedupeKey)) {
            emittedCollapse.add(dedupeKey);
            result.push({
              key: slot.key,
              temporalCollapse: {
                field: tInfo.field,
                parentBucket,
                parentGrain: tInfo.hierarchyLevels[lvl]!,
                modifiedColKey,
              },
            });
          }
          collapsed = true;
          break;
        }
      }
      if (collapsed) break;
    }

    if (!collapsed) {
      result.push(slot);
    }
  }

  return result;
}

/**
 * Toggle a temporal parent's collapse state.
 * Returns the updated collapsed_temporal_groups record.
 */
export function toggleTemporalCollapse(
  current: Record<string, string[]> | undefined,
  field: string,
  collapseKey: string,
): Record<string, string[]> {
  const existing = current?.[field] ?? [];
  const set = new Set(existing);
  if (set.has(collapseKey)) {
    set.delete(collapseKey);
  } else {
    set.add(collapseKey);
  }
  const updated = { ...current };
  if (set.size === 0) {
    delete updated[field];
  } else {
    updated[field] = [...set];
  }
  return updated;
}
