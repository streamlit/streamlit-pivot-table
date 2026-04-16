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

import { makeKeyString, type GroupedRow } from "../engine/PivotData";
import {
  extractParentBuckets,
  buildModifiedColKey,
  buildModifiedRowKey,
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

export type TemporalRowInfo = TemporalColInfo;

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

export type RowHeaderLevelMapping = HeaderLevelMapping;

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
function computeTemporalInfosForFields(
  fields: string[],
  config: PivotConfigV1,
  columnTypes?: ColumnTypeMap,
  adaptiveDateGrains?: Record<string, DateGrain>,
): TemporalColInfo[] {
  const result: TemporalColInfo[] = [];
  for (let i = 0; i < fields.length; i++) {
    const field = fields[i]!;
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

export function computeTemporalColInfos(
  config: PivotConfigV1,
  columnTypes?: ColumnTypeMap,
  adaptiveDateGrains?: Record<string, DateGrain>,
): TemporalColInfo[] {
  return computeTemporalInfosForFields(
    config.columns,
    config,
    columnTypes,
    adaptiveDateGrains,
  );
}

export function computeTemporalRowInfos(
  config: PivotConfigV1,
  columnTypes?: ColumnTypeMap,
  adaptiveDateGrains?: Record<string, DateGrain>,
): TemporalRowInfo[] {
  return computeTemporalInfosForFields(
    config.rows,
    config,
    columnTypes,
    adaptiveDateGrains,
  );
}

/**
 * Build the array of header level mappings.
 * Each entry describes what a physical header row represents.
 */
function computeAxisHeaderLevels(
  fields: string[],
  config: PivotConfigV1,
  temporalInfos: TemporalColInfo[],
): HeaderLevelMapping[] {
  const temporalByIndex = new Map(temporalInfos.map((t) => [t.dimIndex, t]));
  const levels: HeaderLevelMapping[] = [];

  for (let i = 0; i < fields.length; i++) {
    const field = fields[i]!;
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

export function computeHeaderLevels(
  config: PivotConfigV1,
  temporalInfos: TemporalColInfo[],
): HeaderLevelMapping[] {
  return computeAxisHeaderLevels(config.columns, config, temporalInfos);
}

export function computeRowHeaderLevels(
  config: PivotConfigV1,
  temporalInfos: TemporalRowInfo[],
): RowHeaderLevelMapping[] {
  return computeAxisHeaderLevels(config.rows, config, temporalInfos);
}

/**
 * Compute the total number of header rows including temporal hierarchy.
 */
function computeAxisHeaderCount(
  fields: string[],
  temporalInfos: TemporalColInfo[],
): number {
  if (fields.length === 0) return 1;
  const temporalByIndex = new Map(temporalInfos.map((t) => [t.dimIndex, t]));
  let total = 0;
  for (let i = 0; i < fields.length; i++) {
    const tInfo = temporalByIndex.get(i);
    total += tInfo ? tInfo.hierarchyLevels.length : 1;
  }
  return total;
}

export function computeNumHeaderLevels(
  config: PivotConfigV1,
  temporalInfos: TemporalColInfo[],
): number {
  return computeAxisHeaderCount(config.columns, temporalInfos);
}

export function computeNumRowHeaderLevels(
  config: PivotConfigV1,
  temporalInfos: TemporalRowInfo[],
): number {
  return computeAxisHeaderCount(config.rows, temporalInfos);
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

// ---------------------------------------------------------------------------
// Row-side temporal hierarchy helpers
// ---------------------------------------------------------------------------

export interface TemporalParentRowMeta {
  field: string;
  parentBucket: string;
  parentGrain: DateGrain;
  modifiedRowKey: string[];
  rowDimIndex: number;
}

export interface TemporalParentRow {
  type: "temporal_parent";
  key: string[];
  level: number;
  temporalParent: TemporalParentRowMeta;
}

export type VisibleRowEntry = GroupedRow | TemporalParentRow;

export type ProjectedRowEntry = VisibleRowEntry & {
  headerValues: string[];
  headerSpanValues: string[];
  headerVisible: boolean[];
  headerSpacer: boolean[];
  headerIsTotal: boolean[];
};

function buildHierarchyValues(bucket: string, grain: DateGrain): string[] {
  const levels = getTemporalHierarchyLevels(grain);
  const parents = extractParentBuckets(bucket, grain);
  if (parents.length > 0) {
    return [...parents, bucket];
  }
  return levels.map(() => bucket);
}

function getHeaderLevelRanges(
  headerLevels: RowHeaderLevelMapping[],
): Map<number, { start: number; end: number }> {
  const ranges = new Map<number, { start: number; end: number }>();
  for (let i = 0; i < headerLevels.length; i++) {
    const level = headerLevels[i]!;
    const current = ranges.get(level.dimIndex);
    if (current) current.end = i + 1;
    else ranges.set(level.dimIndex, { start: i, end: i + 1 });
  }
  return ranges;
}

function fillProjectedCells(
  projected: ProjectedRowEntry,
  ranges: Map<number, { start: number; end: number }>,
  dimIndex: number,
  values: string[],
  visibleCount?: number,
): void {
  const range = ranges.get(dimIndex);
  if (!range) return;
  const count = visibleCount ?? values.length;
  for (let i = 0; i < range.end - range.start; i++) {
    projected.headerValues[range.start + i] = values[i] ?? "";
    projected.headerSpanValues[range.start + i] = values[i] ?? "";
    projected.headerVisible[range.start + i] = i < count;
  }
}

export function applyTemporalRowCollapse(
  entries: GroupedRow[],
  temporalInfos: TemporalRowInfo[],
  config: PivotConfigV1,
): VisibleRowEntry[] {
  if (temporalInfos.length === 0) return entries;
  const hierarchyMode = config.row_layout === "hierarchy";

  const collapsedSets = new Map<string, Set<string>>();
  for (const tInfo of temporalInfos) {
    const keys = config.collapsed_temporal_row_groups?.[tInfo.field];
    if (keys && keys.length > 0) {
      collapsedSets.set(tInfo.field, new Set(keys));
    }
  }
  if (collapsedSets.size === 0 && !hierarchyMode) return entries;

  const result: VisibleRowEntry[] = [];
  const emittedCollapse = new Set<string>();

  for (const entry of entries) {
    let collapsed = false;

    for (const tInfo of temporalInfos) {
      if (entry.key.length <= tInfo.dimIndex) continue;
      const collapsedKeys = collapsedSets.get(tInfo.field);

      const leafKey = entry.key[tInfo.dimIndex] ?? "";
      const parentBuckets = extractParentBuckets(leafKey, tInfo.grain);
      const effectiveBuckets =
        parentBuckets.length > 0
          ? parentBuckets
          : tInfo.hierarchyLevels.slice(0, -1).map(() => leafKey);

      for (let lvl = 0; lvl < effectiveBuckets.length; lvl++) {
        const parentBucket = effectiveBuckets[lvl]!;
        const modifiedRowKey = buildModifiedRowKey(
          entry.key,
          tInfo.dimIndex,
          tInfo.field,
          parentBucket,
        );
        const collapsePrefix = modifiedRowKey.slice(0, tInfo.dimIndex + 1);
        const collapseKey = makeKeyString(collapsePrefix);

        const outerPrefixStr = makeKeyString(
          entry.key.slice(0, tInfo.dimIndex),
        );
        const dedupeKey = `${tInfo.field}\x01${parentBucket}\x01${outerPrefixStr}`;
        if (hierarchyMode && !emittedCollapse.has(dedupeKey)) {
          emittedCollapse.add(dedupeKey);
          result.push({
            type: "temporal_parent",
            key: [...entry.key.slice(0, tInfo.dimIndex), parentBucket],
            level: tInfo.dimIndex,
            temporalParent: {
              field: tInfo.field,
              parentBucket,
              parentGrain: tInfo.hierarchyLevels[lvl]!,
              modifiedRowKey: collapsePrefix,
              rowDimIndex: tInfo.dimIndex,
            },
          });
        }
        if (!collapsedKeys?.has(collapseKey)) continue;

        if (!emittedCollapse.has(dedupeKey)) {
          emittedCollapse.add(dedupeKey);
          result.push({
            type: "temporal_parent",
            key: [...entry.key.slice(0, tInfo.dimIndex), parentBucket],
            level: tInfo.dimIndex,
            temporalParent: {
              field: tInfo.field,
              parentBucket,
              parentGrain: tInfo.hierarchyLevels[lvl]!,
              modifiedRowKey: collapsePrefix,
              rowDimIndex: tInfo.dimIndex,
            },
          });
        }
        collapsed = true;
        break;
      }

      if (collapsed) break;
    }

    if (!collapsed) {
      result.push(entry);
    }
  }

  return result;
}

export function projectVisibleRowEntries(
  entries: VisibleRowEntry[],
  config: PivotConfigV1,
  rowHeaderLevels: RowHeaderLevelMapping[],
  temporalInfos: TemporalRowInfo[],
): ProjectedRowEntry[] {
  const ranges = getHeaderLevelRanges(rowHeaderLevels);
  const temporalByIndex = new Map(temporalInfos.map((t) => [t.dimIndex, t]));
  const width = Math.max(rowHeaderLevels.length, 1);

  return entries.map((entry, entryIdx) => {
    const projected: ProjectedRowEntry = {
      ...entry,
      headerValues: new Array(width).fill(""),
      headerSpanValues: new Array(width).fill(""),
      headerVisible: new Array(width).fill(false),
      headerSpacer: new Array(width).fill(false),
      headerIsTotal: new Array(width).fill(false),
    };

    if (config.rows.length === 0) {
      projected.headerVisible[0] = true;
      projected.headerValues[0] =
        entry.type === "temporal_parent" ? "" : "Total";
      projected.headerSpanValues[0] = projected.headerValues[0]!;
      return projected;
    }

    if (entry.type === "data") {
      for (let dimIndex = 0; dimIndex < config.rows.length; dimIndex++) {
        const key = entry.key[dimIndex] ?? "";
        const tInfo = temporalByIndex.get(dimIndex);
        if (tInfo) {
          fillProjectedCells(
            projected,
            ranges,
            dimIndex,
            buildHierarchyValues(key, tInfo.grain),
          );
        } else {
          fillProjectedCells(projected, ranges, dimIndex, [key], 1);
        }
      }
      return projected;
    }

    if (entry.type === "subtotal") {
      for (let dimIndex = 0; dimIndex < entry.level; dimIndex++) {
        const key = entry.key[dimIndex] ?? "";
        const tInfo = temporalByIndex.get(dimIndex);
        if (tInfo) {
          fillProjectedCells(
            projected,
            ranges,
            dimIndex,
            buildHierarchyValues(key, tInfo.grain),
          );
        } else {
          fillProjectedCells(projected, ranges, dimIndex, [key], 1);
        }
      }

      const key = entry.key[entry.level] ?? "";
      const tInfo = temporalByIndex.get(entry.level);
      if (tInfo) {
        const values = buildHierarchyValues(key, tInfo.grain);
        fillProjectedCells(projected, ranges, entry.level, values);
        const range = ranges.get(entry.level);
        if (range) {
          const labelIdx = range.end - 1;
          projected.headerSpanValues[labelIdx] =
            `${values[values.length - 1] ?? ""}\x01subtotal`;
          projected.headerIsTotal[labelIdx] = true;
        }
      } else {
        fillProjectedCells(projected, ranges, entry.level, [key], 1);
        const range = ranges.get(entry.level);
        if (range) {
          projected.headerSpanValues[range.start] = `${key}\x01subtotal`;
          projected.headerIsTotal[range.start] = true;
        }
      }
      for (
        let dimIndex = entry.level + 1;
        dimIndex < config.rows.length;
        dimIndex++
      ) {
        const range = ranges.get(dimIndex);
        if (!range) continue;
        for (let i = range.start; i < range.end; i++) {
          projected.headerVisible[i] = true;
          projected.headerSpacer[i] = true;
          projected.headerSpanValues[i] = `__spacer__${entryIdx}_${i}`;
        }
      }
      return projected;
    }

    const parentEntry = entry as TemporalParentRow;
    for (
      let dimIndex = 0;
      dimIndex < parentEntry.temporalParent.rowDimIndex;
      dimIndex++
    ) {
      const key = parentEntry.key[dimIndex] ?? "";
      const tInfo = temporalByIndex.get(dimIndex);
      if (tInfo) {
        fillProjectedCells(
          projected,
          ranges,
          dimIndex,
          buildHierarchyValues(key, tInfo.grain),
        );
      } else {
        fillProjectedCells(projected, ranges, dimIndex, [key], 1);
      }
    }

    const rowDimIndex = parentEntry.temporalParent.rowDimIndex;
    const values = buildHierarchyValues(
      parentEntry.temporalParent.parentBucket,
      parentEntry.temporalParent.parentGrain,
    );
    fillProjectedCells(projected, ranges, rowDimIndex, values, values.length);
    const ownRange = ranges.get(rowDimIndex);
    if (ownRange) {
      for (let i = ownRange.start + values.length; i < ownRange.end; i++) {
        projected.headerVisible[i] = true;
        projected.headerSpacer[i] = true;
        projected.headerSpanValues[i] = `__spacer__${entryIdx}_${i}`;
      }
    }
    for (
      let dimIndex = rowDimIndex + 1;
      dimIndex < config.rows.length;
      dimIndex++
    ) {
      const range = ranges.get(dimIndex);
      if (!range) continue;
      for (let i = range.start; i < range.end; i++) {
        projected.headerVisible[i] = true;
        projected.headerSpacer[i] = true;
        projected.headerSpanValues[i] = `__spacer__${entryIdx}_${i}`;
      }
    }
    return projected;
  });
}

export function computeProjectedRowHeaderSpans(
  entries: ProjectedRowEntry[],
): number[][] {
  if (entries.length === 0) return [];
  const numCols = entries[0]!.headerValues.length;
  const spans = entries.map(() => new Array(numCols).fill(0));

  for (let colIdx = 0; colIdx < numCols; colIdx++) {
    let i = 0;
    while (i < entries.length) {
      const entry = entries[i]!;
      if (!entry.headerVisible[colIdx]) {
        i++;
        continue;
      }
      let span = 1;
      while (
        i + span < entries.length &&
        entries[i + span]!.headerVisible[colIdx] &&
        entries[i + span]!.headerSpanValues.slice(0, colIdx + 1).every(
          (v, idx) => v === entry.headerSpanValues[idx],
        )
      ) {
        span++;
      }
      spans[i]![colIdx] = span;
      i += span;
    }
  }

  return spans;
}

export function toggleTemporalRowCollapse(
  current: Record<string, string[]> | undefined,
  field: string,
  collapseKey: string,
): Record<string, string[]> {
  return toggleTemporalCollapse(current, field, collapseKey);
}
