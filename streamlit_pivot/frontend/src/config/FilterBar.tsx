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

import {
  type FC,
  type ReactElement,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type {
  DateGrain,
  DimensionFilter,
  MemberGroup,
  PivotConfigV1,
} from "../engine/types";
import { getDimensionLabel } from "../engine/types";
import type { PivotData } from "../engine/PivotData";
import { FilterPickerPopover } from "../shared/FilterPickerPopover";
import styles from "./FilterBar.module.css";
import toolbarStyles from "./Toolbar.module.css";

// ---------------------------------------------------------------------------
// FilterBar
// ---------------------------------------------------------------------------

export interface FilterBarProps {
  filterFields: string[];
  filters: Record<string, DimensionFilter> | undefined;
  pivotData: PivotData;
  config: PivotConfigV1;
  adaptiveDateGrains?: Record<string, DateGrain>;
  /** Pre-computed unique resolved values for off-axis fields in hybrid mode. */
  filterFieldValues?: Record<string, string[]>;
  onFilterChange: (field: string, filter: DimensionFilter | undefined) => void;
  /** Removes the field from filter_fields entirely (and clears its filter). */
  onRemoveField?: (field: string) => void;
  menuLimit?: number;
  /** Element inside the component tree to portal the picker into (for CSS var inheritance). */
  portalTarget?: Element | null;
  /** Active member groups across all dimensions (passed from config). */
  memberGroups?: MemberGroup[];
  /** Callback to clear all groups for a specific field. */
  onClearFieldGroups?: (field: string) => void;
  /** Opens the GroupManagerDialog for the given field. */
  onOpenGroupManager?: (field: string) => void;
}

export const FilterBar: FC<FilterBarProps> = ({
  filterFields,
  filters,
  pivotData,
  config,
  filterFieldValues,
  onFilterChange,
  onRemoveField,
  menuLimit,
  portalTarget,
  memberGroups,
  onClearFieldGroups,
  onOpenGroupManager,
}): ReactElement => {
  const [openField, setOpenField] = useState<string | null>(null);
  const [anchorRect, setAnchorRect] = useState<DOMRect | null>(null);
  // Maps field name → chip span element for scroll-tracking and keyboard activation.
  const chipRefs = useRef<Map<string, HTMLElement>>(new Map());
  // The currently open chip element for scroll/resize repositioning.
  const openChipRef = useRef<HTMLElement | null>(null);

  const openPickerForField = useCallback((field: string, el: HTMLElement) => {
    setOpenField(field);
    setAnchorRect(el.getBoundingClientRect());
    openChipRef.current = el;
  }, []);

  const handleChipClick = useCallback(
    (field: string, e: React.MouseEvent<HTMLElement>) => {
      if (openField === field) {
        setOpenField(null);
        setAnchorRect(null);
        openChipRef.current = null;
      } else {
        openPickerForField(field, e.currentTarget);
      }
    },
    [openField, openPickerForField],
  );

  const handleChipKeyDown = useCallback(
    (field: string, e: React.KeyboardEvent<HTMLSpanElement>) => {
      if (e.key !== "Enter" && e.key !== " ") return;
      e.preventDefault();
      const el = chipRefs.current.get(field);
      if (!el) return;
      if (openField === field) {
        setOpenField(null);
        setAnchorRect(null);
        openChipRef.current = null;
      } else {
        openPickerForField(field, el);
      }
    },
    [openField, openPickerForField],
  );

  // Keep picker anchored to the chip while the page or any ancestor scrolls.
  useEffect(() => {
    if (!openField || !openChipRef.current) return;
    const el = openChipRef.current;
    const update = () => {
      if (el.isConnected) setAnchorRect(el.getBoundingClientRect());
    };
    window.addEventListener("scroll", update, true);
    window.addEventListener("resize", update);
    return () => {
      window.removeEventListener("scroll", update, true);
      window.removeEventListener("resize", update);
    };
  }, [openField]);

  const handleRemoveChip = useCallback(
    (field: string, e: React.MouseEvent<HTMLButtonElement>) => {
      e.stopPropagation();
      if (openField === field) {
        setOpenField(null);
        setAnchorRect(null);
      }
      onRemoveField?.(field);
    },
    [openField, onRemoveField],
  );

  const handlePickerClose = useCallback(() => {
    setOpenField(null);
    setAnchorRect(null);
  }, []);

  const handleFilterChange = useCallback(
    (filter: DimensionFilter | undefined) => {
      if (openField) onFilterChange(openField, filter);
    },
    [openField, onFilterChange],
  );

  const explicitFilterFields = filterFields;
  const explicitFilterFieldSet = useMemo(
    () => new Set(explicitFilterFields),
    [explicitFilterFields],
  );
  const displayFilterFields = useMemo(() => {
    const merged = [...explicitFilterFields];
    for (const field of Object.keys(filters ?? {})) {
      if (!explicitFilterFieldSet.has(field)) merged.push(field);
    }
    return merged;
  }, [explicitFilterFields, explicitFilterFieldSet, filters]);
  const activeCount = displayFilterFields.filter((f) => !!filters?.[f]).length;

  // Aggregate active member groups by field for the groups badge row.
  const groupsByField = useMemo(() => {
    const map = new Map<string, MemberGroup[]>();
    for (const g of memberGroups ?? []) {
      const list = map.get(g.field) ?? [];
      list.push(g);
      map.set(g.field, list);
    }
    return map;
  }, [memberGroups]);
  const activeGroupCount = memberGroups?.length ?? 0;

  return (
    <div
      className={styles.filterBar}
      data-testid="filter-bar"
      role="toolbar"
      aria-label="Active filters"
    >
      {displayFilterFields.length > 0 && (
        <div
          className={styles.filterBarSection}
          data-testid="filter-bar-filters-section"
        >
          <span className={styles.filterTitle}>Filters</span>
          {activeCount > 0 && (
            <span className={toolbarStyles.countBadge}>{activeCount}</span>
          )}
          {displayFilterFields.map((field) => {
            const filter = filters?.[field];
            const label = getDimensionLabel(config, field);
            const isActive = !!filter;
            return (
              <span
                key={field}
                ref={(el) => {
                  if (el) chipRefs.current.set(field, el);
                  else chipRefs.current.delete(field);
                }}
                role="button"
                tabIndex={0}
                className={`${styles.filterChipGroup} ${isActive ? styles.filterChipGroupActive : ""}`}
                onClick={(e) => handleChipClick(field, e)}
                onKeyDown={(e) => handleChipKeyDown(field, e)}
                aria-pressed={openField === field}
                data-testid={`filter-chip-${field}`}
              >
                <span className={styles.filterChipLabel}>{label}</span>
                {/* Standalone ▾ button — same visual as chipMenuBtn on Values chips.
                    Clicks bubble to the outer span; tabIndex=-1 keeps focus on the span. */}
                <button
                  type="button"
                  className={styles.filterChipMenuBtn}
                  tabIndex={-1}
                  aria-hidden="true"
                  data-testid={`filter-chip-body-${field}`}
                >
                  ▾
                </button>
                {/* Remove button — removes field from filter_fields */}
                {onRemoveField && explicitFilterFieldSet.has(field) && (
                  <button
                    type="button"
                    className={styles.filterChipClear}
                    onClick={(e) => handleRemoveChip(field, e)}
                    aria-label={`Remove ${label} filter`}
                    data-testid={`filter-chip-clear-${field}`}
                  >
                    ×
                  </button>
                )}
              </span>
            );
          })}
        </div>
      )}

      {/* Member Groups chips — one per on-axis field with active groups */}
      {groupsByField.size > 0 && (
        <div
          className={styles.filterBarSection}
          data-testid="filter-bar-groups-section"
        >
          <span className={styles.filterTitle}>Groups</span>
          {activeGroupCount > 0 && (
            <span className={toolbarStyles.countBadge}>{activeGroupCount}</span>
          )}
          {[...groupsByField.entries()].map(([field, groups]) => (
            <span
              key={`groups-${field}`}
              role={onOpenGroupManager ? "button" : undefined}
              tabIndex={onOpenGroupManager ? 0 : undefined}
              className={`${styles.filterChipGroup} ${styles.filterChipGroupActive}${onOpenGroupManager ? ` ${styles.filterChipGroupClickable}` : ""}`}
              onClick={
                onOpenGroupManager ? () => onOpenGroupManager(field) : undefined
              }
              onKeyDown={
                onOpenGroupManager
                  ? (e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        onOpenGroupManager(field);
                      }
                    }
                  : undefined
              }
              aria-label={
                onOpenGroupManager
                  ? `Manage groups for ${getDimensionLabel(config, field)}`
                  : undefined
              }
              data-testid={`groups-chip-${field}`}
            >
              <span className={styles.filterChipLabel}>
                {getDimensionLabel(config, field)} · {groups.length} group
                {groups.length !== 1 ? "s" : ""}
              </span>
              {onOpenGroupManager && (
                <button
                  type="button"
                  className={styles.filterChipMenuBtn}
                  tabIndex={-1}
                  aria-hidden="true"
                  data-testid={`groups-chip-body-${field}`}
                >
                  ▾
                </button>
              )}
              {onClearFieldGroups && (
                <button
                  type="button"
                  className={styles.filterChipClear}
                  onClick={(e) => {
                    e.stopPropagation();
                    onClearFieldGroups(field);
                  }}
                  aria-label={`Clear groups for ${getDimensionLabel(config, field)}`}
                  data-testid={`groups-chip-clear-${field}`}
                >
                  ×
                </button>
              )}
            </span>
          ))}
        </div>
      )}

      {openField && anchorRect && (
        <FilterPickerPopover
          field={openField}
          filter={filters?.[openField]}
          uniqueValues={
            filterFieldValues?.[openField] ??
            pivotData.getUniqueValues(openField)
          }
          menuLimit={menuLimit}
          onFilterChange={handleFilterChange}
          onClose={handlePickerClose}
          anchorRect={anchorRect}
          portalTarget={portalTarget}
        />
      )}
    </div>
  );
};
