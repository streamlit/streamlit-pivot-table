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
  FC,
  KeyboardEvent,
  ReactElement,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type {
  DateGrain,
  DimensionFilter,
  PivotConfigV1,
  ShowValuesAs,
  SortConfig,
  TopNFilter,
  ValueFilter,
} from "../engine/types";
import {
  DATE_GRAIN_LABELS,
  DATE_GRAINS,
  getDrilledDateGrain,
  showSubtotalForDim,
  showTotalForMeasure,
  showRowTotals,
  showColumnTotals,
} from "../engine/types";
import { useClickOutside } from "../shared/useClickOutside";
import styles from "./HeaderMenu.module.css";

export interface HeaderMenuProps {
  /** The dimension name shown in the header (e.g. "Year", "Region"). */
  dimension: string;
  /** Optional display title when the underlying field has extra state like date grain. */
  title?: string;
  /** Which pivot axis this header belongs to. */
  axis: "row" | "col";
  /** Current sort config for this axis. */
  sortConfig?: SortConfig;
  /** When undefined, the sort section is hidden (e.g. non-interactive mode). */
  onSortChange?: (sort: SortConfig | undefined) => void;
  /** Current filter for this dimension. */
  filter?: DimensionFilter;
  /** Unique values for the filter checklist. */
  uniqueValues: string[];
  onFilterChange: (field: string, filter: DimensionFilter | undefined) => void;
  /** Available value fields for "sort by value" (e.g. Revenue, Profit). */
  valueFields?: string[];
  /** Column keys for row "sort by value in column X" picker. */
  colKeys?: string[][];
  /** Max items to show in the filter checklist. */
  menuLimit?: number;
  /** Whether to show the filter section (default true). */
  showFilter?: boolean;
  /** Current "Show Values As" mode for this field (only for value headers). */
  showValuesAs?: ShowValuesAs;
  /** Callback to change "Show Values As" mode (only for value headers). */
  onShowValuesAsChange?: (mode: ShowValuesAs) => void;
  /** Pivot config needed to compute checked states. */
  config?: PivotConfigV1;
  /** Callback to toggle subtotals for this dimension (row dim headers only). */
  onSubtotalToggle?: (dimension: string) => void;
  /** Callback to toggle a measure in row/column totals (value headers only). */
  onTotalToggle?: (measure: string, axis: "row" | "col") => void;
  /** Format a canonical key into a display label (e.g. ISO date → "Jan 15, 2024"). */
  formatLabel?: (key: string) => string;
  /** Active date grain for temporal dimensions. */
  dateGrain?: DateGrain;
  /** Callback to change date grain for temporal dimensions. */
  onDateGrainChange?: (grain: DateGrain | undefined) => void;
  /** Callback to drill the current grain up/down. */
  onDateDrill?: (direction: "up" | "down") => void;
  /** Whether period-comparison display modes should be shown. */
  supportsPeriodComparison?: boolean;
  /** Active Top N filter for this dimension (undefined = no active filter). */
  topNFilter?: TopNFilter;
  /** Callback to set/clear the Top N filter for this dimension. */
  onTopNFilterChange?: (filter: TopNFilter | undefined) => void;
  /** Active value filter for this dimension (undefined = no active filter). */
  valueFilter?: ValueFilter;
  /** Callback to set/clear the value filter for this dimension. */
  onValueFilterChange?: (filter: ValueFilter | undefined) => void;
  onClose: () => void;
}

type SortPreset = "key_asc" | "key_desc" | "value_asc" | "value_desc";

function sortConfigToPreset(sc: SortConfig | undefined): SortPreset | null {
  if (!sc) return null;
  if (sc.by === "key" && sc.direction === "asc") return "key_asc";
  if (sc.by === "key" && sc.direction === "desc") return "key_desc";
  if (sc.by === "value" && sc.direction === "asc") return "value_asc";
  if (sc.by === "value" && sc.direction === "desc") return "value_desc";
  return null;
}

const DEFAULT_MENU_LIMIT = 50;

const HeaderMenu: FC<HeaderMenuProps> = ({
  dimension,
  title,
  axis,
  sortConfig,
  onSortChange,
  filter,
  uniqueValues,
  onFilterChange,
  valueFields,
  colKeys,
  menuLimit = DEFAULT_MENU_LIMIT,
  showFilter = true,
  showValuesAs,
  onShowValuesAsChange,
  config,
  onSubtotalToggle,
  onTotalToggle,
  formatLabel,
  dateGrain,
  onDateGrainChange,
  onDateDrill,
  supportsPeriodComparison = false,
  topNFilter,
  onTopNFilterChange,
  valueFilter,
  onValueFilterChange,
  onClose,
}): ReactElement => {
  // Derive filter axis ("rows"/"columns") from the existing "row"/"col" axis prop.
  const filterAxis: "rows" | "columns" = axis === "col" ? "columns" : "rows";

  // Local state for Top N inputs (pre-populated from active filter, or defaults).
  const [topNDirection, setTopNDirection] = useState<"top" | "bottom">(
    topNFilter?.direction ?? "top",
  );
  const [topNValue, setTopNValue] = useState<string>(
    topNFilter?.n !== undefined ? String(topNFilter.n) : "10",
  );
  const [topNBy, setTopNBy] = useState<string>(topNFilter?.by ?? "");

  // Local state for value filter inputs.
  const [vfBy, setVfBy] = useState<string>(valueFilter?.by ?? "");
  const [vfOperator, setVfOperator] = useState<ValueFilter["operator"]>(
    valueFilter?.operator ?? "gt",
  );
  const [vfValue, setVfValue] = useState<string>(
    valueFilter?.value !== undefined ? String(valueFilter.value) : "",
  );
  const [vfValue2, setVfValue2] = useState<string>(
    valueFilter?.value2 !== undefined ? String(valueFilter.value2) : "",
  );

  const showSort = !!onSortChange;
  const containerRef = useRef<HTMLDivElement>(null);

  const applyFilter = useCallback(
    (next: DimensionFilter | undefined) => {
      onFilterChange(dimension, next);
    },
    [dimension, onFilterChange],
  );

  const applyTopN = useCallback(() => {
    if (!onTopNFilterChange) return;
    const n = parseInt(topNValue, 10);
    if (!topNBy || isNaN(n) || n < 1) return;
    onTopNFilterChange({
      field: dimension,
      n,
      by: topNBy,
      direction: topNDirection,
      axis: filterAxis,
    });
  }, [
    onTopNFilterChange,
    dimension,
    topNValue,
    topNBy,
    topNDirection,
    filterAxis,
  ]);

  const clearTopN = useCallback(() => {
    onTopNFilterChange?.(undefined);
  }, [onTopNFilterChange]);

  const applyValueFilter = useCallback(() => {
    if (!onValueFilterChange) return;
    const val = parseFloat(vfValue);
    if (!vfBy || isNaN(val)) return;
    const filter: ValueFilter = {
      field: dimension,
      by: vfBy,
      operator: vfOperator,
      value: val,
      axis: filterAxis,
    };
    if (vfOperator === "between") {
      const val2 = parseFloat(vfValue2);
      if (isNaN(val2)) return;
      filter.value2 = val2;
    }
    onValueFilterChange(filter);
  }, [
    onValueFilterChange,
    dimension,
    vfBy,
    vfOperator,
    vfValue,
    vfValue2,
    filterAxis,
  ]);

  const clearValueFilter = useCallback(() => {
    onValueFilterChange?.(undefined);
  }, [onValueFilterChange]);

  useClickOutside(containerRef, onClose, true);

  const currentPreset = sortConfigToPreset(sortConfig);
  const hasValueFields = valueFields && valueFields.length > 0;

  // ---- Sort handlers (only used when showSort is true) ----
  const handleSort = useCallback(
    (preset: SortPreset) => {
      if (!onSortChange) return;
      if (sortConfigToPreset(sortConfig) === preset) {
        onSortChange(undefined);
        return;
      }
      if (preset === "key_asc" || preset === "key_desc") {
        onSortChange({
          by: "key",
          direction: preset === "key_asc" ? "asc" : "desc",
        });
      } else {
        const vf = sortConfig?.value_field ?? valueFields?.[0];
        onSortChange({
          by: "value",
          direction: preset === "value_asc" ? "asc" : "desc",
          value_field: vf,
          col_key: sortConfig?.col_key,
        });
      }
    },
    [onSortChange, sortConfig, valueFields],
  );

  const handleValueFieldChange = useCallback(
    (field: string) => {
      onSortChange?.({
        by: "value",
        direction: sortConfig?.direction ?? "desc",
        value_field: field,
        col_key: sortConfig?.col_key,
      });
    },
    [onSortChange, sortConfig],
  );

  const handleColKeyChange = useCallback(
    (colKeyStr: string) => {
      const colKey = colKeyStr === "" ? undefined : colKeyStr.split("\x00");
      onSortChange?.({
        by: "value",
        direction: sortConfig?.direction ?? "desc",
        value_field: sortConfig?.value_field ?? valueFields?.[0],
        col_key: colKey,
      });
    },
    [onSortChange, sortConfig, valueFields],
  );

  const isValueSort = sortConfig?.by === "value";

  // ---- Filter state (draft-based) ----
  const [search, setSearch] = useState("");

  const includedSet = useMemo(() => {
    if (!filter?.include || filter.include.length === 0) return null;
    return new Set(filter.include);
  }, [filter?.include]);

  const excludedSet = useMemo(() => {
    if (!filter?.exclude || filter.exclude.length === 0) return null;
    return new Set(filter.exclude);
  }, [filter?.exclude]);

  const isChecked = useCallback(
    (val: string): boolean => {
      if (includedSet) return includedSet.has(val);
      if (excludedSet) return !excludedSet.has(val);
      return true;
    },
    [includedSet, excludedSet],
  );

  const filteredValues = useMemo(() => {
    if (!search) return uniqueValues;
    const lower = search.toLowerCase();
    return uniqueValues.filter((v) => {
      if (v.toLowerCase().includes(lower)) return true;
      const label = formatLabel?.(v);
      return label ? label.toLowerCase().includes(lower) : false;
    });
  }, [uniqueValues, search, formatLabel]);

  const visibleValues = filteredValues.slice(0, menuLimit);
  const overflowCount = filteredValues.length - visibleValues.length;

  const toggleValue = useCallback(
    (val: string) => {
      const checked = isChecked(val);
      if (checked) {
        if (includedSet) {
          const newInclude = [...includedSet].filter((v) => v !== val);
          applyFilter(
            newInclude.length > 0 ? { include: newInclude } : undefined,
          );
        } else {
          const newExclude = [...(excludedSet ?? []), val];
          applyFilter({ exclude: newExclude });
        }
      } else {
        if (excludedSet) {
          const newExclude = [...excludedSet].filter((v) => v !== val);
          applyFilter(
            newExclude.length > 0 ? { exclude: newExclude } : undefined,
          );
        } else if (includedSet) {
          const newInclude = [...includedSet, val];
          applyFilter(
            newInclude.length === uniqueValues.length
              ? undefined
              : { include: newInclude },
          );
        }
      }
    },
    [isChecked, includedSet, excludedSet, uniqueValues, applyFilter],
  );

  const selectAll = useCallback(() => {
    applyFilter(undefined);
  }, [applyFilter]);

  const clearAll = useCallback(() => {
    applyFilter({ exclude: [...uniqueValues] });
  }, [uniqueValues, applyFilter]);

  // ---- Unified keyboard navigation ----
  // All navigable items are marked with data-menu-nav. ArrowDown/ArrowUp cycles
  // through them as one continuous list (WAI-ARIA Menu pattern).

  // Focus first navigable item on open
  useEffect(() => {
    requestAnimationFrame(() => {
      const first =
        containerRef.current?.querySelector<HTMLElement>("[data-menu-nav]");
      first?.focus();
    });
  }, []);

  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
        return;
      }

      if (e.key === "ArrowDown" || e.key === "ArrowUp" || e.key === "Tab") {
        e.preventDefault();
        const items =
          containerRef.current?.querySelectorAll<HTMLElement>(
            "[data-menu-nav]",
          );
        if (!items || items.length === 0) return;
        const root = containerRef.current?.getRootNode() as
          | Document
          | ShadowRoot;
        const active = root?.activeElement as HTMLElement | null;
        let idx = Array.from(items).findIndex(
          (el) => el === active || el.contains(active),
        );
        const forward =
          e.key === "ArrowDown" || (e.key === "Tab" && !e.shiftKey);
        if (forward) idx = idx < items.length - 1 ? idx + 1 : 0;
        else idx = idx > 0 ? idx - 1 : items.length - 1;
        items[idx]?.focus();
        return;
      }

      if (e.key === "Home") {
        e.preventDefault();
        containerRef.current
          ?.querySelector<HTMLElement>("[data-menu-nav]")
          ?.focus();
        return;
      }
      if (e.key === "End") {
        e.preventDefault();
        const all =
          containerRef.current?.querySelectorAll<HTMLElement>(
            "[data-menu-nav]",
          );
        all?.[all.length - 1]?.focus();
        return;
      }
    },
    [onClose],
  );

  const displayOptions = useMemo(
    () =>
      [
        { value: "raw", label: "Raw values" },
        { value: "pct_of_total", label: "% of grand total" },
        { value: "pct_of_row", label: "% of row total" },
        { value: "pct_of_col", label: "% of column total" },
        { value: "running_total", label: "Running total" },
        { value: "pct_running_total", label: "% running total" },
        { value: "rank", label: "Rank" },
        { value: "pct_of_parent", label: "% of parent total" },
        { value: "index", label: "Index" },
        ...(supportsPeriodComparison
          ? [
              { value: "diff_from_prev", label: "Change vs previous period" },
              {
                value: "pct_diff_from_prev",
                label: "% change vs previous period",
              },
              {
                value: "diff_from_prev_year",
                label: "Change vs prior year",
              },
              {
                value: "pct_diff_from_prev_year",
                label: "% change vs prior year",
              },
            ]
          : []),
      ] as { value: ShowValuesAs; label: string }[],
    [supportsPeriodComparison],
  );

  return (
    <div
      ref={containerRef}
      className={styles.headerMenu}
      data-testid={`header-menu-${dimension}`}
      onKeyDown={handleKeyDown}
      role="menu"
      aria-label={`${dimension} options`}
    >
      {/* Dimension title */}
      <div className={styles.menuTitle} data-testid="header-menu-title">
        {((s) => s.charAt(0).toUpperCase() + s.slice(1))(title ?? dimension)}
      </div>

      {/* Sort section (hidden when locked / no onSortChange) */}
      {showSort && (
        <>
          <div
            className={styles.sortSection}
            data-testid="header-menu-sort"
            role="group"
            aria-label={`Sort ${dimension}`}
          >
            <button
              type="button"
              role="menuitem"
              aria-pressed={currentPreset === "key_asc"}
              tabIndex={-1}
              data-menu-nav
              className={`${styles.menuItem} ${currentPreset === "key_asc" ? styles.menuItemActive : ""}`}
              onClick={() => handleSort("key_asc")}
              data-testid="header-sort-key-asc"
            >
              <SortAscIcon />
              <span className={styles.menuItemLabel}>Sort A → Z</span>
              {currentPreset === "key_asc" && <CheckIcon />}
            </button>
            <button
              type="button"
              role="menuitem"
              aria-pressed={currentPreset === "key_desc"}
              tabIndex={-1}
              data-menu-nav
              className={`${styles.menuItem} ${currentPreset === "key_desc" ? styles.menuItemActive : ""}`}
              onClick={() => handleSort("key_desc")}
              data-testid="header-sort-key-desc"
            >
              <SortDescIcon />
              <span className={styles.menuItemLabel}>Sort Z → A</span>
              {currentPreset === "key_desc" && <CheckIcon />}
            </button>
            {hasValueFields && (
              <>
                <button
                  type="button"
                  role="menuitem"
                  aria-pressed={currentPreset === "value_asc"}
                  tabIndex={-1}
                  data-menu-nav
                  className={`${styles.menuItem} ${currentPreset === "value_asc" ? styles.menuItemActive : ""}`}
                  onClick={() => handleSort("value_asc")}
                  data-testid="header-sort-value-asc"
                >
                  <ValueAscIcon />
                  <span className={styles.menuItemLabel}>Sort by value ↑</span>
                  {currentPreset === "value_asc" && <CheckIcon />}
                </button>
                <button
                  type="button"
                  role="menuitem"
                  aria-pressed={currentPreset === "value_desc"}
                  tabIndex={-1}
                  data-menu-nav
                  className={`${styles.menuItem} ${currentPreset === "value_desc" ? styles.menuItemActive : ""}`}
                  onClick={() => handleSort("value_desc")}
                  data-testid="header-sort-value-desc"
                >
                  <ValueDescIcon />
                  <span className={styles.menuItemLabel}>Sort by value ↓</span>
                  {currentPreset === "value_desc" && <CheckIcon />}
                </button>
              </>
            )}
            {isValueSort && hasValueFields && (
              <div className={styles.sortValueConfig}>
                <label className={styles.sortValueLabel}>
                  <span>Field:</span>
                  <select
                    className={styles.sortValueSelect}
                    data-menu-nav
                    tabIndex={-1}
                    value={sortConfig?.value_field ?? valueFields[0]}
                    onChange={(e) => handleValueFieldChange(e.target.value)}
                    data-testid="header-sort-value-field"
                  >
                    {valueFields.map((vf) => (
                      <option key={vf} value={vf}>
                        {vf}
                      </option>
                    ))}
                  </select>
                </label>
                {axis === "row" && colKeys && colKeys.length > 0 && (
                  <label className={styles.sortValueLabel}>
                    <span>Column:</span>
                    <select
                      className={styles.sortValueSelect}
                      data-menu-nav
                      tabIndex={-1}
                      value={sortConfig?.col_key?.join("\x00") ?? ""}
                      onChange={(e) => handleColKeyChange(e.target.value)}
                      data-testid="header-sort-col-key"
                    >
                      <option value="">Total</option>
                      {colKeys.map((ck) => (
                        <option key={ck.join("\x00")} value={ck.join("\x00")}>
                          {ck.join(" / ")}
                        </option>
                      ))}
                    </select>
                  </label>
                )}
              </div>
            )}
          </div>

          <div className={styles.divider} />
        </>
      )}

      {/* Subtotals section (row dimension headers only) */}
      {onSubtotalToggle && (
        <>
          <div
            className={styles.sortSection}
            data-testid="header-menu-subtotals"
            role="group"
            aria-label={`Subtotals for ${dimension}`}
          >
            <span className={styles.menuSectionLabel}>Subtotals</span>
            <label
              className={styles.filterItem}
              role="menuitemcheckbox"
              aria-checked={
                config ? showSubtotalForDim(config, dimension) : false
              }
              data-menu-nav
              tabIndex={-1}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  onSubtotalToggle(dimension);
                }
              }}
            >
              <input
                type="checkbox"
                checked={config ? showSubtotalForDim(config, dimension) : false}
                onChange={() => onSubtotalToggle(dimension)}
                tabIndex={-1}
              />
              <span>Show subtotals</span>
            </label>
          </div>
          <div className={styles.divider} />
        </>
      )}

      {onDateGrainChange && (
        <>
          <div
            className={styles.dateSection}
            role="group"
            aria-label={`Date grouping for ${dimension}`}
          >
            <label className={styles.grainLabel}>
              <span className={styles.grainLabelText}>Group by</span>
              <select
                className={styles.grainSelect}
                data-menu-nav
                tabIndex={-1}
                value={dateGrain ?? ""}
                onChange={(e) =>
                  onDateGrainChange(
                    e.target.value ? (e.target.value as DateGrain) : undefined,
                  )
                }
                data-testid="header-date-grain"
              >
                <option value="">Original</option>
                {DATE_GRAINS.map((grain) => (
                  <option key={grain} value={grain}>
                    {DATE_GRAIN_LABELS[grain]}
                  </option>
                ))}
              </select>
            </label>
            {dateGrain && onDateDrill && (
              <div className={styles.drillRow}>
                <button
                  type="button"
                  className={styles.actionButton}
                  data-menu-nav
                  tabIndex={-1}
                  onClick={() => onDateDrill("up")}
                  disabled={!getDrilledDateGrain(dateGrain, "up")}
                  data-testid="header-date-drill-up"
                >
                  <svg
                    width="10"
                    height="10"
                    viewBox="0 0 10 10"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    style={{ marginRight: 2 }}
                  >
                    <polyline points="2,6 5,3 8,6" />
                  </svg>
                  Drill Up
                </button>
                <button
                  type="button"
                  className={styles.actionButton}
                  data-menu-nav
                  tabIndex={-1}
                  onClick={() => onDateDrill("down")}
                  disabled={!getDrilledDateGrain(dateGrain, "down")}
                  data-testid="header-date-drill-down"
                >
                  <svg
                    width="10"
                    height="10"
                    viewBox="0 0 10 10"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    style={{ marginRight: 2 }}
                  >
                    <polyline points="2,4 5,7 8,4" />
                  </svg>
                  Drill Down
                </button>
              </div>
            )}
          </div>
          <div className={styles.divider} />
        </>
      )}

      {/* Display section (value headers only) — shown before Totals */}
      {onShowValuesAsChange && (
        <>
          <div
            className={styles.displaySection}
            data-testid="header-menu-display"
            role="group"
            aria-label={`Display mode for ${dimension}`}
          >
            <span className={styles.menuSectionLabel}>Display</span>
            {displayOptions.map((opt) => (
              <button
                key={opt.value}
                type="button"
                role="menuitemradio"
                aria-checked={showValuesAs === opt.value}
                tabIndex={-1}
                data-menu-nav
                className={`${styles.menuItem} ${showValuesAs === opt.value ? styles.menuItemActive : ""}`}
                onClick={() => onShowValuesAsChange(opt.value)}
                data-testid={`header-display-${opt.value}`}
              >
                <DisplayIcon mode={opt.value} />
                <span className={styles.menuItemLabel}>{opt.label}</span>
                {showValuesAs === opt.value && <CheckIcon />}
              </button>
            ))}
          </div>
          {onTotalToggle && config && <div className={styles.divider} />}
        </>
      )}

      {/* Totals section (value headers only) */}
      {onTotalToggle && config && (
        <>
          <div
            className={styles.sortSection}
            data-testid="header-menu-totals"
            role="group"
            aria-label={`Totals for ${dimension}`}
          >
            <span className={styles.menuSectionLabel}>Totals</span>
            <label
              className={styles.filterItem}
              role="menuitemcheckbox"
              aria-checked={showTotalForMeasure(config, dimension, "row")}
              data-menu-nav
              tabIndex={-1}
              onKeyDown={(e) => {
                if (
                  showRowTotals(config) &&
                  (e.key === "Enter" || e.key === " ")
                ) {
                  e.preventDefault();
                  onTotalToggle(dimension, "row");
                }
              }}
            >
              <input
                type="checkbox"
                checked={showTotalForMeasure(config, dimension, "row")}
                onChange={() => onTotalToggle(dimension, "row")}
                disabled={!showRowTotals(config)}
                tabIndex={-1}
              />
              <span>Include in row totals</span>
            </label>
            <label
              className={styles.filterItem}
              role="menuitemcheckbox"
              aria-checked={showTotalForMeasure(config, dimension, "col")}
              data-menu-nav
              tabIndex={-1}
              onKeyDown={(e) => {
                if (
                  showColumnTotals(config) &&
                  (e.key === "Enter" || e.key === " ")
                ) {
                  e.preventDefault();
                  onTotalToggle(dimension, "col");
                }
              }}
            >
              <input
                type="checkbox"
                checked={showTotalForMeasure(config, dimension, "col")}
                onChange={() => onTotalToggle(dimension, "col")}
                disabled={!showColumnTotals(config)}
                tabIndex={-1}
              />
              <span>Include in column totals</span>
            </label>
          </div>
        </>
      )}

      {/* Top N / Bottom N section (dimension headers only, when value fields available) */}
      {onTopNFilterChange && valueFields && valueFields.length > 0 && (
        <>
          <div
            className={styles.sortSection}
            data-testid="header-menu-top-n"
            role="group"
            aria-label={`Top N filter for ${dimension}`}
          >
            <span className={styles.menuSectionLabel}>Top / Bottom N</span>
            {/* Caption sits directly under heading as a section subtitle */}
            <span className={styles.analyticCaption}>
              Totals always reflect full data, not just visible members.
            </span>

            {/* Row 1: direction toggle + N count, with deliberate gap between them */}
            <div className={styles.analyticControls} style={{ gap: 8 }}>
              {/* Top / Bottom as a connected pill toggle */}
              <div className={styles.toggleGroup}>
                {(["top", "bottom"] as const).map((dir) => (
                  <button
                    key={dir}
                    type="button"
                    className={`${styles.toggleBtn}${topNDirection === dir ? ` ${styles.toggleBtnActive}` : ""}`}
                    onClick={() => setTopNDirection(dir)}
                    data-menu-nav
                    tabIndex={-1}
                    data-testid={`header-top-n-dir-${dir}`}
                  >
                    {dir === "top" ? "Top" : "Bottom"}
                  </button>
                ))}
              </div>
              {/* N count — fixed compact width so it doesn't dominate the row */}
              <input
                type="number"
                className={styles.analyticInput}
                style={{ width: 85, flex: "0 0 auto" }}
                min={1}
                value={topNValue}
                onChange={(e) => setTopNValue(e.target.value)}
                placeholder="N"
                data-menu-nav
                tabIndex={-1}
                data-testid="header-top-n-count"
              />
            </div>
            {/* Row 2: "by" label + measure field selector, with visual breathing room */}
            <div
              className={styles.analyticControls}
              style={{ marginTop: 6, gap: 8 }}
            >
              <span className={styles.analyticConnector}>by</span>
              <div
                className={styles.analyticSelectWrap}
                style={{ flex: "0 1 auto", minWidth: 80 }}
              >
                <select
                  className={`${styles.analyticSelect}${topNBy === "" ? ` ${styles.isPlaceholder}` : ""}`}
                  value={topNBy}
                  onChange={(e) => setTopNBy(e.target.value)}
                  data-menu-nav
                  tabIndex={-1}
                  data-testid="header-top-n-by"
                >
                  <option value="">Select field</option>
                  {valueFields.map((f) => (
                    <option key={f} value={f}>
                      {f}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {/* Apply + Clear always visible; Clear disabled when no filter is active */}
            <div className={styles.analyticApplyRow}>
              <button
                type="button"
                className={styles.actionButton}
                onClick={clearTopN}
                disabled={!topNFilter}
                data-menu-nav
                tabIndex={-1}
                data-testid="header-top-n-clear"
              >
                Clear
              </button>
              <button
                type="button"
                className={styles.actionButton}
                onClick={applyTopN}
                data-menu-nav
                tabIndex={-1}
                data-testid="header-top-n-apply"
              >
                Apply
              </button>
            </div>
          </div>
        </>
      )}

      {/* Value filter section (dimension headers only, when value fields available) */}
      {onValueFilterChange && valueFields && valueFields.length > 0 && (
        <>
          <div className={styles.divider} />
          <div
            className={styles.sortSection}
            data-testid="header-menu-value-filter"
            role="group"
            aria-label={`Value filter for ${dimension}`}
          >
            <span className={styles.menuSectionLabel}>Filter by value</span>
            {/* Caption sits directly under heading as a section subtitle */}
            <span className={styles.analyticCaption}>
              Totals always reflect full data, not just visible members.
            </span>

            {/* Measure field selector — full width */}
            <div className={styles.analyticControls}>
              <div
                className={styles.analyticSelectWrap}
                style={{ flex: 1, minWidth: 0 }}
              >
                <select
                  className={`${styles.analyticSelect}${vfBy === "" ? ` ${styles.isPlaceholder}` : ""}`}
                  value={vfBy}
                  onChange={(e) => setVfBy(e.target.value)}
                  data-menu-nav
                  tabIndex={-1}
                  data-testid="header-value-filter-by"
                >
                  <option value="">field…</option>
                  {valueFields.map((f) => (
                    <option key={f} value={f}>
                      {f}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {/* Operator strip — all 7 operators visible as labeled buttons */}
            <div
              className={styles.opGroup}
              role="group"
              aria-label="Comparison operator"
              data-testid="header-value-filter-operator-group"
            >
              {(
                [
                  ["gt", ">", "Greater than"],
                  ["gte", "≥", "Greater than or equal"],
                  ["lt", "<", "Less than"],
                  ["lte", "≤", "Less than or equal"],
                  ["eq", "=", "Equal"],
                  ["neq", "≠", "Not equal"],
                  ["between", "btw", "Between two values"],
                ] as [ValueFilter["operator"], string, string][]
              ).map(([op, label, tooltip]) => (
                <span
                  key={op}
                  className={styles.tooltipWrap}
                  data-tooltip={tooltip}
                >
                  <button
                    type="button"
                    className={`${styles.opBtn}${vfOperator === op ? ` ${styles.opBtnActive}` : ""}`}
                    onClick={() => setVfOperator(op)}
                    aria-label={tooltip}
                    data-menu-nav
                    tabIndex={-1}
                    data-testid={`header-value-filter-op-${op}`}
                  >
                    {label}
                  </button>
                </span>
              ))}
            </div>

            {/* Value input(s) */}
            <div className={styles.analyticValueRow}>
              <input
                type="number"
                className={styles.analyticInput}
                style={{ flex: 1, minWidth: 0 }}
                value={vfValue}
                onChange={(e) => setVfValue(e.target.value)}
                placeholder={vfOperator === "between" ? "lower" : "value"}
                data-menu-nav
                tabIndex={-1}
                data-testid="header-value-filter-value"
              />
              {vfOperator === "between" && (
                <>
                  <span className={styles.analyticConnector}>–</span>
                  <input
                    type="number"
                    className={styles.analyticInput}
                    style={{ flex: 1, minWidth: 0 }}
                    value={vfValue2}
                    onChange={(e) => setVfValue2(e.target.value)}
                    placeholder="upper"
                    data-menu-nav
                    tabIndex={-1}
                    data-testid="header-value-filter-value2"
                  />
                </>
              )}
            </div>

            {/* Apply + Clear always visible; Clear disabled when no filter is active */}
            <div className={styles.analyticApplyRow}>
              <button
                type="button"
                className={styles.actionButton}
                onClick={clearValueFilter}
                disabled={!valueFilter}
                data-menu-nav
                tabIndex={-1}
                data-testid="header-value-filter-clear"
              >
                Clear
              </button>
              <button
                type="button"
                className={styles.actionButton}
                onClick={applyValueFilter}
                data-menu-nav
                tabIndex={-1}
                data-testid="header-value-filter-apply"
              >
                Apply
              </button>
            </div>
          </div>
        </>
      )}

      {/* Divider between analytical filters and Filter Values checklist (only when both present) */}
      {showFilter &&
        onValueFilterChange &&
        valueFields &&
        valueFields.length > 0 && <div className={styles.divider} />}

      {/* Filter section */}
      {showFilter && (
        <div className={styles.filterSection} data-testid="header-menu-filter">
          <span className={styles.menuSectionLabel}>Filter Values</span>
          <input
            type="text"
            className={styles.searchInput}
            placeholder="Filter values..."
            value={search}
            data-menu-nav
            tabIndex={-1}
            onChange={(e) => setSearch(e.target.value)}
            data-testid={`header-filter-search-${dimension}`}
          />
          <div className={styles.actionRow}>
            <button
              type="button"
              className={styles.actionButton}
              data-menu-nav
              tabIndex={-1}
              onClick={selectAll}
            >
              Select All
            </button>
            <button
              type="button"
              className={styles.actionButton}
              data-menu-nav
              tabIndex={-1}
              onClick={clearAll}
            >
              Clear All
            </button>
          </div>
          <div
            className={styles.itemList}
            role="group"
            aria-label={`Filter values for ${dimension}`}
          >
            {visibleValues.map((val) => (
              <label
                key={val}
                className={styles.filterItem}
                role="menuitemcheckbox"
                aria-checked={isChecked(val)}
                data-menu-nav
                tabIndex={-1}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    toggleValue(val);
                  }
                }}
              >
                <input
                  type="checkbox"
                  checked={isChecked(val)}
                  onChange={() => toggleValue(val)}
                  tabIndex={-1}
                />
                <span>{(formatLabel?.(val) ?? val) || "(empty)"}</span>
              </label>
            ))}
            {overflowCount > 0 && (
              <div className={styles.moreIndicator}>
                and {overflowCount} more...
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

const DisplayIcon: FC<{ mode: ShowValuesAs }> = ({ mode }) => {
  if (mode === "raw") {
    return (
      <svg
        width="14"
        height="14"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <text
          x="5"
          y="18"
          fontSize="16"
          fill="currentColor"
          stroke="none"
          fontWeight="600"
        >
          #
        </text>
      </svg>
    );
  }
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <text
        x="3"
        y="18"
        fontSize="16"
        fill="currentColor"
        stroke="none"
        fontWeight="600"
      >
        %
      </text>
    </svg>
  );
};

// ---- SVG Icons ----

const CheckIcon: FC = () => (
  <svg
    width="12"
    height="12"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="3"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={styles.checkIcon}
    aria-hidden="true"
  >
    <polyline points="20 6 9 17 4 12" />
  </svg>
);

const SortAscIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    <path d="M11 5h10" />
    <path d="M11 9h7" />
    <path d="M11 13h4" />
    <path d="M3 17l3 3 3-3" />
    <path d="M6 18V4" />
  </svg>
);

const SortDescIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    <path d="M11 5h4" />
    <path d="M11 9h7" />
    <path d="M11 13h10" />
    <path d="M3 17l3 3 3-3" />
    <path d="M6 18V4" />
  </svg>
);

const ValueAscIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    <path d="M3 7l3-3 3 3" />
    <path d="M6 4v16" />
    <rect x="13" y="4" width="8" height="4" rx="1" />
    <rect x="13" y="12" width="5" height="4" rx="1" />
  </svg>
);

const ValueDescIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    <path d="M3 17l3 3 3-3" />
    <path d="M6 4v16" />
    <rect x="13" y="4" width="5" height="4" rx="1" />
    <rect x="13" y="12" width="8" height="4" rx="1" />
  </svg>
);

export default HeaderMenu;
