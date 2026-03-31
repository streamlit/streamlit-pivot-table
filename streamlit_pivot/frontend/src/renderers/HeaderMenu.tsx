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
  DimensionFilter,
  PivotConfigV1,
  ShowValuesAs,
  SortConfig,
} from "../engine/types";
import {
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
  onClose,
}): ReactElement => {
  const showSort = !!onSortChange;
  const containerRef = useRef<HTMLDivElement>(null);

  const applyFilter = useCallback(
    (next: DimensionFilter | undefined) => {
      onFilterChange(dimension, next);
    },
    [dimension, onFilterChange],
  );

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
    return uniqueValues.filter((v) => v.toLowerCase().includes(lower));
  }, [uniqueValues, search]);

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
        {dimension}
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
          {!showSort && <div className={styles.divider} />}
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

      {/* Totals section (value headers only) */}
      {onTotalToggle && config && (
        <>
          <div className={styles.divider} />
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

      {/* Display section (value headers only) */}
      {onShowValuesAsChange && (
        <div
          className={styles.displaySection}
          data-testid="header-menu-display"
          role="group"
          aria-label={`Display mode for ${dimension}`}
        >
          {SHOW_VALUES_AS_OPTIONS.map((opt) => (
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
      )}

      {/* Filter section */}
      {showFilter && (
        <div className={styles.filterSection} data-testid="header-menu-filter">
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
                <span>{val || "(empty)"}</span>
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

// ---- Display options ----

const SHOW_VALUES_AS_OPTIONS: { value: ShowValuesAs; label: string }[] = [
  { value: "raw", label: "Raw Value" },
  { value: "pct_of_total", label: "of Grand Total" },
  { value: "pct_of_row", label: "of Row Total" },
  { value: "pct_of_col", label: "of Column Total" },
];

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
