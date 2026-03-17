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
  ReactElement,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  KeyboardEvent,
  MouseEvent,
} from "react";
import {
  makeKeyString,
  type PivotData,
  type GroupedRow,
} from "../engine/PivotData";
import { formatWithPattern, formatPercent } from "../engine/formatters";
import {
  getRenderedValueFields,
  getRenderedValueLabel,
  getSyntheticMeasureFormat,
  isSyntheticMeasure,
  showRowTotals,
  showColumnTotals,
  showTotalForMeasure,
  type CellClickPayload,
  type DimensionFilter,
  type PivotConfigV1,
  type ShowValuesAs,
  type SortConfig,
} from "../engine/types";
import { computeCellStyle } from "./ConditionalFormat";
import HeaderMenu from "./HeaderMenu";
import { useHeaderMenu } from "./useHeaderMenu";
import styles from "./TableRenderer.module.css";

export type MenuAxis = "row" | "col" | "value";

export interface HeaderMenuTarget {
  dimension: string;
  axis: MenuAxis;
  rect: DOMRect;
}

/**
 * Represents a visible column in the table — either a regular full column key
 * or a collapsed group represented by its prefix.
 */
export interface ColSlot {
  key: string[];
  /** If set, this slot represents a collapsed group at the given level. */
  collapsedLevel?: number;
}

/**
 * Approximate pixel height of a `.headerCell` row (padding + line-height + border).
 * Used to compute `top` offsets for multi-row sticky headers.
 */
export const HEADER_ROW_HEIGHT = 37;

/** Slugify a dimension name for use in data-testid attributes. */
function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

/** +/− icon for dimension-level collapse toggles (axis-neutral, matches groupToggle). */
function DimToggleIcon({ collapsed }: { collapsed: boolean }): ReactElement {
  return (
    <svg
      className={styles.dimensionToggleIcon}
      width="10"
      height="10"
      viewBox="0 0 10 10"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
    >
      {collapsed ? (
        <>
          <line x1="5" y1="1" x2="5" y2="9" />
          <line x1="1" y1="5" x2="9" y2="5" />
        </>
      ) : (
        <line x1="1" y1="5" x2="9" y2="5" />
      )}
    </svg>
  );
}

/**
 * Expand the "__ALL__" sentinel into explicit level-0 group keys.
 * Must be called before any mutation to collapsed state.
 */
function normalizeCollapsed(
  collapsed: string[],
  level0Prefixes: string[],
): Set<string> {
  const result = new Set(collapsed);
  if (result.has("__ALL__")) {
    result.delete("__ALL__");
    for (const p of level0Prefixes) result.add(p);
  }
  return result;
}

/**
 * Check whether all groups at a given dimension level are collapsed.
 */
function isDimCollapsed(
  collapsedArr: string[],
  keys: string[][],
  level: number,
): boolean {
  if (keys.length === 0) return false;
  const level0Prefixes = [
    ...new Set(keys.map((k) => makeKeyString(k.slice(0, 1)))),
  ];
  const targetPrefixes = [
    ...new Set(keys.map((k) => makeKeyString(k.slice(0, level + 1)))),
  ];
  const normalized = normalizeCollapsed(collapsedArr, level0Prefixes);
  return (
    targetPrefixes.length > 0 && targetPrefixes.every((p) => normalized.has(p))
  );
}

/**
 * Compute visible column slots: replace children of collapsed column groups
 * with a single subtotal slot.
 */
export function computeColSlots(
  colKeys: string[][],
  collapsedColGroups: string[] | undefined,
  numColDims: number,
): ColSlot[] {
  if (
    !collapsedColGroups ||
    collapsedColGroups.length === 0 ||
    numColDims < 2
  ) {
    return colKeys.map((key) => ({ key }));
  }

  let collapsedSet: Set<string>;
  if (collapsedColGroups.includes("__ALL__")) {
    collapsedSet = new Set<string>();
    const seen = new Set<string>();
    for (const ck of colKeys) {
      const topKey = makeKeyString(ck.slice(0, 1));
      if (!seen.has(topKey)) {
        seen.add(topKey);
        collapsedSet.add(topKey);
      }
    }
  } else {
    collapsedSet = new Set(collapsedColGroups);
  }

  const slots: ColSlot[] = [];
  const emittedPrefixes = new Set<string>();

  for (const colKey of colKeys) {
    let matchedLevel: number | undefined;
    for (let lvl = 0; lvl < numColDims - 1; lvl++) {
      const prefix = colKey.slice(0, lvl + 1);
      if (collapsedSet.has(makeKeyString(prefix))) {
        matchedLevel = lvl;
        break;
      }
    }
    if (matchedLevel !== undefined) {
      const prefix = colKey.slice(0, matchedLevel + 1);
      const prefixStr = makeKeyString(prefix);
      if (!emittedPrefixes.has(prefixStr)) {
        emittedPrefixes.add(prefixStr);
        slots.push({ key: prefix, collapsedLevel: matchedLevel });
      }
    } else {
      slots.push({ key: colKey });
    }
  }
  return slots;
}

export interface TableRendererProps {
  pivotData: PivotData;
  config: PivotConfigV1;
  onCellClick?: (payload: CellClickPayload) => void;
  maxColumns?: number;
  maxRows?: number;
  onSortChange?: (axis: "row" | "col", sort: SortConfig | undefined) => void;
  onFilterChange?: (field: string, filter: DimensionFilter | undefined) => void;
  onConfigChange?: (config: PivotConfigV1) => void;
  onShowValuesAsChange?: (field: string, mode: ShowValuesAs) => void;
  menuLimit?: number;
  /** When true, the wrapper becomes a flex item that fills remaining space
   *  in a flex-column parent, enabling a single internal scrollbar. */
  scrollable?: boolean;
  /** Max height (px) applied when scrollable is false. Table scrolls once
   *  content exceeds this value. */
  maxHeight?: number;
  /** Fires when overflow state changes (content taller than visible area). */
  onOverflowChange?: (isOverflowing: boolean) => void;
}

const TOTAL_KEY: readonly string[] = ["Total"];

export function buildCellClickPayload(
  rowKey: readonly string[],
  colKey: readonly string[],
  value: number | null,
  config: PivotConfigV1,
  valueField?: string,
): CellClickPayload {
  const filters: Record<string, string> = {};
  config.rows.forEach((dim, i) => {
    if (i < rowKey.length && rowKey[i] !== "Total") {
      filters[dim] = rowKey[i];
    }
  });
  config.columns.forEach((dim, i) => {
    if (i < colKey.length && colKey[i] !== "Total") {
      filters[dim] = colKey[i];
    }
  });
  return {
    rowKey: [...rowKey],
    colKey: [...colKey],
    value,
    filters,
    valueField,
  };
}

/**
 * Format a cell value considering show_values_as, number_format, and column_alignment.
 */
function formatCellValue(
  rawValue: number | null,
  valField: string,
  config: PivotConfigV1,
  pivotData: PivotData,
  rowKey: string[],
  colKey: string[],
  emptyCellValue: string,
): { text: string; style?: React.CSSProperties } {
  if (rawValue === null) return { text: emptyCellValue };

  const showAs: ShowValuesAs | undefined = config.show_values_as?.[valField];

  if (showAs && showAs !== "raw") {
    let denominator: number | null = null;
    if (showAs === "pct_of_total") {
      denominator = pivotData.getGrandTotal(valField).value();
    } else if (showAs === "pct_of_row") {
      denominator = pivotData.getRowTotal(rowKey, valField).value();
    } else if (showAs === "pct_of_col") {
      denominator = pivotData.getColTotal(colKey, valField).value();
    }
    if (denominator != null && denominator !== 0) {
      return { text: formatPercent(rawValue / denominator) };
    }
    return { text: emptyCellValue };
  }

  const pattern =
    getSyntheticMeasureFormat(config, valField) ??
    config.number_format?.[valField] ??
    config.number_format?.["__all__"];
  if (pattern) {
    return { text: formatWithPattern(rawValue, pattern) };
  }

  return { text: "" }; // fallback: use default agg.format()
}

function getCellAlignment(
  valField: string,
  config: PivotConfigV1,
): string | undefined {
  return config.column_alignment?.[valField];
}

/**
 * Build inline styles for a total/subtotal cell, applying alignment and conditional formatting.
 */
function buildTotalCellStyle(
  value: number | null,
  valField: string,
  config: PivotConfigV1,
  pivotData: PivotData,
): React.CSSProperties | undefined {
  const align = getCellAlignment(valField, config);
  const condStyle = config.conditional_formatting
    ? computeCellStyle(
        value,
        valField,
        config.conditional_formatting,
        pivotData,
        true,
      )
    : undefined;
  const style: React.CSSProperties = {
    ...(align ? { textAlign: align as React.CSSProperties["textAlign"] } : {}),
    ...condStyle,
  };
  return Object.keys(style).length > 0 ? style : undefined;
}

/**
 * Format a total/subtotal cell value respecting number_format and show_values_as.
 */
function formatTotalCellValue(
  agg: { value(): number | null; format(empty: string): string },
  valField: string,
  config: PivotConfigV1,
  pivotData: PivotData,
  isTotalOfShowAsAxis: "row" | "col" | "grand" | null,
): string {
  const rawValue = agg.value();
  const showAs = config.show_values_as?.[valField];

  if (showAs && showAs !== "raw" && rawValue !== null) {
    if (isTotalOfShowAsAxis === "row" && showAs === "pct_of_row")
      return formatPercent(1);
    if (isTotalOfShowAsAxis === "col" && showAs === "pct_of_col")
      return formatPercent(1);
    if (isTotalOfShowAsAxis === "grand") return formatPercent(1);
    if (showAs === "pct_of_total") {
      const grand = pivotData.getGrandTotal(valField).value();
      return grand ? formatPercent(rawValue / grand) : config.empty_cell_value;
    }
    if (showAs === "pct_of_row") {
      return config.empty_cell_value;
    }
    if (showAs === "pct_of_col") {
      const colTotal = rawValue;
      const grand = pivotData.getGrandTotal(valField).value();
      return grand ? formatPercent(colTotal / grand) : config.empty_cell_value;
    }
  }

  if (rawValue === null) return config.empty_cell_value;

  const pattern =
    getSyntheticMeasureFormat(config, valField) ??
    config.number_format?.[valField] ??
    config.number_format?.["__all__"];
  if (pattern) return formatWithPattern(rawValue, pattern);

  return agg.format(config.empty_cell_value);
}

function SortArrowIcon({
  direction,
}: {
  direction: "asc" | "desc";
}): ReactElement {
  return (
    <svg
      width="8"
      height="8"
      viewBox="0 0 8 8"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={styles.sortIndicator}
      aria-hidden="true"
      data-testid={`sort-indicator-${direction}`}
    >
      {direction === "asc" ? (
        <polyline points="1,5.5 4,2 7,5.5" />
      ) : (
        <polyline points="1,2.5 4,6 7,2.5" />
      )}
    </svg>
  );
}

function MenuTriggerButton({
  dimension,
  axis,
  isOpen,
  onOpen,
}: {
  dimension: string;
  axis: MenuAxis;
  isOpen: boolean;
  onOpen: (
    dimension: string,
    axis: MenuAxis,
    rect: DOMRect,
    triggerEl: HTMLElement,
  ) => void;
}) {
  const handleClick = useCallback(
    (e: MouseEvent<HTMLButtonElement>) => {
      e.stopPropagation();
      const rect = e.currentTarget.getBoundingClientRect();
      onOpen(dimension, axis, rect, e.currentTarget);
    },
    [dimension, axis, onOpen],
  );

  const stopMouseDown = useCallback((e: MouseEvent<HTMLButtonElement>) => {
    e.stopPropagation();
  }, []);

  return (
    <button
      type="button"
      className={styles.headerMenuBtn}
      onClick={handleClick}
      onMouseDown={stopMouseDown}
      aria-label={`${dimension} options`}
      aria-expanded={isOpen}
      data-testid={`header-menu-trigger-${dimension}`}
    >
      <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor">
        <circle cx="12" cy="5" r="2" />
        <circle cx="12" cy="12" r="2" />
        <circle cx="12" cy="19" r="2" />
      </svg>
    </button>
  );
}

export function renderColumnHeaders(
  colSlots: ColSlot[],
  config: PivotConfigV1,
  numRowDims: number,
  hasMultipleValues: boolean,
  colRange?: [number, number],
  onOpenMenu?: (
    dimension: string,
    axis: MenuAxis,
    rect: DOMRect,
    triggerEl: HTMLElement,
  ) => void,
  activeMenuDimension?: string,
  onToggleColGroup?: (groupKey: string) => void,
  pivotData?: PivotData,
  onConfigChange?: (config: PivotConfigV1) => void,
): ReactElement[] {
  const renderedValueFields = getRenderedValueFields(config);
  const visibleSlots = colRange
    ? colSlots.slice(colRange[0], colRange[1])
    : colSlots;
  const rows: ReactElement[] = [];
  const numColLevels = Math.max(config.columns.length, 1);
  const hasMenu = !!onOpenMenu;

  const handleDimToggle = (axis: "row" | "col", level: number) => {
    if (!onConfigChange || !pivotData) return;
    const keys =
      axis === "row" ? pivotData.getRowKeys() : pivotData.getColKeys();
    const current =
      axis === "row"
        ? (config.collapsed_groups ?? [])
        : (config.collapsed_col_groups ?? []);
    const configField =
      axis === "row"
        ? ("collapsed_groups" as const)
        : ("collapsed_col_groups" as const);

    const level0Prefixes = [
      ...new Set(keys.map((k) => makeKeyString(k.slice(0, 1)))),
    ].sort();
    const targetPrefixes = [
      ...new Set(keys.map((k) => makeKeyString(k.slice(0, level + 1)))),
    ].sort();

    const working = normalizeCollapsed(current, level0Prefixes);
    const isCollapsed =
      targetPrefixes.length > 0 && targetPrefixes.every((p) => working.has(p));

    if (isCollapsed) {
      for (const p of targetPrefixes) working.delete(p);
    } else {
      for (const p of targetPrefixes) working.add(p);
    }

    onConfigChange({ ...config, [configField]: [...working].sort() });
  };

  const canDimToggle = !!onConfigChange && !!pivotData;

  for (let level = 0; level < numColLevels; level++) {
    const cells: ReactElement[] = [];

    const isLastColLevel = level === numColLevels - 1;
    const dimCellRowSpan = 1 + (hasMultipleValues ? 1 : 0);
    const stickyTop =
      level > 0 ? { top: level * HEADER_ROW_HEIGHT } : undefined;

    if (numColLevels > 1 && !isLastColLevel) {
      const colDimName = config.columns[level];
      const colDimCollapsed = pivotData
        ? isDimCollapsed(
            config.collapsed_col_groups ?? [],
            pivotData.getColKeys(),
            level,
          )
        : false;
      const showColDimToggle = canDimToggle && config.columns.length >= 2;
      cells.push(
        showColDimToggle ? (
          <th
            key={`col-dim-label-${level}`}
            className={`${styles.emptyCorner} ${styles.headerCell} ${styles.colDimLabel} ${styles.dimensionToggleCell}`}
            colSpan={numRowDims}
            style={stickyTop}
            onClick={() => handleDimToggle("col", level)}
            role="button"
            tabIndex={0}
            onKeyDown={(e) => {
              if (e.key === "Enter" || e.key === " ") {
                e.preventDefault();
                handleDimToggle("col", level);
              }
            }}
            aria-expanded={!colDimCollapsed}
            aria-label={`${colDimCollapsed ? "Expand" : "Collapse"} all ${colDimName} groups`}
            data-testid={`pivot-dim-toggle-col-${level}-${slugify(colDimName)}`}
          >
            <div
              className={styles.headerCellInner}
              style={{ justifyContent: "flex-start" }}
            >
              <DimToggleIcon collapsed={colDimCollapsed} />
              <span>{colDimName}</span>
            </div>
          </th>
        ) : (
          <th
            key={`col-dim-label-${level}`}
            className={`${styles.emptyCorner} ${styles.headerCell} ${styles.colDimLabel}`}
            colSpan={numRowDims}
            style={stickyTop}
          >
            <div
              className={styles.headerCellInner}
              style={{ justifyContent: "flex-start" }}
            >
              <span>{colDimName}</span>
            </div>
          </th>
        ),
      );
    }

    if (isLastColLevel) {
      if (config.rows.length > 0) {
        const showRowDimToggle =
          canDimToggle && config.rows.length >= 2 && !!config.show_subtotals;
        config.rows.forEach((dim, dimIdx) => {
          const isFiltered = !!config.filters?.[dim];
          const isFirstRowDim = dimIdx === 0;
          const sortTargetDim = config.row_sort?.dimension;
          const showSortOnThisDim = config.row_sort
            ? sortTargetDim
              ? sortTargetDim === dim
              : isFirstRowDim
            : false;
          const rowSortDir = showSortOnThisDim
            ? config.row_sort!.direction
            : undefined;
          const isInnermost = dimIdx === config.rows.length - 1;
          const rowDimCollapsed =
            showRowDimToggle && !isInnermost && pivotData
              ? isDimCollapsed(
                  config.collapsed_groups ?? [],
                  pivotData.getRowKeys(),
                  dimIdx,
                )
              : false;
          const parentCollapsed =
            showRowDimToggle && dimIdx > 0 && pivotData
              ? Array.from({ length: dimIdx }, (_, lvl) => lvl).some((lvl) =>
                  isDimCollapsed(
                    config.collapsed_groups ?? [],
                    pivotData.getRowKeys(),
                    lvl,
                  ),
                )
              : false;
          const canToggleThisDim = showRowDimToggle && !isInnermost;
          const dimToggleEnabled = canToggleThisDim && !parentCollapsed;
          cells.push(
            <th
              key={`row-dim-${dimIdx}`}
              className={`${styles.headerCell} ${rowSortDir ? styles.headerSorted : ""} ${isFirstRowDim ? styles.headerRowPinned : ""} ${canToggleThisDim ? styles.dimensionToggleCell : ""} ${canToggleThisDim && !dimToggleEnabled ? styles.dimensionToggleDisabled : ""}`}
              rowSpan={dimCellRowSpan}
              style={stickyTop}
              data-testid={
                canToggleThisDim
                  ? `pivot-dim-toggle-row-${dimIdx}-${slugify(dim)}`
                  : `pivot-row-dim-label-${dim}`
              }
              aria-sort={
                rowSortDir === "asc"
                  ? "ascending"
                  : rowSortDir === "desc"
                    ? "descending"
                    : undefined
              }
              title={
                canToggleThisDim && !dimToggleEnabled
                  ? `Expand ${config.rows[dimIdx - 1]} first`
                  : undefined
              }
              {...(dimToggleEnabled
                ? {
                    onClick: (e: MouseEvent) => {
                      if (
                        (e.target as HTMLElement).closest(
                          `.${styles.headerMenuBtn}`,
                        )
                      )
                        return;
                      handleDimToggle("row", dimIdx);
                    },
                    role: "button",
                    tabIndex: 0,
                    onKeyDown: (e: KeyboardEvent) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        handleDimToggle("row", dimIdx);
                      }
                    },
                    "aria-expanded": !rowDimCollapsed,
                    "aria-label": `${rowDimCollapsed ? "Expand" : "Collapse"} all ${dim} groups`,
                  }
                : {})}
            >
              <div className={styles.headerCellInner}>
                {canToggleThisDim && (
                  <DimToggleIcon collapsed={rowDimCollapsed} />
                )}
                <span className={isFiltered ? styles.headerFiltered : ""}>
                  {dim}
                </span>
                {rowSortDir && <SortArrowIcon direction={rowSortDir} />}
                {hasMenu && (
                  <MenuTriggerButton
                    dimension={dim}
                    axis="row"
                    isOpen={activeMenuDimension === dim}
                    onOpen={onOpenMenu}
                  />
                )}
              </div>
            </th>,
          );
        });
      } else {
        cells.push(
          <th
            key="corner-empty"
            className={styles.emptyCorner}
            rowSpan={dimCellRowSpan}
            style={stickyTop}
          />,
        );
      }
    }

    if (config.columns.length > 0) {
      const dimName = config.columns[level];
      const isFirstColLevel = level === 0;
      const colSortDir =
        isFirstColLevel && config.col_sort
          ? config.col_sort.direction
          : undefined;
      let i = 0;
      while (i < visibleSlots.length) {
        const slot = visibleSlots[i];
        const isCollapsedSlot = slot.collapsedLevel !== undefined;
        const collapsedAtOrAbove =
          isCollapsedSlot && slot.collapsedLevel! <= level;

        // Skip sub-level headers for slots collapsed at a higher level
        if (isCollapsedSlot && level > slot.collapsedLevel!) {
          i++;
          continue;
        }

        const val = slot.key[level] ?? "";
        let span = 1;
        // Group consecutive slots with the same value at this level (only for non-collapsed)
        if (!collapsedAtOrAbove) {
          while (
            i + span < visibleSlots.length &&
            visibleSlots[i + span].collapsedLevel === undefined &&
            visibleSlots[i + span].key[level] === val &&
            (level === 0 ||
              visibleSlots[i + span].key
                .slice(0, level)
                .every((v, idx) => v === slot.key[idx]))
          ) {
            span++;
          }
        }

        const colSpanVal = hasMultipleValues
          ? span * renderedValueFields.length
          : span;

        const isFiltered = !!config.filters?.[dimName];
        const showColSortIndicator = !!colSortDir && isFirstColLevel && i === 0;

        // Collapsed groups at their collapse level get a rowSpan to cover remaining levels + value row
        const isCollapseLevel =
          isCollapsedSlot && slot.collapsedLevel === level;
        const remainingLevels = numColLevels - level;
        const rowSpanVal = isCollapseLevel
          ? remainingLevels + (hasMultipleValues ? 1 : 0)
          : undefined;

        const canToggle =
          isCollapseLevel ||
          (!isCollapsedSlot &&
            config.columns.length >= 2 &&
            level < config.columns.length - 1 &&
            onToggleColGroup);
        const groupKeyStr = makeKeyString(slot.key.slice(0, level + 1));

        cells.push(
          <th
            key={`col-${level}-${i}`}
            scope="col"
            className={`${styles.headerCell} ${isCollapseLevel ? styles.totalsCol : ""} ${showColSortIndicator ? styles.headerSorted : ""} ${canToggle ? styles.groupToggleCell : ""}`}
            colSpan={colSpanVal > 1 ? colSpanVal : undefined}
            rowSpan={rowSpanVal}
            style={stickyTop}
            data-testid={
              canToggle && onToggleColGroup
                ? `pivot-col-group-toggle-${groupKeyStr}`
                : "pivot-header-cell"
            }
            aria-sort={
              showColSortIndicator
                ? colSortDir === "asc"
                  ? "ascending"
                  : "descending"
                : undefined
            }
            {...(canToggle && onToggleColGroup
              ? {
                  onClick: (e: MouseEvent) => {
                    if (
                      (e.target as HTMLElement).closest(
                        `.${styles.headerMenuBtn}`,
                      )
                    )
                      return;
                    onToggleColGroup(groupKeyStr);
                  },
                  role: "button",
                  tabIndex: 0,
                  onKeyDown: (e: KeyboardEvent) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      onToggleColGroup!(groupKeyStr);
                    }
                  },
                  "aria-expanded": !isCollapseLevel,
                  "aria-label": isCollapseLevel
                    ? `Expand ${val}`
                    : `Collapse ${val}`,
                }
              : {})}
          >
            <div className={styles.headerCellInner}>
              {canToggle && onToggleColGroup && (
                <svg
                  className={styles.groupToggleIcon}
                  width="10"
                  height="10"
                  viewBox="0 0 10 10"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.5"
                  strokeLinecap="round"
                >
                  {isCollapseLevel ? (
                    <>
                      <line x1="5" y1="1" x2="5" y2="9" />
                      <line x1="1" y1="5" x2="9" y2="5" />
                    </>
                  ) : (
                    <line x1="1" y1="5" x2="9" y2="5" />
                  )}
                </svg>
              )}
              <span className={isFiltered ? styles.headerFiltered : ""}>
                {val || "(empty)"}
              </span>
              {showColSortIndicator && (
                <SortArrowIcon direction={colSortDir!} />
              )}
              {hasMenu && (
                <MenuTriggerButton
                  dimension={dimName}
                  axis="col"
                  isOpen={activeMenuDimension === dimName}
                  onOpen={onOpenMenu}
                />
              )}
            </div>
          </th>,
        );
        i += span;
      }
    } else {
      const colSpanVal = hasMultipleValues ? renderedValueFields.length : 1;
      const headerLabel = hasMultipleValues
        ? "Values"
        : getRenderedValueLabel(config, renderedValueFields[0] ?? "") ||
          "Values";
      cells.push(
        <th
          key="col-single"
          scope="col"
          className={styles.headerCell}
          colSpan={colSpanVal}
          style={stickyTop}
          data-testid="pivot-header-cell"
        >
          {headerLabel}
        </th>,
      );
    }

    if (showRowTotals(config) && level === 0) {
      const totalColSpan = hasMultipleValues ? renderedValueFields.length : 1;
      cells.push(
        <th
          key="total-header"
          scope="col"
          className={`${styles.headerCell} ${styles.totalsCol}`}
          rowSpan={numColLevels}
          colSpan={totalColSpan}
          data-testid="pivot-header-cell"
        >
          Total
        </th>,
      );
    }

    rows.push(<tr key={`header-${level}`}>{cells}</tr>);
  }

  if (hasMultipleValues) {
    const valueLabelTop = numColLevels * HEADER_ROW_HEIGHT;
    const valueCells: ReactElement[] = [];
    for (const slot of visibleSlots) {
      if (slot.collapsedLevel !== undefined) continue;
      for (const valField of renderedValueFields) {
        valueCells.push(
          <th
            key={`val-${slot.key.join("\x00")}-${valField}`}
            className={styles.valueLabel}
            style={{ top: valueLabelTop }}
            data-testid="pivot-value-label"
          >
            <div className={styles.headerCellInner}>
              <span>{getRenderedValueLabel(config, valField)}</span>
              {hasMenu && (
                <MenuTriggerButton
                  dimension={valField}
                  axis="value"
                  isOpen={activeMenuDimension === valField}
                  onOpen={onOpenMenu}
                />
              )}
            </div>
          </th>,
        );
      }
    }
    if (showRowTotals(config)) {
      for (const valField of renderedValueFields) {
        valueCells.push(
          <th
            key={`val-total-${valField}`}
            className={`${styles.valueLabel} ${styles.totalsCol}`}
            style={{ top: valueLabelTop }}
            data-testid="pivot-value-label"
          >
            <div className={styles.headerCellInner}>
              <span>{getRenderedValueLabel(config, valField)}</span>
              {hasMenu && (
                <MenuTriggerButton
                  dimension={valField}
                  axis="value"
                  isOpen={activeMenuDimension === valField}
                  onOpen={onOpenMenu}
                />
              )}
            </div>
          </th>,
        );
      }
    }
    rows.push(<tr key="value-labels">{valueCells}</tr>);
  }

  return rows;
}

/**
 * Compute rowSpan values for grouped row headers.
 * Returns a 2D array: spans[rowIdx][dimIdx] = rowSpan (or 0 to skip).
 */
export function computeRowHeaderSpans(rowKeys: string[][]): number[][] {
  const numRows = rowKeys.length;
  if (numRows === 0) return [];
  const numDims = rowKeys[0].length;
  const spans: number[][] = rowKeys.map(() => new Array(numDims).fill(0));

  for (let dimIdx = 0; dimIdx < numDims; dimIdx++) {
    let i = 0;
    while (i < numRows) {
      let span = 1;
      while (
        i + span < numRows &&
        rowKeys[i + span]
          .slice(0, dimIdx + 1)
          .every((v, idx) => v === rowKeys[i][idx])
      ) {
        span++;
      }
      spans[i][dimIdx] = span;
      i += span;
    }
  }
  return spans;
}

export function renderDataRow(
  rowKey: string[],
  colSlots: ColSlot[],
  pivotData: PivotData,
  config: PivotConfigV1,
  hasMultipleValues: boolean,
  onCellClick: ((payload: CellClickPayload) => void) | undefined,
  onCellKeyDown:
    | ((
        e: KeyboardEvent,
        rowKey: readonly string[],
        colKey: readonly string[],
        value: number | null,
        valueField: string,
      ) => void)
    | undefined,
  colRange?: [number, number],
  headerSpans?: number[],
): ReactElement {
  const visibleSlots = colRange
    ? colSlots.slice(colRange[0], colRange[1])
    : colSlots;
  const renderedValueFields = getRenderedValueFields(config);
  const valueFields = hasMultipleValues
    ? renderedValueFields
    : [renderedValueFields[0] ?? ""];
  const interactive = !!onCellClick;
  return (
    <tr key={rowKey.join("\x00")} data-testid="pivot-data-row">
      {rowKey.map((part, dimIdx) => {
        const span = headerSpans ? headerSpans[dimIdx] : 1;
        if (span === 0) return null;
        return (
          <th
            key={dimIdx}
            scope="row"
            className={`${styles.rowHeaderCell} ${dimIdx === 0 ? styles.rowHeaderCellPinned : ""}`}
            data-testid="pivot-row-header"
            rowSpan={span > 1 ? span : undefined}
            data-dim-index={dimIdx}
          >
            {part || "(empty)"}
          </th>
        );
      })}
      {rowKey.length === 0 && (
        <th
          scope="row"
          className={`${styles.rowHeaderCell} ${styles.rowHeaderCellPinned}`}
        >
          Total
        </th>
      )}
      {visibleSlots.map((slot) => {
        if (slot.collapsedLevel !== undefined) {
          return valueFields.map((valField) => {
            const agg = pivotData.getColGroupSubtotal(
              rowKey,
              slot.key,
              valField,
            );
            const cellValue = agg.value();
            const text = formatTotalCellValue(
              agg,
              valField,
              config,
              pivotData,
              null,
            );
            const cellStyle = buildTotalCellStyle(
              cellValue,
              valField,
              config,
              pivotData,
            );
            const hasCondFmt =
              cellStyle && cellStyle.backgroundColor !== undefined;
            const cellInteractive = interactive && cellValue !== null;
            return (
              <td
                key={`cg-${slot.key.join("\x00")}\x01${valField}`}
                className={`${styles.dataCell} ${styles.totalsCol}${hasCondFmt ? ` ${styles.condFormatted}` : ""}`}
                data-testid="pivot-col-group-subtotal"
                style={cellStyle}
                {...(cellInteractive
                  ? {
                      tabIndex: 0,
                      role: "gridcell",
                      onClick: () =>
                        onCellClick(
                          buildCellClickPayload(
                            rowKey,
                            slot.key,
                            cellValue,
                            config,
                            valField,
                          ),
                        ),
                      onKeyDown: onCellKeyDown
                        ? (e: KeyboardEvent) =>
                            onCellKeyDown(
                              e,
                              rowKey,
                              slot.key,
                              cellValue,
                              valField,
                            )
                        : undefined,
                    }
                  : {})}
              >
                {text}
              </td>
            );
          });
        }
        const colKey = slot.key;
        return valueFields.map((valField) => {
          const agg = pivotData.getAggregator(rowKey, colKey, valField);
          const cellValue = agg.value();
          const fmt = formatCellValue(
            cellValue,
            valField,
            config,
            pivotData,
            rowKey,
            colKey,
            config.empty_cell_value,
          );
          const displayText = fmt.text || agg.format(config.empty_cell_value);
          const align = getCellAlignment(valField, config);
          const condStyle = config.conditional_formatting
            ? computeCellStyle(
                cellValue,
                valField,
                config.conditional_formatting,
                pivotData,
                false,
              )
            : undefined;
          const cellStyle: React.CSSProperties = {
            ...(align
              ? { textAlign: align as React.CSSProperties["textAlign"] }
              : {}),
            ...condStyle,
          };
          const cellInteractive = interactive && cellValue !== null;
          return (
            <td
              key={`${colKey.join("\x00")}\x01${valField}`}
              className={`${styles.dataCell}${condStyle ? ` ${styles.condFormatted}` : ""}`}
              data-testid="pivot-data-cell"
              style={Object.keys(cellStyle).length > 0 ? cellStyle : undefined}
              {...(cellInteractive
                ? {
                    tabIndex: 0,
                    role: "gridcell",
                    onClick: () =>
                      onCellClick(
                        buildCellClickPayload(
                          rowKey,
                          colKey,
                          cellValue,
                          config,
                          valField,
                        ),
                      ),
                    onKeyDown: onCellKeyDown
                      ? (e: KeyboardEvent) =>
                          onCellKeyDown(e, rowKey, colKey, cellValue, valField)
                      : undefined,
                  }
                : {})}
            >
              {displayText}
            </td>
          );
        });
      })}
      {showRowTotals(config) && (
        <>
          {valueFields.map((valField) => {
            if (!showTotalForMeasure(config, valField, "row")) {
              return (
                <td
                  key={`total-${valField}`}
                  className={`${styles.dataCell} ${styles.totalsCol} ${styles.excludedTotal}`}
                  data-testid="pivot-excluded-total"
                >
                  –
                </td>
              );
            }
            const agg = pivotData.getRowTotal(rowKey, valField);
            const cellValue = agg.value();
            const totalText = formatTotalCellValue(
              agg,
              valField,
              config,
              pivotData,
              "row",
            );
            const totalStyle = buildTotalCellStyle(
              cellValue,
              valField,
              config,
              pivotData,
            );
            return (
              <td
                key={`total-${valField}`}
                className={`${styles.dataCell} ${styles.totalsCol}`}
                data-testid="pivot-row-total"
                style={totalStyle}
                {...(interactive
                  ? {
                      tabIndex: 0,
                      role: "gridcell",
                      onClick: () =>
                        onCellClick(
                          buildCellClickPayload(
                            rowKey,
                            TOTAL_KEY,
                            cellValue,
                            config,
                            valField,
                          ),
                        ),
                      onKeyDown: onCellKeyDown
                        ? (e: KeyboardEvent) =>
                            onCellKeyDown(
                              e,
                              rowKey,
                              TOTAL_KEY,
                              cellValue,
                              valField,
                            )
                        : undefined,
                    }
                  : {})}
              >
                {totalText}
              </td>
            );
          })}
        </>
      )}
    </tr>
  );
}

/**
 * Render a subtotal row for a collapsed/expanded group.
 */
export function renderSubtotalRow(
  parentKey: string[],
  level: number,
  colSlots: ColSlot[],
  pivotData: PivotData,
  config: PivotConfigV1,
  hasMultipleValues: boolean,
  isCollapsed: boolean,
  onToggleGroup?: (groupKey: string) => void,
  colRange?: [number, number],
  onCellClick?: (payload: CellClickPayload) => void,
  onCellKeyDown?: (
    e: KeyboardEvent,
    rowKey: readonly string[],
    colKey: readonly string[],
    value: number | null,
    valueField: string,
  ) => void,
): ReactElement {
  const visibleSlots = colRange
    ? colSlots.slice(colRange[0], colRange[1])
    : colSlots;
  const renderedValueFields = getRenderedValueFields(config);
  const valueFields = hasMultipleValues
    ? renderedValueFields
    : [renderedValueFields[0] ?? ""];
  const numRowDims = Math.max(config.rows.length, 1);
  const label = parentKey[parentKey.length - 1] || "(empty)";
  const groupKeyStr = parentKey.join("\x00");

  return (
    <tr
      key={`subtotal-${groupKeyStr}-${level}`}
      className={styles.subtotalRow}
      data-testid="pivot-subtotal-row"
      aria-label={`Subtotal for ${parentKey.join(" / ")}`}
      data-level={level}
    >
      {/* Row header cells: span from level to numRowDims */}
      {Array.from({ length: numRowDims }, (_, dimIdx) => {
        if (dimIdx < level) {
          return null; // spanned by parent
        }
        if (dimIdx === level) {
          const colSpan = numRowDims - level;
          const canToggle = !!onToggleGroup;
          return (
            <th
              key={`sub-hdr-${dimIdx}`}
              scope="row"
              className={`${styles.rowHeaderCell} ${styles.rowHeaderCellPinned} ${styles.subtotalHeaderCell} ${canToggle ? styles.groupToggleCell : ""}`}
              colSpan={colSpan > 1 ? colSpan : undefined}
              {...(canToggle
                ? {
                    onClick: () => onToggleGroup(groupKeyStr),
                    role: "button",
                    tabIndex: 0,
                    onKeyDown: (e: KeyboardEvent) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        onToggleGroup(groupKeyStr);
                      }
                    },
                    "aria-expanded": !isCollapsed,
                    "aria-label": isCollapsed
                      ? `Expand ${label}`
                      : `Collapse ${label}`,
                  }
                : {})}
              data-testid={
                canToggle ? `pivot-group-toggle-${groupKeyStr}` : undefined
              }
            >
              {canToggle && (
                <svg
                  className={styles.groupToggleIcon}
                  width="10"
                  height="10"
                  viewBox="0 0 10 10"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.5"
                  strokeLinecap="round"
                >
                  {isCollapsed ? (
                    <>
                      <line x1="5" y1="1" x2="5" y2="9" />
                      <line x1="1" y1="5" x2="9" y2="5" />
                    </>
                  ) : (
                    <line x1="1" y1="5" x2="9" y2="5" />
                  )}
                </svg>
              )}
              <span
                className={styles.subtotalLabel}
                style={{ paddingInlineStart: `${level * 6}px` }}
              >
                {label} Total
              </span>
            </th>
          );
        }
        return null;
      })}
      {/* Data cells: subtotal values */}
      {visibleSlots.map((slot) =>
        valueFields.map((valField) => {
          const agg =
            slot.collapsedLevel !== undefined
              ? pivotData.getSubtotalColGroupAgg(parentKey, slot.key, valField)
              : pivotData.getSubtotalAggregator(parentKey, slot.key, valField);
          const cellValue = agg.value();
          const text = formatTotalCellValue(
            agg,
            valField,
            config,
            pivotData,
            null,
          );
          const cellStyle = buildTotalCellStyle(
            cellValue,
            valField,
            config,
            pivotData,
          );
          return (
            <td
              key={`${slot.key.join("\x00")}\x01${valField}`}
              className={styles.dataCell}
              data-testid="pivot-subtotal-cell"
              style={cellStyle}
              {...(onCellClick
                ? {
                    tabIndex: 0,
                    role: "gridcell",
                    onClick: () =>
                      onCellClick(
                        buildCellClickPayload(
                          parentKey,
                          slot.key,
                          cellValue,
                          config,
                          valField,
                        ),
                      ),
                    onKeyDown: onCellKeyDown
                      ? (e: KeyboardEvent) =>
                          onCellKeyDown(
                            e,
                            parentKey,
                            slot.key,
                            cellValue,
                            valField,
                          )
                      : undefined,
                  }
                : {})}
            >
              {text}
            </td>
          );
        }),
      )}
      {showRowTotals(config) && (
        <>
          {valueFields.map((valField) => {
            if (!showTotalForMeasure(config, valField, "row")) {
              return (
                <td
                  key={`total-${valField}`}
                  className={`${styles.dataCell} ${styles.totalsCol} ${styles.excludedTotal}`}
                  data-testid="pivot-excluded-total"
                >
                  –
                </td>
              );
            }
            const agg = pivotData.getSubtotalAggregator(
              parentKey,
              [],
              valField,
            );
            const cellValue = agg.value();
            const text = formatTotalCellValue(
              agg,
              valField,
              config,
              pivotData,
              "row",
            );
            const cellStyle = buildTotalCellStyle(
              cellValue,
              valField,
              config,
              pivotData,
            );
            return (
              <td
                key={`total-${valField}`}
                className={`${styles.dataCell} ${styles.totalsCol}`}
                data-testid="pivot-subtotal-total"
                style={cellStyle}
                {...(onCellClick
                  ? {
                      tabIndex: 0,
                      role: "gridcell",
                      onClick: () =>
                        onCellClick(
                          buildCellClickPayload(
                            parentKey,
                            TOTAL_KEY,
                            cellValue,
                            config,
                            valField,
                          ),
                        ),
                      onKeyDown: onCellKeyDown
                        ? (e: KeyboardEvent) =>
                            onCellKeyDown(
                              e,
                              parentKey,
                              TOTAL_KEY,
                              cellValue,
                              valField,
                            )
                        : undefined,
                    }
                  : {})}
              >
                {text}
              </td>
            );
          })}
        </>
      )}
    </tr>
  );
}

export function renderTotalsRow(
  colSlots: ColSlot[],
  pivotData: PivotData,
  config: PivotConfigV1,
  numRowDims: number,
  hasMultipleValues: boolean,
  colRange?: [number, number],
  onCellClick?: (payload: CellClickPayload) => void,
  onCellKeyDown?: (
    e: KeyboardEvent,
    rowKey: readonly string[],
    colKey: readonly string[],
    value: number | null,
    valueField: string,
  ) => void,
): ReactElement {
  const visibleSlots = colRange
    ? colSlots.slice(colRange[0], colRange[1])
    : colSlots;
  const renderedValueFields = getRenderedValueFields(config);
  const valueFields = hasMultipleValues
    ? renderedValueFields
    : [renderedValueFields[0] ?? ""];
  return (
    <tr className={styles.totalsRow} data-testid="pivot-totals-row">
      <th
        scope="row"
        className={`${styles.rowHeaderCell} ${styles.rowHeaderCellPinned}`}
        colSpan={numRowDims}
      >
        Grand Total
      </th>
      {visibleSlots.map((slot) =>
        valueFields.map((valField) => {
          if (!showTotalForMeasure(config, valField, "col")) {
            return (
              <td
                key={`coltotal-${slot.key.join("\x00")}-${valField}`}
                className={`${styles.dataCell} ${styles.excludedTotal}`}
                data-testid="pivot-excluded-total"
              >
                –
              </td>
            );
          }
          const agg =
            slot.collapsedLevel !== undefined
              ? pivotData.getColGroupGrandSubtotal(slot.key, valField)
              : pivotData.getColTotal(slot.key, valField);
          const cellValue = agg.value();
          const text = formatTotalCellValue(
            agg,
            valField,
            config,
            pivotData,
            "col",
          );
          const cellStyle = buildTotalCellStyle(
            cellValue,
            valField,
            config,
            pivotData,
          );
          return (
            <td
              key={`coltotal-${slot.key.join("\x00")}-${valField}`}
              className={styles.dataCell}
              data-testid="pivot-col-total"
              style={cellStyle}
              {...(onCellClick
                ? {
                    tabIndex: 0,
                    role: "gridcell",
                    onClick: () =>
                      onCellClick(
                        buildCellClickPayload(
                          TOTAL_KEY,
                          slot.key,
                          cellValue,
                          config,
                          valField,
                        ),
                      ),
                    onKeyDown: onCellKeyDown
                      ? (e: KeyboardEvent) =>
                          onCellKeyDown(
                            e,
                            TOTAL_KEY,
                            slot.key,
                            cellValue,
                            valField,
                          )
                      : undefined,
                  }
                : {})}
            >
              {text}
            </td>
          );
        }),
      )}
      {showRowTotals(config) &&
        valueFields.map((valField) => {
          if (!showTotalForMeasure(config, valField, "grand")) {
            return (
              <td
                key={`grand-${valField}`}
                className={`${styles.dataCell} ${styles.totalsCol} ${styles.excludedTotal}`}
                data-testid="pivot-excluded-total"
              >
                –
              </td>
            );
          }
          const grandAgg = pivotData.getGrandTotal(valField);
          const cellValue = grandAgg.value();
          const text = formatTotalCellValue(
            grandAgg,
            valField,
            config,
            pivotData,
            "grand",
          );
          const cellStyle = buildTotalCellStyle(
            cellValue,
            valField,
            config,
            pivotData,
          );
          return (
            <td
              key={`grand-${valField}`}
              className={`${styles.dataCell} ${styles.totalsCol}`}
              data-testid="pivot-grand-total"
              style={cellStyle}
              {...(onCellClick
                ? {
                    tabIndex: 0,
                    role: "gridcell",
                    onClick: () =>
                      onCellClick(
                        buildCellClickPayload(
                          TOTAL_KEY,
                          TOTAL_KEY,
                          cellValue,
                          config,
                          valField,
                        ),
                      ),
                    onKeyDown: onCellKeyDown
                      ? (e: KeyboardEvent) =>
                          onCellKeyDown(
                            e,
                            TOTAL_KEY,
                            TOTAL_KEY,
                            cellValue,
                            valField,
                          )
                      : undefined,
                  }
                : {})}
            >
              {text}
            </td>
          );
        })}
    </tr>
  );
}

const TableRenderer: FC<TableRendererProps> = ({
  pivotData,
  config,
  onCellClick,
  maxColumns,
  maxRows,
  onSortChange,
  onFilterChange,
  onConfigChange,
  onShowValuesAsChange,
  menuLimit,
  scrollable,
  maxHeight,
  onOverflowChange,
}): ReactElement => {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const prevOverflowRef = useRef(false);

  useEffect(() => {
    const el = wrapperRef.current;
    if (!el || !onOverflowChange) return;
    const check = () => {
      const overflowing = el.scrollHeight > el.clientHeight + 1;
      if (overflowing !== prevOverflowRef.current) {
        prevOverflowRef.current = overflowing;
        onOverflowChange(overflowing);
      }
    };
    const observer = new ResizeObserver(check);
    observer.observe(el);
    check();
    return () => observer.disconnect();
  }, [onOverflowChange]);

  const allRowKeys = pivotData.getRowKeys();
  const allColKeys = pivotData.getColKeys();
  const colKeys =
    maxColumns != null ? allColKeys.slice(0, maxColumns) : allColKeys;
  const renderedValueFields = getRenderedValueFields(config);
  const hasMultipleValues = renderedValueFields.length > 1;
  const numRowDims = Math.max(config.rows.length, 1);
  const numColDims = config.columns.length;

  const colSlots = useMemo(
    () => computeColSlots(colKeys, config.collapsed_col_groups, numColDims),
    [colKeys, config.collapsed_col_groups, numColDims],
  );

  const useSubtotals = !!config.show_subtotals && config.rows.length >= 2;
  const collapsedSet = useMemo(() => {
    const raw = config.collapsed_groups ?? [];
    if (raw.includes("__ALL__")) {
      const level0 = [
        ...new Set(allRowKeys.map((k) => makeKeyString(k.slice(0, 1)))),
      ];
      return normalizeCollapsed(raw, level0);
    }
    return new Set(raw);
  }, [config.collapsed_groups, allRowKeys]);

  const groupedRows: GroupedRow[] | null = useMemo(
    () => (useSubtotals ? pivotData.getGroupedRowKeys() : null),
    [useSubtotals, pivotData],
  );

  // Compute spans for data rows inside grouped (subtotals) mode.
  // rowSpan must count ALL <tr> elements (data + subtotal) within each group
  // so that parent cells correctly cover interleaved subtotal rows.
  const groupedDataSpans = useMemo(() => {
    if (!groupedRows || config.repeat_row_labels) return null;
    const sliced =
      maxRows != null ? groupedRows.slice(0, maxRows) : groupedRows;
    const numDims = config.rows.length;
    if (numDims <= 1) return null;

    const map = new Map<number, number[]>();
    for (let i = 0; i < sliced.length; i++) {
      if (sliced[i].type === "data") {
        map.set(i, new Array(numDims).fill(0));
      }
    }

    for (let d = 0; d < numDims; d++) {
      let i = 0;
      while (i < sliced.length) {
        while (i < sliced.length && sliced[i].type !== "data") i++;
        if (i >= sliced.length) break;

        const prefix = sliced[i].key.slice(0, d + 1);
        const groupStart = i;
        let j = i + 1;
        while (j < sliced.length) {
          const e = sliced[j];
          if (e.type === "data") {
            if (!e.key.slice(0, d + 1).every((v, idx) => v === prefix[idx]))
              break;
          } else if (e.level <= d) {
            break;
          }
          j++;
        }
        map.get(groupStart)![d] = j - groupStart;
        i = j;
      }
    }

    return map;
  }, [groupedRows, config.repeat_row_labels, maxRows, config.rows.length]);

  // For non-subtotal mode, compute standard row spans
  const flatRowKeys = useMemo(() => {
    if (groupedRows) return null;
    const keys = maxRows != null ? allRowKeys.slice(0, maxRows) : allRowKeys;
    return keys;
  }, [groupedRows, allRowKeys, maxRows]);

  const rowSpans = useMemo(() => {
    if (!flatRowKeys || config.repeat_row_labels) return null;
    return flatRowKeys.length > 0 && flatRowKeys[0].length > 1
      ? computeRowHeaderSpans(flatRowKeys)
      : null;
  }, [flatRowKeys, config.repeat_row_labels]);

  const handleToggleGroup = useCallback(
    (groupKeyStr: string) => {
      if (!onConfigChange) return;
      const collapsed = new Set(config.collapsed_groups ?? []);
      if (collapsed.has(groupKeyStr)) {
        collapsed.delete(groupKeyStr);
      } else {
        collapsed.add(groupKeyStr);
      }
      onConfigChange({ ...config, collapsed_groups: [...collapsed] });
    },
    [config, onConfigChange],
  );

  const handleToggleColGroup = useCallback(
    (groupKeyStr: string) => {
      if (!onConfigChange) return;
      const collapsed = new Set(config.collapsed_col_groups ?? []);
      if (collapsed.has(groupKeyStr)) {
        collapsed.delete(groupKeyStr);
      } else {
        collapsed.add(groupKeyStr);
      }
      onConfigChange({ ...config, collapsed_col_groups: [...collapsed] });
    },
    [config, onConfigChange],
  );

  const {
    menuTarget,
    handleOpenMenu,
    handleCloseMenu,
    handleMenuSortChange,
    handleMenuFilterChange,
    hasHeaderMenu,
    menuPosition,
    menuUniqueValues,
    menuSortConfig,
    menuShowFilter,
    menuShowValuesAs,
    menuOnShowValuesAsChange,
    menuOnSubtotalToggle,
    menuOnTotalToggle,
    menuConfig,
    handleCellKeyDown,
  } = useHeaderMenu({
    config,
    pivotData,
    onSortChange,
    onFilterChange,
    onCellClick,
    onShowValuesAsChange,
    onConfigChange,
  });

  const rowKeys = flatRowKeys ?? allRowKeys;
  const hasActiveFilters =
    config.filters && Object.keys(config.filters).length > 0;
  if (rowKeys.length === 0 && colSlots.length === 0 && !hasActiveFilters) {
    return (
      <div data-testid="pivot-table-empty" className={styles.emptyState}>
        No data to display. Configure rows, columns, and values.
      </div>
    );
  }

  const renderBody = () => {
    if (groupedRows) {
      const sliced =
        maxRows != null ? groupedRows.slice(0, maxRows) : groupedRows;
      return sliced.map((entry, idx) => {
        if (entry.type === "subtotal") {
          return renderSubtotalRow(
            entry.key,
            entry.level,
            colSlots,
            pivotData,
            config,
            hasMultipleValues,
            collapsedSet.has(makeKeyString(entry.key)),
            onConfigChange ? handleToggleGroup : undefined,
            undefined,
            onCellClick,
            onCellClick ? handleCellKeyDown : undefined,
          );
        }
        return renderDataRow(
          entry.key,
          colSlots,
          pivotData,
          config,
          hasMultipleValues,
          onCellClick,
          onCellClick ? handleCellKeyDown : undefined,
          undefined,
          groupedDataSpans?.get(idx),
        );
      });
    }

    return flatRowKeys!.map((rowKey, rowIdx) =>
      renderDataRow(
        rowKey,
        colSlots,
        pivotData,
        config,
        hasMultipleValues,
        onCellClick,
        onCellClick ? handleCellKeyDown : undefined,
        undefined,
        rowSpans ? rowSpans[rowIdx] : undefined,
      ),
    );
  };

  return (
    <>
      <div
        ref={wrapperRef}
        className={styles.tableWrapper}
        style={
          scrollable
            ? { flex: "1 1 0", minHeight: 0 }
            : maxHeight != null
              ? { maxHeight }
              : undefined
        }
      >
        <table
          data-testid="pivot-table"
          className={`${styles.pivotTable} ${config.sticky_headers === false ? styles.noSticky : ""}`}
          role="grid"
        >
          <thead>
            {renderColumnHeaders(
              colSlots,
              config,
              numRowDims,
              hasMultipleValues,
              undefined,
              hasHeaderMenu ? handleOpenMenu : undefined,
              menuTarget?.dimension,
              numColDims >= 2 && onConfigChange
                ? handleToggleColGroup
                : undefined,
              pivotData,
              onConfigChange,
            )}
          </thead>
          <tbody>
            {renderBody()}
            {rowKeys.length === 0 && (
              <tr>
                <td
                  colSpan={999}
                  className={styles.emptyFilterRow}
                  data-testid="pivot-empty-filter-row"
                >
                  All values filtered out. Use the header menu to adjust
                  filters.
                </td>
              </tr>
            )}
            {rowKeys.length > 0 &&
              showColumnTotals(config) &&
              renderTotalsRow(
                colSlots,
                pivotData,
                config,
                numRowDims,
                hasMultipleValues,
                undefined,
                onCellClick,
                onCellClick ? handleCellKeyDown : undefined,
              )}
          </tbody>
        </table>
      </div>

      {menuTarget && menuPosition && (
        <div
          className={styles.headerMenuOverlay}
          style={{ top: menuPosition.top, left: menuPosition.left }}
        >
          <HeaderMenu
            dimension={menuTarget.dimension}
            axis={menuTarget.axis === "value" ? "col" : menuTarget.axis}
            sortConfig={menuSortConfig}
            onSortChange={handleMenuSortChange}
            filter={config.filters?.[menuTarget.dimension]}
            uniqueValues={menuUniqueValues}
            onFilterChange={handleMenuFilterChange}
            valueFields={renderedValueFields}
            colKeys={
              menuTarget.axis === "row" ? pivotData.getColKeys() : undefined
            }
            menuLimit={menuLimit}
            showFilter={menuShowFilter}
            showValuesAs={menuShowValuesAs}
            onShowValuesAsChange={
              menuTarget?.axis === "value" &&
              isSyntheticMeasure(config, menuTarget.dimension)
                ? undefined
                : menuOnShowValuesAsChange
            }
            config={menuConfig}
            onSubtotalToggle={menuOnSubtotalToggle}
            onTotalToggle={menuOnTotalToggle}
            onClose={handleCloseMenu}
          />
        </div>
      )}
    </>
  );
};

export default TableRenderer;
