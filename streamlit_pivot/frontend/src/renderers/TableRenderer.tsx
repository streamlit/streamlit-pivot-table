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
  useState,
  KeyboardEvent,
  MouseEvent,
} from "react";
import {
  makeKeyString,
  type PivotData,
  type GroupedRow,
} from "../engine/PivotData";
import {
  formatNumber,
  formatWithPattern,
  formatPercent,
} from "../engine/formatters";
import {
  DATE_GRAIN_LABELS,
  getDimensionLabel,
  getPeriodComparisonMode,
  getRenderedValueFields,
  getRenderedValueLabel,
  getSyntheticMeasureFormat,
  isSyntheticMeasure,
  showRowTotals,
  showColumnTotals,
  showSubtotalForDim,
  showTotalForMeasure,
  type CellClickPayload,
  type DateGrain,
  type DimensionFilter,
  type PivotConfigV1,
  type ShowValuesAs,
  type SortConfig,
} from "../engine/types";
import { computeCellStyle } from "./ConditionalFormat";
import HeaderMenu from "./HeaderMenu";
import { useHeaderMenu } from "./useHeaderMenu";
import {
  computeTemporalColInfos,
  computeHeaderLevels,
  computeNumHeaderLevels,
  computeNumRowHeaderLevels,
  computeParentGroups,
  computeProjectedRowHeaderSpans,
  computeRowHeaderLevels,
  computeTemporalRowInfos,
  computeTemporalColSlots,
  projectVisibleRowEntries,
  applyTemporalRowCollapse,
  toggleTemporalCollapse,
  toggleTemporalRowCollapse,
  type TemporalColSlot,
  type TemporalColInfo,
  type HeaderLevelMapping,
  type ProjectedRowEntry,
  type RowHeaderLevelMapping,
  type TemporalParentRow,
  type VisibleRowEntry,
} from "./temporalHierarchy";
import {
  buildModifiedColKey,
  buildModifiedRowKey,
  formatTemporalParentLabel,
} from "../engine/dateGrouping";
import { getEffectiveDateGrain, type ColumnTypeMap } from "../engine/types";
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
const MIN_COL_WIDTH = 40;

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
      width="8"
      height="8"
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
  onCollapseChange?: (axis: "row" | "col", collapsed: string[]) => void;
  adaptiveDateGrains?: Record<string, DateGrain>;
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

function formatComparisonDisplay(
  value: number | null,
  valField: string,
  config: PivotConfigV1,
  mode: ShowValuesAs,
): string {
  if (value === null) return config.empty_cell_value;
  if (mode === "pct_diff_from_prev" || mode === "pct_diff_from_prev_year") {
    return formatPercent(value);
  }
  const pattern =
    getSyntheticMeasureFormat(config, valField) ??
    config.number_format?.[valField] ??
    config.number_format?.["__all__"];
  return pattern ? formatWithPattern(value, pattern) : formatNumber(value);
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
  const comparisonMode = getPeriodComparisonMode(config, valField);

  if (comparisonMode) {
    const comparisonValue = pivotData.getCellComparisonValue(
      rowKey,
      colKey,
      valField,
      comparisonMode,
    );
    return {
      text: formatComparisonDisplay(
        comparisonValue,
        valField,
        config,
        comparisonMode,
      ),
    };
  }

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
 *
 * When isTotalOfShowAsAxis is null (subtotal data cells, col-group subtotals),
 * callers supply showAsDenominators so percentages use the correct base values
 * (row total for pct_of_row, column total for pct_of_col).
 */
function formatTotalCellValue(
  agg: { value(): number | null; format(empty: string): string },
  valField: string,
  config: PivotConfigV1,
  pivotData: PivotData,
  isTotalOfShowAsAxis: "row" | "col" | "grand" | null,
  comparisonValue?: number | null,
  showAsDenominators?: { row?: number | null; col?: number | null },
  forceRawAggregate?: boolean,
): string {
  const rawValue = agg.value();
  if (forceRawAggregate) {
    if (rawValue === null) return config.empty_cell_value;
    const pattern =
      getSyntheticMeasureFormat(config, valField) ??
      config.number_format?.[valField] ??
      config.number_format?.["__all__"];
    if (pattern) return formatWithPattern(rawValue, pattern);
    return agg.format(config.empty_cell_value);
  }
  const showAs = config.show_values_as?.[valField];
  const comparisonMode = getPeriodComparisonMode(config, valField);

  if (comparisonMode) {
    return formatComparisonDisplay(
      comparisonValue ?? null,
      valField,
      config,
      comparisonMode,
    );
  }

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
      const denom = showAsDenominators?.row;
      if (denom != null && denom !== 0) return formatPercent(rawValue / denom);
      return config.empty_cell_value;
    }
    if (showAs === "pct_of_col") {
      const denom = showAsDenominators?.col;
      if (denom != null && denom !== 0) return formatPercent(rawValue / denom);
      return config.empty_cell_value;
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
  onCollapseChange?: (axis: "row" | "col", collapsed: string[]) => void,
  onResizeMouseDown?: (
    colSlotIndex: number,
    e: React.MouseEvent<HTMLDivElement>,
  ) => void,
  columnWidthMap?: Map<number, number>,
  headerRowOffsets?: number[],
  onResizeDoubleClick?: (
    colSlotIndex: number,
    e: React.MouseEvent<HTMLDivElement>,
  ) => void,
  adaptiveDateGrains?: Record<string, DateGrain>,
  temporalInfos?: TemporalColInfo[],
  headerLevels?: HeaderLevelMapping[],
  onTemporalToggle?: (field: string, collapseKey: string) => void,
  columnTypes?: ColumnTypeMap,
  rowHeaderLevels?: RowHeaderLevelMapping[],
): ReactElement[] {
  const renderedValueFields = getRenderedValueFields(config);
  const visibleSlots = colRange
    ? colSlots.slice(colRange[0], colRange[1])
    : colSlots;
  const slotOffset = colRange?.[0] ?? 0;
  const rows: ReactElement[] = [];
  const effectiveHeaderLevels = headerLevels ?? [];
  const hasTemporalHierarchy =
    effectiveHeaderLevels.length > 0 &&
    effectiveHeaderLevels.some((l) => l.isTemporal && !l.isLeaf);
  const numColLevels = hasTemporalHierarchy
    ? effectiveHeaderLevels.length
    : Math.max(config.columns.length, 1);
  const hasMenu = !!onOpenMenu;

  const elevateCell = (e: React.MouseEvent<HTMLDivElement>) => {
    const th = (e.target as HTMLElement).closest("th");
    if (th) th.style.zIndex = "10";
  };
  const resetCell = (e: React.MouseEvent<HTMLDivElement>) => {
    const th = (e.target as HTMLElement).closest("th");
    if (th) th.style.zIndex = "";
  };

  const handleDimToggle = (axis: "row" | "col", level: number) => {
    if (!onCollapseChange || !pivotData) return;
    const keys =
      axis === "row" ? pivotData.getRowKeys() : pivotData.getColKeys();
    const current =
      axis === "row"
        ? (config.collapsed_groups ?? [])
        : (config.collapsed_col_groups ?? []);
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

    onCollapseChange(axis, [...working].sort());
  };

  const canDimToggle = !!onCollapseChange && !!pivotData;

  for (let level = 0; level < numColLevels; level++) {
    const cells: ReactElement[] = [];

    // Map physical header row to column dimension
    const hlMapping = hasTemporalHierarchy
      ? effectiveHeaderLevels[level]
      : undefined;
    const effectiveDimIndex = hlMapping?.dimIndex ?? level;

    const isLastColLevel = level === numColLevels - 1;
    const dimCellRowSpan = 1 + (hasMultipleValues ? 1 : 0);
    const stickyTop =
      level > 0 && headerRowOffsets?.[level] != null
        ? { top: headerRowOffsets[level] }
        : level > 0
          ? { top: level * HEADER_ROW_HEIGHT }
          : undefined;

    // When a single temporal column expands into multiple header rows,
    // render row dim labels at level 0 spanning all levels (matching
    // Excel / Power BI), and skip corner cells on subsequent levels.
    const singleTemporalColumn =
      hasTemporalHierarchy && config.columns.length === 1;

    const renderRowDimCorner =
      (singleTemporalColumn && level === 0) ||
      (isLastColLevel && !singleTemporalColumn);
    const skipCorner = singleTemporalColumn && level > 0;

    if (!skipCorner && numColLevels > 1 && !renderRowDimCorner) {
      // Column-dimension label corner cell (multi-column layouts only)
      const isTemporalParentCorner =
        hasTemporalHierarchy &&
        hlMapping?.isTemporal &&
        hlMapping.hierarchyOffset > 0;

      if (!isTemporalParentCorner) {
        const colDimName = config.columns[effectiveDimIndex];
        const cornerRowSpan =
          hasTemporalHierarchy && hlMapping?.isTemporal
            ? (temporalInfos?.find((t) => t.field === hlMapping.field)
                ?.hierarchyLevels.length ?? 1)
            : 1;
        const colDimCollapsed = pivotData
          ? isDimCollapsed(
              config.collapsed_col_groups ?? [],
              pivotData.getColKeys(),
              effectiveDimIndex,
            )
          : false;
        const showColDimToggle =
          canDimToggle && config.columns.length >= 2 && !hasTemporalHierarchy;
        cells.push(
          showColDimToggle ? (
            <th
              key={`col-dim-label-${level}`}
              className={`${styles.emptyCorner} ${styles.headerCell} ${styles.colDimLabel} ${styles.dimensionToggleCell}`}
              colSpan={numRowDims}
              rowSpan={cornerRowSpan > 1 ? cornerRowSpan : undefined}
              style={stickyTop}
              onClick={() => handleDimToggle("col", effectiveDimIndex)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  handleDimToggle("col", effectiveDimIndex);
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
              rowSpan={cornerRowSpan > 1 ? cornerRowSpan : undefined}
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
    }

    if (renderRowDimCorner) {
      const cornerFullRowSpan = singleTemporalColumn
        ? numColLevels + (hasMultipleValues ? 1 : 0)
        : dimCellRowSpan;
      if (config.rows.length > 0) {
        const showRowDimToggle =
          canDimToggle && config.rows.length >= 2 && !!config.show_subtotals;
        const effectiveRowHeaderLevels =
          rowHeaderLevels && rowHeaderLevels.length > 0
            ? rowHeaderLevels
            : config.rows.map((field, dimIndex) => ({
                dimIndex,
                field,
                grain: field as DateGrain,
                hierarchyOffset: 0,
                isLeaf: true,
                isTemporal: false,
              }));
        effectiveRowHeaderLevels.forEach((rowLevel, rowHeaderIdx) => {
          const dim = rowLevel.field;
          const dimIdx = rowLevel.dimIndex;
          const isFiltered = !!config.filters?.[dim];
          const isFirstRowDim = rowHeaderIdx === 0;
          const hasSubtotals =
            !!config.show_subtotals && config.rows.length >= 2;
          const sortTargetDim = config.row_sort?.dimension;
          const sortTargetIdx = sortTargetDim
            ? config.rows.indexOf(sortTargetDim)
            : -1;
          const showSortOnThisDim = config.row_sort
            ? sortTargetDim
              ? hasSubtotals
                ? dimIdx >= sortTargetIdx && sortTargetIdx !== -1
                : sortTargetDim === dim
              : hasSubtotals
                ? rowLevel.isLeaf
                : isFirstRowDim
            : false;
          const rowSortDir = showSortOnThisDim
            ? config.row_sort!.direction
            : undefined;
          const isInnermost = dimIdx === config.rows.length - 1;
          const rowDimCollapsed =
            showRowDimToggle && !isInnermost && pivotData && rowLevel.isLeaf
              ? isDimCollapsed(
                  config.collapsed_groups ?? [],
                  pivotData.getRowKeys(),
                  dimIdx,
                )
              : false;
          const parentCollapsed =
            showRowDimToggle && dimIdx > 0 && pivotData && rowLevel.isLeaf
              ? Array.from({ length: dimIdx }, (_, lvl) => lvl).some((lvl) =>
                  isDimCollapsed(
                    config.collapsed_groups ?? [],
                    pivotData.getRowKeys(),
                    lvl,
                  ),
                )
              : false;
          const canToggleThisDim =
            showRowDimToggle &&
            rowLevel.isLeaf &&
            !rowLevel.isTemporal &&
            !isInnermost;
          const dimToggleEnabled = canToggleThisDim && !parentCollapsed;
          const isGroupingDimHeader =
            !!config.show_subtotals &&
            config.rows.length >= 2 &&
            !isInnermost &&
            rowLevel.isLeaf;

          const rowDimResizeIdx = -(rowHeaderIdx + 1);
          const rowDimResizeWidth = columnWidthMap?.get(rowDimResizeIdx);
          const rowDimCellStyle: React.CSSProperties | undefined =
            rowDimResizeWidth != null
              ? {
                  ...stickyTop,
                  width: rowDimResizeWidth,
                  minWidth: rowDimResizeWidth,
                  maxWidth: rowDimResizeWidth,
                }
              : stickyTop;

          cells.push(
            <th
              key={`row-dim-${rowHeaderIdx}`}
              className={`${styles.headerCell} ${rowSortDir ? styles.headerSorted : ""} ${isFirstRowDim ? styles.headerRowPinned : ""} ${canToggleThisDim ? styles.dimensionToggleCell : ""} ${canToggleThisDim && !dimToggleEnabled ? styles.dimensionToggleDisabled : ""} ${isGroupingDimHeader ? styles.groupingDimHeader : ""}`}
              rowSpan={cornerFullRowSpan}
              style={rowDimCellStyle}
              data-testid={
                canToggleThisDim
                  ? `pivot-dim-toggle-row-${rowHeaderIdx}-${slugify(dim)}`
                  : rowHeaderLevels && rowHeaderLevels.length > 0
                    ? `pivot-row-dim-label-${slugify(dim)}-${rowHeaderIdx}`
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
                    "aria-label": `${rowDimCollapsed ? "Expand" : "Collapse"} all ${getDimensionLabel(config, dim, pivotData?.getColumnType(dim), adaptiveDateGrains?.[dim])} groups`,
                  }
                : {})}
            >
              <div className={styles.headerCellInner}>
                {canToggleThisDim && (
                  <DimToggleIcon collapsed={rowDimCollapsed} />
                )}
                <span className={isFiltered ? styles.headerFiltered : ""}>
                  {rowLevel.isTemporal
                    ? `${DATE_GRAIN_LABELS[rowLevel.grain]}`
                    : getDimensionLabel(
                        config,
                        dim,
                        pivotData?.getColumnType(dim),
                        adaptiveDateGrains?.[dim],
                      )}
                </span>
                {rowSortDir && <SortArrowIcon direction={rowSortDir} />}
                {hasMenu && rowLevel.isLeaf && (
                  <MenuTriggerButton
                    dimension={dim}
                    axis="row"
                    isOpen={activeMenuDimension === dim}
                    onOpen={onOpenMenu}
                  />
                )}
              </div>
              {onResizeMouseDown && (
                <div
                  className={styles.resizeHandle}
                  data-testid={`resize-handle-row-dim-${dimIdx}`}
                  onMouseDown={(e) => {
                    e.stopPropagation();
                    onResizeMouseDown(rowDimResizeIdx, e);
                  }}
                  onDoubleClick={
                    onResizeDoubleClick
                      ? (e) => onResizeDoubleClick(rowDimResizeIdx, e)
                      : undefined
                  }
                  onMouseEnter={elevateCell}
                  onMouseLeave={resetCell}
                />
              )}
            </th>,
          );
        });
      } else {
        cells.push(
          <th
            key="corner-empty"
            className={styles.emptyCorner}
            rowSpan={cornerFullRowSpan}
            style={stickyTop}
          />,
        );
      }
    }

    if (config.columns.length > 0) {
      const dimName = config.columns[effectiveDimIndex];
      const isTemporalParent = hlMapping?.isTemporal && !hlMapping.isLeaf;

      // Render temporal parent header row
      if (isTemporalParent && hlMapping && (temporalInfos?.length ?? 0) > 0) {
        const tInfo = temporalInfos!.find((t) => t.field === hlMapping.field);
        if (tInfo) {
          const parentGroups = computeParentGroups(
            colSlots,
            slotOffset,
            visibleSlots.length,
            tInfo,
            hlMapping.grain,
            hlMapping.hierarchyOffset,
            config,
            config.dimension_format?.[hlMapping.field],
          );

          for (const group of parentGroups) {
            const span = group.endIdx - group.startIdx;
            const colSpanVal = hasMultipleValues
              ? span * renderedValueFields.length
              : span;
            const remainingLevels = numColLevels - level;
            const rowSpanVal = group.isCollapsed
              ? remainingLevels + (hasMultipleValues ? 1 : 0)
              : undefined;

            cells.push(
              <th
                key={`col-tp-${level}-${group.collapseKey}`}
                scope="col"
                className={`${styles.headerCell} ${group.isCollapsed ? styles.totalsCol : ""} ${styles.groupToggleCell}`}
                colSpan={colSpanVal > 1 ? colSpanVal : undefined}
                rowSpan={rowSpanVal}
                style={stickyTop}
                data-testid={`pivot-temporal-header-${slugify(hlMapping.field)}-${group.parentBucket}`}
                aria-expanded={!group.isCollapsed}
              >
                <div className={styles.headerCellInner}>
                  <button
                    type="button"
                    className={styles.temporalToggleBtn}
                    data-testid={`temporal-toggle-${slugify(hlMapping.field)}-${group.parentBucket}`}
                    onClick={(e: MouseEvent) => {
                      e.stopPropagation();
                      onTemporalToggle?.(hlMapping.field, group.collapseKey);
                    }}
                    aria-label={`${group.isCollapsed ? "Expand" : "Collapse"} ${group.label}`}
                    title={`${group.isCollapsed ? "Expand" : "Collapse"} ${group.label}`}
                  >
                    <svg
                      className={styles.groupToggleIcon}
                      width="8"
                      height="8"
                      viewBox="0 0 10 10"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      strokeLinecap="round"
                    >
                      {group.isCollapsed ? (
                        <>
                          <line x1="5" y1="1" x2="5" y2="9" />
                          <line x1="1" y1="5" x2="9" y2="5" />
                        </>
                      ) : (
                        <line x1="1" y1="5" x2="9" y2="5" />
                      )}
                    </svg>
                  </button>
                  <span>{group.label}</span>
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
          }
        }
      } else {
        // Original non-temporal / leaf column header rendering.
        // keyIndex maps the physical header row to the column key index.
        // With temporal hierarchy, multiple physical rows can map to the
        // same key dimension, so keyIndex may differ from level.
        const keyIndex = effectiveDimIndex;
        const isFirstColLevel = level === 0;
        const colSortDir =
          isFirstColLevel && config.col_sort
            ? config.col_sort.direction
            : undefined;
        let i = 0;
        while (i < visibleSlots.length) {
          const slotIdx = slotOffset + i;
          const slot = visibleSlots[i];
          const isCollapsedSlot = slot.collapsedLevel !== undefined;
          const collapsedAtOrAbove =
            isCollapsedSlot && slot.collapsedLevel! <= level;

          // Skip sub-level headers for slots collapsed at a higher level
          if (isCollapsedSlot && level > slot.collapsedLevel!) {
            i++;
            continue;
          }

          // Skip temporal-collapsed slots only on temporal header rows — the
          // collapsed parent's rowSpan covers them.  On non-temporal rows
          // (e.g. "region" before the date field), the slot still carries a
          // valid key value that must participate in header grouping.
          const tSlotHeader = slot as TemporalColSlot;
          if (tSlotHeader.temporalCollapse && hlMapping?.isTemporal) {
            i++;
            continue;
          }

          const val = slot.key[keyIndex] ?? "";
          let span = 1;
          // Group consecutive slots with the same value at this level (only for non-collapsed)
          if (!collapsedAtOrAbove) {
            while (
              i + span < visibleSlots.length &&
              visibleSlots[i + span].collapsedLevel === undefined &&
              (!(visibleSlots[i + span] as TemporalColSlot).temporalCollapse ||
                !hlMapping?.isTemporal) &&
              visibleSlots[i + span].key[keyIndex] === val &&
              (keyIndex === 0 ||
                visibleSlots[i + span].key
                  .slice(0, keyIndex)
                  .every((v, idx) => v === slot.key[idx]))
            ) {
              span++;
            }
          }

          const colSpanVal = hasMultipleValues
            ? span * renderedValueFields.length
            : span;

          const isFiltered = !!config.filters?.[dimName];
          const showColSortIndicator =
            !!colSortDir && isFirstColLevel && i === 0;

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
              keyIndex < config.columns.length - 1 &&
              onToggleColGroup);
          const groupKeyStr = makeKeyString(slot.key.slice(0, keyIndex + 1));

          const resizeWidth =
            columnWidthMap && (level === numColLevels - 1 || isCollapseLevel)
              ? columnWidthMap.get(slotIdx)
              : undefined;
          const cellStyle: React.CSSProperties | undefined =
            resizeWidth != null
              ? {
                  ...stickyTop,
                  width: resizeWidth,
                  minWidth: resizeWidth,
                  maxWidth: resizeWidth,
                }
              : stickyTop;

          cells.push(
            <th
              key={`col-${level}-${i}`}
              scope="col"
              className={`${styles.headerCell} ${isCollapseLevel ? styles.totalsCol : ""} ${showColSortIndicator ? styles.headerSorted : ""} ${canToggle ? styles.groupToggleCell : ""}`}
              colSpan={colSpanVal > 1 ? colSpanVal : undefined}
              rowSpan={rowSpanVal}
              style={cellStyle}
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
                    width="8"
                    height="8"
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
                  {(val
                    ? (pivotData?.formatDimLabel(dimName, val) ?? val)
                    : "") || "(empty)"}
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
              {onResizeMouseDown &&
                (level === numColLevels - 1 || isCollapseLevel) && (
                  <div
                    className={styles.resizeHandle}
                    data-testid={`resize-handle-${slotIdx}`}
                    onMouseDown={(e) => {
                      e.stopPropagation();
                      onResizeMouseDown(slotIdx, e);
                    }}
                    onDoubleClick={
                      onResizeDoubleClick
                        ? (e) => onResizeDoubleClick(slotIdx, e)
                        : undefined
                    }
                    onMouseEnter={elevateCell}
                    onMouseLeave={resetCell}
                  />
                )}
            </th>,
          );
          i += span;
        }
      } // close temporal parent else
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
    const valueLabelTop =
      headerRowOffsets?.[numColLevels] ?? numColLevels * HEADER_ROW_HEIGHT;
    const valueCells: ReactElement[] = [];
    for (let si = 0; si < visibleSlots.length; si++) {
      const slot = visibleSlots[si];
      if (slot.collapsedLevel !== undefined) continue;
      if ((slot as TemporalColSlot).temporalCollapse) continue;
      for (let vfi = 0; vfi < renderedValueFields.length; vfi++) {
        const valField = renderedValueFields[vfi];
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
            {onResizeMouseDown && (
              <div
                className={styles.resizeHandle}
                data-testid={`resize-handle-val-${slotOffset + si}-${vfi}`}
                onMouseDown={(e) => {
                  e.stopPropagation();
                  onResizeMouseDown(slotOffset + si, e);
                }}
                onDoubleClick={
                  onResizeDoubleClick
                    ? (e) => onResizeDoubleClick(slotOffset + si, e)
                    : undefined
                }
                onMouseEnter={elevateCell}
                onMouseLeave={resetCell}
              />
            )}
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

function GroupToggleIcon({ isCollapsed }: { isCollapsed: boolean }) {
  return (
    <svg
      className={styles.groupToggleIcon}
      width="8"
      height="8"
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
  );
}

export interface GroupContext {
  onToggleGroup?: (groupKey: string) => void;
  collapsedSet?: Set<string>;
  subtotalsEnabled?: boolean;
  numGroupingDims?: number;
}

interface ProjectedRowRenderContext {
  projectedEntry: ProjectedRowEntry;
  rowHeaderLevels: RowHeaderLevelMapping[];
  onTemporalToggle?: (field: string, collapseKey: string) => void;
}

function renderProjectedRowHeaderCells(
  projected: ProjectedRowEntry,
  headerSpans: number[] | undefined,
  pivotData: PivotData,
  config: PivotConfigV1,
  rowHeaderLevels: RowHeaderLevelMapping[],
  groupContext?: GroupContext,
  onTemporalToggle?: (field: string, collapseKey: string) => void,
  onToggleGroup?: (groupKey: string) => void,
  isSubtotalRow?: boolean,
  subtotalCollapsed?: boolean,
): ReactElement[] {
  const subtotalsOn = groupContext?.subtotalsEnabled && config.rows.length >= 2;
  const leafDimIdx = config.rows.length - 1;
  const cells: ReactElement[] = [];

  for (let colIdx = 0; colIdx < projected.headerValues.length; colIdx++) {
    const span = headerSpans ? (headerSpans[colIdx] ?? 1) : 1;
    if (!projected.headerVisible[colIdx]) continue;

    if (projected.headerSpacer[colIdx]) {
      cells.push(
        <th
          key={`row-spacer-${colIdx}`}
          scope="row"
          className={`${styles.rowHeaderCell} ${colIdx === 0 ? styles.rowHeaderCellPinned : ""}`}
          data-testid="pivot-row-header-spacer"
        />,
      );
      continue;
    }

    if (span === 0) continue;

    const mapping = rowHeaderLevels[colIdx];
    if (!mapping) continue;
    const value = projected.headerValues[colIdx] ?? "";
    const temporalPattern =
      config.dimension_format?.[mapping.field] ??
      config.dimension_format?.["__all__"];
    const formatted =
      value !== ""
        ? mapping.isTemporal
          ? formatTemporalParentLabel(value, mapping.grain, temporalPattern)
          : pivotData.formatDimLabel(mapping.field, value)
        : "(empty)";
    const text = projected.headerIsTotal[colIdx]
      ? `${formatted} Total`
      : formatted;

    const isGroupingDim =
      subtotalsOn && mapping.dimIndex < leafDimIdx && mapping.isLeaf;
    const isLeafDim =
      subtotalsOn && mapping.dimIndex === leafDimIdx && mapping.isLeaf;

    const temporalToggleKey =
      mapping.isTemporal &&
      !mapping.isLeaf &&
      value !== "" &&
      projected.key.length > mapping.dimIndex
        ? projected.type === "temporal_parent" &&
          projected.temporalParent.rowDimIndex === mapping.dimIndex &&
          projected.temporalParent.parentGrain === mapping.grain
          ? makeKeyString(projected.temporalParent.modifiedRowKey)
          : makeKeyString(
              buildModifiedRowKey(
                projected.key,
                mapping.dimIndex,
                mapping.field,
                value,
              ).slice(0, mapping.dimIndex + 1),
            )
        : undefined;
    const temporalCollapsed =
      temporalToggleKey !== undefined &&
      (config.collapsed_temporal_row_groups?.[mapping.field] ?? []).includes(
        temporalToggleKey,
      );
    const showTemporalToggle =
      temporalToggleKey !== undefined &&
      !!onTemporalToggle &&
      ((projected.type === "temporal_parent" &&
        projected.temporalParent.parentGrain === mapping.grain) ||
        span > 1);

    const showSubtotalRowToggle =
      !!isSubtotalRow &&
      !!onToggleGroup &&
      mapping.isLeaf &&
      !mapping.isTemporal &&
      mapping.dimIndex === projected.level;
    const showDataRowToggle =
      !isSubtotalRow &&
      isGroupingDim &&
      mapping.isLeaf &&
      !mapping.isTemporal &&
      span > 1 &&
      !!groupContext?.onToggleGroup &&
      !showSubtotalForDim(config, config.rows[mapping.dimIndex] ?? "");
    const showGroupToggle = showSubtotalRowToggle || showDataRowToggle;
    const groupKeyStr = showGroupToggle
      ? projected.key.slice(0, mapping.dimIndex + 1).join("\x00")
      : "";
    const isCollapsed = showSubtotalRowToggle
      ? !!subtotalCollapsed
      : showDataRowToggle
        ? (groupContext?.collapsedSet?.has(groupKeyStr) ?? false)
        : false;

    const dimClasses = [
      styles.rowHeaderCell,
      colIdx === 0 ? styles.rowHeaderCellPinned : "",
      isGroupingDim ? styles.groupingDimCell : "",
      isLeafDim ? styles.leafDimCell : "",
      showTemporalToggle || showGroupToggle ? styles.groupToggleCell : "",
      projected.type === "temporal_parent" ? styles.subtotalHeaderCell : "",
    ]
      .filter(Boolean)
      .join(" ");

    cells.push(
      <th
        key={`row-header-${colIdx}`}
        scope="row"
        className={dimClasses}
        data-testid={
          showGroupToggle
            ? `pivot-group-toggle-${groupKeyStr}`
            : "pivot-row-header"
        }
        rowSpan={span > 1 ? span : undefined}
        data-dim-index={mapping.dimIndex}
        {...(showGroupToggle
          ? {
              onClick: () =>
                showSubtotalRowToggle
                  ? onToggleGroup?.(groupKeyStr)
                  : groupContext?.onToggleGroup?.(groupKeyStr),
              role: "button",
              tabIndex: 0,
              onKeyDown: (e: KeyboardEvent) => {
                if (e.key === "Enter" || e.key === " ") {
                  e.preventDefault();
                  if (showSubtotalRowToggle) onToggleGroup?.(groupKeyStr);
                  else groupContext?.onToggleGroup?.(groupKeyStr);
                }
              },
              "aria-expanded": !isCollapsed,
            }
          : {})}
      >
        {showTemporalToggle && (
          <button
            type="button"
            className={styles.temporalToggleBtn}
            data-testid={`pivot-temporal-row-toggle-${mapping.field}-${value}`}
            onClick={(e: MouseEvent<HTMLButtonElement>) => {
              e.stopPropagation();
              onTemporalToggle?.(mapping.field, temporalToggleKey!);
            }}
            aria-label={`${temporalCollapsed ? "Expand" : "Collapse"} ${text}`}
            title={`${temporalCollapsed ? "Expand" : "Collapse"} ${text}`}
          >
            <GroupToggleIcon isCollapsed={temporalCollapsed} />
          </button>
        )}
        {!showTemporalToggle && showGroupToggle && (
          <GroupToggleIcon isCollapsed={isCollapsed} />
        )}
        {text}
      </th>,
    );
  }

  return cells;
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
  groupBoundaryLevel?: number,
  groupContext?: GroupContext,
  isEvenRow?: boolean,
  projectedContext?: ProjectedRowRenderContext,
): ReactElement {
  const visibleSlots = colRange
    ? colSlots.slice(colRange[0], colRange[1])
    : colSlots;
  const renderedValueFields = getRenderedValueFields(config);
  const valueFields = hasMultipleValues
    ? renderedValueFields
    : [renderedValueFields[0] ?? ""];
  const interactive = !!onCellClick;
  const subtotalsOn = groupContext?.subtotalsEnabled && config.rows.length >= 2;
  const numGroupingDims = groupContext?.numGroupingDims ?? 0;
  const leafDimIdx = config.rows.length - 1;
  const trClasses = [
    groupBoundaryLevel !== undefined
      ? (styles[
          `groupBoundaryL${Math.min(groupBoundaryLevel, 2)}` as keyof typeof styles
        ] ?? "")
      : "",
    isEvenRow ? styles.evenDataRow : "",
  ]
    .filter(Boolean)
    .join(" ");
  return (
    <tr
      key={rowKey.join("\x00")}
      data-testid="pivot-data-row"
      className={trClasses || undefined}
    >
      {projectedContext
        ? renderProjectedRowHeaderCells(
            projectedContext.projectedEntry,
            headerSpans,
            pivotData,
            config,
            projectedContext.rowHeaderLevels,
            groupContext,
            projectedContext.onTemporalToggle,
          )
        : rowKey.map((part, dimIdx) => {
            const span = headerSpans ? headerSpans[dimIdx] : 1;
            if (span === 0) return null;
            const isGroupingDim = subtotalsOn && dimIdx < leafDimIdx;
            const isLeafDim = subtotalsOn && dimIdx === leafDimIdx;
            // Only show toggle on data rows when this dimension has NO subtotal row.
            // When subtotals exist, the subtotal row has the toggle to avoid redundancy.
            const hasSubtotalRow = showSubtotalForDim(
              config,
              config.rows[dimIdx] ?? "",
            );
            const showToggle =
              isGroupingDim &&
              span > 1 &&
              groupContext?.onToggleGroup &&
              !hasSubtotalRow;
            const groupKeyStr = showToggle
              ? rowKey.slice(0, dimIdx + 1).join("\x00")
              : "";
            const isCollapsed = showToggle
              ? (groupContext.collapsedSet?.has(groupKeyStr) ?? false)
              : false;
            const dimClasses = [
              styles.rowHeaderCell,
              dimIdx === 0 ? styles.rowHeaderCellPinned : "",
              isGroupingDim ? styles.groupingDimCell : "",
              isLeafDim ? styles.leafDimCell : "",
              showToggle ? styles.groupToggleCell : "",
            ]
              .filter(Boolean)
              .join(" ");
            return (
              <th
                key={dimIdx}
                scope="row"
                className={dimClasses}
                data-testid={
                  showToggle
                    ? `pivot-group-toggle-${groupKeyStr}`
                    : "pivot-row-header"
                }
                rowSpan={span > 1 ? span : undefined}
                data-dim-index={dimIdx}
                {...(showToggle
                  ? {
                      onClick: () => groupContext.onToggleGroup!(groupKeyStr),
                      role: "button",
                      tabIndex: 0,
                      onKeyDown: (e: KeyboardEvent) => {
                        if (e.key === "Enter" || e.key === " ") {
                          e.preventDefault();
                          groupContext.onToggleGroup!(groupKeyStr);
                        }
                      },
                      "aria-expanded": !isCollapsed,
                    }
                  : {})}
              >
                {showToggle && <GroupToggleIcon isCollapsed={isCollapsed} />}
                {(part
                  ? pivotData.formatDimLabel(config.rows[dimIdx] ?? "", part)
                  : "") || "(empty)"}
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
        const tSlot = slot as TemporalColSlot;
        if (tSlot.temporalCollapse) {
          const tc = tSlot.temporalCollapse;
          return valueFields.map((valField) => {
            const agg = pivotData.getTemporalColSubtotal(
              rowKey,
              tc.modifiedColKey,
              valField,
            );
            const cellValue = agg.value();
            const text = formatTotalCellValue(
              agg,
              valField,
              config,
              pivotData,
              null,
              undefined,
              undefined,
              true,
            );
            const cellStyle = buildTotalCellStyle(
              cellValue,
              valField,
              config,
              pivotData,
            );
            const hasCondFmt =
              cellStyle && cellStyle.backgroundColor !== undefined;
            return (
              <td
                key={`tc-${makeKeyString(tc.modifiedColKey)}\x01${valField}`}
                className={`${styles.dataCell} ${styles.totalsCol}${hasCondFmt ? ` ${styles.condFormatted}` : ""}`}
                data-testid="pivot-temporal-collapse-cell"
                style={cellStyle}
              >
                {text}
              </td>
            );
          });
        }
        if (slot.collapsedLevel !== undefined) {
          return valueFields.map((valField) => {
            const agg = pivotData.getColGroupSubtotal(
              rowKey,
              slot.key,
              valField,
            );
            const comparisonMode = getPeriodComparisonMode(config, valField);
            const cellValue = agg.value();
            const text = formatTotalCellValue(
              agg,
              valField,
              config,
              pivotData,
              null,
              comparisonMode
                ? pivotData.getColGroupComparisonValue(
                    rowKey,
                    slot.key,
                    valField,
                    comparisonMode,
                  )
                : undefined,
              {
                row: pivotData.getRowTotal(rowKey, valField).value(),
                col: pivotData
                  .getColGroupGrandSubtotal(slot.key, valField)
                  .value(),
              },
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
            const comparisonMode = getPeriodComparisonMode(config, valField);
            const cellValue = agg.value();
            const totalText = formatTotalCellValue(
              agg,
              valField,
              config,
              pivotData,
              "row",
              comparisonMode
                ? pivotData.getRowTotalComparisonValue(
                    rowKey,
                    valField,
                    comparisonMode,
                  )
                : undefined,
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
  headerSpans?: number[],
  projectedContext?: ProjectedRowRenderContext,
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
      {projectedContext
        ? renderProjectedRowHeaderCells(
            projectedContext.projectedEntry,
            headerSpans,
            pivotData,
            config,
            projectedContext.rowHeaderLevels,
            undefined,
            projectedContext.onTemporalToggle,
            onToggleGroup,
            true,
            isCollapsed,
          )
        : Array.from({ length: numRowDims }, (_, dimIdx) => {
            const span = headerSpans ? headerSpans[dimIdx] : 1;
            if (span === 0) return null;

            if (dimIdx < level) {
              // Parent dimension cell — show value from parentKey
              return (
                <th
                  key={`sub-hdr-${dimIdx}`}
                  scope="row"
                  className={`${styles.rowHeaderCell} ${dimIdx === 0 ? styles.rowHeaderCellPinned : ""} ${styles.groupingDimCell}`}
                  rowSpan={span > 1 ? span : undefined}
                  data-dim-index={dimIdx}
                >
                  {parentKey[dimIdx] || "(empty)"}
                </th>
              );
            }
            if (dimIdx === level) {
              const canToggle = !!onToggleGroup;
              return (
                <th
                  key={`sub-hdr-${dimIdx}`}
                  scope="row"
                  className={`${styles.rowHeaderCell} ${dimIdx === 0 ? styles.rowHeaderCellPinned : ""} ${styles.subtotalHeaderCell} ${canToggle ? styles.groupToggleCell : ""}`}
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
                  {canToggle && <GroupToggleIcon isCollapsed={isCollapsed} />}
                  <span className={styles.subtotalLabel}>{label} Total</span>
                </th>
              );
            }
            // Inner dimensions (dimIdx > level): empty cell
            return (
              <th
                key={`sub-hdr-${dimIdx}`}
                scope="row"
                className={styles.rowHeaderCell}
              />
            );
          })}
      {/* Data cells: subtotal values */}
      {visibleSlots.map((slot) =>
        valueFields.map((valField) => {
          const tSlotSub = slot as TemporalColSlot;
          const comparisonMode = tSlotSub.temporalCollapse
            ? undefined
            : getPeriodComparisonMode(config, valField);
          const agg = tSlotSub.temporalCollapse
            ? pivotData.getTemporalColSubtotal(
                parentKey,
                tSlotSub.temporalCollapse.modifiedColKey,
                valField,
              )
            : slot.collapsedLevel !== undefined
              ? pivotData.getSubtotalColGroupAgg(parentKey, slot.key, valField)
              : pivotData.getSubtotalAggregator(parentKey, slot.key, valField);
          const cellValue = agg.value();
          const text = formatTotalCellValue(
            agg,
            valField,
            config,
            pivotData,
            null,
            comparisonMode
              ? slot.collapsedLevel !== undefined
                ? pivotData.getColGroupComparisonValue(
                    parentKey,
                    slot.key,
                    valField,
                    comparisonMode,
                  )
                : pivotData.getSubtotalComparisonValue(
                    parentKey,
                    slot.key,
                    valField,
                    comparisonMode,
                  )
              : undefined,
            {
              row: pivotData
                .getSubtotalAggregator(parentKey, [], valField)
                .value(),
              col:
                slot.collapsedLevel !== undefined
                  ? pivotData
                      .getColGroupGrandSubtotal(slot.key, valField)
                      .value()
                  : pivotData.getColTotal(slot.key, valField).value(),
            },
            !!tSlotSub.temporalCollapse,
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
            const comparisonMode = getPeriodComparisonMode(config, valField);
            const cellValue = agg.value();
            const text = formatTotalCellValue(
              agg,
              valField,
              config,
              pivotData,
              "row",
              comparisonMode
                ? pivotData.getSubtotalComparisonValue(
                    parentKey,
                    [],
                    valField,
                    comparisonMode,
                  )
                : undefined,
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

export function renderTemporalParentRow(
  entry: ProjectedRowEntry & TemporalParentRow,
  colSlots: ColSlot[],
  pivotData: PivotData,
  config: PivotConfigV1,
  hasMultipleValues: boolean,
  rowHeaderLevels: RowHeaderLevelMapping[],
  onTemporalToggle?: (field: string, collapseKey: string) => void,
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
  const trClasses = [styles.subtotalRow].filter(Boolean).join(" ");
  const resolveAgg = (slot: ColSlot, valField: string) => {
    const tSlot = slot as TemporalColSlot;
    if (tSlot.temporalCollapse) {
      return pivotData.getTemporalRowSubtotal(
        entry.temporalParent.modifiedRowKey,
        tSlot.temporalCollapse.modifiedColKey,
        valField,
      );
    }
    if (slot.collapsedLevel !== undefined) {
      return pivotData.getTemporalRowSubtotal(
        entry.temporalParent.modifiedRowKey,
        slot.key,
        valField,
      );
    }
    return pivotData.getTemporalRowSubtotal(
      entry.temporalParent.modifiedRowKey,
      slot.key,
      valField,
    );
  };

  return (
    <tr
      key={`temporal-parent-${entry.temporalParent.modifiedRowKey.join("\x00")}`}
      data-testid="pivot-temporal-parent-row"
      className={trClasses || undefined}
    >
      {renderProjectedRowHeaderCells(
        entry,
        headerSpans,
        pivotData,
        config,
        rowHeaderLevels,
        undefined,
        onTemporalToggle,
      )}
      {visibleSlots.map((slot) =>
        valueFields.map((valField) => {
          const agg = resolveAgg(slot, valField);
          const cellValue = agg.value();
          const text = formatTotalCellValue(
            agg,
            valField,
            config,
            pivotData,
            null,
            undefined,
            undefined,
            true,
          );
          const cellStyle = buildTotalCellStyle(
            cellValue,
            valField,
            config,
            pivotData,
          );
          return (
            <td
              key={`trp-${entry.temporalParent.modifiedRowKey.join("\x00")}\x01${slot.key.join("\x00")}\x01${valField}`}
              className={`${styles.dataCell} ${styles.totalsCol}`}
              data-testid="pivot-temporal-row-collapse-cell"
              style={cellStyle}
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
                  key={`trp-total-${valField}`}
                  className={`${styles.dataCell} ${styles.totalsCol} ${styles.excludedTotal}`}
                  data-testid="pivot-excluded-total"
                >
                  –
                </td>
              );
            }
            const agg = pivotData.getTemporalRowSubtotalGrand(
              entry.temporalParent.modifiedRowKey,
              valField,
            );
            const cellValue = agg.value();
            const totalText = formatTotalCellValue(
              agg,
              valField,
              config,
              pivotData,
              "row",
              undefined,
              undefined,
              true,
            );
            const totalStyle = buildTotalCellStyle(
              cellValue,
              valField,
              config,
              pivotData,
            );
            return (
              <td
                key={`trp-total-${valField}`}
                className={`${styles.dataCell} ${styles.totalsCol}`}
                data-testid="pivot-row-total"
                style={totalStyle}
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
          const tSlot = slot as TemporalColSlot;
          const agg = tSlot.temporalCollapse
            ? pivotData.getTemporalColSubtotalGrand(
                tSlot.temporalCollapse.modifiedColKey,
                valField,
              )
            : slot.collapsedLevel !== undefined
              ? pivotData.getColGroupGrandSubtotal(slot.key, valField)
              : pivotData.getColTotal(slot.key, valField);
          const comparisonMode = tSlot.temporalCollapse
            ? undefined
            : getPeriodComparisonMode(config, valField);
          const cellValue = agg.value();
          const text = formatTotalCellValue(
            agg,
            valField,
            config,
            pivotData,
            "col",
            comparisonMode
              ? slot.collapsedLevel !== undefined
                ? pivotData.getColGroupComparisonValue(
                    [],
                    slot.key,
                    valField,
                    comparisonMode,
                  )
                : pivotData.getColTotalComparisonValue(
                    slot.key,
                    valField,
                    comparisonMode,
                  )
              : undefined,
            undefined,
            !!tSlot.temporalCollapse,
          );
          const cellStyle = buildTotalCellStyle(
            cellValue,
            valField,
            config,
            pivotData,
          );
          const cellInteractive = onCellClick && !tSlot.temporalCollapse;
          return (
            <td
              key={`coltotal-${slot.key.join("\x00")}-${valField}`}
              className={`${styles.dataCell}${tSlot.temporalCollapse ? ` ${styles.totalsCol}` : ""}`}
              data-testid={
                tSlot.temporalCollapse
                  ? "pivot-temporal-collapse-total"
                  : "pivot-col-total"
              }
              style={cellStyle}
              {...(cellInteractive
                ? {
                    tabIndex: 0,
                    role: "gridcell",
                    onClick: () =>
                      onCellClick!(
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
            undefined,
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
  onCollapseChange,
  adaptiveDateGrains,
  menuLimit,
  scrollable,
  maxHeight,
  onOverflowChange,
}): ReactElement => {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const theadRef = useRef<HTMLTableSectionElement>(null);
  const prevOverflowRef = useRef(false);
  const [headerRowOffsets, setHeaderRowOffsets] = useState<number[]>([]);

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

  useEffect(() => {
    const thead = theadRef.current;
    if (!thead || typeof ResizeObserver === "undefined") return;
    const measure = () => {
      const rows = thead.querySelectorAll("tr");
      const offsets: number[] = [0];
      let cumulative = 0;
      rows.forEach((row) => {
        cumulative += row.getBoundingClientRect().height;
        offsets.push(Math.round(cumulative));
      });
      setHeaderRowOffsets(offsets);
    };
    const observer = new ResizeObserver(measure);
    observer.observe(thead);
    measure();
    return () => observer.disconnect();
  }, [config.columns.length, config.values.length]);

  const [columnWidthMap, setColumnWidthMap] = useState<Map<number, number>>(
    () => new Map(),
  );
  const [isResizing, setIsResizing] = useState(false);
  const resizeDragRef = useRef<{
    slotIndex: number;
    startX: number;
    startWidth: number;
  } | null>(null);

  const handleResizeDoubleClick = useCallback(
    (slotIndex: number, e: React.MouseEvent<HTMLDivElement>) => {
      e.preventDefault();
      e.stopPropagation();

      const th = (e.target as HTMLElement).closest("th");
      const table = wrapperRef.current?.querySelector("table");
      if (!th || !table) return;

      const headerRow = th.parentElement as HTMLTableRowElement;
      let physicalCol = 0;
      for (const cell of headerRow.cells) {
        if (cell === th) break;
        physicalCol += cell.colSpan || 1;
      }

      let maxWidth = MIN_COL_WIDTH;
      for (const row of table.querySelectorAll("tbody tr")) {
        const cells = (row as HTMLTableRowElement).cells;
        let col = 0;
        for (const cell of cells) {
          const span = cell.colSpan || 1;
          if (col <= physicalCol && physicalCol < col + span) {
            const ruler = document.createElement("span");
            ruler.style.visibility = "hidden";
            ruler.style.position = "absolute";
            ruler.style.whiteSpace = "nowrap";
            ruler.style.font = getComputedStyle(cell).font;
            ruler.textContent = cell.textContent;
            document.body.appendChild(ruler);
            const w = Math.ceil(ruler.getBoundingClientRect().width);
            document.body.removeChild(ruler);

            const cs = getComputedStyle(cell);
            const padH =
              parseFloat(cs.paddingLeft) + parseFloat(cs.paddingRight);
            const natural = Math.ceil(w + padH) + 2;
            const perCol = Math.ceil(natural / span);
            if (perCol > maxWidth) maxWidth = perCol;
            break;
          }
          col += span;
        }
      }

      const headerCells = th.closest("thead")?.querySelectorAll("th");
      if (headerCells) {
        for (const hc of headerCells) {
          const hRow = hc.parentElement as HTMLTableRowElement;
          let hCol = 0;
          for (const cell of hRow.cells) {
            if (cell === hc) break;
            hCol += cell.colSpan || 1;
          }
          const hSpan = hc.colSpan || 1;
          if (hCol <= physicalCol && physicalCol < hCol + hSpan) {
            const inner =
              hc.querySelector<HTMLElement>("[class*='headerCellInner']") ||
              hc.querySelector<HTMLElement>("[class*='valueLabel']") ||
              hc;
            const ruler = document.createElement("span");
            ruler.style.visibility = "hidden";
            ruler.style.position = "absolute";
            ruler.style.whiteSpace = "nowrap";
            ruler.style.font = getComputedStyle(inner).font;
            ruler.textContent = inner.textContent;
            document.body.appendChild(ruler);
            const w = Math.ceil(ruler.getBoundingClientRect().width);
            document.body.removeChild(ruler);

            const cs = getComputedStyle(hc);
            const padH =
              parseFloat(cs.paddingLeft) + parseFloat(cs.paddingRight);
            const natural = Math.ceil(w + padH) + 18;
            const perCol = Math.ceil(natural / hSpan);
            if (perCol > maxWidth) maxWidth = perCol;
          }
        }
      }

      setColumnWidthMap((prev) => {
        const next = new Map(prev);
        next.set(slotIndex, maxWidth);
        return next;
      });
    },
    [],
  );

  const handleResizeMouseDown = useCallback(
    (slotIndex: number, e: React.MouseEvent<HTMLDivElement>) => {
      if (e.detail >= 2) return;
      e.preventDefault();
      e.stopPropagation();
      const el = (e.target as HTMLElement).closest("th");
      const startWidth = el ? el.offsetWidth : MIN_COL_WIDTH;
      resizeDragRef.current = { slotIndex, startX: e.clientX, startWidth };
      setIsResizing(true);

      const onMouseMove = (ev: globalThis.MouseEvent) => {
        const drag = resizeDragRef.current;
        if (!drag) return;
        ev.preventDefault();
        const delta = ev.clientX - drag.startX;
        const newWidth = Math.max(MIN_COL_WIDTH, drag.startWidth + delta);
        const idx = drag.slotIndex;
        setColumnWidthMap((prev) => {
          const next = new Map(prev);
          next.set(idx, newWidth);
          return next;
        });
      };

      const cleanup = () => {
        resizeDragRef.current = null;
        setIsResizing(false);
        window.removeEventListener("mousemove", onMouseMove);
        window.removeEventListener("mouseup", cleanup);
        window.removeEventListener("mouseleave", cleanup);
      };

      window.addEventListener("mousemove", onMouseMove);
      window.addEventListener("mouseup", cleanup);
      window.addEventListener("mouseleave", cleanup);
    },
    [],
  );

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

  const columnTypes = pivotData.getColumnTypes();
  const temporalInfos = useMemo(
    () => computeTemporalColInfos(config, columnTypes, adaptiveDateGrains),
    [config, columnTypes, adaptiveDateGrains],
  );
  const headerLevels = useMemo(
    () => computeHeaderLevels(config, temporalInfos),
    [config, temporalInfos],
  );
  const rowTemporalInfos = useMemo(
    () => computeTemporalRowInfos(config, columnTypes, adaptiveDateGrains),
    [config, columnTypes, adaptiveDateGrains],
  );
  const rowHeaderLevels = useMemo(
    () => computeRowHeaderLevels(config, rowTemporalInfos),
    [config, rowTemporalInfos],
  );
  const effectiveNumRowDims =
    config.rows.length === 0
      ? 1
      : rowTemporalInfos.length > 0
        ? computeNumRowHeaderLevels(config, rowTemporalInfos)
        : numRowDims;

  const effectiveColSlots: ColSlot[] = useMemo(
    () => computeTemporalColSlots(colSlots, temporalInfos, config),
    [colSlots, temporalInfos, config],
  );

  const handleTemporalToggle = useCallback(
    (field: string, collapseKey: string) => {
      if (!onConfigChange) return;
      const updated = toggleTemporalCollapse(
        config.collapsed_temporal_groups,
        field,
        collapseKey,
      );
      onConfigChange({
        ...config,
        collapsed_temporal_groups:
          Object.keys(updated).length > 0 ? updated : undefined,
      });
    },
    [config, onConfigChange],
  );
  const handleTemporalRowToggle = useCallback(
    (field: string, collapseKey: string) => {
      if (!onConfigChange) return;
      const updated = toggleTemporalRowCollapse(
        config.collapsed_temporal_row_groups,
        field,
        collapseKey,
      );
      onConfigChange({
        ...config,
        collapsed_temporal_row_groups:
          Object.keys(updated).length > 0 ? updated : undefined,
      });
    },
    [config, onConfigChange],
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

  const visibleRowEntries: VisibleRowEntry[] | null = useMemo(() => {
    if (rowTemporalInfos.length === 0) return null;
    const baseEntries: GroupedRow[] = groupedRows
      ? groupedRows
      : allRowKeys.map((key) => ({
          type: "data" as const,
          key,
          level: config.rows.length - 1,
        }));
    return applyTemporalRowCollapse(baseEntries, rowTemporalInfos, config);
  }, [rowTemporalInfos, groupedRows, allRowKeys, config]);

  const projectedRowEntries = useMemo(() => {
    if (!visibleRowEntries) return null;
    const projected = projectVisibleRowEntries(
      visibleRowEntries,
      config,
      rowHeaderLevels,
      rowTemporalInfos,
    );
    return maxRows != null ? projected.slice(0, maxRows) : projected;
  }, [visibleRowEntries, config, rowHeaderLevels, rowTemporalInfos, maxRows]);

  const projectedRowSpans = useMemo(() => {
    if (!projectedRowEntries || config.repeat_row_labels) return null;
    return computeProjectedRowHeaderSpans(projectedRowEntries);
  }, [projectedRowEntries, config.repeat_row_labels]);

  // Compute row spans for ALL rows (data + subtotal) in grouped mode.
  // Subtotals at their own level are standalone (span=1) so their label
  // cell always renders.  Parent cells in subtotal rows participate in
  // normal span groups so that the parent dimension value is shared with
  // adjacent data rows or other subtotals in the same parent group.
  const groupedRowSpans = useMemo(() => {
    if (!groupedRows || config.repeat_row_labels) return null;
    const sliced =
      maxRows != null ? groupedRows.slice(0, maxRows) : groupedRows;
    const numDims = config.rows.length;
    if (numDims <= 1) return null;

    const map = new Map<number, number[]>();
    for (let i = 0; i < sliced.length; i++) {
      map.set(i, new Array(numDims).fill(1));
    }

    function getPrefix(entry: GroupedRow, d: number): string[] | null {
      if (entry.type === "data") return entry.key.slice(0, d + 1);
      return entry.level >= d ? entry.key.slice(0, d + 1) : null;
    }

    for (let d = 0; d < numDims; d++) {
      let i = 0;
      while (i < sliced.length) {
        const entry = sliced[i];

        // Subtotals at their own level are standalone at that dimension
        if (entry.type === "subtotal" && entry.level === d) {
          map.get(i)![d] = 1;
          i++;
          continue;
        }

        const prefix = getPrefix(entry, d);
        if (prefix === null) {
          map.get(i)![d] = 1;
          i++;
          continue;
        }

        const groupStart = i;
        let j = i + 1;
        while (j < sliced.length) {
          const next = sliced[j];
          // Subtotal at its own level or shallower breaks the group
          if (next.type === "subtotal" && next.level <= d) break;
          const np = getPrefix(next, d);
          if (np === null || !prefix.every((v, k) => np[k] === v)) break;
          j++;
        }

        map.get(groupStart)![d] = j - groupStart;
        for (let k = groupStart + 1; k < j; k++) {
          map.get(k)![d] = 0;
        }
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
      if (!onCollapseChange) return;
      const collapsed = new Set(config.collapsed_groups ?? []);
      if (collapsed.has(groupKeyStr)) {
        collapsed.delete(groupKeyStr);
      } else {
        collapsed.add(groupKeyStr);
      }
      onCollapseChange("row", [...collapsed].sort());
    },
    [config, onCollapseChange],
  );

  const handleToggleColGroup = useCallback(
    (groupKeyStr: string) => {
      if (!onCollapseChange) return;
      const collapsed = new Set(config.collapsed_col_groups ?? []);
      if (collapsed.has(groupKeyStr)) {
        collapsed.delete(groupKeyStr);
      } else {
        collapsed.add(groupKeyStr);
      }
      onCollapseChange("col", [...collapsed].sort());
    },
    [config, onCollapseChange],
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
    menuTitle,
    menuDateGrain,
    menuOnDateGrainChange,
    menuOnDateDrill,
    menuSupportsPeriodComparison,
    menuFormatLabel,
    handleCellKeyDown,
  } = useHeaderMenu({
    config,
    pivotData,
    onSortChange,
    onFilterChange,
    onCellClick,
    onShowValuesAsChange,
    onConfigChange,
    adaptiveDateGrains,
  });

  const rowKeys = projectedRowEntries
    ? projectedRowEntries.map((entry) => entry.key)
    : (flatRowKeys ?? allRowKeys);
  const hasActiveFilters =
    config.filters && Object.keys(config.filters).length > 0;
  if (rowKeys.length === 0 && colSlots.length === 0 && !hasActiveFilters) {
    return (
      <div data-testid="pivot-table-empty" className={styles.emptyState}>
        No data to display. Configure rows, columns, and values.
      </div>
    );
  }

  const numGroupingDims = useSubtotals ? config.rows.length - 1 : 0;
  const grpContext: GroupContext | undefined = useSubtotals
    ? {
        onToggleGroup: onCollapseChange ? handleToggleGroup : undefined,
        collapsedSet,
        subtotalsEnabled: true,
        numGroupingDims,
      }
    : undefined;

  const renderBody = () => {
    if (projectedRowEntries) {
      let prevVisibleKey: string[] | null = null;
      let visibleIdx = 0;
      return projectedRowEntries.map((entry, idx) => {
        if (entry.type === "subtotal") {
          visibleIdx = 0;
          return renderSubtotalRow(
            entry.key,
            entry.level,
            effectiveColSlots,
            pivotData,
            config,
            hasMultipleValues,
            collapsedSet.has(makeKeyString(entry.key)),
            onCollapseChange ? handleToggleGroup : undefined,
            undefined,
            onCellClick,
            onCellClick ? handleCellKeyDown : undefined,
            projectedRowSpans ? projectedRowSpans[idx] : undefined,
            {
              projectedEntry: entry,
              rowHeaderLevels,
              onTemporalToggle: handleTemporalRowToggle,
            },
          );
        }
        let boundaryLevel: number | undefined;
        if (prevVisibleKey) {
          for (
            let d = 0;
            d <
            Math.min(numGroupingDims, entry.key.length, prevVisibleKey.length);
            d++
          ) {
            if (entry.key[d] !== prevVisibleKey[d]) {
              boundaryLevel = d;
              if (d === 0) visibleIdx = 0;
              break;
            }
          }
        }
        const isEven = visibleIdx % 2 === 1;
        visibleIdx++;
        prevVisibleKey = entry.key;
        if (entry.type === "temporal_parent") {
          return renderTemporalParentRow(
            entry,
            effectiveColSlots,
            pivotData,
            config,
            hasMultipleValues,
            rowHeaderLevels,
            handleTemporalRowToggle,
            undefined,
            projectedRowSpans ? projectedRowSpans[idx] : undefined,
          );
        }
        return renderDataRow(
          entry.key,
          effectiveColSlots,
          pivotData,
          config,
          hasMultipleValues,
          onCellClick,
          onCellClick ? handleCellKeyDown : undefined,
          undefined,
          projectedRowSpans ? projectedRowSpans[idx] : undefined,
          boundaryLevel,
          grpContext,
          isEven,
          {
            projectedEntry: entry,
            rowHeaderLevels,
            onTemporalToggle: handleTemporalRowToggle,
          },
        );
      });
    }

    if (groupedRows) {
      const sliced =
        maxRows != null ? groupedRows.slice(0, maxRows) : groupedRows;
      let prevDataKey: string[] | null = null;
      let groupDataIdx = 0;
      return sliced.map((entry, idx) => {
        if (entry.type === "subtotal") {
          groupDataIdx = 0;
          return renderSubtotalRow(
            entry.key,
            entry.level,
            effectiveColSlots,
            pivotData,
            config,
            hasMultipleValues,
            collapsedSet.has(makeKeyString(entry.key)),
            onCollapseChange ? handleToggleGroup : undefined,
            undefined,
            onCellClick,
            onCellClick ? handleCellKeyDown : undefined,
            groupedRowSpans?.get(idx),
          );
        }
        let boundaryLevel: number | undefined;
        if (prevDataKey) {
          for (let d = 0; d < numGroupingDims; d++) {
            if (entry.key[d] !== prevDataKey[d]) {
              boundaryLevel = d;
              if (d === 0) groupDataIdx = 0;
              break;
            }
          }
        }
        const isEven = groupDataIdx % 2 === 1;
        groupDataIdx++;
        prevDataKey = entry.key;
        return renderDataRow(
          entry.key,
          effectiveColSlots,
          pivotData,
          config,
          hasMultipleValues,
          onCellClick,
          onCellClick ? handleCellKeyDown : undefined,
          undefined,
          groupedRowSpans?.get(idx),
          boundaryLevel,
          grpContext,
          isEven,
        );
      });
    }

    return flatRowKeys!.map((rowKey, rowIdx) =>
      renderDataRow(
        rowKey,
        effectiveColSlots,
        pivotData,
        config,
        hasMultipleValues,
        onCellClick,
        onCellClick ? handleCellKeyDown : undefined,
        undefined,
        rowSpans ? rowSpans[rowIdx] : undefined,
        undefined,
        undefined,
        rowIdx % 2 === 1,
      ),
    );
  };

  return (
    <>
      {isResizing && (
        <div
          data-testid="resize-overlay"
          style={{
            position: "fixed",
            inset: 0,
            zIndex: 9999,
            cursor: "col-resize",
            opacity: 0,
          }}
        />
      )}
      <div
        ref={wrapperRef}
        className={styles.tableWrapper}
        style={{
          ...(scrollable
            ? { flex: "1 1 0", minHeight: 0 }
            : maxHeight != null
              ? { maxHeight }
              : undefined),
          ...(isResizing ? { userSelect: "none" } : undefined),
        }}
      >
        <table
          data-testid="pivot-table"
          className={`${styles.pivotTable} ${config.sticky_headers === false ? styles.noSticky : ""}`}
          role="grid"
        >
          <thead ref={theadRef}>
            {renderColumnHeaders(
              effectiveColSlots,
              config,
              effectiveNumRowDims,
              hasMultipleValues,
              undefined,
              hasHeaderMenu ? handleOpenMenu : undefined,
              menuTarget?.dimension,
              numColDims >= 2 && onCollapseChange
                ? handleToggleColGroup
                : undefined,
              pivotData,
              onCollapseChange,
              handleResizeMouseDown,
              columnWidthMap,
              headerRowOffsets.length > 1 ? headerRowOffsets : undefined,
              handleResizeDoubleClick,
              adaptiveDateGrains,
              temporalInfos.length > 0 ? temporalInfos : undefined,
              temporalInfos.length > 0 ? headerLevels : undefined,
              handleTemporalToggle,
              columnTypes,
              rowTemporalInfos.length > 0 ? rowHeaderLevels : undefined,
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
                effectiveColSlots,
                pivotData,
                config,
                effectiveNumRowDims,
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
            title={menuTitle}
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
            formatLabel={menuFormatLabel}
            dateGrain={menuDateGrain}
            onDateGrainChange={menuOnDateGrainChange}
            onDateDrill={menuOnDateDrill}
            supportsPeriodComparison={menuSupportsPeriodComparison}
            onClose={handleCloseMenu}
          />
        </div>
      )}
    </>
  );
};

export default TableRenderer;
