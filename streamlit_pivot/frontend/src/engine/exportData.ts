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

import type { PivotData, GroupedRow } from "./PivotData";
import { getValueFieldForRowKey } from "./PivotData";
import type {
  ColumnTypeMap,
  DateGrain,
  PivotConfigV1,
  ShowValuesAs,
  AnyConditionalFormatRule,
} from "./types";
import {
  getEffectiveDateGrain,
  getDimensionLabel,
  getPeriodComparisonMode,
  getRenderedValueFields,
  getRenderedValueLabel,
  getSyntheticMeasureFormat,
  isValuesOnRows,
  isSyntheticMeasure,
  showRowTotals,
  showColumnTotals,
  showTotalForMeasure,
} from "./types";
import { formatWithPattern, formatPercent, normalizeToUTC } from "./formatters";
import { formatTemporalParentLabel } from "./dateGrouping";
import {
  applyTemporalRowCollapse,
  computeRowHeaderLevels,
  computeTemporalRowInfos,
  projectVisibleRowEntries,
  type ProjectedRowEntry,
  type VisibleRowEntry,
} from "../renderers/temporalHierarchy";
import {
  getRunningTotal,
  getPctRunningTotal,
  getRank,
  getPctOfParent,
  getIndex,
} from "./showValuesAs";

export type ExportFormat = "csv" | "tsv" | "clipboard" | "xlsx";
export type ExportContent = "formatted" | "raw";

/** Strip floating-point noise (e.g. 24758.289999999997 → "24758.29"). */
function cleanNumber(v: number): string {
  return parseFloat(v.toPrecision(12)).toString();
}

export interface ExportOptions {
  format: ExportFormat;
  content: ExportContent;
}

// ---------------------------------------------------------------------------
// Shared intermediate representation
// ---------------------------------------------------------------------------

export type CellKind =
  | "header"
  | "data"
  | "subtotal"
  | "row-total"
  | "col-total"
  | "grand-total"
  | "empty";

export interface ExportCell {
  /** Formatted display string (consumed by CSV/TSV). */
  display: string;
  /** Raw value for Excel: number, Date (temporal dims), or null (labels/empty). */
  raw: number | Date | null;
  /** Semantic type driving Excel styling. */
  kind: CellKind;
  /** Merge rightward N additional cells (column headers). */
  mergeRight?: number;
  /** Merge downward N additional cells (row dimension labels when repeat_row_labels=false). */
  mergeDown?: number;
  /** Excel-compatible number format code (e.g. "#,##0.00", "0.0%"). */
  numberFormat?: string;
}

/** Column range for a value field in the export grid (0-based). */
export interface ValueFieldColumns {
  field: string;
  columns: number[];
}

export interface ExportGrid {
  /** 2D grid of typed cells. */
  cells: ExportCell[][];
  /** Number of leading rows that are headers (for freeze panes). */
  headerRowCount: number;
  /** Number of leading columns that are row dimensions (for freeze panes). */
  rowDimCount: number;
  /** Mapping of value fields to their column indices (for conditional formatting). */
  valueFieldColumns?: ValueFieldColumns[];
  /** Conditional formatting rules from the pivot config. */
  conditionalFormatting?: AnyConditionalFormatRule[];
}

const HIERARCHY_EXPORT_INDENT = "  ";

function indentHierarchyLabel(label: string, depth: number): string {
  return `${HIERARCHY_EXPORT_INDENT.repeat(Math.max(depth, 0))}${label}`;
}

function getDeepestVisibleProjectedIndex(entry: ProjectedRowEntry): number {
  for (let idx = entry.headerValues.length - 1; idx >= 0; idx--) {
    if (!entry.headerVisible[idx] || entry.headerSpacer[idx]) continue;
    return idx;
  }
  return -1;
}

// ---------------------------------------------------------------------------
// Number-format pattern → Excel format code translation
// ---------------------------------------------------------------------------

/**
 * Translate internal format patterns (e.g. "$,.2f", ".1%") to Excel format
 * codes (e.g. "$#,##0.00", "0.0%").
 */
export function patternToExcelFormat(pattern: string): string | undefined {
  let cursor = 0;
  let prefix = "";

  const firstChar = pattern[cursor];
  const currencySymbols: Record<string, string> = {
    $: "$",
    "€": "€",
    "£": "£",
    "¥": "¥",
  };
  if (firstChar && firstChar in currencySymbols) {
    prefix = currencySymbols[firstChar]!;
    cursor++;
    const codeMatch = pattern.slice(cursor).match(/^([A-Z]{3})/);
    if (codeMatch) cursor += 3;
  }

  const useGrouping = pattern[cursor] === ",";
  if (useGrouping) cursor++;

  const rest = pattern.slice(cursor);
  const match = rest.match(/^\.(\d+)([f%])$/);
  if (!match) return undefined;

  const decimals = parseInt(match[1], 10);
  const isPercent = match[2] === "%";

  if (isPercent) {
    const decPart = decimals > 0 ? "." + "0".repeat(decimals) : "";
    return `0${decPart}%`;
  }

  const intPart = useGrouping ? "#,##0" : "0";
  const decPart = decimals > 0 ? "." + "0".repeat(decimals) : "";
  return `${prefix}${intPart}${decPart}`;
}

// ---------------------------------------------------------------------------
// Formatting helpers (produce display string + resolve Excel format code)
// ---------------------------------------------------------------------------

interface FormatResult {
  display: string;
  raw: number | null;
  numberFormat?: string;
}

function formatComparisonResult(
  comparisonValue: number | null,
  valField: string,
  config: PivotConfigV1,
  mode: ShowValuesAs,
): FormatResult {
  if (comparisonValue === null) {
    return { display: config.empty_cell_value, raw: null };
  }
  if (mode === "pct_diff_from_prev" || mode === "pct_diff_from_prev_year") {
    return {
      display: formatPercent(comparisonValue),
      raw: comparisonValue,
      numberFormat: "0.0%",
    };
  }
  const pattern =
    getSyntheticMeasureFormat(config, valField) ??
    config.number_format?.[valField] ??
    config.number_format?.["__all__"];
  if (pattern) {
    return {
      display: formatWithPattern(comparisonValue, pattern),
      raw: comparisonValue,
      numberFormat: patternToExcelFormat(pattern),
    };
  }
  return { display: cleanNumber(comparisonValue), raw: comparisonValue };
}

function formatExportValue(
  rawValue: number | null,
  valField: string,
  config: PivotConfigV1,
  pivotData: PivotData,
  rowKey: string[],
  colKey: string[],
  mode: ExportContent,
): FormatResult {
  if (rawValue === null) {
    return {
      display: mode === "formatted" ? config.empty_cell_value : "",
      raw: null,
    };
  }

  if (mode === "raw") return { display: cleanNumber(rawValue), raw: rawValue };

  const showAs: ShowValuesAs | undefined = isSyntheticMeasure(config, valField)
    ? undefined
    : config.show_values_as?.[valField];
  const comparisonMode = getPeriodComparisonMode(config, valField);
  if (comparisonMode) {
    return formatComparisonResult(
      pivotData.getCellComparisonValue(
        rowKey,
        colKey,
        valField,
        comparisonMode,
      ),
      valField,
      config,
      comparisonMode,
    );
  }
  if (showAs && showAs !== "raw") {
    // ── Existing percentage modes ────────────────────────────────────────────
    if (
      showAs === "pct_of_total" ||
      showAs === "pct_of_row" ||
      showAs === "pct_of_col"
    ) {
      let denominator: number | null = null;
      if (showAs === "pct_of_total") {
        denominator = pivotData.getGrandTotal(valField).value();
      } else if (showAs === "pct_of_row") {
        denominator = pivotData.getRowTotal(rowKey, valField).value();
      } else if (showAs === "pct_of_col") {
        denominator = pivotData.getColTotal(colKey, valField).value();
      }
      if (denominator != null && denominator !== 0) {
        const pct = rawValue / denominator;
        return { display: formatPercent(pct), raw: pct, numberFormat: "0.0%" };
      }
      return { display: config.empty_cell_value, raw: null };
    }

    // ── 0.5.0 analytical modes ───────────────────────────────────────────────
    if (showAs === "running_total") {
      const rt = getRunningTotal(pivotData, rowKey, colKey, valField);
      if (rt === null) return { display: config.empty_cell_value, raw: null };
      const pattern =
        getSyntheticMeasureFormat(config, valField) ??
        config.number_format?.[valField] ??
        config.number_format?.["__all__"];
      if (pattern) {
        return {
          display: formatWithPattern(rt, pattern),
          raw: rt,
          numberFormat: patternToExcelFormat(pattern),
        };
      }
      return { display: cleanNumber(rt), raw: rt };
    }
    if (showAs === "pct_running_total") {
      const pct = getPctRunningTotal(pivotData, rowKey, colKey, valField);
      if (pct === null) return { display: config.empty_cell_value, raw: null };
      return { display: formatPercent(pct), raw: pct, numberFormat: "0.0%" };
    }
    if (showAs === "rank") {
      const r = getRank(pivotData, rowKey, colKey, valField);
      if (r === null) return { display: config.empty_cell_value, raw: null };
      return { display: String(r), raw: r };
    }
    if (showAs === "pct_of_parent") {
      const pct = getPctOfParent(rawValue, pivotData, rowKey, colKey, valField);
      if (pct === null) return { display: config.empty_cell_value, raw: null };
      return { display: formatPercent(pct), raw: pct, numberFormat: "0.0%" };
    }
    if (showAs === "index") {
      const idx = getIndex(rawValue, pivotData, rowKey, colKey, valField);
      if (idx === null) return { display: config.empty_cell_value, raw: null };
      return { display: cleanNumber(idx), raw: idx };
    }
  }

  const pattern =
    getSyntheticMeasureFormat(config, valField) ??
    config.number_format?.[valField] ??
    config.number_format?.["__all__"];
  if (pattern) {
    return {
      display: formatWithPattern(rawValue, pattern),
      raw: rawValue,
      numberFormat: patternToExcelFormat(pattern),
    };
  }

  const agg = pivotData.getAggregator(rowKey, colKey, valField);
  return { display: agg.format(config.empty_cell_value), raw: rawValue };
}

function formatExportTotalValue(
  rawValue: number | null,
  valField: string,
  config: PivotConfigV1,
  pivotData: PivotData,
  mode: ExportContent,
  isTotalOfShowAsAxis: "row" | "col" | "grand" | null,
  agg: { format: (empty: string) => string },
  comparisonValue?: number | null,
  showAsDenominators?: { row?: number | null; col?: number | null },
): FormatResult {
  if (rawValue === null) {
    return {
      display: mode === "formatted" ? config.empty_cell_value : "",
      raw: null,
    };
  }
  if (mode === "raw") return { display: cleanNumber(rawValue), raw: rawValue };

  const showAs = isSyntheticMeasure(config, valField)
    ? undefined
    : config.show_values_as?.[valField];
  const comparisonMode = getPeriodComparisonMode(config, valField);
  if (comparisonMode) {
    return formatComparisonResult(
      comparisonValue ?? null,
      valField,
      config,
      comparisonMode,
    );
  }
  if (showAs && showAs !== "raw") {
    // ── 0.5.0 analytical modes — totals export raw aggregate ─────────────────
    if (
      showAs === "running_total" ||
      showAs === "pct_running_total" ||
      showAs === "rank"
    ) {
      // Fall through to raw formatting below
    } else if (showAs === "pct_of_parent") {
      if (isTotalOfShowAsAxis === "grand")
        return { display: config.empty_cell_value, raw: null };
      const denom = showAsDenominators?.row;
      if (denom != null && denom !== 0) {
        const pct = rawValue / denom;
        return { display: formatPercent(pct), raw: pct, numberFormat: "0.0%" };
      }
      return { display: config.empty_cell_value, raw: null };
    } else if (showAs === "index") {
      return { display: config.empty_cell_value, raw: null };
    } else {
      // ── Existing percentage modes ──────────────────────────────────────────
      if (isTotalOfShowAsAxis === "row" && showAs === "pct_of_row")
        return { display: formatPercent(1), raw: 1, numberFormat: "0.0%" };
      if (isTotalOfShowAsAxis === "col" && showAs === "pct_of_col")
        return { display: formatPercent(1), raw: 1, numberFormat: "0.0%" };
      if (isTotalOfShowAsAxis === "grand")
        return { display: formatPercent(1), raw: 1, numberFormat: "0.0%" };
      if (showAs === "pct_of_total") {
        const grand = pivotData.getGrandTotal(valField).value();
        if (grand) {
          const pct = rawValue / grand;
          return {
            display: formatPercent(pct),
            raw: pct,
            numberFormat: "0.0%",
          };
        }
        return { display: config.empty_cell_value, raw: null };
      }
      if (showAs === "pct_of_row") {
        const denom = showAsDenominators?.row;
        if (denom != null && denom !== 0) {
          const pct = rawValue / denom;
          return {
            display: formatPercent(pct),
            raw: pct,
            numberFormat: "0.0%",
          };
        }
        return { display: config.empty_cell_value, raw: null };
      }
      if (showAs === "pct_of_col") {
        const denom = showAsDenominators?.col;
        if (denom != null && denom !== 0) {
          const pct = rawValue / denom;
          return {
            display: formatPercent(pct),
            raw: pct,
            numberFormat: "0.0%",
          };
        }
        return { display: config.empty_cell_value, raw: null };
      }
    } // end else (existing percentage modes)
  } // end if (showAs && showAs !== "raw")

  const pattern =
    getSyntheticMeasureFormat(config, valField) ??
    config.number_format?.[valField] ??
    config.number_format?.["__all__"];
  if (pattern) {
    return {
      display: formatWithPattern(rawValue, pattern),
      raw: rawValue,
      numberFormat: patternToExcelFormat(pattern),
    };
  }

  return { display: agg.format(config.empty_cell_value), raw: rawValue };
}

/**
 * Convert a raw dimension value to a Date object for typed Excel export.
 * Handles epoch ms (number), ISO strings (from Utf8 object columns), and Date objects.
 */
function toExportDate(raw: unknown): Date | null {
  if (raw instanceof Date) return raw;
  if (typeof raw === "number" && isFinite(raw)) return new Date(raw);
  if (typeof raw === "string") {
    const d = new Date(normalizeToUTC(raw));
    if (!isNaN(d.getTime())) return d;
  }
  return null;
}

// ---------------------------------------------------------------------------
// Shared IR builder
// ---------------------------------------------------------------------------

function cell(
  display: string,
  kind: CellKind,
  raw: number | Date | null = null,
  extra?: Partial<
    Pick<ExportCell, "mergeRight" | "mergeDown" | "numberFormat">
  >,
): ExportCell {
  const c: ExportCell = { display, raw, kind };
  if (extra?.mergeRight) c.mergeRight = extra.mergeRight;
  if (extra?.mergeDown) c.mergeDown = extra.mergeDown;
  if (extra?.numberFormat) c.numberFormat = extra.numberFormat;
  return c;
}

function buildHierarchyExportLabel(
  entry: VisibleRowEntry | ProjectedRowEntry,
  config: PivotConfigV1,
  pivotData: PivotData,
  rowHeaderLevels?: ReturnType<typeof computeRowHeaderLevels>,
  columnTypes?: ColumnTypeMap,
  adaptiveDateGrains?: Record<string, DateGrain>,
): { display: string; kind: CellKind; raw: Date | null } {
  if ("headerValues" in entry && rowHeaderLevels) {
    const idx = getDeepestVisibleProjectedIndex(entry);
    if (idx >= 0) {
      const mapping = rowHeaderLevels[idx];
      const value = entry.headerValues[idx] ?? "";
      const temporalPattern =
        config.dimension_format?.[mapping?.field ?? ""] ??
        config.dimension_format?.["__all__"];
      const formatted =
        value !== ""
          ? mapping?.isTemporal
            ? formatTemporalParentLabel(value, mapping.grain, temporalPattern)
            : pivotData.formatDimLabel(mapping?.field ?? "", value)
          : "(empty)";
      const label = formatted;
      return {
        display: indentHierarchyLabel(label, idx),
        kind: entry.type === "data" ? "data" : "subtotal",
        raw: null,
      };
    }
  }

  if (entry.type === "subtotal") {
    const depth = entry.level;
    const dimName = config.rows[depth] ?? "";
    const rawLabel = entry.key[depth] ?? "";
    const label = rawLabel
      ? pivotData.formatDimLabel(dimName, rawLabel)
      : "(empty)";
    return {
      display: indentHierarchyLabel(label, depth),
      kind: "subtotal",
      raw: null,
    };
  }

  // When values_axis="rows", the key has a trailing "__vf__:<field>" segment;
  // strip it so depth and dimKey refer to the actual dimension, not the pseudo-dim.
  const encodedVf = getValueFieldForRowKey(entry.key);
  const rowKey = encodedVf !== null ? entry.key.slice(0, -1) : entry.key;
  const depth = Math.max(rowKey.length - 1, 0);
  const dimName = config.rows[depth] ?? "";
  const dimKey = rowKey[depth] ?? "";
  const display = dimKey
    ? pivotData.formatDimLabel(dimName, dimKey)
    : "(empty)";
  const colType = columnTypes?.get(dimName);
  if (colType === "datetime" || colType === "date") {
    const grain = getEffectiveDateGrain(
      config,
      dimName,
      colType,
      adaptiveDateGrains?.[dimName],
    );
    const rawVal = pivotData.getRawDimValue(dimName, dimKey);
    const exportDate =
      !grain && rawVal !== undefined ? toExportDate(rawVal) : null;
    return {
      display: indentHierarchyLabel(display, depth),
      kind: "data",
      raw: exportDate,
    };
  }
  return {
    display: indentHierarchyLabel(display, depth),
    kind: "data",
    raw: null,
  };
}

/**
 * Walk the PivotData structure and produce a rich intermediate representation
 * with typed cells, raw values, merge spans, and number format codes.
 *
 * Export always uses the full (expanded) column and row keys, regardless
 * of any collapsed_groups / collapsed_col_groups / collapsed_temporal_groups /
 * collapsed_temporal_row_groups
 * display state. This is intentional — export provides complete data, not
 * a visual screenshot.
 */
export function buildExportIR(
  pivotData: PivotData,
  config: PivotConfigV1,
  mode: ExportContent,
  adaptiveDateGrains?: Record<string, DateGrain>,
): ExportGrid {
  const columnTypes: ColumnTypeMap | undefined = pivotData.getColumnTypes();
  const rowKeys = pivotData.getRowKeys();
  const colKeys = pivotData.getColKeys();
  const values = getRenderedValueFields(config);
  const hasMultipleValues = values.length > 1;
  const includeRowTotals = showRowTotals(config);
  const includeColTotals = showColumnTotals(config);
  const rowTotalValues = values.filter((valField) =>
    showTotalForMeasure(config, valField, "row"),
  );
  const includeVisibleRowTotals = includeRowTotals && rowTotalValues.length > 0;
  const rowDims = config.rows;
  const colDims = config.columns;
  const hierarchyMode = config.row_layout === "hierarchy";
  const valuesOnRows = isValuesOnRows(config);
  // When rows=[] and values_axis="rows", the value-label IS the only leading
  // column — no blank placeholder. So skip the +1 in that specific combination.
  const numRowDimCols = hierarchyMode
    ? valuesOnRows
      ? 2
      : 1
    : Math.max(rowDims.length, 1) +
      (valuesOnRows && rowDims.length > 0 ? 1 : 0);
  // When values_axis="rows", each colKey maps to 1 data column (not values.length).
  const colsPerKey = valuesOnRows ? 1 : hasMultipleValues ? values.length : 1;

  const grid: ExportCell[][] = [];
  let headerRowCount = 0;

  // --- Column header rows ---
  for (let level = 0; level < colDims.length; level++) {
    const row: ExportCell[] = [];
    if (level === 0) {
      if (hierarchyMode) {
        // For values_axis="rows" + hierarchy, the layout has two leading columns:
        // col 0 = value-field label ("Values"), col 1 = hierarchy row header.
        // This matches the renderer which prepends the "Values" header first.
        if (valuesOnRows) row.push(cell("Values", "header"));
        row.push(
          cell(
            rowDims.length === 1
              ? getDimensionLabel(
                  config,
                  rowDims[0]!,
                  columnTypes?.get(rowDims[0]!),
                  adaptiveDateGrains?.[rowDims[0]!],
                )
              : rowDims.length > 1
                ? "Rows"
                : "",
            "header",
          ),
        );
      } else {
        // "Values" column header is always first when value fields are on the row
        // axis (matching the renderer which prepends it before dimension headers).
        if (valuesOnRows) row.push(cell("Values", "header"));
        for (let d = 0; d < rowDims.length; d++) {
          row.push(
            cell(
              getDimensionLabel(
                config,
                rowDims[d]!,
                columnTypes?.get(rowDims[d]!),
                adaptiveDateGrains?.[rowDims[d]!],
              ),
              "header",
            ),
          );
        }
        // When rows=[] and !valuesOnRows, add a blank placeholder so widths match.
        if (rowDims.length === 0 && !valuesOnRows) row.push(cell("", "header"));
      }
    } else {
      for (let d = 0; d < numRowDimCols; d++) {
        row.push(cell("", "header"));
      }
    }

    for (const colKey of colKeys) {
      const rawLabel = colKey[level] ?? "";
      const dimName = colDims[level] ?? "";
      const label = rawLabel ? pivotData.formatDimLabel(dimName, rawLabel) : "";
      if (hasMultipleValues) {
        row.push(cell(label, "header", null, { mergeRight: colsPerKey - 1 }));
        for (let v = 1; v < colsPerKey; v++) {
          row.push(cell(label, "header"));
        }
      } else {
        row.push(cell(label, "header"));
      }
    }
    if (includeVisibleRowTotals) {
      const label = level === 0 ? "Total" : "";
      // With values_axis="rows" each data row has exactly 1 row-total cell
      // (the measure is baked into the row, not the column). Use a single header
      // cell here too so header width matches data row width.
      if (hasMultipleValues && !valuesOnRows) {
        row.push(
          cell(label, "header", null, {
            mergeRight: rowTotalValues.length - 1,
          }),
        );
        for (let v = 1; v < rowTotalValues.length; v++) {
          row.push(cell(label, "header"));
        }
      } else {
        row.push(cell(label, "header"));
      }
    }
    grid.push(row);
    headerRowCount++;
  }

  // Value-label header row (not needed when values_axis="rows" — labels are in row headers)
  if (hasMultipleValues && !valuesOnRows) {
    const row: ExportCell[] = [];
    for (let d = 0; d < numRowDimCols; d++) {
      row.push(cell("", "header"));
    }
    for (const _colKey of colKeys) {
      for (const val of values) {
        row.push(cell(getRenderedValueLabel(config, val), "header"));
      }
    }
    if (includeVisibleRowTotals) {
      for (const val of rowTotalValues) {
        row.push(cell(getRenderedValueLabel(config, val), "header"));
      }
    }
    grid.push(row);
    headerRowCount++;
  }

  // No column dimensions: single header row
  if (colDims.length === 0) {
    const row: ExportCell[] = [];
    // "Values" column is always first when values are on the row axis.
    if (valuesOnRows) row.push(cell("Values", "header"));
    if (hierarchyMode) {
      row.push(
        cell(
          rowDims.length === 1
            ? getDimensionLabel(
                config,
                rowDims[0]!,
                columnTypes?.get(rowDims[0]!),
                adaptiveDateGrains?.[rowDims[0]!],
              )
            : rowDims.length > 1
              ? "Rows"
              : "",
          "header",
        ),
      );
    } else {
      // Emit one named header per actual row dimension (same count as data rows).
      for (let d = 0; d < rowDims.length; d++) {
        row.push(
          cell(
            getDimensionLabel(
              config,
              rowDims[d]!,
              columnTypes?.get(rowDims[d]!),
              adaptiveDateGrains?.[rowDims[d]!],
            ),
            "header",
          ),
        );
      }
      // When there are no row dims and not valuesOnRows, add blank placeholder.
      if (rowDims.length === 0 && !valuesOnRows) row.push(cell("", "header"));
    }
    // Single numeric column header ("Value") — "Values" label was already emitted above.
    if (valuesOnRows) {
      row.push(cell("Value", "header")); // single numeric data column
    } else if (hasMultipleValues) {
      for (const val of values) {
        row.push(cell(getRenderedValueLabel(config, val), "header"));
      }
    } else {
      row.push(
        cell(
          getRenderedValueLabel(config, values[0] ?? "") || "Value",
          "header",
        ),
      );
    }
    if (includeVisibleRowTotals) {
      row.push(cell("Total", "header"));
    }
    grid.push(row);
    headerRowCount++;
  }

  // --- Data rows ---
  const baseGroupedRows: GroupedRow[] =
    config.show_subtotals || hierarchyMode
      ? hierarchyMode
        ? pivotData.getHierarchyRowKeys(true)
        : pivotData.getGroupedRowKeys(true)
      : rowKeys.map((key) => ({ type: "data" as const, key, level: 0 }));
  const rowTemporalInfos =
    hierarchyMode && rowDims.length > 0
      ? computeTemporalRowInfos(config, columnTypes, adaptiveDateGrains)
      : [];
  const rowHeaderLevels =
    hierarchyMode && rowTemporalInfos.length > 0
      ? computeRowHeaderLevels(config, rowTemporalInfos)
      : undefined;
  const rowEntries: Array<GroupedRow | ProjectedRowEntry> =
    hierarchyMode && rowTemporalInfos.length > 0
      ? projectVisibleRowEntries(
          applyTemporalRowCollapse(baseGroupedRows, rowTemporalInfos, config),
          config,
          rowHeaderLevels!,
          rowTemporalInfos,
        )
      : baseGroupedRows;

  for (const groupedRow of rowEntries) {
    const row: ExportCell[] = [];

    if (hierarchyMode) {
      const labelInfo = buildHierarchyExportLabel(
        groupedRow,
        config,
        pivotData,
        rowHeaderLevels,
        columnTypes,
        adaptiveDateGrains,
      );
      // For values_axis="rows" + hierarchy, "Values" column is first (col 0)
      // and the hierarchy row header comes second (col 1), matching the renderer.
      if (valuesOnRows) {
        const encodedVf = getValueFieldForRowKey(groupedRow.key);
        const rowValField = encodedVf ?? values[0] ?? "";
        const cellKind =
          groupedRow.type === "subtotal"
            ? ("subtotal" as const)
            : ("data" as const);
        row.push(cell(getRenderedValueLabel(config, rowValField), cellKind));
      }
      row.push(cell(labelInfo.display, labelInfo.kind, labelInfo.raw));
    }

    if (groupedRow.type === "subtotal") {
      // When values_axis="rows", the key has an encoded __vf__ segment; strip it.
      const subtotalEncodedField = valuesOnRows
        ? getValueFieldForRowKey(groupedRow.key)
        : null;
      const dimSubtotalKey =
        subtotalEncodedField !== null
          ? groupedRow.key.slice(0, -1)
          : groupedRow.key;
      const subtotalValField = subtotalEncodedField ?? values[0] ?? "";
      if (!hierarchyMode) {
        // "Values" column (value-field label) is always first, matching the renderer.
        if (valuesOnRows) {
          row.push(
            cell(getRenderedValueLabel(config, subtotalValField), "subtotal"),
          );
        }
        const dimCols = numRowDimCols - (valuesOnRows ? 1 : 0);
        for (let d = 0; d < dimCols; d++) {
          if (d < dimSubtotalKey.length) {
            const sk = dimSubtotalKey[d];
            const sdn = rowDims[d] ?? "";
            const sdisplay = sk ? pivotData.formatDimLabel(sdn, sk) : "";
            row.push(cell(sdisplay, "subtotal"));
          } else if (d === dimSubtotalKey.length) {
            row.push(cell("Subtotal", "subtotal"));
          } else {
            row.push(cell("", "subtotal"));
          }
        }
      }
      for (const colKey of colKeys) {
        const subtotalIterValues = valuesOnRows ? [subtotalValField] : values;
        for (const valField of subtotalIterValues) {
          const agg = pivotData.getSubtotalAggregator(
            dimSubtotalKey,
            colKey,
            valField,
          );
          const rawValue = agg.value();
          const comparisonMode = getPeriodComparisonMode(config, valField);
          const fmt = formatExportTotalValue(
            rawValue,
            valField,
            config,
            pivotData,
            mode,
            null,
            agg,
            comparisonMode
              ? pivotData.getSubtotalComparisonValue(
                  dimSubtotalKey,
                  colKey,
                  valField,
                  comparisonMode,
                )
              : undefined,
            {
              row: pivotData
                .getSubtotalAggregator(dimSubtotalKey, [], valField)
                .value(),
              col: pivotData.getColTotal(colKey, valField).value(),
            },
          );
          row.push(
            cell(fmt.display, "subtotal", fmt.raw, {
              numberFormat: fmt.numberFormat,
            }),
          );
        }
      }
      if (includeVisibleRowTotals) {
        const subtotalRowTotalValues = valuesOnRows
          ? [subtotalValField]
          : rowTotalValues;
        for (const valField of subtotalRowTotalValues) {
          const agg = pivotData.getSubtotalAggregator(
            dimSubtotalKey,
            [],
            valField,
          );
          const rawValue = agg.value();
          const comparisonMode = valuesOnRows
            ? undefined
            : getPeriodComparisonMode(config, valField);
          const fmt = formatExportTotalValue(
            rawValue,
            valField,
            config,
            pivotData,
            mode,
            "row",
            agg,
            comparisonMode
              ? pivotData.getSubtotalComparisonValue(
                  dimSubtotalKey,
                  [],
                  valField,
                  comparisonMode,
                )
              : undefined,
          );
          row.push(
            cell(fmt.display, "subtotal", fmt.raw, {
              numberFormat: fmt.numberFormat,
            }),
          );
        }
      }
    } else if (groupedRow.type === "temporal_parent") {
      for (const colKey of colKeys) {
        for (const valField of values) {
          const agg = pivotData.getTemporalRowSubtotal(
            groupedRow.temporalParent.modifiedRowKey,
            colKey,
            valField,
          );
          const rawValue = agg.value();
          const fmt = formatExportTotalValue(
            rawValue,
            valField,
            config,
            pivotData,
            mode,
            null,
            agg,
            undefined,
            undefined,
          );
          row.push(
            cell(fmt.display, "subtotal", fmt.raw, {
              numberFormat: fmt.numberFormat,
            }),
          );
        }
      }
      if (includeVisibleRowTotals) {
        for (const valField of rowTotalValues) {
          const agg = pivotData.getTemporalRowSubtotalGrand(
            groupedRow.temporalParent.modifiedRowKey,
            valField,
          );
          const rawValue = agg.value();
          const fmt = formatExportTotalValue(
            rawValue,
            valField,
            config,
            pivotData,
            mode,
            "row",
            agg,
            undefined,
            undefined,
          );
          row.push(
            cell(fmt.display, "subtotal", fmt.raw, {
              numberFormat: fmt.numberFormat,
            }),
          );
        }
      }
    } else {
      const rowKey = groupedRow.key;
      // When values_axis="rows", strip the trailing __vf__ segment for lookups.
      const dataEncodedField = valuesOnRows
        ? getValueFieldForRowKey(rowKey)
        : null;
      const dimRowKey =
        dataEncodedField !== null ? rowKey.slice(0, -1) : rowKey;
      const dataValField = dataEncodedField ?? values[0] ?? "";
      if (!hierarchyMode) {
        // "Values" column (value-field label) is always first, matching the renderer.
        if (valuesOnRows) {
          row.push(cell(getRenderedValueLabel(config, dataValField), "data"));
        }
        const dimCols = numRowDimCols - (valuesOnRows ? 1 : 0);
        for (let d = 0; d < dimCols; d++) {
          const dimKey = dimRowKey[d] ?? "";
          const dimName = rowDims[d] ?? "";
          const display = dimKey
            ? pivotData.formatDimLabel(dimName, dimKey)
            : "";
          const colType = columnTypes?.get(dimName);
          if (colType === "datetime" || colType === "date") {
            const grain = getEffectiveDateGrain(
              config,
              dimName,
              colType,
              adaptiveDateGrains?.[dimName],
            );
            const rawVal = pivotData.getRawDimValue(dimName, dimKey);
            const exportDate =
              !grain && rawVal !== undefined ? toExportDate(rawVal) : null;
            row.push(
              cell(
                display,
                "data",
                exportDate,
                exportDate
                  ? {
                      numberFormat:
                        colType === "date" ? "yyyy-mm-dd" : "yyyy-mm-dd hh:mm",
                    }
                  : undefined,
              ),
            );
          } else {
            row.push(cell(display, "data"));
          }
        }
      }
      for (const colKey of colKeys) {
        const dataIterValues = valuesOnRows ? [dataValField] : values;
        for (const valField of dataIterValues) {
          const agg = pivotData.getAggregator(dimRowKey, colKey, valField);
          const rawValue = agg.value();
          const fmt = formatExportValue(
            rawValue,
            valField,
            config,
            pivotData,
            dimRowKey,
            colKey,
            mode,
          );
          row.push(
            cell(fmt.display, "data", fmt.raw, {
              numberFormat: fmt.numberFormat,
            }),
          );
        }
      }
      if (includeVisibleRowTotals) {
        const dataRowTotalValues = valuesOnRows
          ? [dataValField]
          : rowTotalValues;
        for (const valField of dataRowTotalValues) {
          const agg = pivotData.getRowTotal(dimRowKey, valField);
          const rawValue = agg.value();
          const comparisonMode = valuesOnRows
            ? undefined
            : getPeriodComparisonMode(config, valField);
          const fmt = formatExportTotalValue(
            rawValue,
            valField,
            config,
            pivotData,
            mode,
            "row",
            agg,
            comparisonMode
              ? pivotData.getRowTotalComparisonValue(
                  dimRowKey,
                  valField,
                  comparisonMode,
                )
              : undefined,
          );
          row.push(
            cell(fmt.display, "row-total", fmt.raw, {
              numberFormat: fmt.numberFormat,
            }),
          );
        }
      }
    }
    grid.push(row);
  }

  // --- Column totals row ---
  if (includeColTotals) {
    // For values_axis="rows", emit one totals row per value field.
    const totalRowsValueFields = valuesOnRows ? values : [null as null];
    for (const [
      totalRowIdx,
      totalValFieldOrNull,
    ] of totalRowsValueFields.entries()) {
      const isFirstTotalRow = totalRowIdx === 0;
      const row: ExportCell[] = [];
      // Leading cells mirror the data-row column order: Values first, then dims.
      if (valuesOnRows) {
        // col 0: value-field label (always present)
        row.push(
          cell(
            getRenderedValueLabel(
              config,
              totalValFieldOrNull ?? values[0] ?? "",
            ),
            "col-total",
          ),
        );
        // col 1..n: "Total" on first total row, blank on subsequent rows
        for (let d = 1; d < numRowDimCols; d++) {
          row.push(
            cell(d === 1 && isFirstTotalRow ? "Total" : "", "col-total"),
          );
        }
      } else {
        row.push(cell(isFirstTotalRow ? "Total" : "", "col-total"));
        for (let d = 1; d < numRowDimCols; d++) {
          row.push(cell("", "col-total"));
        }
      }
      const totalIterValues = valuesOnRows ? [totalValFieldOrNull!] : values;
      for (const colKey of colKeys) {
        for (const valField of totalIterValues) {
          if (!showTotalForMeasure(config, valField, "col")) {
            row.push(cell("", "col-total"));
          } else {
            const agg = pivotData.getColTotal(colKey, valField);
            const rawValue = agg.value();
            const comparisonMode = getPeriodComparisonMode(config, valField);
            const fmt = formatExportTotalValue(
              rawValue,
              valField,
              config,
              pivotData,
              mode,
              "col",
              agg,
              comparisonMode
                ? pivotData.getColTotalComparisonValue(
                    colKey,
                    valField,
                    comparisonMode,
                  )
                : undefined,
            );
            row.push(
              cell(fmt.display, "col-total", fmt.raw, {
                numberFormat: fmt.numberFormat,
              }),
            );
          }
        }
      } // end colKey loop
      if (includeVisibleRowTotals) {
        const totalGrandValues = valuesOnRows
          ? [totalValFieldOrNull!]
          : rowTotalValues;
        for (const valField of totalGrandValues) {
          if (!showTotalForMeasure(config, valField, "grand")) {
            row.push(cell("", "grand-total"));
          } else {
            const agg = pivotData.getGrandTotal(valField);
            const rawValue = agg.value();
            const fmt = formatExportTotalValue(
              rawValue,
              valField,
              config,
              pivotData,
              mode,
              "grand",
              agg,
              undefined,
            );
            row.push(
              cell(fmt.display, "grand-total", fmt.raw, {
                numberFormat: fmt.numberFormat,
              }),
            );
          }
        }
      }
      grid.push(row);
    } // end totalValFieldOrNull loop
  }

  // --- Row dimension merging (mergeDown) ---
  if (!hierarchyMode && !config.repeat_row_labels && rowDims.length > 0) {
    // When values_axis="rows", the "Values" label occupies column 0 and row
    // dimensions start at column 1. Offset all dim-column accesses accordingly.
    const dimColOffset = valuesOnRows ? 1 : 0;
    const dataStartRow = headerRowCount;
    for (let d = 0; d < rowDims.length; d++) {
      const col = dimColOffset + d;
      let spanStart = dataStartRow;
      for (let r = dataStartRow + 1; r <= grid.length; r++) {
        const atEnd = r === grid.length;
        const curVal = atEnd ? null : grid[r]![col]?.display;
        const startVal = grid[spanStart]![col]?.display;
        const curKind = atEnd ? null : grid[r]![col]?.kind;
        const startKind = grid[spanStart]![col]?.kind;
        if (
          atEnd ||
          curVal !== startVal ||
          curKind !== startKind ||
          curKind === "subtotal" ||
          curKind === "col-total"
        ) {
          const spanLen = r - spanStart;
          if (spanLen > 1 && startKind === "data") {
            grid[spanStart]![col]!.mergeDown = spanLen - 1;
          }
          spanStart = r;
        }
      }
    }
  }

  // Build value field → column index mapping for conditional formatting.
  // With values_axis="rows", measures share the same data columns and differ
  // by row — column-based per-field targeting is not meaningful, so we emit
  // an empty column list for every field to avoid misapplied formatting.
  const valueFieldCols: ValueFieldColumns[] = values.map((field, vIdx) => {
    if (valuesOnRows) return { field, columns: [] };
    const cols: number[] = [];
    for (let ck = 0; ck < colKeys.length; ck++) {
      cols.push(numRowDimCols + ck * colsPerKey + vIdx);
    }
    return { field, columns: cols };
  });

  const result: ExportGrid = {
    cells: grid,
    headerRowCount,
    rowDimCount: numRowDimCols,
    valueFieldColumns: valueFieldCols,
  };

  if (
    config.conditional_formatting &&
    config.conditional_formatting.length > 0
  ) {
    result.conditionalFormatting = config.conditional_formatting;
  }

  return result;
}

/**
 * Backward-compatible wrapper: builds the IR then extracts display strings.
 */
export function buildExportGrid(
  pivotData: PivotData,
  config: PivotConfigV1,
  mode: ExportContent,
  adaptiveDateGrains?: Record<string, DateGrain>,
): string[][] {
  const ir = buildExportIR(pivotData, config, mode, adaptiveDateGrains);
  return ir.cells.map((row) => row.map((c) => c.display));
}

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

function escapeCSVField(field: string): string {
  if (
    field.includes(",") ||
    field.includes('"') ||
    field.includes("\n") ||
    field.includes("\r")
  ) {
    return '"' + field.replace(/"/g, '""') + '"';
  }
  return field;
}

function escapeTSVField(field: string): string {
  if (
    field.includes("\t") ||
    field.includes('"') ||
    field.includes("\n") ||
    field.includes("\r")
  ) {
    return '"' + field.replace(/"/g, '""') + '"';
  }
  return field;
}

export function gridToCSV(grid: string[][]): string {
  return grid.map((row) => row.map(escapeCSVField).join(",")).join("\n");
}

export function gridToTSV(grid: string[][]): string {
  return grid.map((row) => row.map(escapeTSVField).join("\t")).join("\n");
}

// ---------------------------------------------------------------------------
// Export actions
// ---------------------------------------------------------------------------

export function downloadFile(
  content: string,
  filename: string,
  mimeType: string,
): boolean {
  try {
    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    return true;
  } catch {
    return false;
  }
}

export async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    return false;
  }
}

/**
 * Perform a full export: build the grid, serialize, and either download or copy.
 */
export async function exportPivotData(
  pivotData: PivotData,
  config: PivotConfigV1,
  options: ExportOptions,
  baseFilename?: string,
  adaptiveDateGrains?: Record<string, DateGrain>,
): Promise<boolean> {
  const now = new Date();
  const ts = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
  const name = `${baseFilename || "pivot-table"}_${ts}`;

  if (options.format === "xlsx") {
    const ir = buildExportIR(
      pivotData,
      config,
      options.content,
      adaptiveDateGrains,
    );
    const { exportExcel } = await import("./exportExcel");
    return exportExcel(ir, name);
  }

  const grid = buildExportGrid(
    pivotData,
    config,
    options.content,
    adaptiveDateGrains,
  );

  switch (options.format) {
    case "csv": {
      const csv = gridToCSV(grid);
      return downloadFile(csv, `${name}.csv`, "text/csv;charset=utf-8");
    }
    case "tsv": {
      const tsv = gridToTSV(grid);
      return downloadFile(
        tsv,
        `${name}.tsv`,
        "text/tab-separated-values;charset=utf-8",
      );
    }
    case "clipboard": {
      const tsv = gridToTSV(grid);
      return copyToClipboard(tsv);
    }
  }
}
