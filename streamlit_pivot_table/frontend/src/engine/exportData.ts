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

import type { PivotData } from "./PivotData";
import type { PivotConfigV1, ShowValuesAs } from "./types";
import {
  getRenderedValueFields,
  getRenderedValueLabel,
  getSyntheticMeasureFormat,
  isSyntheticMeasure,
  showRowTotals,
  showColumnTotals,
  showTotalForMeasure,
} from "./types";
import { formatWithPattern, formatPercent } from "./formatters";

export type ExportFormat = "csv" | "tsv" | "clipboard";
export type ExportContent = "formatted" | "raw";

/** Strip floating-point noise (e.g. 24758.289999999997 → "24758.29"). */
function cleanNumber(v: number): string {
  return parseFloat(v.toPrecision(12)).toString();
}

export interface ExportOptions {
  format: ExportFormat;
  content: ExportContent;
}

/**
 * Format a single cell value for export, mirroring the display logic in TableRenderer.
 */
function formatExportValue(
  rawValue: number | null,
  valField: string,
  config: PivotConfigV1,
  pivotData: PivotData,
  rowKey: string[],
  colKey: string[],
  mode: ExportContent,
): string {
  if (rawValue === null)
    return mode === "formatted" ? config.empty_cell_value : "";

  if (mode === "raw") return cleanNumber(rawValue);

  const showAs: ShowValuesAs | undefined = isSyntheticMeasure(config, valField)
    ? undefined
    : config.show_values_as?.[valField];
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
      return formatPercent(rawValue / denominator);
    }
    return config.empty_cell_value;
  }

  const pattern =
    getSyntheticMeasureFormat(config, valField) ??
    config.number_format?.[valField] ??
    config.number_format?.["__all__"];
  if (pattern) return formatWithPattern(rawValue, pattern);

  const agg = pivotData.getAggregator(rowKey, colKey, valField);
  return agg.format(config.empty_cell_value);
}

/**
 * Format a total cell value for export.
 * Receives the actual Aggregator so the fallback `.format()` call uses
 * the correct value (row total, col total, or grand total).
 */
function formatExportTotalValue(
  rawValue: number | null,
  valField: string,
  config: PivotConfigV1,
  pivotData: PivotData,
  mode: ExportContent,
  isTotalOfShowAsAxis: "row" | "col" | "grand" | null,
  agg: { format: (empty: string) => string },
): string {
  if (rawValue === null)
    return mode === "formatted" ? config.empty_cell_value : "";
  if (mode === "raw") return cleanNumber(rawValue);

  const showAs = isSyntheticMeasure(config, valField)
    ? undefined
    : config.show_values_as?.[valField];
  if (showAs && showAs !== "raw") {
    if (isTotalOfShowAsAxis === "row" && showAs === "pct_of_row")
      return formatPercent(1);
    if (isTotalOfShowAsAxis === "col" && showAs === "pct_of_col")
      return formatPercent(1);
    if (isTotalOfShowAsAxis === "grand") return formatPercent(1);
    if (showAs === "pct_of_total") {
      const grand = pivotData.getGrandTotal(valField).value();
      return grand ? formatPercent(rawValue / grand) : config.empty_cell_value;
    }
    if (showAs === "pct_of_row") return config.empty_cell_value;
    if (showAs === "pct_of_col") {
      const grand = pivotData.getGrandTotal(valField).value();
      return grand ? formatPercent(rawValue / grand) : config.empty_cell_value;
    }
  }

  const pattern =
    getSyntheticMeasureFormat(config, valField) ??
    config.number_format?.[valField] ??
    config.number_format?.["__all__"];
  if (pattern) return formatWithPattern(rawValue, pattern);

  return agg.format(config.empty_cell_value);
}

/**
 * Walk the PivotData structure and produce a 2D string array representing
 * the full pivot table (headers + data + totals).
 *
 * Note: Export always uses the full (expanded) column and row keys, regardless
 * of any collapsed_groups / collapsed_col_groups display state. This is
 * intentional — export provides complete data, not a visual screenshot.
 */
export function buildExportGrid(
  pivotData: PivotData,
  config: PivotConfigV1,
  mode: ExportContent,
): string[][] {
  const rowKeys = pivotData.getRowKeys();
  const colKeys = pivotData.getColKeys();
  const values = getRenderedValueFields(config);
  const hasMultipleValues = values.length > 1;
  const includeRowTotals = showRowTotals(config);
  const includeColTotals = showColumnTotals(config);
  const rowDims = config.rows;
  const colDims = config.columns;
  const numRowDimCols = Math.max(rowDims.length, 1);

  const grid: string[][] = [];

  // --- Column header rows ---
  // One row per column dimension level, plus a value-label row when multiple values
  for (let level = 0; level < colDims.length; level++) {
    const row: string[] = new Array(numRowDimCols).fill("");
    // Label the first cell of the first header row with dimension names
    if (level === 0) {
      for (let d = 0; d < rowDims.length; d++) {
        row[d] = rowDims[d];
      }
    }

    for (const colKey of colKeys) {
      if (hasMultipleValues) {
        for (let v = 0; v < values.length; v++) {
          row.push(colKey[level] ?? "");
        }
      } else {
        row.push(colKey[level] ?? "");
      }
    }
    if (includeRowTotals) {
      if (hasMultipleValues) {
        for (let v = 0; v < values.length; v++) {
          row.push(level === 0 ? "Total" : "");
        }
      } else {
        row.push(level === 0 ? "Total" : "");
      }
    }
    grid.push(row);
  }

  // Value-label header row (only when multiple values)
  if (hasMultipleValues) {
    const row: string[] = new Array(numRowDimCols).fill("");
    for (const _colKey of colKeys) {
      for (const val of values) {
        row.push(getRenderedValueLabel(config, val));
      }
    }
    if (includeRowTotals) {
      for (const val of values) {
        row.push(getRenderedValueLabel(config, val));
      }
    }
    grid.push(row);
  }

  // If no column dimensions, add a single header row with row dim names + value names
  if (colDims.length === 0) {
    const row: string[] = [];
    for (let d = 0; d < rowDims.length; d++) {
      row.push(rowDims[d]);
    }
    if (hasMultipleValues) {
      for (const val of values) {
        row.push(getRenderedValueLabel(config, val));
      }
    } else {
      row.push(getRenderedValueLabel(config, values[0] ?? "") || "Value");
    }
    if (includeRowTotals) {
      row.push("Total");
    }
    grid.push(row);
  }

  // --- Data rows ---
  const groupedRows = config.show_subtotals
    ? pivotData.getGroupedRowKeys(true)
    : rowKeys.map((key) => ({ type: "data" as const, key, level: 0 }));

  for (const groupedRow of groupedRows) {
    const row: string[] = [];

    if (groupedRow.type === "subtotal") {
      for (let d = 0; d < numRowDimCols; d++) {
        if (d < groupedRow.key.length) {
          row.push(groupedRow.key[d]);
        } else if (d === groupedRow.key.length) {
          row.push("Subtotal");
        } else {
          row.push("");
        }
      }
      for (const colKey of colKeys) {
        for (const valField of values) {
          const agg = pivotData.getSubtotalAggregator(
            groupedRow.key,
            colKey,
            valField,
          );
          const rawValue = agg.value();
          row.push(
            formatExportTotalValue(
              rawValue,
              valField,
              config,
              pivotData,
              mode,
              null,
              agg,
            ),
          );
        }
      }
      if (includeRowTotals) {
        for (const valField of values) {
          if (!showTotalForMeasure(config, valField, "row")) {
            row.push("");
          } else {
            const agg = pivotData.getSubtotalAggregator(
              groupedRow.key,
              [],
              valField,
            );
            const rawValue = agg.value();
            row.push(
              formatExportTotalValue(
                rawValue,
                valField,
                config,
                pivotData,
                mode,
                "row",
                agg,
              ),
            );
          }
        }
      }
    } else {
      // Data row
      const rowKey = groupedRow.key;
      for (let d = 0; d < numRowDimCols; d++) {
        row.push(rowKey[d] ?? "");
      }
      for (const colKey of colKeys) {
        for (const valField of values) {
          const agg = pivotData.getAggregator(rowKey, colKey, valField);
          const rawValue = agg.value();
          row.push(
            formatExportValue(
              rawValue,
              valField,
              config,
              pivotData,
              rowKey,
              colKey,
              mode,
            ),
          );
        }
      }
      if (includeRowTotals) {
        for (const valField of values) {
          if (!showTotalForMeasure(config, valField, "row")) {
            row.push("");
          } else {
            const agg = pivotData.getRowTotal(rowKey, valField);
            const rawValue = agg.value();
            row.push(
              formatExportTotalValue(
                rawValue,
                valField,
                config,
                pivotData,
                mode,
                "row",
                agg,
              ),
            );
          }
        }
      }
    }
    grid.push(row);
  }

  // --- Column totals row ---
  if (includeColTotals) {
    const row: string[] = [];
    row.push("Total");
    for (let d = 1; d < numRowDimCols; d++) {
      row.push("");
    }
    for (const colKey of colKeys) {
      for (const valField of values) {
        if (!showTotalForMeasure(config, valField, "col")) {
          row.push("");
        } else {
          const agg = pivotData.getColTotal(colKey, valField);
          const rawValue = agg.value();
          row.push(
            formatExportTotalValue(
              rawValue,
              valField,
              config,
              pivotData,
              mode,
              "col",
              agg,
            ),
          );
        }
      }
    }
    if (includeRowTotals) {
      for (const valField of values) {
        if (!showTotalForMeasure(config, valField, "grand")) {
          row.push("");
        } else {
          const agg = pivotData.getGrandTotal(valField);
          const rawValue = agg.value();
          row.push(
            formatExportTotalValue(
              rawValue,
              valField,
              config,
              pivotData,
              mode,
              "grand",
              agg,
            ),
          );
        }
      }
    }
    grid.push(row);
  }

  return grid;
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
): Promise<boolean> {
  const grid = buildExportGrid(pivotData, config, options.content);
  const now = new Date();
  const ts = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
  const name = `${baseFilename || "pivot-table"}_${ts}`;

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
