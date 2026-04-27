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

import type {
  ExportGrid,
  ExportCell,
  ValueFieldColumns,
  ValueFieldRows,
} from "./exportData";
import type {
  AnyConditionalFormatRule,
  ColorScaleRule,
  DataBarsRule,
  ThresholdRule,
  ThresholdCondition,
} from "./types";

type ExcelJS = typeof import("exceljs");

const HEADER_FILL = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFF2F2F2" },
};
const SUBTOTAL_FILL = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFE6E6E6" },
};
const TOTAL_FILL = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFE6E6E6" },
};
const GRAND_TOTAL_FILL = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFD9D9D9" },
};
const BAND_FILL = {
  type: "pattern" as const,
  pattern: "solid" as const,
  fgColor: { argb: "FFF8F8F8" },
};

const BOLD_FONT = { bold: true };
const THIN_BORDER_BOTTOM = {
  bottom: { style: "thin" as const, color: { argb: "FFD0D0D0" } },
};
const THIN_BORDER_TOP = {
  top: { style: "thin" as const, color: { argb: "FFD0D0D0" } },
};
const THIN_BORDER_LEFT = {
  left: { style: "thin" as const, color: { argb: "FFD0D0D0" } },
};

/**
 * Build an ExcelJS Workbook from the shared ExportGrid IR.
 * Exported for testing; callers should normally use `exportExcel()`.
 */
export function buildExcelWorkbook(
  ExcelModule: ExcelJS,
  grid: ExportGrid,
  sheetName?: string,
): InstanceType<ExcelJS["Workbook"]> {
  const workbook = new ExcelModule.Workbook();
  const sheet = workbook.addWorksheet(sheetName || "Pivot Table");

  const { cells, headerRowCount, rowDimCount } = grid;
  const rowCount = cells.length;
  const colCount = rowCount > 0 ? cells[0].length : 0;

  // Track merges to avoid writing into merged regions
  const merged = new Set<string>();

  // Write cells
  for (let r = 0; r < rowCount; r++) {
    const rowCells = cells[r];
    for (let c = 0; c < rowCells.length; c++) {
      const key = `${r},${c}`;
      if (merged.has(key)) continue;

      const ec: ExportCell = rowCells[c];
      const excelRow = r + 1;
      const excelCol = c + 1;
      const ws = sheet.getCell(excelRow, excelCol);

      // Write value: prefer raw numeric for data cells, display for labels
      if (ec.raw != null && ec.kind !== "header") {
        ws.value = ec.raw;
      } else {
        ws.value = ec.display;
      }

      // Number format
      if (ec.numberFormat) {
        ws.numFmt = ec.numberFormat;
      }

      // Styling by kind
      applyStyle(ws, ec, r, headerRowCount, rowDimCount, c);

      // Handle merges
      if (ec.mergeRight && ec.mergeRight > 0) {
        const endCol = excelCol + ec.mergeRight;
        sheet.mergeCells(excelRow, excelCol, excelRow, endCol);
        for (let mc = c + 1; mc <= c + ec.mergeRight; mc++) {
          merged.add(`${r},${mc}`);
        }
      }
      if (ec.mergeDown && ec.mergeDown > 0) {
        const endRow = excelRow + ec.mergeDown;
        sheet.mergeCells(excelRow, excelCol, endRow, excelCol);
        for (let mr = r + 1; mr <= r + ec.mergeDown; mr++) {
          merged.add(`${mr},${c}`);
        }
      }
    }
  }

  // Column widths: auto-fit from content, capped at 30
  for (let c = 0; c < colCount; c++) {
    let maxLen = 8;
    for (let r = 0; r < rowCount; r++) {
      const display = cells[r][c]?.display ?? "";
      maxLen = Math.max(maxLen, display.length + 2);
    }
    const col = sheet.getColumn(c + 1);
    col.width = Math.min(maxLen, 30);
  }

  // Freeze panes: freeze header rows + row dimension columns
  sheet.views = [
    {
      state: "frozen",
      xSplit: rowDimCount,
      ySplit: headerRowCount,
    },
  ];

  // Conditional formatting — supports both column-based (normal) and
  // row-based (values_axis="rows") targeting.
  if (
    grid.conditionalFormatting &&
    (grid.valueFieldColumns || grid.valueFieldRows)
  ) {
    applyConditionalFormatting(
      sheet,
      grid.conditionalFormatting,
      grid.valueFieldColumns ?? [],
      headerRowCount,
      rowCount,
      grid.cells,
      grid.valueFieldRows,
      grid.rowDimCount,
    );
  }

  return workbook;
}

function applyStyle(
  ws: { font?: object; fill?: object; border?: object; alignment?: object },
  ec: ExportCell,
  rowIdx: number,
  headerRowCount: number,
  rowDimCount: number,
  colIdx: number,
): void {
  const isDataArea = colIdx >= rowDimCount;
  const bandRow = (rowIdx - headerRowCount) % 2 === 1;

  switch (ec.kind) {
    case "header":
      ws.font = BOLD_FONT;
      ws.fill = HEADER_FILL;
      ws.alignment = isDataArea
        ? { horizontal: "center" }
        : { horizontal: "left" };
      ws.border = THIN_BORDER_BOTTOM;
      break;
    case "data":
      ws.alignment = isDataArea
        ? { horizontal: "right" }
        : { horizontal: "left", vertical: "top" };
      if (bandRow) ws.fill = BAND_FILL;
      break;
    case "subtotal":
      ws.font = BOLD_FONT;
      ws.fill = SUBTOTAL_FILL;
      ws.border = THIN_BORDER_TOP;
      ws.alignment = isDataArea
        ? { horizontal: "right" }
        : { horizontal: "left" };
      break;
    case "row-total":
      ws.font = BOLD_FONT;
      ws.border = THIN_BORDER_LEFT;
      ws.alignment = { horizontal: "right" };
      if (bandRow) ws.fill = BAND_FILL;
      break;
    case "col-total":
      ws.font = BOLD_FONT;
      ws.fill = TOTAL_FILL;
      ws.border = THIN_BORDER_TOP;
      ws.alignment = isDataArea
        ? { horizontal: "right" }
        : { horizontal: "left" };
      break;
    case "grand-total":
      ws.font = BOLD_FONT;
      ws.fill = GRAND_TOTAL_FILL;
      ws.alignment = { horizontal: "right" };
      break;
  }
}

// ---------------------------------------------------------------------------
// Conditional formatting translation
// ---------------------------------------------------------------------------

/** @internal Exported for testing only. */
export function hexToArgb(hex: string): string {
  const h = hex.replace("#", "");
  if (h.length === 6) return `FF${h.toUpperCase()}`;
  if (h.length === 8) return h.toUpperCase();
  return `FF${h.toUpperCase()}`;
}

/** @internal Exported for testing only. */
export function colLetter(colIndex0: number): string {
  let col = colIndex0 + 1;
  let s = "";
  while (col > 0) {
    const rem = (col - 1) % 26;
    s = String.fromCharCode(65 + rem) + s;
    col = Math.floor((col - 1) / 26);
  }
  return s;
}

/**
 * Build a non-contiguous Excel ref string covering all data columns for the
 * given 0-based row indices.  Each row becomes one range segment spanning
 * `dataColStart` to `dataColEnd` (both 0-based, inclusive).
 *
 * Example: rows=[4,6], dataColStart=2, dataColEnd=5 →
 *   "C5:F5 C7:F7"
 */
function buildRowRef(
  rows: number[],
  dataColStart: number,
  dataColEnd: number,
): string {
  const startLetter = colLetter(dataColStart);
  const endLetter = colLetter(dataColEnd);
  return rows
    .map((r) => `${startLetter}${r + 1}:${endLetter}${r + 1}`)
    .join(" ");
}

/**
 * Build an Excel ref string covering the given 1-based Excel row numbers in a
 * single column. Contiguous runs are compressed into range notation.
 *
 * Example: rows=[2,3,5,6], colL="B" → "B2:B3 B5:B6"
 * Example: rows=[2,3,4],   colL="C" → "C2:C4"
 */
function buildCompressedColRef(rows: number[], colL: string): string {
  if (rows.length === 0) return "";
  const sorted = [...rows].sort((a, b) => a - b);
  const segs: string[] = [];
  let start = sorted[0]!;
  let prev = sorted[0]!;
  for (let i = 1; i < sorted.length; i++) {
    const cur = sorted[i]!;
    if (cur === prev + 1) {
      prev = cur;
    } else {
      segs.push(
        start === prev ? `${colL}${start}` : `${colL}${start}:${colL}${prev}`,
      );
      start = cur;
      prev = cur;
    }
  }
  segs.push(
    start === prev ? `${colL}${start}` : `${colL}${start}:${colL}${prev}`,
  );
  return segs.join(" ");
}

function applyConditionalFormatting(
  sheet: {
    addConditionalFormatting: (
      opts: import("exceljs").ConditionalFormattingOptions,
    ) => void;
  },
  rules: AnyConditionalFormatRule[],
  valueFieldCols: ValueFieldColumns[],
  headerRowCount: number,
  totalRowCount: number,
  cells: ExportCell[][],
  valueFieldRows?: ValueFieldRows[],
  rowDimCount?: number,
): void {
  // ── Row-based mode (values_axis="rows") ─────────────────────────────────
  // Each value field lives in specific rows; the CF range is a non-contiguous
  // set of row bands covering all data columns.
  if (valueFieldRows && valueFieldRows.length > 0) {
    const dataColStart = rowDimCount ?? 0;
    // Scan the first strict data row (kind === "data") to determine the last
    // data column.  Subtotal rows must be skipped here because their trailing
    // row-total-equivalent cells have kind "subtotal" (not "row-total"), so
    // they would not stop the inner scan and dataColEnd would be too wide.
    let dataColEnd = dataColStart;
    let foundDataRow = false;
    for (let r = headerRowCount; r < cells.length; r++) {
      if (cells[r]?.[0]?.kind !== "data") continue;
      const sampleRow = cells[r]!;
      for (let c = dataColStart; c < sampleRow.length; c++) {
        if (sampleRow[c]?.kind !== "data") break;
        dataColEnd = c;
      }
      foundDataRow = true;
      break; // All data rows share the same column structure
    }
    if (!foundDataRow || dataColEnd < dataColStart) return;

    // Build a lookup of field → { data rows, subtotal rows }.
    const fieldRowMap = new Map<
      string,
      { data: number[]; subtotal: number[] }
    >();
    for (const vfr of valueFieldRows) {
      if (vfr.rows.length > 0 || (vfr.subtotalRows?.length ?? 0) > 0) {
        fieldRowMap.set(vfr.field, {
          data: vfr.rows,
          subtotal: vfr.subtotalRows ?? [],
        });
      }
    }
    if (fieldRowMap.size === 0) return;

    let priority = 1;

    for (const rule of rules) {
      const targetFields =
        rule.apply_to.length > 0 ? rule.apply_to : [...fieldRowMap.keys()];

      // Include subtotal rows only when the rule opts in.
      const includeTotals = rule.include_totals === true;
      const targetRows: number[] = [];
      for (const f of targetFields) {
        const bucket = fieldRowMap.get(f);
        if (!bucket) continue;
        targetRows.push(...bucket.data);
        if (includeTotals) targetRows.push(...bucket.subtotal);
      }
      if (targetRows.length === 0) continue;

      // Sort rows so refs and formula anchor are stable.
      targetRows.sort((a, b) => a - b);

      // Determine scope for color_scale / data_bars.
      // undefined (omitted) → default: one ref per value field, each with its
      //   own independent scale. Correct when apply_to spans multiple measures
      //   with different units/magnitudes (Revenue, Units, Margin, …).
      // "global" → one ref covering all targeted rows across all data columns.
      //   Use when you explicitly want a single scale that normalises all
      //   selected measures together.
      // "per_column" → one ref per data column.
      const rowScope = rule.scope; // undefined = per-field default

      // Build the list of (ref, anchorCell) pairs to emit.
      const rowRefs: { ref: string; anchorCell: string }[] = [];
      if (rowScope === "per_column") {
        // One CF entry per data column; rows are a non-contiguous union within
        // each column, keeping each column's scale independent.
        for (let c = dataColStart; c <= dataColEnd; c++) {
          const colL = colLetter(c);
          rowRefs.push({
            ref: targetRows.map((r) => `${colL}${r + 1}`).join(" "),
            anchorCell: `${colL}${targetRows[0]! + 1}`,
          });
        }
      } else if (rowScope === "global") {
        // Single scale across all target rows and all data columns.
        rowRefs.push({
          ref: buildRowRef(targetRows, dataColStart, dataColEnd),
          anchorCell: `${colLetter(dataColStart)}${targetRows[0]! + 1}`,
        });
      } else {
        // Default (no scope): one CF entry per value field in targetFields.
        // Each measure gets its own independent min/max, so Revenue and Units
        // are never normalised together on the same gradient.
        for (const f of targetFields) {
          const bucket = fieldRowMap.get(f);
          if (!bucket) continue;
          const fieldRows = [...bucket.data];
          if (includeTotals) fieldRows.push(...bucket.subtotal);
          if (fieldRows.length === 0) continue;
          fieldRows.sort((a, b) => a - b);
          rowRefs.push({
            ref: buildRowRef(fieldRows, dataColStart, dataColEnd),
            anchorCell: `${colLetter(dataColStart)}${fieldRows[0]! + 1}`,
          });
        }
      }

      // scope only affects color_scale and data_bars; threshold always uses a
      // single global ref so its formula anchoring is unaffected by scope.
      for (const { ref, anchorCell } of rowRefs) {
        if (rule.type === "color_scale") {
          const csRule = rule as ColorScaleRule;
          const hasMidValue =
            csRule.mid_color !== undefined &&
            typeof csRule.mid_value === "number" &&
            Number.isFinite(csRule.mid_value);
          const midCfvo = hasMidValue
            ? ({ type: "num", value: csRule.mid_value } as unknown as {
                type: "percentile";
                value: number;
              })
            : { type: "percentile" as const, value: 50 };
          const cfvo = csRule.mid_color
            ? [{ type: "min" as const }, midCfvo, { type: "max" as const }]
            : [{ type: "min" as const }, { type: "max" as const }];
          const color = csRule.mid_color
            ? [
                { argb: hexToArgb(csRule.min_color) },
                { argb: hexToArgb(csRule.mid_color) },
                { argb: hexToArgb(csRule.max_color) },
              ]
            : [
                { argb: hexToArgb(csRule.min_color) },
                { argb: hexToArgb(csRule.max_color) },
              ];
          sheet.addConditionalFormatting({
            ref,
            rules: [{ type: "colorScale", priority: priority++, cfvo, color }],
          });
        } else if (rule.type === "data_bars") {
          const dbRule = rule as DataBarsRule;
          const dataBarRule = {
            type: "dataBar" as const,
            priority: priority++,
            gradient: dbRule.fill === "gradient",
            cfvo: [{ type: "min" as const }, { type: "max" as const }],
            color: { argb: hexToArgb(dbRule.color || "#638EC6") },
          };
          sheet.addConditionalFormatting({
            ref,
            rules: [
              dataBarRule as unknown as import("exceljs").DataBarRuleType,
            ],
          });
        }
      }

      // Threshold: scope is ignored — always one entry with a global ref so
      // that include_totals is respected but formula anchoring is unambiguous.
      if (rule.type === "threshold") {
        const tRef = buildRowRef(targetRows, dataColStart, dataColEnd);
        const tAnchor = `${colLetter(dataColStart)}${targetRows[0]! + 1}`;
        const tRule = rule as ThresholdRule;
        for (const cond of tRule.conditions) {
          const style: Record<string, unknown> = {};
          if (cond.background) {
            style.fill = {
              type: "pattern",
              pattern: "solid",
              bgColor: { argb: hexToArgb(cond.background) },
            };
          }
          if (cond.bold || cond.color) {
            const font: Record<string, unknown> = {};
            if (cond.bold) font.bold = true;
            if (cond.color) font.color = { argb: hexToArgb(cond.color) };
            style.font = font;
          }
          const formula = buildThresholdFormula(tAnchor, cond);
          if (!formula) continue;
          sheet.addConditionalFormatting({
            ref: tRef,
            rules: [
              {
                type: "expression",
                priority: priority++,
                formulae: [formula],
                style,
              },
            ],
          });
        }
      }
    }
    return;
  }

  // ── Column-based mode (normal values_axis="columns") ─────────────────────
  const dataStartRow = headerRowCount + 1;
  let dataEndRow = totalRowCount;
  for (let r = totalRowCount - 1; r >= headerRowCount; r--) {
    const kind = cells[r]?.[0]?.kind;
    if (kind === "col-total" || kind === "grand-total") {
      dataEndRow = r;
    } else {
      break;
    }
  }
  if (dataEndRow <= headerRowCount) return;

  // Classify each row in [dataStartRow, dataEndRow] (1-based Excel rows) as
  // data or subtotal. Excel row er maps to cells[er - 1].
  // This lets include_totals control whether mid-table subtotal rows are
  // included in the CF range, matching the row-based path's behavior.
  const colDataRows: number[] = [];
  const colSubtotalRows: number[] = [];
  for (let er = dataStartRow; er <= dataEndRow; er++) {
    const kind = cells[er - 1]?.[0]?.kind;
    if (kind === "data") colDataRows.push(er);
    else if (kind === "subtotal") colSubtotalRows.push(er);
  }
  if (colDataRows.length === 0) return;

  const fieldColMap = new Map<string, number[]>();
  for (const vfc of valueFieldCols) {
    fieldColMap.set(vfc.field, vfc.columns);
  }

  let priority = 1;

  for (const rule of rules) {
    const targetFields =
      rule.apply_to.length > 0 ? rule.apply_to : [...fieldColMap.keys()];

    const targetCols: number[] = [];
    for (const f of targetFields) {
      const cols = fieldColMap.get(f);
      if (cols) targetCols.push(...cols);
    }
    if (targetCols.length === 0) continue;

    // Honour include_totals: subtotal rows are excluded by default, matching
    // the row-based path and the documented contract.
    const includeTotals = rule.include_totals === true;
    const targetRows: number[] = includeTotals
      ? [...colDataRows, ...colSubtotalRows].sort((a, b) => a - b)
      : colDataRows;
    if (targetRows.length === 0) continue;

    // Determine scope: "per_column" (default) keeps an independent scale per
    // data column; "global" merges all target columns into one scale.
    const colScope = rule.scope ?? "per_column";

    // Build the list of (ref, anchorCell) pairs to emit.
    const colRefs: { ref: string; anchorCell: string }[] = [];
    if (colScope === "global") {
      // Single scale spanning all target columns (non-contiguous union).
      const sortedCols = [...targetCols].sort((a, b) => a - b);
      const ref = sortedCols
        .map((c) => buildCompressedColRef(targetRows, colLetter(c)))
        .filter(Boolean)
        .join(" ");
      if (!ref) continue;
      colRefs.push({
        ref,
        anchorCell: `${colLetter(sortedCols[0]!)}${targetRows[0]!}`,
      });
    } else {
      // Per-column (default): one CF entry per data column.
      for (const col0 of targetCols) {
        const colL = colLetter(col0);
        const ref = buildCompressedColRef(targetRows, colL);
        if (!ref) continue;
        colRefs.push({
          ref,
          anchorCell: `${colL}${targetRows[0]!}`,
        });
      }
    }

    // scope only affects color_scale and data_bars; threshold always uses
    // per-column refs so its formula anchoring is unambiguous.
    for (const { ref, anchorCell: colAnchor } of colRefs) {
      if (rule.type === "color_scale") {
        const csRule = rule as ColorScaleRule;
        const hasMidValue =
          csRule.mid_color !== undefined &&
          typeof csRule.mid_value === "number" &&
          Number.isFinite(csRule.mid_value);
        // Midpoint CFVO: numeric anchor when mid_value is provided,
        // otherwise fall back to the legacy 50th-percentile midpoint so
        // existing mid_color-only rules keep producing the same workbook.
        // ExcelJS's TS types don't enumerate { type: "num" } on color-scale
        // CFVOs, so cast via unknown like the data-bar branch below.
        const midCfvo = hasMidValue
          ? ({ type: "num", value: csRule.mid_value } as unknown as {
              type: "percentile";
              value: number;
            })
          : { type: "percentile" as const, value: 50 };
        const cfvo = csRule.mid_color
          ? [{ type: "min" as const }, midCfvo, { type: "max" as const }]
          : [{ type: "min" as const }, { type: "max" as const }];
        const color = csRule.mid_color
          ? [
              { argb: hexToArgb(csRule.min_color) },
              { argb: hexToArgb(csRule.mid_color) },
              { argb: hexToArgb(csRule.max_color) },
            ]
          : [
              { argb: hexToArgb(csRule.min_color) },
              { argb: hexToArgb(csRule.max_color) },
            ];

        sheet.addConditionalFormatting({
          ref,
          rules: [{ type: "colorScale", priority: priority++, cfvo, color }],
        });
      } else if (rule.type === "data_bars") {
        const dbRule = rule as DataBarsRule;
        // ExcelJS runtime supports `color` on dataBar rules but the
        // TypeScript declarations omit it — cast to satisfy the compiler.
        const dataBarRule = {
          type: "dataBar" as const,
          priority: priority++,
          gradient: dbRule.fill === "gradient",
          cfvo: [{ type: "min" as const }, { type: "max" as const }],
          color: { argb: hexToArgb(dbRule.color || "#638EC6") },
        };
        sheet.addConditionalFormatting({
          ref,
          rules: [dataBarRule as unknown as import("exceljs").DataBarRuleType],
        });
      }
    }

    // Threshold: scope is ignored — always one entry per target column so
    // formula anchoring is independent of how color_scale/data_bars are scoped.
    if (rule.type === "threshold") {
      const tRule = rule as ThresholdRule;
      for (const col0 of targetCols) {
        const colL = colLetter(col0);
        const tRef = buildCompressedColRef(targetRows, colL);
        if (!tRef) continue;
        const tAnchor = `${colL}${targetRows[0]!}`;
        for (const cond of tRule.conditions) {
          const style: Record<string, unknown> = {};
          if (cond.background) {
            style.fill = {
              type: "pattern",
              pattern: "solid",
              bgColor: { argb: hexToArgb(cond.background) },
            };
          }
          if (cond.bold || cond.color) {
            const font: Record<string, unknown> = {};
            if (cond.bold) font.bold = true;
            if (cond.color) font.color = { argb: hexToArgb(cond.color) };
            style.font = font;
          }
          const formula = buildThresholdFormula(tAnchor, cond);
          if (!formula) continue;
          sheet.addConditionalFormatting({
            ref: tRef,
            rules: [
              {
                type: "expression",
                priority: priority++,
                formulae: [formula],
                style,
              },
            ],
          });
        }
      }
    }
  }
}

/** @internal Exported for testing only. */
export function buildThresholdFormula(
  topLeftCell: string,
  cond: ThresholdCondition,
): string | undefined {
  switch (cond.operator) {
    case "gt":
      return `${topLeftCell}>${cond.value}`;
    case "gte":
      return `${topLeftCell}>=${cond.value}`;
    case "lt":
      return `${topLeftCell}<${cond.value}`;
    case "lte":
      return `${topLeftCell}<=${cond.value}`;
    case "eq":
      return `${topLeftCell}=${cond.value}`;
    case "between":
      return `AND(${topLeftCell}>=${cond.value},${topLeftCell}<=${cond.value2 ?? cond.value})`;
    default:
      return undefined;
  }
}

/**
 * Dynamically load ExcelJS, build the workbook, and trigger a download.
 */
export async function exportExcel(
  grid: ExportGrid,
  filename: string,
): Promise<boolean> {
  try {
    const ExcelModule = await import("exceljs");
    const workbook = buildExcelWorkbook(ExcelModule, grid, "Pivot Table");
    const buffer = await workbook.xlsx.writeBuffer();
    const blob = new Blob([buffer], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${filename}.xlsx`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    return true;
  } catch {
    return false;
  }
}
