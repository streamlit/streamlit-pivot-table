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

import type { ExportGrid, ExportCell, ValueFieldColumns } from "./exportData";
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

  // Conditional formatting
  if (grid.conditionalFormatting && grid.valueFieldColumns) {
    applyConditionalFormatting(
      sheet,
      grid.conditionalFormatting,
      grid.valueFieldColumns,
      headerRowCount,
      rowCount,
      grid.cells,
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
): void {
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

    for (const col0 of targetCols) {
      const colL = colLetter(col0);
      const ref = `${colL}${dataStartRow}:${colL}${dataEndRow}`;

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
      } else if (rule.type === "threshold") {
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

          const topLeft = `${colL}${dataStartRow}`;
          const formula = buildThresholdFormula(topLeft, cond);
          if (!formula) continue;

          sheet.addConditionalFormatting({
            ref,
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
