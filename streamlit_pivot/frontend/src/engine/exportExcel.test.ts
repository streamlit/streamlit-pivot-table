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

import { describe, expect, it } from "vitest";
import * as ExcelJS from "exceljs";
import {
  buildExcelWorkbook,
  hexToArgb,
  colLetter,
  buildThresholdFormula,
} from "./exportExcel";
import type {
  ExportGrid,
  ExportCell,
  CellKind,
  ValueFieldRows,
} from "./exportData";
import type { ColorScaleRule, DataBarsRule, ThresholdRule } from "./types";

function cell(
  display: string,
  kind: CellKind,
  raw: number | null = null,
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

function makeGrid(overrides?: Partial<ExportGrid>): ExportGrid {
  return {
    cells: [
      [
        cell("Region", "header"),
        cell("2023", "header"),
        cell("2024", "header"),
        cell("Total", "header"),
      ],
      [
        cell("EU", "data"),
        cell("200", "data", 200),
        cell("250", "data", 250),
        cell("450", "row-total", 450),
      ],
      [
        cell("US", "data"),
        cell("150", "data", 150),
        cell("150", "data", 150),
        cell("300", "row-total", 300),
      ],
      [
        cell("Total", "col-total"),
        cell("350", "col-total", 350),
        cell("400", "col-total", 400),
        cell("750", "grand-total", 750),
      ],
    ],
    headerRowCount: 1,
    rowDimCount: 1,
    ...overrides,
  };
}

describe("hexToArgb", () => {
  it("converts 6-digit hex to 8-digit ARGB with FF prefix", () => {
    expect(hexToArgb("#1976d2")).toBe("FF1976D2");
  });

  it("handles lowercase hex", () => {
    expect(hexToArgb("#aabbcc")).toBe("FFAABBCC");
  });

  it("handles hex without # prefix", () => {
    expect(hexToArgb("ff0000")).toBe("FFFF0000");
  });

  it("passes through 8-digit hex as-is (uppercased)", () => {
    expect(hexToArgb("80ff00ff")).toBe("80FF00FF");
  });
});

describe("colLetter", () => {
  it("converts 0 to A", () => {
    expect(colLetter(0)).toBe("A");
  });

  it("converts 25 to Z", () => {
    expect(colLetter(25)).toBe("Z");
  });

  it("converts 26 to AA", () => {
    expect(colLetter(26)).toBe("AA");
  });

  it("converts 27 to AB", () => {
    expect(colLetter(27)).toBe("AB");
  });

  it("converts 701 to ZZ", () => {
    expect(colLetter(701)).toBe("ZZ");
  });
});

describe("buildThresholdFormula", () => {
  it("gt: A1>100", () => {
    expect(buildThresholdFormula("A1", { operator: "gt", value: 100 })).toBe(
      "A1>100",
    );
  });

  it("gte: A1>=100", () => {
    expect(buildThresholdFormula("A1", { operator: "gte", value: 100 })).toBe(
      "A1>=100",
    );
  });

  it("lt: A1<50", () => {
    expect(buildThresholdFormula("A1", { operator: "lt", value: 50 })).toBe(
      "A1<50",
    );
  });

  it("lte: A1<=50", () => {
    expect(buildThresholdFormula("A1", { operator: "lte", value: 50 })).toBe(
      "A1<=50",
    );
  });

  it("eq: A1=42", () => {
    expect(buildThresholdFormula("A1", { operator: "eq", value: 42 })).toBe(
      "A1=42",
    );
  });

  it("between: AND(A1>=10,A1<=90)", () => {
    expect(
      buildThresholdFormula("A1", {
        operator: "between",
        value: 10,
        value2: 90,
      }),
    ).toBe("AND(A1>=10,A1<=90)");
  });

  it("between without value2 uses value for both bounds", () => {
    expect(
      buildThresholdFormula("B3", { operator: "between", value: 50 }),
    ).toBe("AND(B3>=50,B3<=50)");
  });

  it("returns undefined for unknown operator", () => {
    expect(
      buildThresholdFormula("A1", { operator: "unknown" as never, value: 0 }),
    ).toBeUndefined();
  });
});

describe("buildExcelWorkbook", () => {
  it("creates a workbook with correct cell values", () => {
    const grid = makeGrid();
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    expect(ws.getCell(1, 1).value).toBe("Region");
    expect(ws.getCell(1, 2).value).toBe("2023");
    expect(ws.getCell(2, 1).value).toBe("EU");
    expect(ws.getCell(2, 2).value).toBe(200);
    expect(ws.getCell(2, 3).value).toBe(250);
    expect(ws.getCell(2, 4).value).toBe(450);
    expect(ws.getCell(4, 4).value).toBe(750);
  });

  it("uses the provided sheet name", () => {
    const grid = makeGrid();
    const wb = buildExcelWorkbook(ExcelJS, grid, "Sales Report");
    expect(wb.worksheets[0].name).toBe("Sales Report");
  });

  it("defaults sheet name to 'Pivot Table'", () => {
    const grid = makeGrid();
    const wb = buildExcelWorkbook(ExcelJS, grid);
    expect(wb.worksheets[0].name).toBe("Pivot Table");
  });

  it("applies bold font to header cells", () => {
    const grid = makeGrid();
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    const headerFont = ws.getCell(1, 1).font;
    expect(headerFont?.bold).toBe(true);
  });

  it("applies bold font to col-total cells", () => {
    const grid = makeGrid();
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    const totalFont = ws.getCell(4, 1).font;
    expect(totalFont?.bold).toBe(true);
  });

  it("applies bold font to grand-total cells", () => {
    const grid = makeGrid();
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    const grandFont = ws.getCell(4, 4).font;
    expect(grandFont?.bold).toBe(true);
  });

  it("applies bold font to row-total cells", () => {
    const grid = makeGrid();
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    const rowTotalFont = ws.getCell(2, 4).font;
    expect(rowTotalFont?.bold).toBe(true);
  });

  it("applies number format when specified", () => {
    const grid: ExportGrid = {
      cells: [
        [cell("Region", "header"), cell("Value", "header")],
        [
          cell("EU", "data"),
          cell("$200", "data", 200, { numberFormat: "$#,##0" }),
        ],
      ],
      headerRowCount: 1,
      rowDimCount: 1,
    };
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    expect(ws.getCell(2, 2).numFmt).toBe("$#,##0");
    expect(ws.getCell(2, 2).value).toBe(200);
  });

  it("sets freeze panes based on headerRowCount and rowDimCount", () => {
    const grid = makeGrid();
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    const view = ws.views?.[0] as Record<string, unknown>;
    expect(view).toBeDefined();
    expect(view?.state).toBe("frozen");
    expect(view?.xSplit).toBe(1);
    expect(view?.ySplit).toBe(1);
  });

  it("sets freeze panes with multiple header rows", () => {
    const grid: ExportGrid = {
      cells: [
        [
          cell("", "header"),
          cell("East", "header", null, { mergeRight: 1 }),
          cell("East", "header"),
        ],
        [
          cell("Region", "header"),
          cell("Rev", "header"),
          cell("Profit", "header"),
        ],
        [cell("US", "data"), cell("100", "data", 100), cell("50", "data", 50)],
      ],
      headerRowCount: 2,
      rowDimCount: 1,
    };
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    const view = ws.views?.[0] as Record<string, unknown>;
    expect(view?.ySplit).toBe(2);
    expect(view?.xSplit).toBe(1);
  });

  it("merges cells when mergeRight is set", () => {
    const grid: ExportGrid = {
      cells: [
        [
          cell("", "header"),
          cell("Q1", "header", null, { mergeRight: 1 }),
          cell("Q1", "header"),
        ],
        [
          cell("Region", "header"),
          cell("Rev", "header"),
          cell("Profit", "header"),
        ],
        [cell("US", "data"), cell("100", "data", 100), cell("50", "data", 50)],
      ],
      headerRowCount: 2,
      rowDimCount: 1,
    };
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    const mergedRanges = (ws as unknown as { _merges: Record<string, unknown> })
      ._merges;
    const mergeKeys = Object.keys(mergedRanges);
    expect(mergeKeys.length).toBeGreaterThan(0);
  });

  it("merges cells when mergeDown is set", () => {
    const grid: ExportGrid = {
      cells: [
        [cell("Region", "header"), cell("Value", "header")],
        [cell("US", "data", null, { mergeDown: 1 }), cell("100", "data", 100)],
        [cell("US", "data"), cell("200", "data", 200)],
      ],
      headerRowCount: 1,
      rowDimCount: 1,
    };
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    const mergedRanges = (ws as unknown as { _merges: Record<string, unknown> })
      ._merges;
    const mergeKeys = Object.keys(mergedRanges);
    expect(mergeKeys.length).toBeGreaterThan(0);
  });

  it("sets column widths within expected range", () => {
    const grid = makeGrid();
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    for (let c = 1; c <= 4; c++) {
      const col = ws.getColumn(c);
      expect(col.width).toBeGreaterThanOrEqual(8);
      expect(col.width).toBeLessThanOrEqual(30);
    }
  });

  it("applies subtotal styling", () => {
    const grid: ExportGrid = {
      cells: [
        [cell("A", "header"), cell("Value", "header")],
        [cell("X", "data"), cell("10", "data", 10)],
        [cell("X", "subtotal"), cell("10", "subtotal", 10)],
      ],
      headerRowCount: 1,
      rowDimCount: 1,
    };
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    expect(ws.getCell(3, 1).font?.bold).toBe(true);
    expect(ws.getCell(3, 2).font?.bold).toBe(true);
  });

  it("writes raw numeric values for data cells (not display strings)", () => {
    const grid: ExportGrid = {
      cells: [
        [cell("Region", "header"), cell("Value", "header")],
        [
          cell("EU", "data"),
          cell("$1,234.56", "data", 1234.56, { numberFormat: "$#,##0.00" }),
        ],
      ],
      headerRowCount: 1,
      rowDimCount: 1,
    };
    const wb = buildExcelWorkbook(ExcelJS, grid);
    const ws = wb.worksheets[0];

    expect(ws.getCell(2, 2).value).toBe(1234.56);
    expect(ws.getCell(2, 2).numFmt).toBe("$#,##0.00");
  });

  it("handles empty grid without errors", () => {
    const grid: ExportGrid = {
      cells: [],
      headerRowCount: 0,
      rowDimCount: 0,
    };
    const wb = buildExcelWorkbook(ExcelJS, grid);
    expect(wb.worksheets[0]).toBeDefined();
  });

  describe("conditional formatting", () => {
    type CfEntry = { ref: string; rules: Record<string, unknown>[] };

    function getCf(ws: ExcelJS.Worksheet): CfEntry[] {
      return (
        (ws as unknown as Record<string, CfEntry[]>).conditionalFormattings ??
        []
      );
    }

    function makeCondGrid(): ExportGrid {
      return {
        cells: [
          [
            cell("Region", "header"),
            cell("Revenue", "header"),
            cell("Profit", "header"),
            cell("Units", "header"),
          ],
          [
            cell("East", "data"),
            cell("100", "data", 100),
            cell("50", "data", 50),
            cell("300", "data", 300),
          ],
          [
            cell("West", "data"),
            cell("200", "data", 200),
            cell("80", "data", 80),
            cell("150", "data", 150),
          ],
          [
            cell("Total", "col-total"),
            cell("300", "col-total", 300),
            cell("130", "col-total", 130),
            cell("450", "col-total", 450),
          ],
        ],
        headerRowCount: 1,
        rowDimCount: 1,
        valueFieldColumns: [
          { field: "Revenue", columns: [1] },
          { field: "Profit", columns: [2] },
          { field: "Units", columns: [3] },
        ],
      };
    }

    // ---- Negative / guard tests ----

    it("does not add rules when no conditional formatting config is present", () => {
      const grid = makeCondGrid();
      const wb = buildExcelWorkbook(ExcelJS, grid);
      expect(getCf(wb.worksheets[0]).length).toBe(0);
    });

    it("does not add rules when valueFieldColumns is missing", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#1976d2",
          fill: "gradient",
        } as DataBarsRule,
      ];
      delete grid.valueFieldColumns;
      const wb = buildExcelWorkbook(ExcelJS, grid);
      expect(getCf(wb.worksheets[0]).length).toBe(0);
    });

    it("ignores rules with apply_to pointing to non-existent field", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["DoesNotExist"],
          color: "#aaa",
          fill: "solid",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      expect(getCf(wb.worksheets[0]).length).toBe(0);
    });

    // ---- Color scale ----

    it("color_scale: produces colorScale rule with correct ref, cfvo, and ARGB colors", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "color_scale",
          apply_to: ["Profit"],
          min_color: "#1b2e1b",
          max_color: "#4caf50",
        } as ColorScaleRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs.length).toBe(1);
      expect(cfs[0].ref).toBe("C2:C3");
      const rule = cfs[0].rules[0];
      expect(rule.type).toBe("colorScale");
      expect(rule.cfvo).toEqual([{ type: "min" }, { type: "max" }]);
      expect(rule.color).toEqual([{ argb: "FF1B2E1B" }, { argb: "FF4CAF50" }]);
    });

    it("color_scale: 3-color scale includes midpoint percentile cfvo", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "color_scale",
          apply_to: ["Revenue"],
          min_color: "#ff0000",
          mid_color: "#ffff00",
          max_color: "#00ff00",
        } as ColorScaleRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      const rule = cfs[0].rules[0];
      expect(rule.cfvo).toEqual([
        { type: "min" },
        { type: "percentile", value: 50 },
        { type: "max" },
      ]);
      expect(rule.color).toEqual([
        { argb: "FFFF0000" },
        { argb: "FFFFFF00" },
        { argb: "FF00FF00" },
      ]);
    });

    it("color_scale: mid_value emits a numeric middle cfvo", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "color_scale",
          apply_to: ["Revenue"],
          min_color: "#ff0000",
          mid_color: "#ffffff",
          max_color: "#0000ff",
          mid_value: 0,
        } as ColorScaleRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      const rule = cfs[0].rules[0];
      expect(rule.cfvo).toEqual([
        { type: "min" },
        { type: "num", value: 0 },
        { type: "max" },
      ]);
      expect(rule.color).toEqual([
        { argb: "FFFF0000" },
        { argb: "FFFFFFFF" },
        { argb: "FF0000FF" },
      ]);
    });

    it("color_scale: mid_value without mid_color is ignored (2-color scale)", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "color_scale",
          apply_to: ["Revenue"],
          min_color: "#ff0000",
          max_color: "#0000ff",
          mid_value: 0,
        } as ColorScaleRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      const rule = cfs[0].rules[0];
      // No mid_color means a 2-stop scale regardless of mid_value.
      expect(rule.cfvo).toEqual([{ type: "min" }, { type: "max" }]);
    });

    // ---- Data bars ----

    it("data_bars: produces dataBar rule with gradient flag and correct color", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#1976d2",
          fill: "gradient",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs.length).toBe(1);
      expect(cfs[0].ref).toBe("B2:B3");
      const rule = cfs[0].rules[0];
      expect(rule.type).toBe("dataBar");
      expect(rule.gradient).toBe(true);
      expect(rule.color).toEqual({ argb: "FF1976D2" });
    });

    it("data_bars: solid fill sets gradient=false", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#aabbcc",
          fill: "solid",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const rule = getCf(wb.worksheets[0])[0].rules[0];
      expect(rule.gradient).toBe(false);
    });

    it("data_bars: defaults to Excel blue when no color specified", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          fill: "gradient",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const rule = getCf(wb.worksheets[0])[0].rules[0];
      expect(rule.color).toEqual({ argb: "FF638EC6" });
    });

    // ---- Threshold ----

    it("threshold gt: produces expression formula with >", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "threshold",
          apply_to: ["Units"],
          conditions: [
            { operator: "gt", value: 250, background: "#1565c0", bold: true },
          ],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs.length).toBe(1);
      expect(cfs[0].ref).toBe("D2:D3");
      const rule = cfs[0].rules[0];
      expect(rule.type).toBe("expression");
      expect(rule.formulae).toEqual(["D2>250"]);
      const style = rule.style as Record<string, unknown>;
      expect(style.fill).toEqual({
        type: "pattern",
        pattern: "solid",
        bgColor: { argb: "FF1565C0" },
      });
      expect(style.font).toEqual({ bold: true });
    });

    it("threshold gte: produces formula with >=", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "threshold",
          apply_to: ["Units"],
          conditions: [{ operator: "gte", value: 100 }],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const rule = getCf(wb.worksheets[0])[0].rules[0];
      expect(rule.formulae).toEqual(["D2>=100"]);
    });

    it("threshold lt: produces formula with <", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "threshold",
          apply_to: ["Revenue"],
          conditions: [{ operator: "lt", value: 50 }],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const rule = getCf(wb.worksheets[0])[0].rules[0];
      expect(rule.formulae).toEqual(["B2<50"]);
    });

    it("threshold lte: produces formula with <=", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "threshold",
          apply_to: ["Revenue"],
          conditions: [{ operator: "lte", value: 200 }],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const rule = getCf(wb.worksheets[0])[0].rules[0];
      expect(rule.formulae).toEqual(["B2<=200"]);
    });

    it("threshold eq: produces formula with =", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "threshold",
          apply_to: ["Profit"],
          conditions: [{ operator: "eq", value: 80 }],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const rule = getCf(wb.worksheets[0])[0].rules[0];
      expect(rule.formulae).toEqual(["C2=80"]);
    });

    it("threshold between: produces AND formula", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "threshold",
          apply_to: ["Units"],
          conditions: [{ operator: "between", value: 100, value2: 300 }],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const rule = getCf(wb.worksheets[0])[0].rules[0];
      expect(rule.formulae).toEqual(["AND(D2>=100,D2<=300)"]);
    });

    it("threshold with font color produces font.color in style", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "threshold",
          apply_to: ["Units"],
          conditions: [{ operator: "gt", value: 100, color: "#ff0000" }],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const style = getCf(wb.worksheets[0])[0].rules[0].style as Record<
        string,
        unknown
      >;
      expect(style.font).toEqual({ color: { argb: "FFFF0000" } });
    });

    // ---- Cell range targeting ----

    it("ref range excludes header rows and total rows", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#000",
          fill: "solid",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const ref = getCf(wb.worksheets[0])[0].ref;
      // headerRowCount=1 → data starts at row 2; totalRowCount=4, last row is col-total → data ends at row 3
      expect(ref).toBe("B2:B3");
    });

    it("ref range handles multiple header rows", () => {
      const grid: ExportGrid = {
        cells: [
          [
            cell("", "header"),
            cell("2023", "header", null, { mergeRight: 1 }),
            cell("", "header"),
          ],
          [
            cell("Region", "header"),
            cell("Rev", "header"),
            cell("Units", "header"),
          ],
          [
            cell("East", "data"),
            cell("100", "data", 100),
            cell("50", "data", 50),
          ],
          [
            cell("West", "data"),
            cell("200", "data", 200),
            cell("80", "data", 80),
          ],
          [
            cell("Total", "col-total"),
            cell("300", "col-total", 300),
            cell("130", "col-total", 130),
          ],
        ],
        headerRowCount: 2,
        rowDimCount: 1,
        valueFieldColumns: [
          { field: "Rev", columns: [1] },
          { field: "Units", columns: [2] },
        ],
        conditionalFormatting: [
          {
            type: "data_bars",
            apply_to: ["Rev"],
            color: "#000",
            fill: "solid",
          } as DataBarsRule,
        ],
      };
      const wb = buildExcelWorkbook(ExcelJS, grid);
      expect(getCf(wb.worksheets[0])[0].ref).toBe("B3:B4");
    });

    // ---- Multi-column fields (with column dimensions) ----

    it("creates separate CF entry per column when field spans multiple columns", () => {
      const grid: ExportGrid = {
        cells: [
          [
            cell("Region", "header"),
            cell("2023", "header"),
            cell("2024", "header"),
          ],
          [
            cell("East", "data"),
            cell("100", "data", 100),
            cell("200", "data", 200),
          ],
          [
            cell("West", "data"),
            cell("300", "data", 300),
            cell("400", "data", 400),
          ],
        ],
        headerRowCount: 1,
        rowDimCount: 1,
        valueFieldColumns: [{ field: "Revenue", columns: [1, 2] }],
        conditionalFormatting: [
          {
            type: "data_bars",
            apply_to: ["Revenue"],
            color: "#123456",
            fill: "gradient",
          } as DataBarsRule,
        ],
      };
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs.length).toBe(2);
      expect(cfs[0].ref).toBe("B2:B3");
      expect(cfs[1].ref).toBe("C2:C3");
    });

    // ---- Empty apply_to targets all fields ----

    it("empty apply_to applies rule to all value field columns", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: [],
          color: "#aaa",
          fill: "solid",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs.length).toBe(3);
      expect(cfs.map((c) => c.ref).sort()).toEqual(["B2:B3", "C2:C3", "D2:D3"]);
    });

    // ---- Multiple rules combined ----

    it("applies all three rule types simultaneously", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#1976d2",
          fill: "gradient",
        } as DataBarsRule,
        {
          type: "color_scale",
          apply_to: ["Profit"],
          min_color: "#000",
          max_color: "#fff",
        } as ColorScaleRule,
        {
          type: "threshold",
          apply_to: ["Units"],
          conditions: [{ operator: "gt", value: 250 }],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs.length).toBe(3);
      expect(cfs[0].rules[0].type).toBe("dataBar");
      expect(cfs[1].rules[0].type).toBe("colorScale");
      expect(cfs[2].rules[0].type).toBe("expression");
    });

    // ---- Priority increments ----

    it("each rule gets a unique incrementing priority", () => {
      const grid = makeCondGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#aaa",
          fill: "solid",
        } as DataBarsRule,
        {
          type: "data_bars",
          apply_to: ["Profit"],
          color: "#bbb",
          fill: "solid",
        } as DataBarsRule,
        {
          type: "data_bars",
          apply_to: ["Units"],
          color: "#ccc",
          fill: "solid",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      const priorities = cfs.map(
        (c) => (c.rules[0] as Record<string, unknown>).priority,
      );
      expect(priorities).toEqual([1, 2, 3]);
    });

    // ── Row-based conditional formatting (values_axis="rows") ──────────────

    function makeRowsGrid(): ExportGrid {
      // Layout: col 0 = value-label, col 1 = dim "Region", cols 2-3 = data
      // (two column members: 2023, 2024).
      // Rows: header | Revenue/EU | Revenue/US | Units/EU | Units/US | col-total...
      return {
        cells: [
          // Header row
          [
            cell("Values", "header"),
            cell("Region", "header"),
            cell("2023", "header"),
            cell("2024", "header"),
          ],
          // Revenue rows
          [
            cell("Revenue", "data"),
            cell("EU", "data"),
            cell("200", "data", 200),
            cell("250", "data", 250),
          ],
          [
            cell("Revenue", "data"),
            cell("US", "data"),
            cell("100", "data", 100),
            cell("150", "data", 150),
          ],
          // Units rows
          [
            cell("Units", "data"),
            cell("EU", "data"),
            cell("80", "data", 80),
            cell("100", "data", 100),
          ],
          [
            cell("Units", "data"),
            cell("US", "data"),
            cell("40", "data", 40),
            cell("60", "data", 60),
          ],
          // Grand-total rows
          [
            cell("Revenue", "grand-total"),
            cell("", "grand-total"),
            cell("300", "grand-total", 300),
            cell("400", "grand-total", 400),
          ],
          [
            cell("Units", "grand-total"),
            cell("", "grand-total"),
            cell("120", "grand-total", 120),
            cell("160", "grand-total", 160),
          ],
        ],
        headerRowCount: 1,
        rowDimCount: 2,
        // values_axis="rows": column list is empty for every field
        valueFieldColumns: [
          { field: "Revenue", columns: [] },
          { field: "Units", columns: [] },
        ],
        valueFieldRows: [
          { field: "Revenue", rows: [1, 2], subtotalRows: [] },
          { field: "Units", rows: [3, 4], subtotalRows: [] },
        ] satisfies ValueFieldRows[],
      };
    }

    it("row-based: data_bars applied to Revenue rows only — ref covers rows 2-3", () => {
      const grid = makeRowsGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#1976d2",
          fill: "gradient",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);

      expect(cfs).toHaveLength(1);
      // rowDimCount=2 → data cols start at index 2 (col C). Last col is D.
      // Revenue rows are 0-based [1,2] → Excel rows 2 and 3.
      expect(cfs[0]!.ref).toBe("C2:D2 C3:D3");
      expect((cfs[0]!.rules[0] as Record<string, unknown>).type).toBe(
        "dataBar",
      );
    });

    it("row-based: color_scale for Units rows — ref covers rows 4-5", () => {
      const grid = makeRowsGrid();
      grid.conditionalFormatting = [
        {
          type: "color_scale",
          apply_to: ["Units"],
          min_color: "#ffffff",
          max_color: "#ff0000",
        } as ColorScaleRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);

      expect(cfs).toHaveLength(1);
      // Units rows are 0-based [3,4] → Excel rows 4 and 5.
      expect(cfs[0]!.ref).toBe("C4:D4 C5:D5");
      expect((cfs[0]!.rules[0] as Record<string, unknown>).type).toBe(
        "colorScale",
      );
    });

    it("row-based: apply_to=[] (no scope) produces one CF entry per field — not a shared union", () => {
      // Default (omitted scope) = per-field: Revenue and Units each get their
      // own independent scale so different-unit measures aren't normalised
      // together on the same gradient.
      const grid = makeRowsGrid();
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: [],
          color: "#aaa",
          fill: "solid",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);

      // Two fields → two separate CF entries
      expect(cfs).toHaveLength(2);
      // Revenue rows [1,2] → Excel 2,3; Units rows [3,4] → Excel 4,5
      expect(cfs[0]!.ref).toBe("C2:D2 C3:D3");
      expect(cfs[1]!.ref).toBe("C4:D4 C5:D5");
    });

    it("row-based: threshold rule formula anchors at the first data cell of the first matching row", () => {
      const grid = makeRowsGrid();
      grid.conditionalFormatting = [
        {
          type: "threshold",
          apply_to: ["Revenue"],
          conditions: [
            {
              operator: "gt",
              value: 150,
              background: "#ff0000",
            },
          ],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);

      expect(cfs).toHaveLength(1);
      // ref covers Revenue rows (0-based [1,2] → Excel 2-3), data cols C-D
      expect(cfs[0]!.ref).toBe("C2:D2 C3:D3");
      // formula anchors at C2 (first data cell in first Revenue row)
      const formulae = (cfs[0]!.rules[0] as Record<string, unknown>)
        .formulae as string[];
      expect(formulae[0]).toBe("C2>150");
    });

    it("row-based: ref excludes trailing row-total columns", () => {
      // Grid with row-total cells appended after data cells (cols 4-5)
      const gridWithRowTotals: ExportGrid = {
        cells: [
          // header: Values | Region | 2023 | 2024 | Total
          [
            cell("Values", "header"),
            cell("Region", "header"),
            cell("2023", "header"),
            cell("2024", "header"),
            cell("Total", "header"),
          ],
          // Revenue/EU row: cols 2-3 are data, col 4 is row-total
          [
            cell("Revenue", "data"),
            cell("EU", "data"),
            cell("200", "data", 200),
            cell("250", "data", 250),
            cell("450", "row-total", 450),
          ],
          [
            cell("Revenue", "data"),
            cell("US", "data"),
            cell("100", "data", 100),
            cell("150", "data", 150),
            cell("250", "row-total", 250),
          ],
        ],
        headerRowCount: 1,
        rowDimCount: 2,
        valueFieldColumns: [{ field: "Revenue", columns: [] }],
        valueFieldRows: [{ field: "Revenue", rows: [1, 2], subtotalRows: [] }],
      };
      gridWithRowTotals.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#1976d2",
          fill: "gradient",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, gridWithRowTotals);
      const cfs = getCf(wb.worksheets[0]);

      expect(cfs).toHaveLength(1);
      // dataColStart=2 (rowDimCount), dataColEnd must stop at col 3 (D), not col 4 (E)
      // Revenue rows are 0-based [1,2] → Excel rows 2-3
      expect(cfs[0]!.ref).toBe("C2:D2 C3:D3");
    });

    it("row-based: duplicate field labels do not collapse ownership (unique field IDs)", async () => {
      // Integration test: two measures share the same rendered label.
      // Their valueFieldRows must remain distinct (no overlap).
      const { PivotData } = await import("./PivotData");
      const { buildExportIR } = await import("./exportData");
      const { makeConfig } = await import("../test-utils");

      const data = [
        { region: "EU", year: "2023", revenue: 200, profit: 80 },
        { region: "US", year: "2023", revenue: 100, profit: 40 },
      ];
      // Override field_labels so both measures render with the same text
      const config = makeConfig({
        rows: ["region"],
        columns: ["year"],
        values: ["revenue", "profit"],
        values_axis: "rows",
        field_labels: { revenue: "Metric", profit: "Metric" },
      });
      const pd = new PivotData(data, config);
      const ir = buildExportIR(pd, config, "raw");

      expect(ir.valueFieldRows).toBeDefined();
      const revRows = new Set(
        ir.valueFieldRows!.find((v) => v.field === "revenue")!.rows,
      );
      const profRows = new Set(
        ir.valueFieldRows!.find((v) => v.field === "profit")!.rows,
      );

      // Row sets must be disjoint — no row can belong to both fields
      for (const r of profRows) {
        expect(revRows.has(r)).toBe(false);
      }
      expect(revRows.size).toBeGreaterThan(0);
      expect(profRows.size).toBeGreaterThan(0);
    });

    it("row-based: ref excludes subtotal-row trailing columns (first data row used for width scan)", () => {
      // When subtotal_position="top" the first measure-owned row is a subtotal.
      // Subtotal trailing cells have kind="subtotal", not "row-total", so scanning
      // a subtotal row would incorrectly widen dataColEnd.  The fix: scan the
      // first strict kind="data" row only.
      const gridSubtotalFirst: ExportGrid = {
        cells: [
          // Header
          [
            cell("Values", "header"),
            cell("Region", "header"),
            cell("2023", "header"),
            cell("2024", "header"),
            cell("Total", "header"),
          ],
          // Revenue subtotal row: trailing cell is "subtotal" (NOT "row-total")
          [
            cell("Revenue", "subtotal"),
            cell("Subtotal", "subtotal"),
            cell("300", "subtotal", 300),
            cell("400", "subtotal", 400),
            cell("700", "subtotal", 700), // <-- kind="subtotal", same as data cols
          ],
          // Revenue data rows: trailing cell is "row-total"
          [
            cell("Revenue", "data"),
            cell("EU", "data"),
            cell("200", "data", 200),
            cell("250", "data", 250),
            cell("450", "row-total", 450),
          ],
          [
            cell("Revenue", "data"),
            cell("US", "data"),
            cell("100", "data", 100),
            cell("150", "data", 150),
            cell("250", "row-total", 250),
          ],
        ],
        headerRowCount: 1,
        rowDimCount: 2,
        valueFieldColumns: [{ field: "Revenue", columns: [] }],
        valueFieldRows: [
          {
            field: "Revenue",
            rows: [2, 3], // data rows only
            subtotalRows: [1],
          },
        ],
      };
      gridSubtotalFirst.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#1976d2",
          fill: "gradient",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, gridSubtotalFirst);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs).toHaveLength(1);
      // dataColEnd must be D (col index 3), not E (col 4 which is row-total/subtotal-total)
      // Data rows 0-based [2,3] → Excel rows 3,4
      expect(cfs[0]!.ref).toBe("C3:D3 C4:D4");
    });

    it("row-based: subtotal rows excluded by default (no include_totals)", () => {
      const grid = makeRowsGrid();
      // Add subtotal rows to the valueFieldRows
      grid.valueFieldRows = [
        { field: "Revenue", rows: [1, 2], subtotalRows: [5] },
        { field: "Units", rows: [3, 4], subtotalRows: [6] },
      ];
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#1976d2",
          fill: "gradient",
          // include_totals: not set → subtotal rows excluded
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs).toHaveLength(1);
      // Only data rows [1,2] → Excel rows 2,3 — row 6 (subtotal) must NOT appear
      expect(cfs[0]!.ref).toBe("C2:D2 C3:D3");
      expect(cfs[0]!.ref).not.toContain("6");
    });

    it("row-based: subtotal rows included when include_totals=true", () => {
      const grid = makeRowsGrid();
      grid.valueFieldRows = [
        { field: "Revenue", rows: [1, 2], subtotalRows: [5] },
        { field: "Units", rows: [3, 4], subtotalRows: [6] },
      ];
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#1976d2",
          fill: "gradient",
          include_totals: true,
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs).toHaveLength(1);
      // Data rows [1,2] + subtotal row [5] sorted → [1,2,5] → Excel rows 2,3,6
      expect(cfs[0]!.ref).toBe("C2:D2 C3:D3 C6:D6");
    });

    it("row-based: no rules added when valueFieldRows has no data rows", () => {
      const grid = makeRowsGrid();
      grid.valueFieldRows = [
        { field: "Revenue", rows: [], subtotalRows: [] },
        { field: "Units", rows: [], subtotalRows: [] },
      ];
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: [],
          color: "#aaa",
          fill: "solid",
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      expect(getCf(wb.worksheets[0]).length).toBe(0);
    });

    it("buildExportIR populates valueFieldRows for values_axis='rows'", async () => {
      // Integration test: verify buildExportIR sets valueFieldRows correctly.
      const { PivotData } = await import("./PivotData");
      const { buildExportIR } = await import("./exportData");
      const { makeConfig } = await import("../test-utils");

      const data = [
        { region: "EU", year: "2023", revenue: 200, profit: 80 },
        { region: "US", year: "2023", revenue: 100, profit: 40 },
      ];
      const config = makeConfig({
        rows: ["region"],
        columns: ["year"],
        values: ["revenue", "profit"],
        values_axis: "rows",
      });
      const pd = new PivotData(data, config);
      const ir = buildExportIR(pd, config, "raw");

      expect(ir.valueFieldRows).toBeDefined();
      expect(ir.valueFieldRows!.length).toBe(2);

      const revRows = ir.valueFieldRows!.find((vfr) => vfr.field === "revenue");
      const profRows = ir.valueFieldRows!.find((vfr) => vfr.field === "profit");
      expect(revRows).toBeDefined();
      expect(profRows).toBeDefined();

      // Revenue and profit rows must be non-overlapping
      const revSet = new Set(revRows!.rows);
      const profSet = new Set(profRows!.rows);
      for (const r of profSet) {
        expect(revSet.has(r)).toBe(false);
      }

      // Each entry must have a subtotalRows array (empty when no subtotals)
      for (const vfr of ir.valueFieldRows!) {
        expect(Array.isArray(vfr.subtotalRows)).toBe(true);
      }

      // All row indices must be within data range (>= headerRowCount)
      for (const vfr of ir.valueFieldRows!) {
        for (const r of [...vfr.rows, ...(vfr.subtotalRows ?? [])]) {
          expect(r).toBeGreaterThanOrEqual(ir.headerRowCount);
        }
      }

      // valueFieldColumns must still be present but with empty column lists
      expect(
        ir.valueFieldColumns!.every((vfc) => vfc.columns.length === 0),
      ).toBe(true);
    });

    // ── scope parameter tests ─────────────────────────────────────────────

    it("column-based: scope='global' produces one union ref for all target columns", () => {
      // Two-column pivot (2023 in col 1, 2024 in col 2) for "Revenue".
      // With scope="global", both columns should share a single CF entry
      // instead of the default two separate per-column entries.
      const grid = makeGrid({
        valueFieldColumns: [{ field: "revenue", columns: [1, 2] }],
        conditionalFormatting: [
          {
            type: "color_scale",
            apply_to: ["revenue"],
            min_color: "#ffffff",
            max_color: "#ff0000",
            scope: "global",
          } as ColorScaleRule,
        ],
      });
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      // global scope → one CF entry covering both columns (union ref)
      expect(cfs).toHaveLength(1);
      // Both B and C columns included; dataStartRow=2, dataEndRow strips col-total row→3
      expect(cfs[0]!.ref).toBe("B2:B3 C2:C3");
    });

    it("column-based: scope='per_column' (default) produces one ref per column", () => {
      // Verify that omitting scope keeps the existing per-column behavior.
      const grid = makeGrid({
        valueFieldColumns: [{ field: "revenue", columns: [1, 2] }],
        conditionalFormatting: [
          {
            type: "data_bars",
            apply_to: ["revenue"],
            color: "#1976d2",
            fill: "gradient",
            // no scope → defaults to "per_column"
          } as DataBarsRule,
        ],
      });
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      // per_column → two separate CF entries, one per column
      expect(cfs).toHaveLength(2);
      expect(cfs[0]!.ref).toBe("B2:B3");
      expect(cfs[1]!.ref).toBe("C2:C3");
    });

    it("row-based: scope='per_column' produces one ref per data column", () => {
      // Two data columns (2023=col 2, 2024=col 3) and Revenue on rows [1,2].
      // With scope="per_column", each column should get its own CF entry.
      const grid = makeRowsGrid();
      grid.valueFieldRows = [
        { field: "Revenue", rows: [1, 2], subtotalRows: [] },
        { field: "Units", rows: [3, 4], subtotalRows: [] },
      ];
      grid.conditionalFormatting = [
        {
          type: "color_scale",
          apply_to: ["Revenue"],
          min_color: "#ffffff",
          max_color: "#ff0000",
          scope: "per_column",
        } as ColorScaleRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      // per_column → one CF entry per data column (C and D)
      expect(cfs).toHaveLength(2);
      // Each ref targets only Revenue rows [1,2] → Excel rows 2,3
      expect(cfs[0]!.ref).toBe("C2 C3");
      expect(cfs[1]!.ref).toBe("D2 D3");
    });

    // ── column include_totals tests ───────────────────────────────────────

    it("column-based: include_totals=false (default) excludes subtotal rows", () => {
      // Grid: header | East data | subtotal | West data | col-total
      // Default include_totals=false should target only East and West (rows 2, 4).
      const grid: ExportGrid = {
        cells: [
          [cell("Region", "header"), cell("Revenue", "header")],
          [cell("East", "data"), cell("100", "data", 100)],
          [cell("Subtotal", "subtotal"), cell("100", "subtotal", 100)],
          [cell("West", "data"), cell("200", "data", 200)],
          [cell("Total", "col-total"), cell("300", "col-total", 300)],
        ],
        headerRowCount: 1,
        rowDimCount: 1,
        valueFieldColumns: [{ field: "Revenue", columns: [1] }],
        conditionalFormatting: [
          {
            type: "data_bars",
            apply_to: ["Revenue"],
            color: "#1976d2",
            fill: "gradient",
            // no include_totals → default false
          } as DataBarsRule,
        ],
      };
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs).toHaveLength(1);
      // Rows 2 (East) and 4 (West) only; subtotal row 3 excluded.
      expect(cfs[0]!.ref).toBe("B2 B4");
    });

    it("column-based: include_totals=true includes subtotal rows", () => {
      // Same grid; with include_totals=true, subtotal row 3 is included.
      const grid: ExportGrid = {
        cells: [
          [cell("Region", "header"), cell("Revenue", "header")],
          [cell("East", "data"), cell("100", "data", 100)],
          [cell("Subtotal", "subtotal"), cell("100", "subtotal", 100)],
          [cell("West", "data"), cell("200", "data", 200)],
          [cell("Total", "col-total"), cell("300", "col-total", 300)],
        ],
        headerRowCount: 1,
        rowDimCount: 1,
        valueFieldColumns: [{ field: "Revenue", columns: [1] }],
        conditionalFormatting: [
          {
            type: "color_scale",
            apply_to: ["Revenue"],
            min_color: "#ffffff",
            max_color: "#ff0000",
            include_totals: true,
          } as ColorScaleRule,
        ],
      };
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs).toHaveLength(1);
      // Rows 2 (East), 3 (Subtotal), 4 (West) — contiguous → range.
      expect(cfs[0]!.ref).toBe("B2:B4");
    });

    it("row-based: no scope with single field produces one per-field ref spanning all data columns", () => {
      // With one field, the per-field default produces the same output as
      // explicit scope="global" — one ref spanning all data columns.
      const grid = makeRowsGrid();
      grid.valueFieldRows = [
        { field: "Revenue", rows: [1, 2], subtotalRows: [] },
      ];
      grid.conditionalFormatting = [
        {
          type: "data_bars",
          apply_to: ["Revenue"],
          color: "#1976d2",
          fill: "gradient",
          // no scope → per-field default (same as global for a single field)
        } as DataBarsRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      expect(cfs).toHaveLength(1);
      expect(cfs[0]!.ref).toBe("C2:D2 C3:D3");
    });

    it("row-based: scope='global' with multiple fields produces one shared ref across all measures", () => {
      // Explicit scope="global" collapses all measure rows into one scale —
      // useful when all targeted fields share the same units.
      const grid = makeRowsGrid();
      grid.conditionalFormatting = [
        {
          type: "color_scale",
          apply_to: [],
          min_color: "#ffffff",
          max_color: "#ff0000",
          scope: "global",
        } as ColorScaleRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      // One CF entry covering all Revenue + Units rows
      expect(cfs).toHaveLength(1);
      expect(cfs[0]!.ref).toBe("C2:D2 C3:D3 C4:D4 C5:D5");
    });

    // ── scope is a no-op for threshold ─────────────────────────────────────

    it("column-based: threshold ignores scope='global' — produces one entry per column", () => {
      // Threshold rules must not be affected by scope; they always use
      // per-column refs so formula anchoring is unambiguous.
      const grid = makeGrid({
        valueFieldColumns: [{ field: "revenue", columns: [1, 2] }],
        conditionalFormatting: [
          {
            type: "threshold",
            apply_to: ["revenue"],
            scope: "global", // should be ignored for threshold
            conditions: [{ operator: "gt", value: 100, background: "#ff0000" }],
          } as ThresholdRule,
        ],
      });
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      // Identical to the no-scope default: one CF entry per target column.
      expect(cfs).toHaveLength(2);
      expect(cfs[0]!.ref).toBe("B2:B3");
      expect(cfs[1]!.ref).toBe("C2:C3");
    });

    it("row-based: threshold ignores scope='per_column' — produces single combined ref", () => {
      // Threshold rules must not be affected by scope; they always use a
      // single combined ref (all target rows × all data columns).
      const grid = makeRowsGrid();
      grid.valueFieldRows = [
        { field: "Revenue", rows: [1, 2], subtotalRows: [] },
      ];
      grid.conditionalFormatting = [
        {
          type: "threshold",
          apply_to: ["Revenue"],
          scope: "per_column", // should be ignored for threshold
          conditions: [{ operator: "gt", value: 100, background: "#ff0000" }],
        } as ThresholdRule,
      ];
      const wb = buildExcelWorkbook(ExcelJS, grid);
      const cfs = getCf(wb.worksheets[0]);
      // scope ignored: one CF entry with combined ref spanning all data cols
      expect(cfs).toHaveLength(1);
      expect(cfs[0]!.ref).toBe("C2:D2 C3:D3");
    });
  });
});
