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
import type { ExportGrid, ExportCell, CellKind } from "./exportData";
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
  });
});
