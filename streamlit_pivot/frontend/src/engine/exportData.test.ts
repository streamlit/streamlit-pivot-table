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
import { PivotData, type DataRecord } from "./PivotData";
import {
  normalizeAggregationConfig,
  type AggregationConfig,
  type AggregationType,
  type PivotConfigV1,
} from "./types";
import {
  buildExportGrid,
  buildExportIR,
  gridToCSV,
  gridToTSV,
  patternToExcelFormat,
  type ExportGrid,
  type CellKind,
} from "./exportData";

type TestConfigOverrides = Partial<Omit<PivotConfigV1, "aggregation">> & {
  aggregation?: AggregationType | AggregationConfig;
};

function makeConfig(overrides: TestConfigOverrides = {}): PivotConfigV1 {
  const { aggregation: aggregationOverride, ...restOverrides } = overrides;
  const values = overrides.values ?? ["revenue"];
  const config = {
    version: 1,
    rows: ["region"],
    columns: ["year"],
    values,
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    ...restOverrides,
  } as PivotConfigV1;
  config.values = values;
  config.aggregation = normalizeAggregationConfig(aggregationOverride, values);
  return config;
}

const SAMPLE_DATA: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100, profit: 40 },
  { region: "US", year: "2024", revenue: 150, profit: 60 },
  { region: "EU", year: "2023", revenue: 200, profit: 80 },
  { region: "EU", year: "2024", revenue: 250, profit: 100 },
  { region: "US", year: "2023", revenue: 50, profit: 20 },
];

describe("buildExportGrid", () => {
  it("produces correct raw grid for basic pivot", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    // Header row: [region, 2023, 2024, Total]
    expect(grid[0]).toEqual(["region", "2023", "2024", "Total"]);
    // Data rows
    expect(grid[1]).toEqual(["EU", "200", "250", "450"]);
    expect(grid[2]).toEqual(["US", "150", "150", "300"]);
    // Column totals
    expect(grid[3]).toEqual(["Total", "350", "400", "750"]);
  });

  it("produces correct formatted grid with number_format", () => {
    const config = makeConfig({ number_format: { revenue: "$,.0f" } });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "formatted");

    // Data values should be formatted as currency
    expect(grid[1][1]).toMatch(/\$200/);
    expect(grid[1][2]).toMatch(/\$250/);
  });

  it("handles multiple values with separate columns", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    // First header row: col dim values (duplicated for each value field)
    expect(grid[0]).toEqual([
      "region",
      "2023",
      "2023",
      "2024",
      "2024",
      "Total",
      "Total",
    ]);
    // Second header row: value labels
    expect(grid[1]).toEqual([
      "",
      "revenue",
      "profit",
      "revenue",
      "profit",
      "revenue",
      "profit",
    ]);
    // EU data row
    expect(grid[2]).toEqual(["EU", "200", "80", "250", "100", "450", "180"]);
  });

  it("excludes row totals when show_row_totals is false", () => {
    const config = makeConfig({ show_row_totals: false });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    expect(grid[0]).toEqual(["region", "2023", "2024"]);
    expect(grid[1]).toEqual(["EU", "200", "250"]);
  });

  it("excludes column totals when show_column_totals is false", () => {
    const config = makeConfig({ show_column_totals: false });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    expect(grid.length).toBe(3); // header + 2 data rows, no totals row
    expect(grid[2]).toEqual(["US", "150", "150", "300"]);
  });

  it("handles no column dimensions", () => {
    const config = makeConfig({ columns: [] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    // With no column dimensions, row totals are suppressed (they'd duplicate the single data column)
    expect(grid[0]).toEqual(["region", "revenue"]);
    expect(grid[1]).toEqual(["EU", "450"]);
    expect(grid[2]).toEqual(["US", "300"]);
  });

  it("includes subtotal rows when show_subtotals is true", () => {
    const multiRowData: DataRecord[] = [
      { cat: "A", sub: "a1", year: "2023", revenue: 10 },
      { cat: "A", sub: "a2", year: "2023", revenue: 20 },
      { cat: "B", sub: "b1", year: "2023", revenue: 30 },
    ];
    const config = makeConfig({
      rows: ["cat", "sub"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
    });
    const pd = new PivotData(multiRowData, config);
    const grid = buildExportGrid(pd, config, "raw");

    // Find the subtotal rows
    const subtotalRows = grid.filter((r) =>
      r.some((cell) => cell === "Subtotal"),
    );
    expect(subtotalRows.length).toBeGreaterThan(0);
    // A subtotal should sum A's children: 10 + 20 = 30
    const aSubtotal = grid.find((r) => r[0] === "A" && r[1] === "Subtotal");
    expect(aSubtotal).toBeDefined();
    expect(aSubtotal![2]).toBe("30");
  });

  it("export includes all data rows even when groups are collapsed", () => {
    const multiRowData: DataRecord[] = [
      { cat: "A", sub: "a1", year: "2023", revenue: 10 },
      { cat: "A", sub: "a2", year: "2023", revenue: 20 },
      { cat: "B", sub: "b1", year: "2023", revenue: 30 },
    ];
    const config = makeConfig({
      rows: ["cat", "sub"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
      collapsed_groups: ["A"],
    });
    const pd = new PivotData(multiRowData, config);
    const grid = buildExportGrid(pd, config, "raw");

    // Even though group A is collapsed in the UI, export should include all data rows
    const dataRows = grid.filter(
      (r) =>
        r[0] !== "" &&
        r[0] !== "cat" &&
        !r.includes("Subtotal") &&
        r[0] !== "Total",
    );
    expect(dataRows.length).toBe(3); // a1, a2, b1 — all present
  });

  it("uses empty_cell_value for nulls in formatted mode", () => {
    const data: DataRecord[] = [{ region: "US", year: "2023", revenue: null }];
    const config = makeConfig({ empty_cell_value: "N/A" });
    const pd = new PivotData(data, config);
    const grid = buildExportGrid(pd, config, "formatted");

    expect(grid[1][1]).toBe("N/A");
  });

  it("uses empty string for nulls in raw mode", () => {
    const data: DataRecord[] = [{ region: "US", year: "2023", revenue: null }];
    const config = makeConfig();
    const pd = new PivotData(data, config);
    const grid = buildExportGrid(pd, config, "raw");

    expect(grid[1][1]).toBe("");
  });

  it("formatted mode without number_format uses correct total values", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "formatted");

    // Row totals should match each row's sum, not the grand total.
    // EU: 200 + 250 = 450, US: 150 (100+50) + 150 = 300
    const euRow = grid[1];
    const usRow = grid[2];
    const totalRow = grid[3];
    // Row total is the last cell in each data row
    expect(euRow[euRow.length - 1]).not.toBe(totalRow[totalRow.length - 1]);
    expect(euRow[euRow.length - 1]).toMatch(/450/);
    expect(usRow[usRow.length - 1]).toMatch(/300/);
    // Grand total
    expect(totalRow[totalRow.length - 1]).toMatch(/750/);
    // Column totals should match per-column sums, not the grand total
    expect(totalRow[1]).toMatch(/350/);
    expect(totalRow[2]).toMatch(/400/);
  });

  it("formatted mode applies number_format to subtotal row-total cells", () => {
    const multiRowData: DataRecord[] = [
      { cat: "A", sub: "a1", year: "2023", revenue: 10 },
      { cat: "A", sub: "a2", year: "2023", revenue: 20 },
      { cat: "B", sub: "b1", year: "2023", revenue: 30 },
    ];
    const config = makeConfig({
      rows: ["cat", "sub"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
      number_format: { revenue: "$,.0f" },
    });
    const pd = new PivotData(multiRowData, config);
    const grid = buildExportGrid(pd, config, "formatted");

    // Find A subtotal row — its row-total cell should be formatted with $
    const aSubtotal = grid.find((r) => r[0] === "A" && r[1] === "Subtotal");
    expect(aSubtotal).toBeDefined();
    const rowTotalCell = aSubtotal![aSubtotal!.length - 1];
    expect(rowTotalCell).toMatch(/\$30/);
  });

  it("formatted mode applies show_values_as to subtotal rows", () => {
    const multiRowData: DataRecord[] = [
      { cat: "A", sub: "a1", year: "2023", revenue: 100 },
      { cat: "A", sub: "a2", year: "2023", revenue: 200 },
      { cat: "B", sub: "b1", year: "2023", revenue: 300 },
    ];
    const config = makeConfig({
      rows: ["cat", "sub"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
      show_values_as: { revenue: "pct_of_total" },
    });
    const pd = new PivotData(multiRowData, config);
    const grid = buildExportGrid(pd, config, "formatted");

    // Grand total = 600.  A subtotal = 300 = 50% of total.
    const aSubtotal = grid.find((r) => r[0] === "A" && r[1] === "Subtotal");
    expect(aSubtotal).toBeDefined();
    expect(aSubtotal![2]).toMatch(/50/);
    expect(aSubtotal![2]).toMatch(/%/);
  });

  it("handles multi-level column keys", () => {
    const data: DataRecord[] = [
      { region: "US", year: "2023", quarter: "Q1", revenue: 100 },
      { region: "US", year: "2023", quarter: "Q2", revenue: 200 },
    ];
    const config = makeConfig({ columns: ["year", "quarter"] });
    const pd = new PivotData(data, config);
    const grid = buildExportGrid(pd, config, "raw");

    // Two header rows for two column levels
    expect(grid[0][1]).toBe("2023");
    expect(grid[0][2]).toBe("2023");
    expect(grid[1][1]).toBe("Q1");
    expect(grid[1][2]).toBe("Q2");
  });

  it("emits empty string for excluded row total measures", () => {
    const config = makeConfig({
      values: ["revenue", "profit"],
      show_row_totals: ["revenue"],
    });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    // Data rows: last two columns are row totals (revenue, profit)
    // EU row: revenue total has value, profit total is excluded (empty)
    const euRow = grid[2];
    const revenueTotalIdx = euRow.length - 2;
    const profitTotalIdx = euRow.length - 1;
    expect(euRow[revenueTotalIdx]).not.toBe("");
    expect(euRow[profitTotalIdx]).toBe("");
  });

  it("emits empty string for excluded column total measures", () => {
    const config = makeConfig({
      values: ["revenue", "profit"],
      show_column_totals: ["revenue"],
    });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    // Totals row: Total, 2023_rev, 2023_profit, 2024_rev, 2024_profit, grand_rev, grand_profit
    // With show_column_totals: ["revenue"], profit column totals (indices 2, 4) are excluded
    const totalRow = grid[grid.length - 1];
    const profitColTotalIndices = [2, 4]; // profit columns for 2023 and 2024
    for (const idx of profitColTotalIndices) {
      expect(totalRow[idx]).toBe("");
    }
  });

  it("emits empty string for excluded grand total measures", () => {
    const config = makeConfig({
      values: ["revenue", "profit"],
      show_row_totals: ["revenue"],
      show_column_totals: ["revenue"],
    });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    // Totals row: last two cells are grand total (revenue, profit)
    // Profit grand total is excluded
    const totalRow = grid[grid.length - 1];
    const grandRevenueIdx = totalRow.length - 2;
    const grandProfitIdx = totalRow.length - 1;
    expect(totalRow[grandRevenueIdx]).not.toBe("");
    expect(totalRow[grandProfitIdx]).toBe("");
  });

  const MULTI_DIM_DATA: DataRecord[] = [
    { region: "US", category: "A", year: "2023", revenue: 100, profit: 40 },
    { region: "US", category: "A", year: "2024", revenue: 120, profit: 50 },
    { region: "US", category: "B", year: "2023", revenue: 150, profit: 60 },
    { region: "US", category: "B", year: "2024", revenue: 180, profit: 70 },
  ];

  it("subtotal data cells are unaffected by show_column_totals exclusion", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue", "profit"],
      show_subtotals: true,
      show_column_totals: ["revenue"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    const subtotalRows = grid.filter((row) =>
      row.some((cell) => cell === "Subtotal"),
    );
    expect(subtotalRows.length).toBeGreaterThan(0);

    for (const row of subtotalRows) {
      const dataCellStart = 2;
      const numColDataCells = 2 * 2;
      for (let i = dataCellStart; i < dataCellStart + numColDataCells; i++) {
        expect(row[i]).not.toBe("");
      }
    }
  });

  it("emits empty string for excluded measures in subtotal row totals", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue", "profit"],
      show_subtotals: true,
      show_row_totals: ["revenue"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");

    const subtotalRows = grid.filter((row) =>
      row.some((cell) => cell === "Subtotal"),
    );
    expect(subtotalRows.length).toBeGreaterThan(0);

    for (const row of subtotalRows) {
      const profitTotalIdx = row.length - 1;
      const revenueTotalIdx = row.length - 2;
      expect(row[revenueTotalIdx]).not.toBe("");
      expect(row[profitTotalIdx]).toBe("");
    }
  });

  it("includes synthetic measure columns and labels in export", () => {
    const config = makeConfig({
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "rev_minus_profit",
          label: "Revenue - Profit",
          operation: "difference",
          numerator: "revenue",
          denominator: "profit",
        },
      ],
    });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "raw");
    expect(grid[1]).toContain("Revenue - Profit");
    // EU 2023 synthetic value = 200 - 80 = 120
    expect(grid[2]).toContain("120");
  });

  it("does not apply show_values_as to synthetic measure exports", () => {
    const config = makeConfig({
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "rev_minus_profit",
          label: "Revenue - Profit",
          operation: "difference",
          numerator: "revenue",
          denominator: "profit",
        },
      ],
      show_values_as: {
        rev_minus_profit: "pct_of_total",
      },
    });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "formatted");
    // EU 2023 synthetic value should remain a numeric value, not a percentage.
    expect(grid[2].join(" ")).toContain("120");
    expect(grid[2].join(" ")).not.toContain("%");
  });

  it("applies synthetic measure-specific format in formatted exports", () => {
    const config = makeConfig({
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "rev_per_profit",
          label: "Revenue / Profit",
          operation: "sum_over_sum",
          numerator: "revenue",
          denominator: "profit",
          format: ".1%",
        },
      ],
    });
    const pd = new PivotData(SAMPLE_DATA, config);
    const grid = buildExportGrid(pd, config, "formatted");
    expect(grid[2].join(" ")).toContain("%");
  });
});

describe("gridToCSV", () => {
  it("serializes a simple grid", () => {
    const grid = [
      ["a", "b"],
      ["1", "2"],
    ];
    expect(gridToCSV(grid)).toBe("a,b\n1,2");
  });

  it("escapes commas in fields", () => {
    const grid = [["hello, world", "ok"]];
    expect(gridToCSV(grid)).toBe('"hello, world",ok');
  });

  it("escapes double quotes in fields", () => {
    const grid = [['say "hi"', "ok"]];
    expect(gridToCSV(grid)).toBe('"say ""hi""",ok');
  });

  it("escapes newlines in fields", () => {
    const grid = [["line1\nline2", "ok"]];
    expect(gridToCSV(grid)).toBe('"line1\nline2",ok');
  });
});

describe("gridToTSV", () => {
  it("serializes a simple grid", () => {
    const grid = [
      ["a", "b"],
      ["1", "2"],
    ];
    expect(gridToTSV(grid)).toBe("a\tb\n1\t2");
  });

  it("escapes tabs in fields", () => {
    const grid = [["hello\tworld", "ok"]];
    expect(gridToTSV(grid)).toBe('"hello\tworld"\tok');
  });
});

// ---------------------------------------------------------------------------
// Shared IR (buildExportIR)
// ---------------------------------------------------------------------------

describe("buildExportIR", () => {
  it("returns correct headerRowCount and rowDimCount for basic pivot", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.headerRowCount).toBe(1);
    expect(ir.rowDimCount).toBe(1);
  });

  it("returns correct headerRowCount with multiple column dims", () => {
    const data: DataRecord[] = [
      { a: "x", b: "p", c: "1", val: 10 },
      { a: "x", b: "q", c: "2", val: 20 },
    ];
    const config = makeConfig({
      rows: ["a"],
      columns: ["b", "c"],
      values: ["val"],
    });
    const pd = new PivotData(data, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.headerRowCount).toBe(2);
    expect(ir.rowDimCount).toBe(1);
  });

  it("returns correct headerRowCount with no columns and single value", () => {
    const config = makeConfig({
      columns: [],
    });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.headerRowCount).toBe(1);
    expect(ir.rowDimCount).toBe(1);
  });

  it("tags header cells with kind='header'", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    for (const c of ir.cells[0]) {
      expect(c.kind).toBe("header");
    }
  });

  it("tags data cells correctly", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.cells[1][0].kind).toBe("data");
    expect(ir.cells[1][1].kind).toBe("data");
  });

  it("tags column total row cells as col-total", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    const lastRow = ir.cells[ir.cells.length - 1];
    expect(lastRow[0].kind).toBe("col-total");
    expect(lastRow[0].display).toBe("Total");
  });

  it("tags grand total cell as grand-total", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    const lastRow = ir.cells[ir.cells.length - 1];
    const lastCell = lastRow[lastRow.length - 1];
    expect(lastCell.kind).toBe("grand-total");
  });

  it("tags row total cells as row-total", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    const dataRow = ir.cells[1];
    const lastCell = dataRow[dataRow.length - 1];
    expect(lastCell.kind).toBe("row-total");
  });

  it("tags subtotal rows correctly", () => {
    const data: DataRecord[] = [
      { a: "X", b: "1", val: 10 },
      { a: "X", b: "2", val: 20 },
      { a: "Y", b: "3", val: 30 },
    ];
    const config = makeConfig({
      rows: ["a", "b"],
      columns: [],
      values: ["val"],
      show_subtotals: true,
    });
    const pd = new PivotData(data, config);
    const ir = buildExportIR(pd, config, "raw");

    const subtotalRows = ir.cells.filter((row) =>
      row.some((c) => c.kind === "subtotal"),
    );
    expect(subtotalRows.length).toBeGreaterThan(0);
    for (const row of subtotalRows) {
      for (const c of row) {
        expect(c.kind).toBe("subtotal");
      }
    }
  });

  it("provides raw numeric values on data cells", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    const euRow = ir.cells[1];
    expect(euRow[1].raw).toBe(200);
    expect(euRow[2].raw).toBe(250);
  });

  it("provides raw numeric values on formatted cells", () => {
    const config = makeConfig({ number_format: { revenue: "$,.0f" } });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "formatted");

    const euRow = ir.cells[1];
    expect(euRow[1].raw).toBe(200);
    expect(euRow[1].display).toMatch(/\$200/);
  });

  it("populates numberFormat from config.number_format", () => {
    const config = makeConfig({ number_format: { revenue: "$,.2f" } });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "formatted");

    const euRow = ir.cells[1];
    expect(euRow[1].numberFormat).toBe("$#,##0.00");
  });

  it("populates numberFormat for show_values_as percentages", () => {
    const config = makeConfig({
      show_values_as: { revenue: "pct_of_total" },
    });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "formatted");

    const euRow = ir.cells[1];
    expect(euRow[1].numberFormat).toBe("0.0%");
  });

  it("sets mergeRight on multi-value column headers", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    const headerRow = ir.cells[0];
    const firstDataHeader = headerRow[1];
    expect(firstDataHeader.mergeRight).toBe(1);
  });

  it("sets mergeDown for row dimension labels when repeat_row_labels is false", () => {
    const data: DataRecord[] = [
      { a: "X", b: "1", val: 10 },
      { a: "X", b: "2", val: 20 },
      { a: "Y", b: "3", val: 30 },
    ];
    const config = makeConfig({
      rows: ["a", "b"],
      columns: [],
      values: ["val"],
      repeat_row_labels: false,
    });
    const pd = new PivotData(data, config);
    const ir = buildExportIR(pd, config, "raw");

    const xRow = ir.cells.find(
      (row) => row[0].display === "X" && row[0].kind === "data",
    );
    expect(xRow).toBeDefined();
    expect(xRow![0].mergeDown).toBe(1);
  });

  it("does not set mergeDown when repeat_row_labels is true", () => {
    const data: DataRecord[] = [
      { a: "X", b: "1", val: 10 },
      { a: "X", b: "2", val: 20 },
      { a: "Y", b: "3", val: 30 },
    ];
    const config = makeConfig({
      rows: ["a", "b"],
      columns: [],
      values: ["val"],
      repeat_row_labels: true,
    });
    const pd = new PivotData(data, config);
    const ir = buildExportIR(pd, config, "raw");

    for (const row of ir.cells) {
      for (const c of row) {
        expect(c.mergeDown).toBeUndefined();
      }
    }
  });

  it("IR display values match buildExportGrid output", () => {
    const config = makeConfig({ number_format: { revenue: "$,.0f" } });
    const pd = new PivotData(SAMPLE_DATA, config);
    const stringGrid = buildExportGrid(pd, config, "formatted");
    const ir = buildExportIR(pd, config, "formatted");

    for (let r = 0; r < stringGrid.length; r++) {
      for (let c = 0; c < stringGrid[r].length; c++) {
        expect(ir.cells[r][c].display).toBe(stringGrid[r][c]);
      }
    }
  });
});

// ---------------------------------------------------------------------------
// buildExportIR: valueFieldColumns and conditionalFormatting
// ---------------------------------------------------------------------------

describe("buildExportIR valueFieldColumns", () => {
  it("maps single value field to correct column indices", () => {
    const config = makeConfig({ values: ["revenue"] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.valueFieldColumns).toBeDefined();
    expect(ir.valueFieldColumns!.length).toBe(1);
    expect(ir.valueFieldColumns![0].field).toBe("revenue");
    for (const col of ir.valueFieldColumns![0].columns) {
      expect(col).toBeGreaterThanOrEqual(ir.rowDimCount);
    }
  });

  it("maps multiple value fields with correct per-field indices", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.valueFieldColumns!.length).toBe(2);
    expect(ir.valueFieldColumns![0].field).toBe("revenue");
    expect(ir.valueFieldColumns![1].field).toBe("profit");

    const revCols = ir.valueFieldColumns![0].columns;
    const profCols = ir.valueFieldColumns![1].columns;
    expect(revCols.length).toBe(profCols.length);
    for (let i = 0; i < revCols.length; i++) {
      expect(profCols[i]).toBe(revCols[i] + 1);
    }
  });

  it("handles no column dimensions (single column group)", () => {
    const config = makeConfig({ columns: [], values: ["revenue"] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.valueFieldColumns!.length).toBe(1);
    expect(ir.valueFieldColumns![0].columns).toEqual([ir.rowDimCount]);
  });

  it("column indices don't overlap between different value fields", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    const allCols = ir.valueFieldColumns!.flatMap((vf) => vf.columns);
    const unique = new Set(allCols);
    expect(unique.size).toBe(allCols.length);
  });

  it("column count matches number of column keys (data columns only)", () => {
    const config = makeConfig({ values: ["revenue"], show_totals: true });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    // 2 year columns from SAMPLE_DATA; total columns are separate
    const cols = ir.valueFieldColumns![0].columns;
    expect(cols.length).toBe(2);
    expect(cols[0]).toBe(ir.rowDimCount);
    expect(cols[1]).toBe(ir.rowDimCount + 1);
  });
});

describe("buildExportIR conditionalFormatting passthrough", () => {
  it("passes conditional_formatting from config to IR", () => {
    const config = makeConfig({ values: ["revenue"] });
    (config as unknown as Record<string, unknown>).conditional_formatting = [
      {
        type: "data_bars",
        apply_to: ["revenue"],
        color: "#123",
        fill: "solid",
      },
    ];
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.conditionalFormatting).toBeDefined();
    expect(ir.conditionalFormatting!.length).toBe(1);
    expect(ir.conditionalFormatting![0].type).toBe("data_bars");
  });

  it("does not set conditionalFormatting when config has none", () => {
    const config = makeConfig({ values: ["revenue"] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.conditionalFormatting).toBeUndefined();
  });

  it("does not set conditionalFormatting when config has empty array", () => {
    const config = makeConfig({ values: ["revenue"] });
    (config as unknown as Record<string, unknown>).conditional_formatting = [];
    const pd = new PivotData(SAMPLE_DATA, config);
    const ir = buildExportIR(pd, config, "raw");

    expect(ir.conditionalFormatting).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// patternToExcelFormat
// ---------------------------------------------------------------------------

describe("patternToExcelFormat", () => {
  it("converts grouped currency", () => {
    expect(patternToExcelFormat("$,.0f")).toBe("$#,##0");
    expect(patternToExcelFormat("$,.2f")).toBe("$#,##0.00");
  });

  it("converts grouped decimal", () => {
    expect(patternToExcelFormat(",.2f")).toBe("#,##0.00");
    expect(patternToExcelFormat(",.0f")).toBe("#,##0");
  });

  it("converts percent", () => {
    expect(patternToExcelFormat(".1%")).toBe("0.0%");
    expect(patternToExcelFormat(".2%")).toBe("0.00%");
    expect(patternToExcelFormat(".0%")).toBe("0%");
  });

  it("converts euro currency", () => {
    expect(patternToExcelFormat("€,.2f")).toBe("€#,##0.00");
  });

  it("converts pound currency", () => {
    expect(patternToExcelFormat("£,.0f")).toBe("£#,##0");
  });

  it("returns undefined for unrecognized patterns", () => {
    expect(patternToExcelFormat("garbage")).toBeUndefined();
    expect(patternToExcelFormat("")).toBeUndefined();
  });
});
