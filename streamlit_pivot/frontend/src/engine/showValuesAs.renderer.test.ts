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

/**
 * Tests for show_values_as percentage computation at the RENDERER/EXPORT
 * layer (formatTotalCellValue / formatExportTotalValue).
 *
 * The golden suite tests PivotData engine math (correct). These tests verify
 * the separate formatting layer that combines raw engine values into
 * displayed percentages — specifically for subtotal rows where the bugs were.
 */

import { describe, expect, it } from "vitest";
import { PivotData, type DataRecord } from "./PivotData";
import { buildExportGrid } from "./exportData";
import {
  normalizeAggregationConfig,
  type AggregationConfig,
  type AggregationType,
  type PivotConfigV1,
} from "./types";

const SAMPLE: DataRecord[] = [
  { region: "US", dept: "East", year: "2023", revenue: 100 },
  { region: "US", dept: "East", year: "2024", revenue: 200 },
  { region: "US", dept: "West", year: "2023", revenue: 300 },
  { region: "US", dept: "West", year: "2024", revenue: 400 },
  { region: "EU", dept: "North", year: "2023", revenue: 500 },
  { region: "EU", dept: "North", year: "2024", revenue: 600 },
  { region: "EU", dept: "South", year: "2023", revenue: 700 },
  { region: "EU", dept: "South", year: "2024", revenue: 800 },
];
// Grand total = 3600
// Col totals: 2023=1600, 2024=2000
// US: 2023=400, 2024=600, total=1000
// EU: 2023=1200, 2024=1400, total=2600

type TestConfigOverrides = Partial<Omit<PivotConfigV1, "aggregation">> & {
  aggregation?: AggregationType | AggregationConfig;
};

function makeConfig(overrides: TestConfigOverrides = {}): PivotConfigV1 {
  const { aggregation: agg, ...rest } = overrides;
  const values = overrides.values ?? ["revenue"];
  const config = {
    version: 1 as const,
    rows: ["region"],
    columns: ["year"],
    values,
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    ...rest,
  } as PivotConfigV1;
  config.values = values;
  config.aggregation = normalizeAggregationConfig(agg, values);
  return config;
}

function parsePercent(text: string): number | null {
  const cleaned = text.trim();
  if (cleaned === "-" || cleaned === "" || cleaned === "–") return null;
  if (cleaned.endsWith("%")) return parseFloat(cleaned.slice(0, -1));
  return parseFloat(cleaned);
}

function findRow(
  grid: string[][],
  col0: string,
  col1: string,
): string[] | undefined {
  return grid.find((r) => r[0] === col0 && r[1] === col1);
}

describe("show_values_as subtotal formatting (renderer/export layer)", () => {
  describe("pct_of_row with subtotals", () => {
    const config = makeConfig({
      rows: ["region", "dept"],
      columns: ["year"],
      aggregation: "sum",
      show_subtotals: true,
      show_values_as: { revenue: "pct_of_row" },
    });
    const pd = new PivotData(SAMPLE, config);
    const grid = buildExportGrid(pd, config, "formatted");

    it("leaf data cells compute pct_of_row correctly", () => {
      // US East: 2023=100, 2024=200, total=300 → 33.3%, 66.7%
      const usEast = findRow(grid, "US", "East");
      expect(usEast).toBeDefined();
      expect(parsePercent(usEast![2]!)).toBeCloseTo(33.3, 0);
      expect(parsePercent(usEast![3]!)).toBeCloseTo(66.7, 0);
    });

    it("subtotal row total shows 100%", () => {
      const usSub = findRow(grid, "US", "Subtotal");
      expect(usSub).toBeDefined();
      expect(parsePercent(usSub![4]!)).toBeCloseTo(100, 0);
    });

    it("subtotal data cells show correct pct_of_row", () => {
      // US subtotal: 2023=400, 2024=600, total=1000
      // pct_of_row: 400/1000=40%, 600/1000=60%
      const usSub = findRow(grid, "US", "Subtotal");
      expect(usSub).toBeDefined();
      expect(parsePercent(usSub![2]!)).toBeCloseTo(40.0, 0);
      expect(parsePercent(usSub![3]!)).toBeCloseTo(60.0, 0);
    });

    it("EU subtotal data cells show correct pct_of_row", () => {
      // EU subtotal: 2023=1200, 2024=1400, total=2600
      // pct_of_row: 1200/2600=46.2%, 1400/2600=53.8%
      const euSub = findRow(grid, "EU", "Subtotal");
      expect(euSub).toBeDefined();
      expect(parsePercent(euSub![2]!)).toBeCloseTo(46.2, 0);
      expect(parsePercent(euSub![3]!)).toBeCloseTo(53.8, 0);
    });
  });

  describe("pct_of_col with subtotals", () => {
    const config = makeConfig({
      rows: ["region", "dept"],
      columns: ["year"],
      aggregation: "sum",
      show_subtotals: true,
      show_values_as: { revenue: "pct_of_col" },
    });
    const pd = new PivotData(SAMPLE, config);
    const grid = buildExportGrid(pd, config, "formatted");

    it("leaf data cells use column total as denominator", () => {
      // US East 2023: 100/1600 = 6.25%
      const usEast = findRow(grid, "US", "East");
      expect(usEast).toBeDefined();
      expect(parsePercent(usEast![2]!)).toBeCloseTo(6.25, 0);
    });

    it("column totals show 100%", () => {
      const totalRow = findRow(grid, "Total", "");
      expect(totalRow).toBeDefined();
      expect(parsePercent(totalRow![2]!)).toBeCloseTo(100, 0);
      expect(parsePercent(totalRow![3]!)).toBeCloseTo(100, 0);
    });

    it("subtotal cells use column total as denominator (not grand total)", () => {
      // US subtotal for 2023 = 400, col total 2023 = 1600
      // Correct: 400/1600 = 25%
      const usSub = findRow(grid, "US", "Subtotal");
      expect(usSub).toBeDefined();
      expect(parsePercent(usSub![2]!)).toBeCloseTo(25.0, 0);

      // US subtotal for 2024 = 600, col total 2024 = 2000
      // Correct: 600/2000 = 30%
      expect(parsePercent(usSub![3]!)).toBeCloseTo(30.0, 0);
    });

    it("EU subtotal cells use column total as denominator", () => {
      // EU subtotal for 2023 = 1200, col total 2023 = 1600 → 75%
      // EU subtotal for 2024 = 1400, col total 2024 = 2000 → 70%
      const euSub = findRow(grid, "EU", "Subtotal");
      expect(euSub).toBeDefined();
      expect(parsePercent(euSub![2]!)).toBeCloseTo(75.0, 0);
      expect(parsePercent(euSub![3]!)).toBeCloseTo(70.0, 0);
    });

    it("subtotal pct_of_col values for a column sum to 100%", () => {
      const usSub = findRow(grid, "US", "Subtotal");
      const euSub = findRow(grid, "EU", "Subtotal");
      expect(usSub).toBeDefined();
      expect(euSub).toBeDefined();

      const us2023 = parsePercent(usSub![2]!)!;
      const eu2023 = parsePercent(euSub![2]!)!;
      expect(us2023 + eu2023).toBeCloseTo(100, 0);
    });
  });

  describe("pct_of_total — baseline correctness", () => {
    const config = makeConfig({
      aggregation: "sum",
      show_values_as: { revenue: "pct_of_total" },
    });
    const pd = new PivotData(SAMPLE, config);
    const grid = buildExportGrid(pd, config, "formatted");

    it("data cell percentages sum to ~100%", () => {
      const headerRow = grid[0]!;
      const numCols = headerRow.length;
      let sum = 0;
      let count = 0;
      for (let r = 1; r < grid.length; r++) {
        const row = grid[r]!;
        if (row[0] === "Grand Total" || row[0] === "Total") continue;
        for (let c = 1; c < numCols - 1; c++) {
          const pct = parsePercent(row[c]!);
          if (pct !== null) {
            sum += pct;
            count++;
          }
        }
      }
      expect(count).toBeGreaterThan(0);
      expect(Math.abs(sum - 100)).toBeLessThan(2);
    });
  });
});

// ── 0.5.0 analytical modes — renderer/export layer ───────────────────────────

import {
  getRunningTotal,
  getPctRunningTotal,
  getRank,
  getPctOfParent,
  getIndex,
} from "./showValuesAs";
import { buildExportIR } from "./exportData";

/**
 * Two-region, two-year dataset.
 * US: 2023=100, 2024=200; EU: 2023=300, 2024=400
 * Col totals: 2023=400, 2024=600; grand=1000
 */
const ANALYTICAL_SAMPLE: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100 },
  { region: "US", year: "2024", revenue: 200 },
  { region: "EU", year: "2023", revenue: 300 },
  { region: "EU", year: "2024", revenue: 400 },
];

describe("0.5.0 analytical modes — engine helpers via renderer layer", () => {
  describe("running_total", () => {
    it("produces correct running total in display order", () => {
      const config = makeConfig({
        show_values_as: { revenue: "running_total" },
      });
      const pd = new PivotData(ANALYTICAL_SAMPLE, config);
      const rowKeys = pd.getSortedLeafRowKeys();
      const colKey = ["2023"];

      // First row: RT = its raw value; second row: RT = first + second
      const rtFirst = getRunningTotal(pd, rowKeys[0]!, colKey, "revenue");
      const rtSecond = getRunningTotal(pd, rowKeys[1]!, colKey, "revenue");
      expect(rtFirst).not.toBeNull();
      expect(rtSecond).not.toBeNull();
      const rawFirst = pd
        .getAggregator(rowKeys[0]!, colKey, "revenue")
        .value()!;
      const rawSecond = pd
        .getAggregator(rowKeys[1]!, colKey, "revenue")
        .value()!;
      expect(rtFirst).toBeCloseTo(rawFirst, 1);
      expect(rtSecond).toBeCloseTo(rawFirst + rawSecond, 1);
    });
  });

  describe("pct_running_total — export", () => {
    it("export cell value equals running_total / parent_group_total (not raw value)", () => {
      // Single-level: denominator is column grand total
      const config = makeConfig({
        show_values_as: { revenue: "pct_running_total" },
      });
      const pd = new PivotData(ANALYTICAL_SAMPLE, config);
      const rowKeys = pd.getSortedLeafRowKeys();
      const colKey = ["2023"];

      const colTotal2023 = pd.getColTotal(colKey, "revenue").value()!; // 400
      const rt = getRunningTotal(pd, rowKeys[0]!, colKey, "revenue")!;
      const expectedPct = rt / colTotal2023;

      const pct = getPctRunningTotal(pd, rowKeys[0]!, colKey, "revenue");
      expect(pct).not.toBeNull();
      expect(pct).toBeCloseTo(expectedPct, 3);

      // Verify the export grid reflects pct_running_total (not raw)
      const grid = buildExportIR(pd, config, "formatted");
      // Find a data row that has a cell with a "%" display (a value cell, not a label)
      const pctCell = grid.cells
        .flat()
        .find((c) => c.kind === "data" && c.display.includes("%"));
      expect(pctCell).toBeDefined();
    });

    it("total row shows raw aggregate (not 100%)", () => {
      const config = makeConfig({
        show_values_as: { revenue: "pct_running_total" },
        show_totals: true,
      });
      const pd = new PivotData(ANALYTICAL_SAMPLE, config);
      // Grand total cell should show the raw grand total value, not "100.0%"
      const grandTotal = pd.getGrandTotal("revenue").value()!;
      // The export grand total cell should be the raw aggregate
      const grid = buildExportIR(pd, config, "formatted");
      const gtRow = grid.cells.find((row) =>
        row.some((c) => c.kind === "grand-total"),
      );
      expect(gtRow).toBeDefined();
      const gtCell = gtRow!.find((c) => c.kind === "grand-total");
      expect(gtCell).toBeDefined();
      // Grand total for pct_running_total should not be a percentage
      expect(gtCell!.display).not.toContain("%");
    });
  });

  describe("rank", () => {
    it("assigns competition rank correctly", () => {
      const config = makeConfig({ show_values_as: { revenue: "rank" } });
      const pd = new PivotData(ANALYTICAL_SAMPLE, config);
      const rowKeys = pd.getSortedLeafRowKeys();
      const colKey = ["2023"];

      // US=100, EU=300 — EU should be rank 1, US rank 2
      const ranks = rowKeys.map((rk) => getRank(pd, rk, colKey, "revenue"));
      expect(ranks).toContain(1);
      expect(ranks).toContain(2);
      // The rank-1 row has value 300 (EU)
      const rank1Idx = ranks.indexOf(1);
      const rawAtRank1 = pd
        .getAggregator(rowKeys[rank1Idx]!, colKey, "revenue")
        .value();
      expect(rawAtRank1).toBeCloseTo(300, 1);
    });
  });

  describe("pct_of_parent", () => {
    it("denominator is column grand total for single-level pivot", () => {
      const config = makeConfig({
        show_values_as: { revenue: "pct_of_parent" },
      });
      const pd = new PivotData(ANALYTICAL_SAMPLE, config);
      // US 2023=100; col total 2023=400 → 100/400=25%
      const pct = getPctOfParent(100, pd, ["US"], ["2023"], "revenue");
      expect(pct).not.toBeNull();
      expect(pct).toBeCloseTo(0.25, 3);
    });
  });

  describe("index", () => {
    it("index for a cell matches Excel formula", () => {
      const config = makeConfig({ show_values_as: { revenue: "index" } });
      const pd = new PivotData(ANALYTICAL_SAMPLE, config);
      // cell=100(US,2023), grand=1000, rowTotal_US=300, colTotal_2023=400
      // index = 100*1000/(300*400) = 100000/120000 ≈ 0.833
      const idx = getIndex(100, pd, ["US"], ["2023"], "revenue");
      expect(idx).not.toBeNull();
      expect(idx).toBeCloseTo((100 * 1000) / (300 * 400), 3);
    });

    it("returns null for null raw value", () => {
      const config = makeConfig({ show_values_as: { revenue: "index" } });
      const pd = new PivotData(ANALYTICAL_SAMPLE, config);
      const idx = getIndex(null, pd, ["US"], ["2023"], "revenue");
      expect(idx).toBeNull();
    });
  });
});
