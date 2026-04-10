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

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { PivotData, type DataRecord } from "./PivotData";
import {
  normalizeAggregationConfig,
  type AggregationConfig,
  type AggregationType,
  type PivotConfigV1,
} from "./types";

const EPS = 0.001;

const __dirname = dirname(fileURLToPath(import.meta.url));
const GOLDEN_DIR = resolve(__dirname, "../../../../tests/golden_data");

function loadCSV(filename: string): DataRecord[] {
  const raw = readFileSync(resolve(GOLDEN_DIR, filename), "utf-8");
  const lines = raw.trim().split("\n");
  const headers = lines[0]!.split(",");
  return lines.slice(1).map((line) => {
    const vals = line.split(",");
    const record: DataRecord = {};
    headers.forEach((h, i) => {
      const v = vals[i]!;
      const num = Number(v);
      record[h] = v !== "" && !isNaN(num) ? num : v;
    });
    return record;
  });
}

const golden = JSON.parse(
  readFileSync(resolve(GOLDEN_DIR, "golden_expected.json"), "utf-8"),
);
const smallRecords = loadCSV("small.csv");

type TestConfigOverrides = Partial<Omit<PivotConfigV1, "aggregation">> & {
  aggregation?: AggregationType | AggregationConfig;
};

function makeConfig(overrides: TestConfigOverrides = {}): PivotConfigV1 {
  const { aggregation: aggregationOverride, ...restOverrides } = overrides;
  const values = overrides.values ?? ["revenue"];
  const config = {
    version: 1 as const,
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

/** Sample data for structural invariant tests (renderer uses the same PivotData accessors for denominators). */
const SAMPLE_DATA: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100, profit: 40, units: 10 },
  { region: "US", year: "2024", revenue: 150, profit: 60, units: 15 },
  { region: "EU", year: "2023", revenue: 200, profit: 80, units: 20 },
  { region: "EU", year: "2024", revenue: 250, profit: 100, units: 25 },
  { region: "US", year: "2023", revenue: 50, profit: 20, units: 5 },
];

/** Two-level rows for subtotal invariants. */
const SUBTOTAL_SAMPLE: DataRecord[] = [
  { region: "US", dept: "East", year: "2023", revenue: 30 },
  { region: "US", dept: "East", year: "2024", revenue: 70 },
  { region: "US", dept: "West", year: "2023", revenue: 50 },
  { region: "US", dept: "West", year: "2024", revenue: 50 },
  { region: "EU", dept: "East", year: "2023", revenue: 100 },
  { region: "EU", dept: "East", year: "2024", revenue: 100 },
  { region: "EU", dept: "West", year: "2023", revenue: 80 },
  { region: "EU", dept: "West", year: "2024", revenue: 120 },
];

function expectClose(
  actual: number | null,
  expected: number,
  label: string,
): void {
  expect(
    actual,
    `${label}: expected ${expected}, got ${actual}`,
  ).not.toBeNull();
  expect(actual).toBeCloseTo(expected, 1);
}

function verifyMeasureAgainstGolden(
  pd: PivotData,
  g: {
    cells: Record<string, Record<string, number>>;
    row_totals: Record<string, number>;
    col_totals: Record<string, number>;
    grand_total: number;
  },
  valField: string,
  label: string,
): void {
  for (const [rowKeyStr, colVals] of Object.entries(g.cells)) {
    const rowKey = rowKeyStr.split("|");
    for (const [colKeyStr, expected] of Object.entries(
      colVals as Record<string, number>,
    )) {
      const colKey = colKeyStr.split("|");
      const actual = pd.getAggregator(rowKey, colKey, valField).value();
      expectClose(
        actual,
        expected,
        `${label} cell [${rowKeyStr}][${colKeyStr}]`,
      );
    }
  }
  for (const [rowKeyStr, expected] of Object.entries(g.row_totals)) {
    const rowKey = rowKeyStr.split("|");
    const actual = pd.getRowTotal(rowKey, valField).value();
    expectClose(actual, expected, `${label} row total [${rowKeyStr}]`);
  }
  for (const [colKeyStr, expected] of Object.entries(g.col_totals)) {
    const colKey = colKeyStr.split("|");
    const actual = pd.getColTotal(colKey, valField).value();
    expectClose(actual, expected, `${label} col total [${colKeyStr}]`);
  }
  const actualGrand = pd.getGrandTotal(valField).value();
  expectClose(actualGrand, g.grand_total, `${label} grand total`);
}

describe("show_values_as structural invariants (PivotData denominators)", () => {
  const valField = "revenue";

  it("pct_of_total: cell/grand ratios over all data cells sum to 1", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    const grand = pd.getGrandTotal(valField).value();
    expect(grand).not.toBeNull();
    expect(grand!).not.toBe(0);

    let sum = 0;
    for (const rowKey of pd.getRowKeys()) {
      for (const colKey of pd.getColKeys()) {
        const v = pd.getAggregator(rowKey, colKey, valField).value();
        if (v != null) sum += v / grand!;
      }
    }
    expect(sum).toBeCloseTo(1, 3);
    expect(Math.abs(sum - 1)).toBeLessThan(EPS);
  });

  it("pct_of_row: for each row, cell/row_total ratios across columns sum to 1", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    for (const rowKey of pd.getRowKeys()) {
      const rowTotal = pd.getRowTotal(rowKey, valField).value();
      expect(rowTotal).not.toBeNull();
      expect(rowTotal!).not.toBe(0);
      let sum = 0;
      for (const colKey of pd.getColKeys()) {
        const v = pd.getAggregator(rowKey, colKey, valField).value();
        if (v != null) sum += v / rowTotal!;
      }
      expect(sum).toBeCloseTo(1, 3);
      expect(Math.abs(sum - 1)).toBeLessThan(EPS);
    }
  });

  it("pct_of_col: for each column, cell/col_total ratios across rows sum to 1", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    for (const colKey of pd.getColKeys()) {
      const colTotal = pd.getColTotal(colKey, valField).value();
      expect(colTotal).not.toBeNull();
      expect(colTotal!).not.toBe(0);
      let sum = 0;
      for (const rowKey of pd.getRowKeys()) {
        const v = pd.getAggregator(rowKey, colKey, valField).value();
        if (v != null) sum += v / colTotal!;
      }
      expect(sum).toBeCloseTo(1, 3);
      expect(Math.abs(sum - 1)).toBeLessThan(EPS);
    }
  });

  it("pct_of_row + subtotals: subtotal row subcell/subtotal-row-total ratios sum to 1", () => {
    const pd = new PivotData(
      SUBTOTAL_SAMPLE,
      makeConfig({
        rows: ["region", "dept"],
        columns: ["year"],
        show_subtotals: true,
      }),
    );
    const colKeys = pd.getColKeys();
    for (const entry of pd.getGroupedRowKeys(true)) {
      if (entry.type !== "subtotal") continue;
      const parentKey = entry.key;
      const denom = pd.getSubtotalAggregator(parentKey, [], valField).value();
      expect(
        denom,
        `subtotal row total for ${parentKey.join("/")}`,
      ).not.toBeNull();
      expect(denom!).not.toBe(0);
      let sum = 0;
      for (const colKey of colKeys) {
        const v = pd.getSubtotalAggregator(parentKey, colKey, valField).value();
        if (v != null) sum += v / denom!;
      }
      expect(sum).toBeCloseTo(1, 3);
      expect(Math.abs(sum - 1)).toBeLessThan(EPS);
    }
  });

  it("pct_of_total + avg aggregator: no crash; pct uses cell avg / grand avg (not sum-1)", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig({ aggregation: "avg" }));
    const grand = pd.getGrandTotal(valField).value();
    expect(grand).not.toBeNull();
    const revenues = SAMPLE_DATA.map((r) => r.revenue as number);
    const overallAvg = revenues.reduce((a, b) => a + b, 0) / revenues.length;
    expect(grand).toBeCloseTo(overallAvg, 5);

    // Renderer would show (cell / grand); spot-check US × 2023 = avg(100,50)=75 → 75/150
    const us2023 = pd.getAggregator(["US"], ["2023"], valField).value();
    expect(us2023).toBeCloseTo(75, 5);
    expect(us2023! / grand!).toBeCloseTo(0.5, 5);

    // Unlike sum, Σ(cell_avg/grand_avg) over cells is not 1 — only assert finite ratios
    for (const rowKey of pd.getRowKeys()) {
      for (const colKey of pd.getColKeys()) {
        const v = pd.getAggregator(rowKey, colKey, valField).value();
        if (v != null) {
          expect(Number.isFinite(v / grand!)).toBe(true);
        }
      }
    }
  });

  it("pct_of_total + per-measure agg: revenue pct invariant holds; profit stays raw engine values", () => {
    const base = makeConfig({
      values: ["revenue", "profit"],
      aggregation: { revenue: "sum", profit: "avg" },
    });
    const withShowAs: PivotConfigV1 = {
      ...base,
      show_values_as: { revenue: "pct_of_total" },
    };

    const pdBase = new PivotData(SAMPLE_DATA, base);
    const pdShow = new PivotData(SAMPLE_DATA, withShowAs);

    for (const rowKey of pdBase.getRowKeys()) {
      for (const colKey of pdBase.getColKeys()) {
        expect(pdShow.getAggregator(rowKey, colKey, "revenue").value()).toBe(
          pdBase.getAggregator(rowKey, colKey, "revenue").value(),
        );
        expect(pdShow.getAggregator(rowKey, colKey, "profit").value()).toBe(
          pdBase.getAggregator(rowKey, colKey, "profit").value(),
        );
      }
    }

    const grandRev = pdShow.getGrandTotal("revenue").value();
    expect(grandRev).not.toBeNull();
    let sumRev = 0;
    for (const rowKey of pdShow.getRowKeys()) {
      for (const colKey of pdShow.getColKeys()) {
        const v = pdShow.getAggregator(rowKey, colKey, "revenue").value();
        if (v != null) sumRev += v / grandRev!;
      }
    }
    expect(Math.abs(sumRev - 1)).toBeLessThan(EPS);

    const grandProfit = pdShow.getGrandTotal("profit").value();
    expect(grandProfit).not.toBeNull();
    let sumProfitFrac = 0;
    for (const rowKey of pdShow.getRowKeys()) {
      for (const colKey of pdShow.getColKeys()) {
        const v = pdShow.getAggregator(rowKey, colKey, "profit").value();
        if (v != null && grandProfit) sumProfitFrac += v / grandProfit;
      }
    }
    expect(sumProfitFrac).not.toBeCloseTo(1, 1);
  });
});

describe("Golden Config C — Revenue sum vs Units avg (golden_expected.json + small.csv)", () => {
  const g = golden.C as {
    config: {
      rows: string[];
      columns: string[];
      values: string[];
      aggregation: AggregationConfig;
    };
    measures: {
      Revenue: {
        cells: Record<string, Record<string, number>>;
        row_totals: Record<string, number>;
        col_totals: Record<string, number>;
        grand_total: number;
      };
      Units: {
        cells: Record<string, Record<string, number>>;
        row_totals: Record<string, number>;
        col_totals: Record<string, number>;
        grand_total: number;
      };
    };
  };

  const config: PivotConfigV1 = {
    version: 1,
    rows: g.config.rows,
    columns: g.config.columns,
    values: g.config.values,
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    aggregation: normalizeAggregationConfig(
      g.config.aggregation,
      g.config.values,
    ),
  };

  it("Revenue cells and totals match pandas golden (sum)", () => {
    const pd = new PivotData(smallRecords, config);
    verifyMeasureAgainstGolden(
      pd,
      g.measures.Revenue,
      "Revenue",
      "Config C Revenue",
    );
  });

  it("Units cells and totals match pandas golden (avg)", () => {
    const pd = new PivotData(smallRecords, config);
    verifyMeasureAgainstGolden(pd, g.measures.Units, "Units", "Config C Units");
  });
});
