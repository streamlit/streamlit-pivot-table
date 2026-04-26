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
  type ShowValuesAs,
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

describe("date-grain period comparisons", () => {
  const DATE_SAMPLE: DataRecord[] = [
    { region: "US", order_date: "2024-01-03", revenue: 100 },
    { region: "US", order_date: "2024-02-10", revenue: 150 },
    { region: "US", order_date: "2025-01-08", revenue: 130 },
    { region: "EU", order_date: "2024-01-04", revenue: 80 },
    { region: "EU", order_date: "2024-02-12", revenue: 95 },
    { region: "EU", order_date: "2025-01-09", revenue: 90 },
  ];

  function makeDateConfig(showAs: ShowValuesAs): PivotConfigV1 {
    return makeConfig({
      rows: ["region"],
      columns: ["order_date"],
      values: ["revenue"],
      date_grains: { order_date: "month" },
      show_values_as: { revenue: showAs },
    });
  }

  it("computes change vs previous period using grouped month buckets", () => {
    const pd = new PivotData(DATE_SAMPLE, makeDateConfig("diff_from_prev"), {
      columnTypes: new Map([["order_date", "date"]]),
    });
    expect(pd.getColKeys()).toEqual([["2024-01"], ["2024-02"], ["2025-01"]]);
    expect(
      pd.getCellComparisonValue(
        ["US"],
        ["2024-02"],
        "revenue",
        "diff_from_prev",
      ),
    ).toBe(50);
    expect(
      pd.getColTotalComparisonValue(["2024-02"], "revenue", "diff_from_prev"),
    ).toBe(65);
  });

  it("computes change vs prior year on the grouped temporal axis", () => {
    const pd = new PivotData(
      DATE_SAMPLE,
      makeDateConfig("diff_from_prev_year"),
      {
        columnTypes: new Map([["order_date", "date"]]),
      },
    );
    expect(
      pd.getCellComparisonValue(
        ["US"],
        ["2025-01"],
        "revenue",
        "diff_from_prev_year",
      ),
    ).toBe(30);
    expect(
      pd.getColTotalComparisonValue(
        ["2025-01"],
        "revenue",
        "diff_from_prev_year",
      ),
    ).toBe(40);
  });

  it("computes percent change vs previous period", () => {
    const pd = new PivotData(
      DATE_SAMPLE,
      makeDateConfig("pct_diff_from_prev"),
      {
        columnTypes: new Map([["order_date", "date"]]),
      },
    );
    expect(
      pd.getCellComparisonValue(
        ["US"],
        ["2024-02"],
        "revenue",
        "pct_diff_from_prev",
      ),
    ).toBeCloseTo(0.5, 6);
  });

  it("auto-detects temporal axes for period comparisons without explicit date_grains", () => {
    const pd = new PivotData(
      DATE_SAMPLE,
      makeConfig({
        rows: ["region"],
        columns: ["order_date"],
        values: ["revenue"],
        show_values_as: { revenue: "diff_from_prev" },
      }),
      {
        columnTypes: new Map([["order_date", "date"]]),
      },
    );
    expect(pd.getPeriodComparisonAxis()).toEqual({
      axis: "col",
      field: "order_date",
      index: 0,
      grain: "month",
    });
    expect(pd.getColKeys()).toEqual([["2024-01"], ["2024-02"], ["2025-01"]]);
    expect(
      pd.getCellComparisonValue(
        ["US"],
        ["2024-02"],
        "revenue",
        "diff_from_prev",
      ),
    ).toBe(50);
  });

  it("supports period comparisons when the grouped temporal axis is on rows", () => {
    const rowAxisData: DataRecord[] = [
      { order_date: "2024-01-03", region: "US", revenue: 100 },
      { order_date: "2024-02-10", region: "US", revenue: 150 },
      { order_date: "2024-01-04", region: "EU", revenue: 80 },
      { order_date: "2024-02-12", region: "EU", revenue: 95 },
    ];
    const pd = new PivotData(
      rowAxisData,
      makeConfig({
        rows: ["order_date"],
        columns: ["region"],
        values: ["revenue"],
        date_grains: { order_date: "month" },
        show_values_as: { revenue: "diff_from_prev" },
      }),
      {
        columnTypes: new Map([["order_date", "date"]]),
      },
    );
    expect(pd.getPeriodComparisonAxis()).toEqual({
      axis: "row",
      field: "order_date",
      index: 0,
      grain: "month",
    });
    expect(
      pd.getCellComparisonValue(
        ["2024-02"],
        ["US"],
        "revenue",
        "diff_from_prev",
      ),
    ).toBe(50);
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

// ── 0.5.0 Show Values As analytical modes ────────────────────────────────────

import {
  computeRunningTotals,
  computeRanks,
  getRunningTotal,
  getPctRunningTotal,
  getRank,
  getPctOfParent,
  getIndex,
} from "./showValuesAs";

/**
 * Single-level: region rows, year columns.
 * US: 2023=150, 2024=150; EU: 2023=200, 2024=250
 * Col totals: 2023=350, 2024=400; grand=750
 */
const SVA_FLAT: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100 },
  { region: "US", year: "2023", revenue: 50 },
  { region: "US", year: "2024", revenue: 150 },
  { region: "EU", year: "2023", revenue: 200 },
  { region: "EU", year: "2024", revenue: 250 },
];

/**
 * Two-level rows: [region, dept].
 * US/East: 2023=30, 2024=70
 * US/West: 2023=50, 2024=50
 * EU/East: 2023=100, 2024=100
 * EU/West: 2023=80, 2024=120
 * US subtotal: 2023=80, 2024=120
 * EU subtotal: 2023=180, 2024=220
 * Col totals: 2023=260, 2024=340; grand=600
 */
const SVA_SUBTOTAL: DataRecord[] = [
  { region: "US", dept: "East", year: "2023", revenue: 30 },
  { region: "US", dept: "East", year: "2024", revenue: 70 },
  { region: "US", dept: "West", year: "2023", revenue: 50 },
  { region: "US", dept: "West", year: "2024", revenue: 50 },
  { region: "EU", dept: "East", year: "2023", revenue: 100 },
  { region: "EU", dept: "East", year: "2024", revenue: 100 },
  { region: "EU", dept: "West", year: "2023", revenue: 80 },
  { region: "EU", dept: "West", year: "2024", revenue: 120 },
];

function makeSvaConfig(
  rows: string[],
  cols: string[],
  values: string[],
): PivotConfigV1 {
  return {
    version: 1,
    rows,
    columns: cols,
    values,
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    aggregation: normalizeAggregationConfig("sum", values),
  };
}

describe("running_total", () => {
  it("accumulates along row axis in display order (flat pivot)", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_FLAT, config);
    const rowKeys = pd.getSortedLeafRowKeys();
    const colKey = ["2023"];

    // Default sort is alphabetical: EU first (200), US second (150)
    // RT: EU=200, US=200+150=350
    const rtMap = computeRunningTotals(pd, colKey, "revenue");
    const rtUS = rtMap.get("US");
    const rtEU = rtMap.get("EU");

    // Both US and EU are top-level (no parent), so accumulation is global.
    // Whichever comes first has RT = its raw value; the second has RT = first + second.
    const rawUS = pd.getAggregator(["US"], colKey, "revenue").value()!; // 150
    const rawEU = pd.getAggregator(["EU"], colKey, "revenue").value()!; // 200
    const total = rawUS + rawEU; // 350

    expect(rtUS).not.toBeNull();
    expect(rtEU).not.toBeNull();
    // One of them is the raw value (first row), the other is the total (second row)
    const vals = new Set([rtUS, rtEU]);
    expect(vals.has(rawUS) || vals.has(rawEU)).toBe(true);
    expect(vals.has(total)).toBe(true);
  });

  it("resets at parent group boundary (two-level pivot)", () => {
    const config = makeSvaConfig(["region", "dept"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_SUBTOTAL, config);
    const colKey = ["2023"];

    const rtMap = computeRunningTotals(pd, colKey, "revenue");

    // Within US group: East=30, West=50 → running totals 30, 80
    const rtUS_East = rtMap.get("US\x01East");
    const rtUS_West = rtMap.get("US\x01West");
    expect(rtUS_East).toBeDefined();
    expect(rtUS_West).toBeDefined();
    // The running total for West should be 30+50=80 (East comes first alphabetically)
    expect(rtUS_East! + 50).toBeCloseTo(rtUS_West!, 1);

    // Within EU group: East=100, West=80 → running totals reset.
    // Display order is alphabetical: East first (100), West second (80).
    // RT: East=100, West=100+80=180
    const rtEU_East = rtMap.get("EU\x01East");
    const rtEU_West = rtMap.get("EU\x01West");
    expect(rtEU_East).toBeDefined();
    expect(rtEU_West).toBeDefined();
    // EU accumulation is independent of US
    expect(Math.min(rtEU_East!, rtEU_West!)).toBeCloseTo(100, 1); // East comes first
    expect(Math.max(rtEU_East!, rtEU_West!)).toBeCloseTo(180, 1);
  });

  it("returns null for null raw values", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const records: DataRecord[] = [
      { region: "US", year: "2023", revenue: null },
      { region: "EU", year: "2023", revenue: 200 },
    ];
    const pd = new PivotData(records, config);
    const rtMap = computeRunningTotals(pd, ["2023"], "revenue");
    expect(rtMap.get("US")).toBeNull();
    expect(rtMap.get("EU")).toBe(200); // null row doesn't contribute to accumulator
  });

  it("getSortedLeafRowKeys returns same keys as getRowKeys", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_FLAT, config);
    expect(pd.getSortedLeafRowKeys()).toEqual(pd.getRowKeys());
  });
});

describe("pct_running_total", () => {
  it("denominator is parent group total for same column (two-level)", () => {
    const config = makeSvaConfig(["region", "dept"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_SUBTOTAL, config);
    const colKey = ["2023"];

    // US/East 2023=30, US subtotal 2023=80 → 30/80 = 0.375
    const pct = getPctRunningTotal(pd, ["US", "East"], colKey, "revenue");
    expect(pct).not.toBeNull();
    expect(pct).toBeCloseTo(30 / 80, 3);
  });

  it("denominator is column grand total for single-level pivot", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_FLAT, config);
    // col grand total 2023 = 350
    // First row (either US or EU), running total / 350
    const rowKeys = pd.getSortedLeafRowKeys();
    const firstKey = rowKeys[0]!;
    const firstRaw = pd.getAggregator(firstKey, ["2023"], "revenue").value()!;
    const pct = getPctRunningTotal(pd, firstKey, ["2023"], "revenue");
    expect(pct).not.toBeNull();
    expect(pct).toBeCloseTo(firstRaw / 350, 3);
  });

  it("returns null when denominator is null/zero", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const records: DataRecord[] = [
      { region: "US", year: "2023", revenue: null },
      { region: "EU", year: "2023", revenue: null },
    ];
    const pd = new PivotData(records, config);
    const pct = getPctRunningTotal(pd, ["US"], ["2023"], "revenue");
    expect(pct).toBeNull();
  });

  it("getParentSubtotal returns column total for top-level rows", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_FLAT, config);
    const colTotal2023 = pd.getColTotal(["2023"], "revenue").value();
    const parentSubtotal = pd.getParentSubtotal(["US"], ["2023"], "revenue");
    expect(parentSubtotal).toBeCloseTo(colTotal2023!, 1);
  });

  it("getParentSubtotal returns subtotal for leaf rows in two-level pivot", () => {
    const config = makeSvaConfig(["region", "dept"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_SUBTOTAL, config);
    // US subtotal 2023 = 80
    const ps = pd.getParentSubtotal(["US", "East"], ["2023"], "revenue");
    expect(ps).toBeCloseTo(80, 1);
  });
});

describe("rank (competition rank)", () => {
  it("assigns competition rank 1,1,3 for tied values", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    // US and EU both 100 for year=2023 — ranks should be 1,1 (no further rows so no skip)
    const records: DataRecord[] = [
      { region: "US", year: "2023", revenue: 100 },
      { region: "EU", year: "2023", revenue: 100 },
      { region: "APAC", year: "2023", revenue: 50 },
    ];
    const pd = new PivotData(records, config);
    const colKey = ["2023"];
    const rankMap = computeRanks(pd, colKey, "revenue");

    const rUS = rankMap.get("US");
    const rEU = rankMap.get("EU");
    const rAPAC = rankMap.get("APAC");

    // US and EU tie at rank 1; APAC gets rank 3 (competition rank)
    expect(rUS).toBe(1);
    expect(rEU).toBe(1);
    expect(rAPAC).toBe(3);
  });

  it("resets rank per parent group", () => {
    const config = makeSvaConfig(["region", "dept"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_SUBTOTAL, config);
    const colKey = ["2023"];
    const rankMap = computeRanks(pd, colKey, "revenue");

    // US group: East=30, West=50 → West is rank 1, East is rank 2
    const rUS_East = rankMap.get("US\x01East");
    const rUS_West = rankMap.get("US\x01West");
    expect(rUS_West).toBe(1);
    expect(rUS_East).toBe(2);

    // EU group: East=100, West=80 → East is rank 1, West is rank 2
    const rEU_East = rankMap.get("EU\x01East");
    const rEU_West = rankMap.get("EU\x01West");
    expect(rEU_East).toBe(1);
    expect(rEU_West).toBe(2);
  });

  it("returns null for null-valued rows", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const records: DataRecord[] = [
      { region: "US", year: "2023", revenue: null },
      { region: "EU", year: "2023", revenue: 200 },
    ];
    const pd = new PivotData(records, config);
    const rankMap = computeRanks(pd, ["2023"], "revenue");
    expect(rankMap.get("US")).toBeNull();
    expect(rankMap.get("EU")).toBe(1);
  });
});

describe("pct_of_parent", () => {
  it("leaf cell denominator is immediate parent subtotal", () => {
    const config = makeSvaConfig(["region", "dept"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_SUBTOTAL, config);
    // US/East 2023=30, US subtotal 2023=80 → 30/80
    const pct = getPctOfParent(30, pd, ["US", "East"], ["2023"], "revenue");
    expect(pct).not.toBeNull();
    expect(pct).toBeCloseTo(30 / 80, 3);
  });

  it("single-level: denominator is column grand total", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_FLAT, config);
    // US 2023=150, col total 2023=350 → 150/350
    const pct = getPctOfParent(150, pd, ["US"], ["2023"], "revenue");
    expect(pct).not.toBeNull();
    expect(pct).toBeCloseTo(150 / 350, 3);
  });

  it("returns null when denominator is zero or null", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const records: DataRecord[] = [
      { region: "US", year: "2023", revenue: null },
    ];
    const pd = new PivotData(records, config);
    const pct = getPctOfParent(0, pd, ["US"], ["2023"], "revenue");
    expect(pct).toBeNull();
  });

  it("returns null for null raw value", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_FLAT, config);
    const pct = getPctOfParent(null, pd, ["US"], ["2023"], "revenue");
    expect(pct).toBeNull();
  });
});

describe("index", () => {
  it("computes Excel INDEX formula correctly", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_FLAT, config);
    // cell=150, grand=750, rowTotal_US=300, colTotal_2023=350
    // index = 150*750 / (300*350) = 112500/105000 ≈ 1.071
    const rowTotal = pd.getRowTotal(["US"], "revenue").value()!; // 300
    const colTotal = pd.getColTotal(["2023"], "revenue").value()!; // 350
    const grand = pd.getGrandTotal("revenue").value()!; // 750
    const expected = (150 * grand) / (rowTotal * colTotal);
    const idx = getIndex(150, pd, ["US"], ["2023"], "revenue");
    expect(idx).not.toBeNull();
    expect(idx).toBeCloseTo(expected, 3);
  });

  it("returns null when grand total is zero", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const records: DataRecord[] = [];
    const pd = new PivotData(records, config);
    const idx = getIndex(0, pd, ["US"], ["2023"], "revenue");
    expect(idx).toBeNull();
  });

  it("returns null for null raw value", () => {
    const config = makeSvaConfig(["region"], ["year"], ["revenue"]);
    const pd = new PivotData(SVA_FLAT, config);
    const idx = getIndex(null, pd, ["US"], ["2023"], "revenue");
    expect(idx).toBeNull();
  });
});
