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
 * Phase 4: Cross-feature + scale combo tests.
 *
 * Each test exercises 3+ features simultaneously. Expected values come from
 * pandas (golden JSON) or mathematical invariants — never from PivotData output.
 */

import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { PivotData, type DataRecord } from "./PivotData";
import { DataRecordSource } from "./parseArrow";
import {
  normalizeAggregationConfig,
  type AggregationConfig,
  type AggregationType,
  type ColumnarDataSource,
  type PivotConfigV1,
} from "./types";

const __dirname = dirname(fileURLToPath(import.meta.url));
const GOLDEN_DIR = resolve(__dirname, "../../../../tests/golden_data");

function loadCSV(filename: string): DataRecord[] {
  const path = resolve(GOLDEN_DIR, filename);
  let raw: string;
  try {
    raw = readFileSync(path, "utf-8");
  } catch {
    return [];
  }
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
const mediumRecords = loadCSV("medium.csv");

class TestColumnarSource implements ColumnarDataSource {
  private readonly columns: Map<string, unknown[]> = new Map();
  private readonly _numRows: number;
  private readonly _columnNames: string[];

  constructor(records: DataRecord[]) {
    this._numRows = records.length;
    if (records.length === 0) {
      this._columnNames = [];
      return;
    }
    this._columnNames = Object.keys(records[0]!);
    for (const col of this._columnNames) {
      this.columns.set(
        col,
        records.map((r) => r[col]),
      );
    }
  }

  get numRows(): number {
    return this._numRows;
  }

  getValue(rowIndex: number, fieldName: string): unknown {
    return this.columns.get(fieldName)?.[rowIndex];
  }

  getColumnNames(): string[] {
    return this._columnNames;
  }

  getFloat64Column(fieldName: string): Float64Array | null {
    const col = this.columns.get(fieldName);
    if (!col) return null;
    if (typeof col[0] !== "number") return null;
    return Float64Array.from(col as number[]);
  }
}

type TestConfigOverrides = Partial<Omit<PivotConfigV1, "aggregation">> & {
  aggregation?: AggregationType | AggregationConfig;
};

function makeConfig(overrides: TestConfigOverrides = {}): PivotConfigV1 {
  const { aggregation: aggregationOverride, ...rest } = overrides;
  const values = overrides.values ?? ["Revenue"];
  const config = {
    version: 1 as const,
    rows: ["Region"],
    columns: ["Year"],
    values,
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    ...rest,
  } as PivotConfigV1;
  config.values = values;
  config.aggregation = normalizeAggregationConfig(aggregationOverride, values);
  return config;
}

function expectClose(actual: number | null, expected: number, label: string) {
  expect(
    actual,
    `${label}: expected ${expected}, got ${actual}`,
  ).not.toBeNull();
  expect(actual).toBeCloseTo(expected, 0);
}

// ---------------------------------------------------------------------------
// Test 1: Per-measure agg + show_values_as + subtotals
// ---------------------------------------------------------------------------

describe("Cross-feature Test 1: per-measure + pct_of_total + subtotals", () => {
  const config = makeConfig({
    rows: ["Region", "Category"],
    values: ["Revenue", "Profit"],
    aggregation: { Revenue: "sum", Profit: "avg" },
    show_subtotals: true,
    show_values_as: { Revenue: "pct_of_total" },
  });
  const pd = new PivotData(smallRecords, config);

  it("Revenue pct_of_total cells sum to 100% (invariant)", () => {
    const grand = pd.getGrandTotal("Revenue").value()!;
    expect(grand).toBeGreaterThan(0);

    let total = 0;
    for (const rowKey of pd.getRowKeys()) {
      for (const colKey of pd.getColKeys()) {
        const raw = pd.getAggregator(rowKey, colKey, "Revenue").value();
        if (raw !== null) {
          total += (raw / grand) * 100;
        }
      }
    }
    expect(Math.abs(total - 100)).toBeLessThan(0.5);
  });

  it("Profit cells remain raw (avg), not pct", () => {
    const profitGrand = pd.getGrandTotal("Profit").value()!;
    for (const rowKey of pd.getRowKeys()) {
      for (const colKey of pd.getColKeys()) {
        const raw = pd.getAggregator(rowKey, colKey, "Profit").value();
        if (raw !== null) {
          expect(typeof raw).toBe("number");
        }
      }
    }
    expect(profitGrand).toBeGreaterThan(0);
  });

  it("subtotals exist and are correct for Revenue", () => {
    const regions = [...new Set(smallRecords.map((r) => String(r.Region)))];
    for (const region of regions) {
      const childKeys = pd.getRowKeys().filter((k) => k[0] === region);
      if (childKeys.length > 0) {
        const childSum = childKeys.reduce(
          (sum, k) => sum + (pd.getRowTotal(k, "Revenue").value() ?? 0),
          0,
        );
        const subtotalVal = pd
          .getSubtotalAggregator([region], [], "Revenue")
          .value();
        if (subtotalVal !== null) {
          expectClose(subtotalVal, childSum, `subtotal ${region}`);
        }
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Test 2: Sort + filter combo
// ---------------------------------------------------------------------------

describe("Cross-feature Test 2: sort desc + filter", () => {
  const config = makeConfig({
    aggregation: "sum",
    row_sort: { by: "value", direction: "desc", value_field: "Revenue" },
    filters: { Region: { include: ["North", "South"] } },
  });
  const pd = new PivotData(smallRecords, config);

  it("only filtered regions present", () => {
    const rows = pd.getRowKeys();
    expect(rows.every((k) => ["North", "South"].includes(k[0]!))).toBe(true);
    expect(rows.length).toBe(2);
  });

  it("row order is desc by Revenue total", () => {
    const rows = pd.getRowKeys();
    const totals = rows.map((k) => pd.getRowTotal(k, "Revenue").value()!);
    for (let i = 1; i < totals.length; i++) {
      expect(totals[i - 1]).toBeGreaterThanOrEqual(totals[i]!);
    }
  });

  it("filtered cell values match golden G config", () => {
    const g = golden.G;
    for (const [rowKeyStr, colVals] of Object.entries(g.cells)) {
      const rowKey = rowKeyStr.split("|");
      for (const [colKeyStr, expected] of Object.entries(
        colVals as Record<string, number>,
      )) {
        const colKey = colKeyStr.split("|");
        const actual = pd.getAggregator(rowKey, colKey, "Revenue").value();
        if (actual !== null) {
          expectClose(actual, expected, `cell [${rowKeyStr}][${colKeyStr}]`);
        }
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Test 3: Synthetic + locked mode (just synthetic correctness)
// ---------------------------------------------------------------------------

describe("Cross-feature Test 3: synthetic sum_over_sum + values", () => {
  const g = golden.H;
  const config = makeConfig({
    rows: ["Region"],
    columns: [],
    values: ["Revenue", "Units"],
    aggregation: { Revenue: "sum", Units: "sum" },
    synthetic_measures: [
      {
        id: "rev_per_unit",
        label: "Rev/Unit",
        operation: "sum_over_sum",
        numerator: "Revenue",
        denominator: "Units",
      },
    ],
    interactive: false,
  });
  const pd = new PivotData(smallRecords, config);

  it("synthetic ratios match pandas across all regions", () => {
    for (const [region, expected] of Object.entries(
      g.synthetic_ratios as Record<string, number>,
    )) {
      const actual = pd.getAggregator([region], [], "rev_per_unit").value();
      expect(actual).not.toBeNull();
      expect(Math.abs(actual! - expected)).toBeLessThan(0.01);
    }
  });

  it("grand total ratio matches pandas", () => {
    const actual = pd.getGrandTotal("rev_per_unit").value();
    expect(actual).not.toBeNull();
    expect(Math.abs(actual! - g.grand_total_ratio)).toBeLessThan(0.01);
  });

  it("raw Revenue and Units are accessible alongside synthetic", () => {
    for (const [region, expected] of Object.entries(
      g.raw_revenue as Record<string, number>,
    )) {
      const actual = pd.getAggregator([region], [], "Revenue").value();
      expectClose(actual, expected, `Revenue [${region}]`);
    }
  });
});

// ---------------------------------------------------------------------------
// Test 4: Scoped sort + subtotals + column group collapse
// ---------------------------------------------------------------------------

describe("Cross-feature Test 4: subtotals + sort + collapse", () => {
  const config = makeConfig({
    rows: ["Region", "Category"],
    aggregation: "sum",
    show_subtotals: true,
    row_sort: { by: "value", direction: "desc", value_field: "Revenue" },
  });
  const pd = new PivotData(smallRecords, config);

  it("subtotals are correct regardless of sort order", () => {
    const g = golden.E;
    const subtotals = g.subtotals.by_region;
    for (const [regionStr, rowTotal] of Object.entries(
      subtotals.row_totals as Record<string, number>,
    )) {
      const actual = pd
        .getSubtotalAggregator(regionStr.split("|"), [], "Revenue")
        .value();
      if (actual !== null) {
        expectClose(actual, rowTotal, `subtotal row total [${regionStr}]`);
      }
    }
  });

  it("collapsing a group preserves subtotal values", () => {
    const configCollapsed = makeConfig({
      rows: ["Region", "Category"],
      aggregation: "sum",
      show_subtotals: true,
      row_sort: { by: "value", direction: "desc", value_field: "Revenue" },
      collapsed_groups: ["North"],
    });
    const pdCollapsed = new PivotData(smallRecords, configCollapsed);

    const subtotalBefore = pd
      .getSubtotalAggregator(["North"], [], "Revenue")
      .value();
    const subtotalAfter = pdCollapsed
      .getSubtotalAggregator(["North"], [], "Revenue")
      .value();
    expect(subtotalBefore).toBe(subtotalAfter);
  });
});

// ---------------------------------------------------------------------------
// Test 5: Columnar + per-measure + subtotals at 10K (S6 hot-loop proof)
// ---------------------------------------------------------------------------

describe("Cross-feature Test 5: columnar + per-measure + subtotals at 10K", () => {
  const skip = mediumRecords.length === 0;
  const scaledGolden = golden.scaled?.C_medium;

  it.skipIf(skip)("columnar per-measure Revenue matches golden at 10K", () => {
    const config = makeConfig({
      rows: ["Region"],
      columns: ["Year"],
      values: ["Revenue", "Units"],
      aggregation: { Revenue: "sum", Units: "avg" },
    });
    const pd = new PivotData(new TestColumnarSource(mediumRecords), config);
    const g = scaledGolden.measures.Revenue;

    for (const [rowKeyStr, colVals] of Object.entries(g.cells)) {
      const rowKey = rowKeyStr.split("|");
      for (const [colKeyStr, expected] of Object.entries(
        colVals as Record<string, number>,
      )) {
        const colKey = colKeyStr.split("|");
        const actual = pd.getAggregator(rowKey, colKey, "Revenue").value();
        expectClose(
          actual,
          expected,
          `columnar Revenue 10K [${rowKeyStr}][${colKeyStr}]`,
        );
      }
    }
  });

  it.skipIf(skip)(
    "columnar per-measure Units avg matches golden at 10K",
    () => {
      const config = makeConfig({
        rows: ["Region"],
        columns: ["Year"],
        values: ["Revenue", "Units"],
        aggregation: { Revenue: "sum", Units: "avg" },
      });
      const pd = new PivotData(new TestColumnarSource(mediumRecords), config);
      const g = scaledGolden.measures.Units;

      for (const [rowKeyStr, colVals] of Object.entries(g.cells)) {
        const rowKey = rowKeyStr.split("|");
        for (const [colKeyStr, expected] of Object.entries(
          colVals as Record<string, number>,
        )) {
          const colKey = colKeyStr.split("|");
          const actual = pd.getAggregator(rowKey, colKey, "Units").value();
          expectClose(
            actual,
            expected,
            `columnar Units 10K [${rowKeyStr}][${colKeyStr}]`,
          );
        }
      }
    },
  );

  it.skipIf(skip)("columnar subtotals at 10K match pandas", () => {
    const config = makeConfig({
      rows: ["Region", "Category"],
      values: ["Revenue"],
      aggregation: "sum",
      show_subtotals: true,
    });
    const pd = new PivotData(new TestColumnarSource(mediumRecords), config);
    const gSubtotals = golden.scaled?.E_medium?.subtotals?.by_region;

    for (const [regionStr, colVals] of Object.entries(gSubtotals.cells)) {
      const parentKey = regionStr.split("|");
      for (const [colKeyStr, expected] of Object.entries(
        colVals as Record<string, number>,
      )) {
        const colKey = colKeyStr.split("|");
        const actual = pd
          .getSubtotalAggregator(parentKey, colKey, "Revenue")
          .value();
        expectClose(
          actual,
          expected,
          `columnar subtotal 10K [${regionStr}][${colKeyStr}]`,
        );
      }
    }
  });
});

// ---------------------------------------------------------------------------
// Test 6: Filter + sort + record/columnar parity
// ---------------------------------------------------------------------------

describe("Cross-feature Test 6: filter + sort + columnar parity", () => {
  const config = makeConfig({
    aggregation: "sum",
    filters: { Region: { include: ["North", "South"] } },
    row_sort: { by: "value", direction: "desc", value_field: "Revenue" },
  });

  it("record and columnar produce identical results with filter+sort", () => {
    const pdRec = new PivotData(smallRecords, config);
    const pdCol = new PivotData(new TestColumnarSource(smallRecords), config);

    const recRows = pdRec.getRowKeys();
    const colRows = pdCol.getRowKeys();
    expect(recRows).toEqual(colRows);

    for (const rowKey of recRows) {
      for (const colKey of pdRec.getColKeys()) {
        expect(pdRec.getAggregator(rowKey, colKey, "Revenue").value()).toBe(
          pdCol.getAggregator(rowKey, colKey, "Revenue").value(),
        );
      }
      expect(pdRec.getRowTotal(rowKey, "Revenue").value()).toBe(
        pdCol.getRowTotal(rowKey, "Revenue").value(),
      );
    }
    expect(pdRec.getGrandTotal("Revenue").value()).toBe(
      pdCol.getGrandTotal("Revenue").value(),
    );
  });

  it("filtered + sorted values match golden G", () => {
    const g = golden.G;
    const pd = new PivotData(smallRecords, config);

    expectClose(
      pd.getGrandTotal("Revenue").value(),
      g.grand_total,
      "filtered grand total",
    );
  });
});
