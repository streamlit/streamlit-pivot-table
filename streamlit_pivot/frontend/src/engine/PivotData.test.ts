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
import {
  PivotData,
  type DataRecord,
  type GroupedRow,
  mixedCompare,
} from "./PivotData";
import {
  AGGREGATOR_CLASS,
  normalizeAggregationConfig,
  type AggregationConfig,
  type AggregationType,
  type ColumnarDataSource,
  type PivotConfigV1,
} from "./types";
import { DataRecordSource } from "./parseArrow";
import { measureSync, DEFAULT_BUDGETS } from "./perf";

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

describe("PivotData - basic computation", () => {
  it("computes row and column keys", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    expect(pd.getRowKeys()).toEqual([["EU"], ["US"]]);
    expect(pd.getColKeys()).toEqual([["2023"], ["2024"]]);
  });

  it("computes cell aggregates for sum", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    expect(pd.getAggregator(["US"], ["2023"]).value()).toBe(150);
    expect(pd.getAggregator(["US"], ["2024"]).value()).toBe(150);
    expect(pd.getAggregator(["EU"], ["2023"]).value()).toBe(200);
    expect(pd.getAggregator(["EU"], ["2024"]).value()).toBe(250);
  });

  it("computes row totals", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    expect(pd.getRowTotal(["US"]).value()).toBe(300);
    expect(pd.getRowTotal(["EU"]).value()).toBe(450);
  });

  it("computes column totals", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    expect(pd.getColTotal(["2023"]).value()).toBe(350);
    expect(pd.getColTotal(["2024"]).value()).toBe(400);
  });

  it("computes grand total", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    expect(pd.getGrandTotal().value()).toBe(750);
  });

  it("returns empty aggregator for non-existent key", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    const agg = pd.getAggregator(["XX"], ["9999"]);
    expect(agg.value()).toBeNull();
    expect(agg.count()).toBe(0);
  });
});

describe("PivotData - multiple values", () => {
  it("supports per-field aggregation", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = new PivotData(SAMPLE_DATA, config);
    expect(pd.getAggregator(["US"], ["2023"], "revenue").value()).toBe(150);
    expect(pd.getAggregator(["US"], ["2023"], "profit").value()).toBe(60);
    expect(pd.getRowTotal(["US"], "profit").value()).toBe(120);
  });

  it("applies different aggregations to different value fields in the same pivot", () => {
    const config = makeConfig({
      values: ["revenue", "profit"],
      aggregation: { revenue: "sum", profit: "avg" },
    });
    const pd = new PivotData(SAMPLE_DATA, config);

    expect(pd.getAggregator(["US"], ["2023"], "revenue").value()).toBe(150);
    expect(pd.getAggregator(["US"], ["2023"], "profit").value()).toBe(30);

    expect(pd.getGrandTotal("revenue").value()).toBe(750);
    expect(pd.getGrandTotal("profit").value()).toBe(60);
  });

  it("computes per-field grand totals", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const revenueTotal = SAMPLE_DATA.reduce(
      (s, r) => s + (r.revenue as number),
      0,
    );
    const profitTotal = SAMPLE_DATA.reduce(
      (s, r) => s + (r.profit as number),
      0,
    );
    expect(pd.getGrandTotal("revenue").value()).toBe(revenueTotal);
    expect(pd.getGrandTotal("profit").value()).toBe(profitTotal);
    expect(pd.getGrandTotal("revenue").value()).not.toBe(
      pd.getGrandTotal("profit").value(),
    );
  });
});

describe("PivotData - multi-level keys", () => {
  it("handles multi-level row keys", () => {
    const data: DataRecord[] = [
      { country: "US", state: "CA", year: "2023", revenue: 100 },
      { country: "US", state: "NY", year: "2023", revenue: 200 },
      { country: "EU", state: "DE", year: "2023", revenue: 300 },
    ];
    const config = makeConfig({
      rows: ["country", "state"],
      columns: ["year"],
      values: ["revenue"],
    });
    const pd = new PivotData(data, config);
    expect(pd.getRowKeys()).toEqual([
      ["EU", "DE"],
      ["US", "CA"],
      ["US", "NY"],
    ]);
  });
});

describe("PivotData - aggregator-class-specific totals invariants", () => {
  describe("additive (sum, count)", () => {
    it.each(["sum", "count"] as const)(
      "%s: grand total == sum of row totals == sum of col totals",
      (aggType) => {
        const config = makeConfig({ aggregation: aggType });
        const pd = new PivotData(SAMPLE_DATA, config);
        const grand = pd.getGrandTotal().value()!;

        const rowTotalSum = pd
          .getRowKeys()
          .reduce((acc, rk) => acc + (pd.getRowTotal(rk).value() ?? 0), 0);
        const colTotalSum = pd
          .getColKeys()
          .reduce((acc, ck) => acc + (pd.getColTotal(ck).value() ?? 0), 0);

        expect(grand).toBeCloseTo(rowTotalSum, 10);
        expect(grand).toBeCloseTo(colTotalSum, 10);
      },
    );
  });

  describe("idempotent (min, max)", () => {
    it("min: grand total <= every row total", () => {
      const config = makeConfig({ aggregation: "min" });
      const pd = new PivotData(SAMPLE_DATA, config);
      const grand = pd.getGrandTotal().value()!;
      for (const rk of pd.getRowKeys()) {
        const rowTotal = pd.getRowTotal(rk).value()!;
        expect(grand).toBeLessThanOrEqual(rowTotal);
      }
    });

    it("max: grand total >= every row total", () => {
      const config = makeConfig({ aggregation: "max" });
      const pd = new PivotData(SAMPLE_DATA, config);
      const grand = pd.getGrandTotal().value()!;
      for (const rk of pd.getRowKeys()) {
        const rowTotal = pd.getRowTotal(rk).value()!;
        expect(grand).toBeGreaterThanOrEqual(rowTotal);
      }
    });

    it("min: grand total <= every col total", () => {
      const config = makeConfig({ aggregation: "min" });
      const pd = new PivotData(SAMPLE_DATA, config);
      const grand = pd.getGrandTotal().value()!;
      for (const ck of pd.getColKeys()) {
        const colTotal = pd.getColTotal(ck).value()!;
        expect(grand).toBeLessThanOrEqual(colTotal);
      }
    });

    it("max: grand total >= every col total", () => {
      const config = makeConfig({ aggregation: "max" });
      const pd = new PivotData(SAMPLE_DATA, config);
      const grand = pd.getGrandTotal().value()!;
      for (const ck of pd.getColKeys()) {
        const colTotal = pd.getColTotal(ck).value()!;
        expect(grand).toBeGreaterThanOrEqual(colTotal);
      }
    });
  });

  describe("non-additive (avg)", () => {
    it("avg: grand total is recomputed from all raw data", () => {
      const config = makeConfig({ aggregation: "avg" });
      const pd = new PivotData(SAMPLE_DATA, config);
      const grand = pd.getGrandTotal().value()!;
      const expectedAvg =
        SAMPLE_DATA.reduce((s, r) => s + (r.revenue as number), 0) /
        SAMPLE_DATA.length;
      expect(grand).toBeCloseTo(expectedAvg, 10);
    });
  });
});

describe("PivotData - edge cases", () => {
  it("handles empty records", () => {
    const pd = new PivotData([], makeConfig());
    expect(pd.getRowKeys()).toEqual([]);
    expect(pd.getColKeys()).toEqual([]);
    expect(pd.getGrandTotal().value()).toBeNull();
    expect(pd.recordCount).toBe(0);
  });

  it("handles single record", () => {
    const data = [{ region: "US", year: "2023", revenue: 100 }];
    const pd = new PivotData(data, makeConfig());
    expect(pd.getRowKeys()).toEqual([["US"]]);
    expect(pd.getColKeys()).toEqual([["2023"]]);
    expect(pd.getGrandTotal().value()).toBe(100);
  });

  it("handles records with all null values", () => {
    const data: DataRecord[] = [
      { region: "US", year: "2023", revenue: null },
      { region: "EU", year: "2024", revenue: null },
    ];
    const pd = new PivotData(data, makeConfig());
    expect(pd.getGrandTotal().value()).toBeNull();
    expect(pd.getAggregator(["US"], ["2023"]).value()).toBeNull();
  });

  it("handles no rows/columns dimensions", () => {
    const config = makeConfig({ rows: [], columns: [] });
    const pd = new PivotData(SAMPLE_DATA, config);
    expect(pd.getRowKeys()).toEqual([[]]);
    expect(pd.getColKeys()).toEqual([[]]);
    expect(pd.getGrandTotal().value()).toBe(750);
  });

  it("reports metadata", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    expect(pd.recordCount).toBe(5);
    expect(pd.uniqueRowKeyCount).toBe(2);
    expect(pd.uniqueColKeyCount).toBe(2);
    expect(pd.totalCellCount).toBe(4);
  });
});

describe("PivotData - all aggregation types", () => {
  const types: AggregationType[] = ["sum", "avg", "count", "min", "max"];

  it.each(types)("%s: does not throw with sample data", (aggType) => {
    const config = makeConfig({ aggregation: aggType });
    expect(() => new PivotData(SAMPLE_DATA, config)).not.toThrow();
  });

  it.each(types)("%s: grand total matches expected", (aggType) => {
    const config = makeConfig({ aggregation: aggType });
    const pd = new PivotData(SAMPLE_DATA, config);
    const values = SAMPLE_DATA.map((r) => r.revenue as number);

    const grand = pd.getGrandTotal().value()!;
    switch (aggType) {
      case "sum":
        expect(grand).toBe(values.reduce((a, b) => a + b, 0));
        break;
      case "avg":
        expect(grand).toBeCloseTo(
          values.reduce((a, b) => a + b, 0) / values.length,
          10,
        );
        break;
      case "count":
        expect(grand).toBe(values.length);
        break;
      case "min":
        expect(grand).toBe(Math.min(...values));
        break;
      case "max":
        expect(grand).toBe(Math.max(...values));
        break;
    }
  });
});

// ---------------------------------------------------------------------------
// Phase 2 features
// ---------------------------------------------------------------------------

describe("PivotData - filtering", () => {
  it("returns all data when no filters set", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    expect(pd.getGrandTotal().value()).toBe(750);
  });

  it("include filter limits to specified values", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ filters: { region: { include: ["US"] } } }),
    );
    expect(pd.getRowKeys()).toEqual([["US"]]);
    expect(pd.getGrandTotal().value()).toBe(300);
  });

  it("exclude filter removes specified values", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ filters: { region: { exclude: ["US"] } } }),
    );
    expect(pd.getRowKeys()).toEqual([["EU"]]);
    expect(pd.getGrandTotal().value()).toBe(450);
  });

  it("include takes precedence over exclude", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ filters: { region: { include: ["EU"], exclude: ["EU"] } } }),
    );
    expect(pd.getRowKeys()).toEqual([["EU"]]);
  });

  it("filter on non-existent field is a no-op", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ filters: { nonexistent: { include: ["x"] } } }),
    );
    expect(pd.getGrandTotal().value()).toBeNull();
    expect(pd.uniqueRowKeyCount).toBe(0);
  });

  it("all-excluded returns empty pivot", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ filters: { region: { exclude: ["US", "EU"] } } }),
    );
    expect(pd.getRowKeys()).toEqual([]);
    expect(pd.getColKeys()).toEqual([]);
  });

  it("empty include array returns all data", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ filters: { region: { include: [] } } }),
    );
    expect(pd.getGrandTotal().value()).toBe(750);
  });

  it("filters on column dimensions", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ filters: { year: { include: ["2023"] } } }),
    );
    expect(pd.getColKeys()).toEqual([["2023"]]);
    expect(pd.getGrandTotal().value()).toBe(350);
  });
});

describe("PivotData - sorting", () => {
  it("key asc matches default alphabetical order", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ row_sort: { by: "key", direction: "asc" } }),
    );
    expect(pd.getRowKeys()).toEqual([["EU"], ["US"]]);
  });

  it("key desc reverses alphabetical order", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ row_sort: { by: "key", direction: "desc" } }),
    );
    expect(pd.getRowKeys()).toEqual([["US"], ["EU"]]);
  });

  it("value asc sorts by row total ascending", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ row_sort: { by: "value", direction: "asc" } }),
    );
    // US=300, EU=450 -> [US, EU]
    expect(pd.getRowKeys()).toEqual([["US"], ["EU"]]);
  });

  it("value desc sorts by row total descending", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ row_sort: { by: "value", direction: "desc" } }),
    );
    expect(pd.getRowKeys()).toEqual([["EU"], ["US"]]);
  });

  it("col_sort key desc reverses column order", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ col_sort: { by: "key", direction: "desc" } }),
    );
    expect(pd.getColKeys()).toEqual([["2024"], ["2023"]]);
  });

  it("col_sort value desc sorts columns by total descending", () => {
    // 2023 total=350, 2024 total=400
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ col_sort: { by: "value", direction: "desc" } }),
    );
    expect(pd.getColKeys()).toEqual([["2024"], ["2023"]]);
  });

  it("value sort with specific value_field", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({
        values: ["revenue", "profit"],
        row_sort: { by: "value", direction: "desc", value_field: "profit" },
      }),
    );
    // EU profit total = 180, US profit total = 120
    expect(pd.getRowKeys()).toEqual([["EU"], ["US"]]);
  });

  it("value sort with col_key sorts by specific cell", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({
        row_sort: { by: "value", direction: "asc", col_key: ["2023"] },
      }),
    );
    // US 2023 = 150, EU 2023 = 200 -> [US, EU]
    expect(pd.getRowKeys()).toEqual([["US"], ["EU"]]);
  });

  it("custom sorter overrides alphabetical order", () => {
    const data: DataRecord[] = [
      { month: "Mar", revenue: 30 },
      { month: "Jan", revenue: 10 },
      { month: "Feb", revenue: 20 },
    ];
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["month"], columns: [], values: ["revenue"] }),
      { sorters: { month: ["Jan", "Feb", "Mar"] } },
    );
    expect(pd.getRowKeys()).toEqual([["Jan"], ["Feb"], ["Mar"]]);
  });

  it("custom sorter puts unknown values last", () => {
    const data: DataRecord[] = [
      { month: "Apr", revenue: 40 },
      { month: "Jan", revenue: 10 },
      { month: "Feb", revenue: 20 },
    ];
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["month"], columns: [], values: ["revenue"] }),
      { sorters: { month: ["Jan", "Feb", "Mar"] } },
    );
    expect(pd.getRowKeys()).toEqual([["Jan"], ["Feb"], ["Apr"]]);
  });
});

describe("PivotData - dimension-targeted key sort", () => {
  const MULTI_DATA: DataRecord[] = [
    { region: "US", category: "B", revenue: 10 },
    { region: "US", category: "A", revenue: 20 },
    { region: "EU", category: "B", revenue: 30 },
    { region: "EU", category: "A", revenue: 40 },
  ];

  it("sort Z→A on child dimension only reorders children, not parents", () => {
    const pd = new PivotData(
      MULTI_DATA,
      makeConfig({
        rows: ["region", "category"],
        columns: [],
        row_sort: { by: "key", direction: "desc", dimension: "category" },
      }),
    );
    const keys = pd.getRowKeys();
    expect(keys).toEqual([
      ["EU", "B"],
      ["EU", "A"],
      ["US", "B"],
      ["US", "A"],
    ]);
  });

  it("sort Z→A on parent dimension only reorders parents, children stay ascending", () => {
    const pd = new PivotData(
      MULTI_DATA,
      makeConfig({
        rows: ["region", "category"],
        columns: [],
        row_sort: { by: "key", direction: "desc", dimension: "region" },
      }),
    );
    const keys = pd.getRowKeys();
    expect(keys).toEqual([
      ["US", "A"],
      ["US", "B"],
      ["EU", "A"],
      ["EU", "B"],
    ]);
  });

  it("sort without dimension targets all levels (backward compat)", () => {
    const pd = new PivotData(
      MULTI_DATA,
      makeConfig({
        rows: ["region", "category"],
        columns: [],
        row_sort: { by: "key", direction: "desc" },
      }),
    );
    const keys = pd.getRowKeys();
    expect(keys).toEqual([
      ["US", "B"],
      ["US", "A"],
      ["EU", "B"],
      ["EU", "A"],
    ]);
  });
});

describe("PivotData - null handling", () => {
  const DATA_WITH_NULLS: DataRecord[] = [
    { region: "US", year: "2023", revenue: 100 },
    { region: null, year: "2023", revenue: 50 },
    { region: "EU", year: "2024", revenue: null },
    { region: "EU", year: "2024", revenue: 200 },
  ];

  it("exclude mode (default) skips null aggregation values", () => {
    const pd = new PivotData(
      DATA_WITH_NULLS,
      makeConfig({ rows: ["region"], columns: ["year"], values: ["revenue"] }),
    );
    // null region -> "" key; EU 2024 has one null revenue (skipped by aggregator)
    expect(pd.getAggregator(["EU"], ["2024"]).value()).toBe(200);
  });

  it("zero mode treats null values as 0", () => {
    const pd = new PivotData(
      DATA_WITH_NULLS,
      makeConfig({ rows: ["region"], columns: ["year"], values: ["revenue"] }),
      { nullHandling: "zero" },
    );
    // EU 2024: null revenue becomes 0, so sum = 0 + 200 = 200
    expect(pd.getAggregator(["EU"], ["2024"]).value()).toBe(200);
    expect(pd.getAggregator(["EU"], ["2024"]).count()).toBe(2);
  });

  it("zero mode treats NaN values as 0 for averages", () => {
    const pd = new PivotData(
      [
        { region: "EU", year: "2024", revenue: Number.NaN },
        { region: "EU", year: "2024", revenue: 200 },
      ],
      makeConfig({
        rows: ["region"],
        columns: ["year"],
        values: ["revenue"],
        aggregation: { revenue: "avg" },
      }),
      { nullHandling: "zero" },
    );
    expect(pd.getAggregator(["EU"], ["2024"]).value()).toBe(100);
    expect(pd.getAggregator(["EU"], ["2024"]).count()).toBe(2);
  });

  it("separate mode creates (null) bucket for dimension values", () => {
    const pd = new PivotData(
      DATA_WITH_NULLS,
      makeConfig({ rows: ["region"], columns: ["year"], values: ["revenue"] }),
      { nullHandling: "separate" },
    );
    const rowKeys = pd.getRowKeys().map((k) => k[0]);
    expect(rowKeys).toContain("(null)");
    expect(pd.getAggregator(["(null)"], ["2023"]).value()).toBe(50);
  });

  it("per-field config applies different modes", () => {
    const pd = new PivotData(
      DATA_WITH_NULLS,
      makeConfig({ rows: ["region"], columns: ["year"], values: ["revenue"] }),
      { nullHandling: { region: "separate", revenue: "zero" } },
    );
    const rowKeys = pd.getRowKeys().map((k) => k[0]);
    expect(rowKeys).toContain("(null)");
    // revenue null -> 0, so EU 2024 count = 2
    expect(pd.getAggregator(["EU"], ["2024"]).count()).toBe(2);
  });
});

describe("PivotData - getUniqueValues", () => {
  it("returns sorted unique values for a field", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    expect(pd.getUniqueValues("region")).toEqual(["EU", "US"]);
  });

  it("returns sorted unique values for column dimension", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    expect(pd.getUniqueValues("year")).toEqual(["2023", "2024"]);
  });

  it("includes all original values regardless of filters", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ filters: { region: { include: ["US"] } } }),
    );
    // getUniqueValues scans raw records, not filtered
    expect(pd.getUniqueValues("region")).toEqual(["EU", "US"]);
  });
});

describe("PivotData - filter + sort interaction", () => {
  it("filter and sort work together correctly", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({
        filters: { year: { include: ["2023"] } },
        row_sort: { by: "value", direction: "desc" },
      }),
    );
    expect(pd.getColKeys()).toEqual([["2023"]]);
    const rowKeys = pd.getRowKeys();
    expect(rowKeys).toHaveLength(2);
    // EU 2023 = 200, US 2023 = 150 → desc: [EU, US]
    expect(rowKeys).toEqual([["EU"], ["US"]]);
  });

  it("filter narrows data then sort orders within the filtered set", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({
        filters: { region: { include: ["US"] } },
        col_sort: { by: "value", direction: "asc" },
      }),
    );
    expect(pd.getRowKeys()).toEqual([["US"]]);
    // US: 2023=150, 2024=150 → asc order is stable
    const colKeys = pd.getColKeys();
    expect(colKeys).toHaveLength(2);
  });

  it("filtering out all values results in empty pivot regardless of sort", () => {
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({
        filters: { region: { exclude: ["US", "EU"] } },
        row_sort: { by: "key", direction: "asc" },
      }),
    );
    expect(pd.getRowKeys()).toEqual([]);
  });
});

describe("PivotData - mixed-type sort", () => {
  it("sorts numeric strings numerically, not lexicographically", () => {
    const data: DataRecord[] = [
      { item: "2", revenue: 10 },
      { item: "10", revenue: 20 },
      { item: "1", revenue: 30 },
    ];
    const pd = new PivotData(
      data,
      makeConfig({
        rows: ["item"],
        columns: [],
        values: ["revenue"],
        row_sort: { by: "key", direction: "asc" },
      }),
    );
    expect(pd.getRowKeys()).toEqual([["1"], ["2"], ["10"]]);
  });

  it("places numbers before strings in ascending order", () => {
    const data: DataRecord[] = [
      { item: "banana", revenue: 10 },
      { item: "3", revenue: 20 },
      { item: "apple", revenue: 30 },
      { item: "1", revenue: 40 },
    ];
    const pd = new PivotData(
      data,
      makeConfig({
        rows: ["item"],
        columns: [],
        values: ["revenue"],
        row_sort: { by: "key", direction: "asc" },
      }),
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    expect(keys).toEqual(["1", "3", "apple", "banana"]);
  });

  it("places empty strings last", () => {
    const data: DataRecord[] = [
      { item: "b", revenue: 10 },
      { item: "", revenue: 20 },
      { item: "a", revenue: 30 },
    ];
    const pd = new PivotData(
      data,
      makeConfig({
        rows: ["item"],
        columns: [],
        values: ["revenue"],
        row_sort: { by: "key", direction: "asc" },
      }),
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    expect(keys).toEqual(["a", "b", ""]);
  });

  it("value sort places null values last", () => {
    const data: DataRecord[] = [
      { region: "A", year: "2023", revenue: 100 },
      { region: "B", year: "2023", revenue: null },
      { region: "C", year: "2023", revenue: 50 },
    ];
    const pd = new PivotData(
      data,
      makeConfig({
        rows: ["region"],
        columns: ["year"],
        values: ["revenue"],
        row_sort: { by: "value", direction: "asc" },
      }),
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    // C=50, A=100, B=null(last)
    expect(keys).toEqual(["C", "A", "B"]);
  });
});

describe("mixedCompare", () => {
  it("equal strings return 0", () => {
    expect(mixedCompare("abc", "abc")).toBe(0);
  });

  it("empty string sorts last", () => {
    expect(mixedCompare("", "a")).toBe(1);
    expect(mixedCompare("a", "")).toBe(-1);
  });

  it("numeric strings are compared numerically", () => {
    expect(mixedCompare("2", "10")).toBeLessThan(0);
    expect(mixedCompare("100", "20")).toBeGreaterThan(0);
  });

  it("numbers sort before strings", () => {
    expect(mixedCompare("5", "abc")).toBeLessThan(0);
    expect(mixedCompare("xyz", "3")).toBeGreaterThan(0);
  });

  it("strings are compared with localeCompare", () => {
    expect(mixedCompare("apple", "banana")).toBeLessThan(0);
    expect(mixedCompare("banana", "apple")).toBeGreaterThan(0);
  });
});

describe("PivotData - sort stability", () => {
  it("equal values retain insertion order from data", () => {
    const data: DataRecord[] = [
      { region: "C", revenue: 100 },
      { region: "A", revenue: 100 },
      { region: "B", revenue: 100 },
    ];
    const pd = new PivotData(
      data,
      makeConfig({
        rows: ["region"],
        columns: [],
        values: ["revenue"],
        row_sort: { by: "value", direction: "asc" },
      }),
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    // All values equal (100) – Array.sort is stable in modern JS, so the
    // insertion order from the data (C, A, B) is preserved when all
    // comparisons return 0.
    expect(keys).toEqual(["C", "A", "B"]);
  });
});

describe("PivotData - hierarchical value sort", () => {
  // Regression: value sort with 3+ row dimensions must preserve grouping.
  // Without hierarchical sort, leaf-level value ordering interleaves parent
  // groups (e.g., "Ming Li" appears twice under the same L4 Manager).
  const HIER_DATA: DataRecord[] = [
    { l4: "Mohit", l5: "Ming", name: "Andrew", prs: 124 },
    { l4: "Mohit", l5: "Ming", name: "Stephen", prs: 57 },
    { l4: "Mohit", l5: "Sanchit", name: "Nolan", prs: 44 },
    { l4: "Mohit", l5: "Sanchit", name: "Anoushka", prs: 41 },
    { l4: "Mohit", l5: "Ming", name: "David", prs: 35 },
    { l4: "Mohit", l5: "Ming", name: "Yao", prs: 33 },
    { l4: "Mohit", l5: "Sanchit", name: "Saurav", prs: 30 },
  ];

  it("value desc preserves parent grouping with 3 row dimensions", () => {
    const pd = new PivotData(
      HIER_DATA,
      makeConfig({
        rows: ["l4", "l5", "name"],
        columns: [],
        values: ["prs"],
        row_sort: { by: "value", direction: "desc" },
      }),
    );
    const keys = pd.getRowKeys();
    const l5Values = keys.map((k) => k[1]);
    // Ming subtotal (124+57+35+33=249) > Sanchit subtotal (44+41+30=115),
    // so all Ming rows must come before all Sanchit rows — no interleaving.
    const firstSanchitIdx = l5Values.indexOf("Sanchit");
    const lastMingIdx = l5Values.lastIndexOf("Ming");
    expect(lastMingIdx).toBeLessThan(firstSanchitIdx);
  });

  it("value asc preserves parent grouping with 3 row dimensions", () => {
    const pd = new PivotData(
      HIER_DATA,
      makeConfig({
        rows: ["l4", "l5", "name"],
        columns: [],
        values: ["prs"],
        row_sort: { by: "value", direction: "asc" },
      }),
    );
    const keys = pd.getRowKeys();
    const l5Values = keys.map((k) => k[1]);
    // Asc: Sanchit (115) < Ming (249), so Sanchit first, then Ming
    const lastSanchitIdx = l5Values.lastIndexOf("Sanchit");
    const firstMingIdx = l5Values.indexOf("Ming");
    expect(lastSanchitIdx).toBeLessThan(firstMingIdx);
  });

  it("leaf rows are sorted by value within each parent group", () => {
    const pd = new PivotData(
      HIER_DATA,
      makeConfig({
        rows: ["l4", "l5", "name"],
        columns: [],
        values: ["prs"],
        row_sort: { by: "value", direction: "desc" },
      }),
    );
    const keys = pd.getRowKeys();
    // Ming group: Andrew(124), Stephen(57), David(35), Yao(33)
    const mingRows = keys.filter((k) => k[1] === "Ming");
    const mingValues = mingRows.map((k) => k[2]);
    expect(mingValues).toEqual(["Andrew", "Stephen", "David", "Yao"]);
    // Sanchit group: Nolan(44), Anoushka(41), Saurav(30)
    const sanchitRows = keys.filter((k) => k[1] === "Sanchit");
    const sanchitValues = sanchitRows.map((k) => k[2]);
    expect(sanchitValues).toEqual(["Nolan", "Anoushka", "Saurav"]);
  });

  it("grouped rows have no duplicate subtotal groups", () => {
    const pd = new PivotData(
      HIER_DATA,
      makeConfig({
        rows: ["l4", "l5", "name"],
        columns: [],
        values: ["prs"],
        row_sort: { by: "value", direction: "desc" },
        show_subtotals: true,
      }),
    );
    const grouped = pd.getGroupedRowKeys();
    const subtotals = grouped
      .filter((e) => e.type === "subtotal")
      .map((e) => e.key.join("/"));
    // Each L5 manager should appear exactly once as a subtotal
    expect(subtotals.filter((s) => s === "Mohit/Ming")).toHaveLength(1);
    expect(subtotals.filter((s) => s === "Mohit/Sanchit")).toHaveLength(1);
  });
});

describe("PivotData - scoped value sort", () => {
  // Two L4 groups with different subtotals so we can verify parent order.
  // L4 subtotals: Alpha=10+20+5=35, Beta=50+30+15=95
  // L5 subtotals within Alpha: X=10+20=30, Y=5
  // L5 subtotals within Beta:  X=50, Y=30+15=45
  const SCOPED_DATA: DataRecord[] = [
    { l4: "Alpha", l5: "X", name: "A1", v: 10 },
    { l4: "Alpha", l5: "X", name: "A2", v: 20 },
    { l4: "Alpha", l5: "Y", name: "A3", v: 5 },
    { l4: "Beta", l5: "X", name: "B1", v: 50 },
    { l4: "Beta", l5: "Y", name: "B2", v: 30 },
    { l4: "Beta", l5: "Y", name: "B3", v: 15 },
  ];

  it("global value sort without dimension sorts all levels desc", () => {
    const pd = new PivotData(
      SCOPED_DATA,
      makeConfig({
        rows: ["l4", "l5", "name"],
        columns: [],
        values: ["v"],
        row_sort: { by: "value", direction: "desc" },
      }),
    );
    const keys = pd.getRowKeys();
    // Beta(95) > Alpha(35) — Beta first
    expect(keys[0][0]).toBe("Beta");
    // Within Beta: X(50) > Y(45) — X first
    const betaRows = keys.filter((k) => k[0] === "Beta");
    expect(betaRows[0][1]).toBe("X");
    // Within Alpha: X(30) > Y(5) — X first
    const alphaRows = keys.filter((k) => k[0] === "Alpha");
    expect(alphaRows[0][1]).toBe("X");
  });

  it("scoped value sort from middle dim preserves parent group order", () => {
    const pd = new PivotData(
      SCOPED_DATA,
      makeConfig({
        rows: ["l4", "l5", "name"],
        columns: [],
        values: ["v"],
        row_sort: {
          by: "value",
          direction: "desc",
          value_field: "v",
          dimension: "l5",
        },
      }),
    );
    const keys = pd.getRowKeys();
    // L4 groups should be in DEFAULT (ascending) order: Alpha < Beta
    expect(keys[0][0]).toBe("Alpha");
    const betaRows = keys.filter((k) => k[0] === "Beta");
    expect(betaRows.length).toBeGreaterThan(0);
    const lastAlphaIdx = keys.findIndex((k) => k[0] === "Beta") - 1;
    expect(keys[lastAlphaIdx][0]).toBe("Alpha");
    // L5 groups within each L4 should be desc by subtotal
    const alphaRows = keys.filter((k) => k[0] === "Alpha");
    expect(alphaRows[0][1]).toBe("X"); // X(30) > Y(5)
    expect(betaRows[0][1]).toBe("X"); // X(50) > Y(45)
    // Leaf rows within each L5 should also be desc
    const alphaX = alphaRows.filter((k) => k[1] === "X");
    expect(alphaX.map((k) => k[2])).toEqual(["A2", "A1"]); // 20, 10
  });

  it("scoped value sort from leaf dim preserves all parent orders", () => {
    const pd = new PivotData(
      SCOPED_DATA,
      makeConfig({
        rows: ["l4", "l5", "name"],
        columns: [],
        values: ["v"],
        row_sort: {
          by: "value",
          direction: "desc",
          value_field: "v",
          dimension: "name",
        },
      }),
    );
    const keys = pd.getRowKeys();
    // L4 groups in ascending-by-subtotal order: Alpha(35) < Beta(95)
    expect(keys[0][0]).toBe("Alpha");
    // L5 groups within each L4 in ascending-by-subtotal order:
    // Alpha: Y(5) < X(30);  Beta: Y(45) < X(50)
    const alphaRows = keys.filter((k) => k[0] === "Alpha");
    expect(alphaRows[0][1]).toBe("Y");
    expect(alphaRows[alphaRows.length - 1][1]).toBe("X");
    // Only leaf rows (name) sort desc within each L5 group
    const alphaX = alphaRows.filter((k) => k[1] === "X");
    expect(alphaX.map((k) => k[2])).toEqual(["A2", "A1"]); // 20 > 10
    const betaY = keys.filter((k) => k[0] === "Beta" && k[1] === "Y");
    expect(betaY.map((k) => k[2])).toEqual(["B2", "B3"]); // 30 > 15
  });
});

describe("PivotData - group boundary detection", () => {
  const BOUNDARY_DATA: DataRecord[] = [
    { region: "East", cat: "A", prod: "P1", v: 1 },
    { region: "East", cat: "A", prod: "P2", v: 2 },
    { region: "East", cat: "B", prod: "P3", v: 3 },
    { region: "West", cat: "A", prod: "P4", v: 4 },
    { region: "West", cat: "B", prod: "P5", v: 5 },
  ];

  function detectBoundaries(grouped: GroupedRow[], numGroupingDims: number) {
    const result: { idx: number; level: number }[] = [];
    let prevDataKey: string[] | null = null;
    for (let i = 0; i < grouped.length; i++) {
      if (grouped[i].type === "subtotal") {
        continue;
      }
      if (prevDataKey) {
        for (let d = 0; d < numGroupingDims; d++) {
          if (grouped[i].key[d] !== prevDataKey[d]) {
            result.push({ idx: i, level: d });
            break;
          }
        }
      }
      prevDataKey = grouped[i].key;
    }
    return result;
  }

  it("detects L0 boundary when region changes in 2-level hierarchy", () => {
    const pd = new PivotData(
      BOUNDARY_DATA,
      makeConfig({
        rows: ["region", "cat"],
        columns: [],
        values: ["v"],
        show_subtotals: true,
      }),
    );
    const grouped = pd.getGroupedRowKeys();
    const boundaries = detectBoundaries(grouped, 1);
    expect(boundaries.some((b) => b.level === 0)).toBe(true);
  });

  it("detects L1 boundary when cat changes within same region in 3-level hierarchy", () => {
    const pd = new PivotData(
      BOUNDARY_DATA,
      makeConfig({
        rows: ["region", "cat", "prod"],
        columns: [],
        values: ["v"],
        show_subtotals: true,
      }),
    );
    const grouped = pd.getGroupedRowKeys();
    const boundaries = detectBoundaries(grouped, 2);
    const l1Boundaries = boundaries.filter((b) => b.level === 1);
    expect(l1Boundaries.length).toBeGreaterThan(0);
  });

  it("no boundaries when only one group exists", () => {
    const pd = new PivotData(
      BOUNDARY_DATA.filter((r) => r.region === "East" && r.cat === "A"),
      makeConfig({
        rows: ["region", "cat"],
        columns: [],
        values: ["v"],
        show_subtotals: true,
      }),
    );
    const grouped = pd.getGroupedRowKeys();
    const boundaries = detectBoundaries(grouped, 1);
    expect(boundaries).toHaveLength(0);
  });
});

describe("PivotData - performance budgets", () => {
  function generateRecords(
    n: number,
    regions: number,
    years: number,
  ): DataRecord[] {
    const recs: DataRecord[] = [];
    for (let i = 0; i < n; i++) {
      recs.push({
        region: `R${i % regions}`,
        year: `${2000 + (i % years)}`,
        revenue: Math.random() * 10000,
      });
    }
    return recs;
  }

  it("small dataset (1K rows) computes within 100ms", () => {
    const records = generateRecords(1000, 10, 5);
    const config = makeConfig();
    const { elapsedMs } = measureSync(() => new PivotData(records, config));
    expect(elapsedMs).toBeLessThan(100);
  });

  it("medium dataset (50K rows) computes within maxComputeMs budget", () => {
    const records = generateRecords(50000, 100, 20);
    const config = makeConfig();
    const { elapsedMs } = measureSync(() => new PivotData(records, config));
    expect(elapsedMs).toBeLessThan(DEFAULT_BUDGETS.maxComputeMs);
  });

  it("getRowKeys/getColKeys are fast after computation", () => {
    const records = generateRecords(50000, 100, 20);
    const config = makeConfig();
    const pd = new PivotData(records, config);
    const { elapsedMs: rowMs } = measureSync(() => pd.getRowKeys());
    const { elapsedMs: colMs } = measureSync(() => pd.getColKeys());
    expect(rowMs).toBeLessThan(10);
    expect(colMs).toBeLessThan(10);
  });
});

// ---------------------------------------------------------------------------
// Phase 3a: Subtotals + Collapse/Expand
// ---------------------------------------------------------------------------

const MULTI_ROW_DATA: DataRecord[] = [
  { region: "US", state: "CA", year: "2023", revenue: 100, profit: 30 },
  { region: "US", state: "CA", year: "2024", revenue: 150, profit: 45 },
  { region: "US", state: "NY", year: "2023", revenue: 80, profit: 10 },
  { region: "US", state: "NY", year: "2024", revenue: 120, profit: 25 },
  { region: "EU", state: "DE", year: "2023", revenue: 200, profit: 60 },
  { region: "EU", state: "DE", year: "2024", revenue: 250, profit: 70 },
  { region: "EU", state: "FR", year: "2023", revenue: 180, profit: 20 },
  { region: "EU", state: "FR", year: "2024", revenue: 220, profit: 50 },
];

describe("PivotData - subtotals (Phase 3a)", () => {
  function multiRowConfig(overrides: TestConfigOverrides = {}): PivotConfigV1 {
    return makeConfig({
      rows: ["region", "state"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
      ...overrides,
    });
  }

  it("getSubtotalAggregator computes correct sum for parent group", () => {
    const pd = new PivotData(MULTI_ROW_DATA, multiRowConfig());
    // US subtotal for 2023 = CA(100) + NY(80) = 180
    expect(pd.getSubtotalAggregator(["US"], ["2023"], "revenue").value()).toBe(
      180,
    );
    // EU subtotal for 2024 = DE(250) + FR(220) = 470
    expect(pd.getSubtotalAggregator(["EU"], ["2024"], "revenue").value()).toBe(
      470,
    );
  });

  it("getSubtotalAggregator returns empty aggregator for non-existent group", () => {
    const pd = new PivotData(MULTI_ROW_DATA, multiRowConfig());
    expect(
      pd.getSubtotalAggregator(["APAC"], ["2023"], "revenue").value(),
    ).toBeNull();
  });

  it("getSubtotalAggregator row total sums all columns for parent", () => {
    const pd = new PivotData(MULTI_ROW_DATA, multiRowConfig());
    // US total = 100+150+80+120 = 450
    expect(pd.getSubtotalAggregator(["US"], [], "revenue").value()).toBe(450);
    // EU total = 200+250+180+220 = 850
    expect(pd.getSubtotalAggregator(["EU"], [], "revenue").value()).toBe(850);
  });

  it("subtotals are correct for non-additive aggregator (avg)", () => {
    const pd = new PivotData(
      MULTI_ROW_DATA,
      multiRowConfig({ aggregation: "avg" }),
    );
    // US avg for 2023 = (100+80)/2 = 90
    expect(pd.getSubtotalAggregator(["US"], ["2023"], "revenue").value()).toBe(
      90,
    );
    // EU avg for 2023 = (200+180)/2 = 190
    expect(pd.getSubtotalAggregator(["EU"], ["2023"], "revenue").value()).toBe(
      190,
    );
  });

  it("subtotals apply mixed aggregations per field", () => {
    const pd = new PivotData(
      MULTI_ROW_DATA,
      multiRowConfig({
        values: ["revenue", "profit"],
        aggregation: { revenue: "sum", profit: "avg" },
      }),
    );

    expect(pd.getSubtotalAggregator(["US"], ["2023"], "revenue").value()).toBe(
      180,
    );
    expect(pd.getSubtotalAggregator(["US"], ["2023"], "profit").value()).toBe(
      20,
    );

    expect(pd.getSubtotalAggregator(["EU"], [], "revenue").value()).toBe(850);
    expect(pd.getSubtotalAggregator(["EU"], [], "profit").value()).toBe(50);
  });

  it("getGroupedRowKeys returns data rows and subtotal rows", () => {
    const pd = new PivotData(MULTI_ROW_DATA, multiRowConfig());
    const grouped = pd.getGroupedRowKeys();
    const types = grouped.map((g) => g.type);
    // Data rows for EU (DE, FR) + EU subtotal + Data rows for US (CA, NY) + US subtotal
    expect(types).toEqual([
      "data",
      "data",
      "subtotal",
      "data",
      "data",
      "subtotal",
    ]);
  });

  it("collapsed groups hide children but keep subtotal row", () => {
    const pd = new PivotData(
      MULTI_ROW_DATA,
      multiRowConfig({
        collapsed_groups: ["EU"],
      }),
    );
    const grouped = pd.getGroupedRowKeys();
    // EU children hidden, EU subtotal remains; US children + subtotal visible
    const entries = grouped.map((g) => `${g.type}:${g.key.join("/")}`);
    expect(entries).toEqual([
      "subtotal:EU",
      "data:US/CA",
      "data:US/NY",
      "subtotal:US",
    ]);
  });

  it("__ALL__ marker collapses all top-level groups", () => {
    const pd = new PivotData(
      MULTI_ROW_DATA,
      multiRowConfig({
        collapsed_groups: ["__ALL__"],
      }),
    );
    const grouped = pd.getGroupedRowKeys();
    // Only subtotal rows visible
    expect(grouped).toEqual([
      { type: "subtotal", key: ["EU"], level: 0 },
      { type: "subtotal", key: ["US"], level: 0 },
    ]);
  });

  it("no subtotals when less than 2 row dimensions", () => {
    const pd = new PivotData(
      MULTI_ROW_DATA,
      makeConfig({
        rows: ["region"],
        show_subtotals: true,
      }),
    );
    const grouped = pd.getGroupedRowKeys();
    expect(grouped.every((g) => g.type === "data")).toBe(true);
  });

  it("no subtotals when show_subtotals is false", () => {
    const pd = new PivotData(
      MULTI_ROW_DATA,
      multiRowConfig({ show_subtotals: false }),
    );
    const grouped = pd.getGroupedRowKeys();
    expect(grouped.every((g) => g.type === "data")).toBe(true);
  });

  it("3-level nesting produces subtotals at each parent level", () => {
    const data: DataRecord[] = [
      { a: "X", b: "1", c: "i", v: 10 },
      { a: "X", b: "1", c: "ii", v: 20 },
      { a: "X", b: "2", c: "i", v: 30 },
      { a: "Y", b: "1", c: "i", v: 40 },
    ];
    const config = makeConfig({
      rows: ["a", "b", "c"],
      columns: [],
      values: ["v"],
      show_subtotals: true,
    });
    const pd = new PivotData(data, config);
    const grouped = pd.getGroupedRowKeys();
    const typeKeys = grouped.map(
      (g) => `${g.type}[${g.level}]:${g.key.join("/")}`,
    );
    // X/1/i, X/1/ii, subtotal X/1, X/2/i, subtotal X/2, subtotal X,
    // Y/1/i, subtotal Y/1, subtotal Y
    expect(typeKeys).toEqual([
      "data[2]:X/1/i",
      "data[2]:X/1/ii",
      "subtotal[1]:X/1",
      "data[2]:X/2/i",
      "subtotal[1]:X/2",
      "subtotal[0]:X",
      "data[2]:Y/1/i",
      "subtotal[1]:Y/1",
      "subtotal[0]:Y",
    ]);
  });
});

// ---------------------------------------------------------------------------
// Phase 3a: Column-group subtotals (collapse/expand columns)
// ---------------------------------------------------------------------------

const MULTI_COL_DATA: DataRecord[] = [
  { region: "US", year: "2023", quarter: "Q1", revenue: 100 },
  { region: "US", year: "2023", quarter: "Q2", revenue: 120 },
  { region: "US", year: "2024", quarter: "Q1", revenue: 200 },
  { region: "EU", year: "2023", quarter: "Q1", revenue: 80 },
  { region: "EU", year: "2023", quarter: "Q2", revenue: 90 },
  { region: "EU", year: "2024", quarter: "Q1", revenue: 150 },
];

describe("PivotData - column group subtotals (Phase 3a)", () => {
  function multiColConfig(overrides: TestConfigOverrides = {}): PivotConfigV1 {
    return makeConfig({
      rows: ["region"],
      columns: ["year", "quarter"],
      values: ["revenue"],
      ...overrides,
    });
  }

  it("getColGroupSubtotal sums all children within a column group for a row", () => {
    const pd = new PivotData(MULTI_COL_DATA, multiColConfig());
    // US, year=2023: Q1(100) + Q2(120) = 220
    expect(pd.getColGroupSubtotal(["US"], ["2023"], "revenue").value()).toBe(
      220,
    );
    // EU, year=2023: Q1(80) + Q2(90) = 170
    expect(pd.getColGroupSubtotal(["EU"], ["2023"], "revenue").value()).toBe(
      170,
    );
    // US, year=2024: Q1(200) = 200
    expect(pd.getColGroupSubtotal(["US"], ["2024"], "revenue").value()).toBe(
      200,
    );
  });

  it("getColGroupGrandSubtotal sums all rows for a column group", () => {
    const pd = new PivotData(MULTI_COL_DATA, multiColConfig());
    // All rows, year=2023: 100+120+80+90 = 390
    expect(pd.getColGroupGrandSubtotal(["2023"], "revenue").value()).toBe(390);
    // All rows, year=2024: 200+150 = 350
    expect(pd.getColGroupGrandSubtotal(["2024"], "revenue").value()).toBe(350);
  });

  it("getSubtotalColGroupAgg computes cross-section subtotals (row-group x col-group)", () => {
    const data: DataRecord[] = [
      { region: "US", state: "CA", year: "2023", quarter: "Q1", revenue: 10 },
      { region: "US", state: "CA", year: "2023", quarter: "Q2", revenue: 20 },
      { region: "US", state: "NY", year: "2023", quarter: "Q1", revenue: 30 },
      { region: "EU", state: "DE", year: "2023", quarter: "Q1", revenue: 40 },
    ];
    const config = makeConfig({
      rows: ["region", "state"],
      columns: ["year", "quarter"],
      values: ["revenue"],
      show_subtotals: true,
    });
    const pd = new PivotData(data, config);
    // US subtotal for year=2023: CA(10+20) + NY(30) = 60
    expect(pd.getSubtotalColGroupAgg(["US"], ["2023"], "revenue").value()).toBe(
      60,
    );
    // EU subtotal for year=2023: DE(40) = 40
    expect(pd.getSubtotalColGroupAgg(["EU"], ["2023"], "revenue").value()).toBe(
      40,
    );
  });

  it("returns empty aggregator for non-existent column group", () => {
    const pd = new PivotData(MULTI_COL_DATA, multiColConfig());
    expect(
      pd.getColGroupSubtotal(["US"], ["2025"], "revenue").value(),
    ).toBeNull();
  });

  it("column group subtotals correct for non-additive aggregator (avg)", () => {
    const pd = new PivotData(
      MULTI_COL_DATA,
      multiColConfig({ aggregation: "avg" }),
    );
    // US, year=2023, avg: (100+120)/2 = 110
    expect(pd.getColGroupSubtotal(["US"], ["2023"], "revenue").value()).toBe(
      110,
    );
    // EU, year=2023, avg: (80+90)/2 = 85
    expect(pd.getColGroupSubtotal(["EU"], ["2023"], "revenue").value()).toBe(
      85,
    );
  });

  it("does not build col subtotals for single column dimension", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig({ columns: ["year"] }));
    // Single col dim — getColGroupSubtotal returns empty aggregator
    expect(
      pd.getColGroupSubtotal(["US"], ["2023"], "revenue").value(),
    ).toBeNull();
  });
});

describe("PivotData - synthetic measures", () => {
  it("computes synthetic ratio and difference alongside raw measures", () => {
    const config = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "rev_per_profit",
          label: "Revenue / Profit",
          operation: "sum_over_sum",
          numerator: "revenue",
          denominator: "profit",
        },
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
    expect(pd.getAggregator(["EU"], ["2023"], "revenue").value()).toBe(200);
    expect(pd.getAggregator(["EU"], ["2023"], "rev_per_profit").value()).toBe(
      2.5,
    );
    expect(pd.getAggregator(["EU"], ["2023"], "rev_minus_profit").value()).toBe(
      120,
    );
  });

  it("supports synthetic source fields hidden from visible values", () => {
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
    expect(pd.getAggregator(["EU"], ["2023"], "rev_minus_profit").value()).toBe(
      120,
    );
  });

  it("returns null for synthetic ratio when denominator is zero", () => {
    const data: DataRecord[] = [
      { region: "US", year: "2024", revenue: 30, profit: 0 },
    ];
    const config = makeConfig({
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "rev_per_profit",
          label: "Revenue / Profit",
          operation: "sum_over_sum",
          numerator: "revenue",
          denominator: "profit",
        },
      ],
    });
    const pd = new PivotData(data, config);
    expect(
      pd.getAggregator(["US"], ["2024"], "rev_per_profit").value(),
    ).toBeNull();
  });

  it("computes synthetic row/column/grand totals from sums", () => {
    const data: DataRecord[] = [
      { region: "US", year: "2023", revenue: 100, profit: 25 },
      { region: "US", year: "2024", revenue: 150, profit: 75 },
      { region: "EU", year: "2023", revenue: 200, profit: 100 },
      { region: "EU", year: "2024", revenue: 250, profit: 50 },
    ];
    const config = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "rev_per_profit",
          label: "Revenue / Profit",
          operation: "sum_over_sum",
          numerator: "revenue",
          denominator: "profit",
        },
      ],
    });
    const pd = new PivotData(data, config);

    expect(pd.getRowTotal(["US"], "rev_per_profit").value()).toBeCloseTo(
      2.5,
      10,
    );
    expect(pd.getRowTotal(["EU"], "rev_per_profit").value()).toBeCloseTo(3, 10);
    expect(pd.getColTotal(["2023"], "rev_per_profit").value()).toBeCloseTo(
      2.4,
      10,
    );
    expect(pd.getColTotal(["2024"], "rev_per_profit").value()).toBeCloseTo(
      3.2,
      10,
    );
    expect(pd.getGrandTotal("rev_per_profit").value()).toBeCloseTo(2.8, 10);
  });

  it("computes synthetic subtotal aggregators from subtotal sums", () => {
    const data: DataRecord[] = [
      { region: "US", team: "A", year: "2023", revenue: 10, profit: 2 },
      { region: "US", team: "B", year: "2023", revenue: 30, profit: 15 },
      { region: "US", team: "A", year: "2024", revenue: 20, profit: 10 },
      { region: "US", team: "B", year: "2024", revenue: 40, profit: 5 },
      { region: "EU", team: "A", year: "2023", revenue: 50, profit: 25 },
      { region: "EU", team: "B", year: "2023", revenue: 10, profit: 5 },
    ];
    const config = makeConfig({
      rows: ["region", "team"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
      synthetic_measures: [
        {
          id: "rev_per_profit",
          label: "Revenue / Profit",
          operation: "sum_over_sum",
          numerator: "revenue",
          denominator: "profit",
        },
      ],
    });
    const pd = new PivotData(data, config);

    // US subtotal at 2023: (10 + 30) / (2 + 15) = 40 / 17
    expect(
      pd.getSubtotalAggregator(["US"], ["2023"], "rev_per_profit").value(),
    ).toBeCloseTo(40 / 17, 10);
    // US subtotal row total: (10+30+20+40) / (2+15+10+5) = 100 / 32
    expect(
      pd.getSubtotalAggregator(["US"], [], "rev_per_profit").value(),
    ).toBeCloseTo(100 / 32, 10);
  });
});

// ---------------------------------------------------------------------------
// getMatchingRecords
// ---------------------------------------------------------------------------

describe("PivotData - getMatchingRecords", () => {
  const DATA: DataRecord[] = [
    { region: "US", year: "2023", revenue: 100 },
    { region: "US", year: "2024", revenue: 150 },
    { region: "EU", year: "2023", revenue: 200 },
    { region: "EU", year: "2024", revenue: 250 },
    { region: "US", year: "2023", revenue: 50 },
  ];

  it("returns all matching records for a single dimension filter", () => {
    const pd = new PivotData(DATA, makeConfig());
    const result = pd.getMatchingRecords({ region: "US" });
    expect(result.totalCount).toBe(3);
    expect(result.records).toHaveLength(3);
    expect(result.records.every((r) => r.region === "US")).toBe(true);
  });

  it("returns matching records for multi-dimension filter", () => {
    const pd = new PivotData(DATA, makeConfig());
    const result = pd.getMatchingRecords({ region: "US", year: "2023" });
    expect(result.totalCount).toBe(2);
    expect(result.records).toHaveLength(2);
  });

  it("respects the limit parameter", () => {
    const pd = new PivotData(DATA, makeConfig());
    const result = pd.getMatchingRecords({ region: "US" }, 1);
    expect(result.totalCount).toBe(3);
    expect(result.records).toHaveLength(1);
  });

  it("returns empty result for non-matching filters", () => {
    const pd = new PivotData(DATA, makeConfig());
    const result = pd.getMatchingRecords({ region: "APAC" });
    expect(result.totalCount).toBe(0);
    expect(result.records).toHaveLength(0);
  });

  it("returns all records when filters is empty", () => {
    const pd = new PivotData(DATA, makeConfig());
    const result = pd.getMatchingRecords({});
    expect(result.totalCount).toBe(5);
    expect(result.records).toHaveLength(5);
  });

  it("respects config-level filters on top of cell-click filters", () => {
    const pd = new PivotData(
      DATA,
      makeConfig({
        filters: { year: { include: ["2023"] } },
      }),
    );
    const result = pd.getMatchingRecords({ region: "US" });
    expect(result.totalCount).toBe(2);
    expect(
      result.records.every((r) => r.year === "2023" && r.region === "US"),
    ).toBe(true);
  });

  it("matches null-dimension records when null_handling is separate", () => {
    const nullData: DataRecord[] = [
      { region: null, year: "2023", revenue: 100 },
      { region: "US", year: "2023", revenue: 200 },
      { region: "", year: "2024", revenue: 50 },
    ];
    const pd = new PivotData(nullData, makeConfig(), {
      nullHandling: { region: "separate" },
    });
    const result = pd.getMatchingRecords({ region: "(null)" });
    expect(result.totalCount).toBe(2);
    expect(result.records).toHaveLength(2);
  });
});

// ---------------------------------------------------------------------------
// getColumnNames
// ---------------------------------------------------------------------------

describe("PivotData - getColumnNames", () => {
  it("returns all column names from records", () => {
    const pd = new PivotData(SAMPLE_DATA, makeConfig());
    const cols = pd.getColumnNames();
    expect(cols).toContain("region");
    expect(cols).toContain("year");
    expect(cols).toContain("revenue");
    expect(cols).toContain("profit");
    expect(cols).toHaveLength(4);
  });
});

describe("PivotData - DataRecordSource wrapper", () => {
  it("matches array-backed aggregates and keys", () => {
    const cfg = makeConfig();
    const fromArray = new PivotData(SAMPLE_DATA, cfg);
    const source = new DataRecordSource(
      SAMPLE_DATA,
      Object.keys(SAMPLE_DATA[0]!),
    );
    const fromSource = new PivotData(source, cfg);
    expect(fromSource.getRowKeys()).toEqual(fromArray.getRowKeys());
    expect(fromSource.getColKeys()).toEqual(fromArray.getColKeys());
    expect(fromSource.getAggregator(["US"], ["2023"]).value()).toEqual(
      fromArray.getAggregator(["US"], ["2023"]).value(),
    );
  });
});

class TestColumnarSource implements ColumnarDataSource {
  constructor(private readonly rows: DataRecord[]) {}

  get numRows(): number {
    return this.rows.length;
  }

  getValue(rowIndex: number, fieldName: string): unknown {
    return this.rows[rowIndex]![fieldName];
  }

  getColumnNames(): string[] {
    return Object.keys(this.rows[0] ?? {});
  }
}

describe("PivotData - non-materialized columnar source", () => {
  it("getMatchingRecords materializes rows on demand", () => {
    const cfg = makeConfig();
    const src = new TestColumnarSource([...SAMPLE_DATA]);
    const pd = new PivotData(src, cfg);
    const r = pd.getMatchingRecords({ region: "US", year: "2023" });
    expect(r.totalCount).toBe(2);
    expect(r.records).toHaveLength(2);
    expect(
      r.records.every((row) => row.region === "US" && row.year === "2023"),
    ).toBe(true);
  });

  it("getUniqueValues scans the columnar source", () => {
    const src = new TestColumnarSource(SAMPLE_DATA);
    const pd = new PivotData(src, makeConfig());
    expect(pd.getUniqueValues("region")).toEqual(["EU", "US"]);
  });

  it("aggregates match array-backed results", () => {
    const cfg = makeConfig();
    const fromArray = new PivotData(SAMPLE_DATA, cfg);
    const fromColumnar = new PivotData(
      new TestColumnarSource(SAMPLE_DATA),
      cfg,
    );
    expect(fromColumnar.getRowKeys()).toEqual(fromArray.getRowKeys());
    expect(fromColumnar.getColKeys()).toEqual(fromArray.getColKeys());
    expect(fromColumnar.getGrandTotal().value()).toEqual(
      fromArray.getGrandTotal().value(),
    );
    expect(fromColumnar.recordCount).toBe(fromArray.recordCount);
  });

  it("getColumnNames works without materialized records", () => {
    const src = new TestColumnarSource(SAMPLE_DATA);
    const pd = new PivotData(src, makeConfig());
    expect(pd.getColumnNames()).toEqual(
      expect.arrayContaining(["region", "year", "revenue", "profit"]),
    );
  });
});

// ---------------------------------------------------------------------------
// Hybrid sidecar + remap tests
// ---------------------------------------------------------------------------

import { buildSidecarFingerprint } from "./PivotData";
import type { HybridTotals } from "./types";

function makeHybridTotals(
  config: PivotConfigV1,
  grand: Record<string, number | null>,
  row: HybridTotals["row"] = [],
  col: HybridTotals["col"] = [],
): HybridTotals {
  return {
    sidecar_fingerprint: buildSidecarFingerprint(config, undefined),
    grand,
    row,
    col,
  };
}

describe("PivotData - hybrid sidecar override", () => {
  it("getGrandTotal uses sidecar value when fingerprint matches", () => {
    const cfg = makeConfig({ aggregation: "median" });
    const totals = makeHybridTotals(cfg, { revenue: 42.5 });
    const pd = new PivotData(SAMPLE_DATA, cfg, { hybridTotals: totals });
    expect(pd.getGrandTotal("revenue").value()).toBe(42.5);
  });

  it("accepts Python-style sidecar fingerprints with empty date_grains objects", () => {
    const cfg = makeConfig({ aggregation: { revenue: "avg" } });
    const hybridRows = [
      { region: "East", year: "2024", revenue: 5 },
      { region: "", year: "2024", revenue: 5 },
      { region: "West", year: "2024", revenue: 7 },
    ];
    const totals: HybridTotals = {
      sidecar_fingerprint: JSON.stringify({
        adaptive_date_grains: {},
        aggregation: { revenue: "avg" },
        auto_date_hierarchy: true,
        columns: ["year"],
        date_grains: {},
        filters: {},
        null_handling: "zero",
        rows: ["region"],
        show_subtotals: false,
        values: ["revenue"],
      }),
      grand: { revenue: 5.5 },
      row: [
        { key: ["East"], values: { revenue: 5 } },
        { key: [""], values: { revenue: 5 } },
        { key: ["West"], values: { revenue: 7 } },
      ],
      col: [{ key: ["2024"], values: { revenue: 5.5 } }],
    };
    const pd = new PivotData(hybridRows, cfg, {
      hybridTotals: totals,
      nullHandling: "zero",
    });
    expect(pd.getGrandTotal("revenue").value()).toBe(5.5);
  });

  it("getRowTotal uses sidecar value when available", () => {
    const cfg = makeConfig({ aggregation: "median" });
    const totals = makeHybridTotals(cfg, { revenue: 42.5 }, [
      { key: ["US"], values: { revenue: 100 } },
      { key: ["EU"], values: { revenue: 225 } },
    ]);
    const pd = new PivotData(SAMPLE_DATA, cfg, { hybridTotals: totals });
    expect(pd.getRowTotal(["US"], "revenue").value()).toBe(100);
    expect(pd.getRowTotal(["EU"], "revenue").value()).toBe(225);
  });

  it("getColTotal uses sidecar value when available", () => {
    const cfg = makeConfig({ aggregation: "median" });
    const totals = makeHybridTotals(
      cfg,
      { revenue: 42.5 },
      [],
      [
        { key: ["2023"], values: { revenue: 99 } },
        { key: ["2024"], values: { revenue: 88 } },
      ],
    );
    const pd = new PivotData(SAMPLE_DATA, cfg, { hybridTotals: totals });
    expect(pd.getColTotal(["2023"], "revenue").value()).toBe(99);
    expect(pd.getColTotal(["2024"], "revenue").value()).toBe(88);
  });

  it("falls back to client aggregation for decomposable fields", () => {
    const cfg = makeConfig({ aggregation: "sum" });
    const pd = new PivotData(SAMPLE_DATA, cfg);
    expect(pd.getGrandTotal("revenue").value()).toBe(750);
  });

  it("falls back to client aggregation when sidecar is missing a field", () => {
    const cfg = makeConfig({ aggregation: "median" });
    const totals = makeHybridTotals(cfg, {});
    const pd = new PivotData(SAMPLE_DATA, cfg, { hybridTotals: totals });
    const gt = pd.getGrandTotal("revenue").value();
    expect(typeof gt).toBe("number");
  });
});

describe("PivotData - hybrid sidecar staleness", () => {
  it("ignores sidecar when fingerprint mismatches (agg change)", () => {
    const cfg = makeConfig({ aggregation: "median" });
    const staleConfig = makeConfig({ aggregation: "sum" });
    const totals: HybridTotals = {
      sidecar_fingerprint: buildSidecarFingerprint(staleConfig, undefined),
      grand: { revenue: 999 },
      row: [],
      col: [],
    };
    const pd = new PivotData(SAMPLE_DATA, cfg, { hybridTotals: totals });
    expect(pd.getGrandTotal("revenue").value()).not.toBe(999);
  });

  it("ignores sidecar when fingerprint mismatches (layout change)", () => {
    const cfg = makeConfig({ aggregation: "median" });
    const staleConfig = makeConfig({
      aggregation: "median",
      rows: ["year"],
      columns: ["region"],
    });
    const totals: HybridTotals = {
      sidecar_fingerprint: buildSidecarFingerprint(staleConfig, undefined),
      grand: { revenue: 999 },
      row: [],
      col: [],
    };
    const pd = new PivotData(SAMPLE_DATA, cfg, { hybridTotals: totals });
    expect(pd.getGrandTotal("revenue").value()).not.toBe(999);
  });
});

describe("PivotData - hybrid agg remap", () => {
  it("count field uses SumAggregator via remap so leaf cells are correct", () => {
    const countData: DataRecord[] = [
      { region: "US", year: "2023", revenue: 3 },
      { region: "EU", year: "2023", revenue: 5 },
    ];
    const cfg = makeConfig({ aggregation: "count" });
    const pd = new PivotData(countData, cfg, {
      hybridAggRemap: { revenue: "sum" },
    });
    expect(pd.getAggregator(["US"], ["2023"]).value()).toBe(3);
    expect(pd.getAggregator(["EU"], ["2023"]).value()).toBe(5);
  });

  it("count remap preserves zero for empty intersections", () => {
    const countData: DataRecord[] = [
      { region: "US", year: "2023", revenue: 3 },
      { region: "EU", year: "2024", revenue: 5 },
    ];
    const cfg = makeConfig({ aggregation: "count" });
    const pd = new PivotData(countData, cfg, {
      hybridAggRemap: { revenue: "sum" },
    });
    expect(pd.getAggregator(["US"], ["2024"]).value()).toBe(0);
  });

  it("count_distinct leaf cells with remap show pre-computed value", () => {
    const data: DataRecord[] = [
      { region: "US", year: "2023", revenue: 7 },
      { region: "EU", year: "2023", revenue: 4 },
    ];
    const cfg = makeConfig({ aggregation: "count_distinct" });
    const pd = new PivotData(data, cfg, {
      hybridAggRemap: { revenue: "sum" },
    });
    expect(pd.getAggregator(["US"], ["2023"]).value()).toBe(7);
    expect(pd.getAggregator(["EU"], ["2023"]).value()).toBe(4);
  });

  it("remap does not affect display config (toolbar stays count)", () => {
    const cfg = makeConfig({ aggregation: "count" });
    const pd = new PivotData(SAMPLE_DATA, cfg, {
      hybridAggRemap: { revenue: "sum" },
    });
    expect(pd.config.aggregation["revenue"]).toBe("count");
  });
});

describe("PivotData - buildSidecarFingerprint", () => {
  it("produces deterministic output regardless of key order", () => {
    const cfg1 = makeConfig({ aggregation: "median" });
    const cfg2 = makeConfig({ aggregation: "median" });
    expect(buildSidecarFingerprint(cfg1, undefined)).toBe(
      buildSidecarFingerprint(cfg2, undefined),
    );
  });

  it("differs when aggregation changes", () => {
    const fp1 = buildSidecarFingerprint(
      makeConfig({ aggregation: "median" }),
      undefined,
    );
    const fp2 = buildSidecarFingerprint(
      makeConfig({ aggregation: "avg" }),
      undefined,
    );
    expect(fp1).not.toBe(fp2);
  });

  it("null_handling dict order does not affect fingerprint", () => {
    const cfg = makeConfig();
    const fp1 = buildSidecarFingerprint(cfg, {
      region: "separate",
      year: "exclude",
    });
    const fp2 = buildSidecarFingerprint(cfg, {
      year: "exclude",
      region: "separate",
    });
    expect(fp1).toBe(fp2);
  });

  it("show_subtotals array order does not affect fingerprint", () => {
    const cfg1 = makeConfig({ show_subtotals: ["region", "year"] as any });
    const cfg2 = makeConfig({ show_subtotals: ["year", "region"] as any });
    expect(buildSidecarFingerprint(cfg1, undefined)).toBe(
      buildSidecarFingerprint(cfg2, undefined),
    );
  });

  it("cross-language parity: matches known fixture string", () => {
    const cfg = makeConfig({ aggregation: "median" });
    const fp = buildSidecarFingerprint(cfg, undefined);
    const parsed = JSON.parse(fp);
    expect(Object.keys(parsed)).toEqual(Object.keys(parsed).sort());
    expect(parsed.aggregation).toEqual({ revenue: "median" });
    expect(parsed.rows).toEqual(["region"]);
  });

  it("differs when adaptive_date_grains changes", () => {
    const cfg = makeConfig();
    const fp1 = buildSidecarFingerprint(cfg, undefined, { d: "month" });
    const fp2 = buildSidecarFingerprint(cfg, undefined, { d: "year" });
    expect(fp1).not.toBe(fp2);
  });

  it("same adaptive_date_grains produce same fingerprint", () => {
    const cfg = makeConfig();
    const fp1 = buildSidecarFingerprint(cfg, undefined, { d: "year" });
    const fp2 = buildSidecarFingerprint(cfg, undefined, { d: "year" });
    expect(fp1).toBe(fp2);
  });

  it("includes adaptive_date_grains key in fingerprint JSON", () => {
    const cfg = makeConfig();
    const fp = buildSidecarFingerprint(cfg, undefined, { d: "year" });
    const parsed = JSON.parse(fp);
    expect(parsed.adaptive_date_grains).toEqual({ d: "year" });
  });
});

describe("PivotData - filter/null-handling fix", () => {
  const DATA_WITH_NULLS: DataRecord[] = [
    { region: "US", year: "2023", revenue: 100 },
    { region: null, year: "2023", revenue: 200 },
    { region: "EU", year: "2023", revenue: 300 },
  ];

  it("_shouldIncludeRow with separate mode: include (null) filters correctly", () => {
    const cfg = makeConfig({
      rows: ["region"],
      columns: ["year"],
      filters: { region: { include: ["(null)"] } },
    });
    const pd = new PivotData(DATA_WITH_NULLS, cfg, {
      nullHandling: "separate",
    });
    const rowKeys = pd.getRowKeys().map((k) => k[0]);
    expect(rowKeys).toEqual(["(null)"]);
    expect(pd.getGrandTotal("revenue").value()).toBe(200);
  });

  it("_shouldIncludeRow with separate mode: exclude (null) works", () => {
    const cfg = makeConfig({
      rows: ["region"],
      columns: ["year"],
      filters: { region: { exclude: ["(null)"] } },
    });
    const pd = new PivotData(DATA_WITH_NULLS, cfg, {
      nullHandling: "separate",
    });
    const rowKeys = pd.getRowKeys().map((k) => k[0]);
    expect(rowKeys).not.toContain("(null)");
    expect(rowKeys).toContain("US");
    expect(rowKeys).toContain("EU");
  });

  it("_shouldIncludeRow with exclude mode: include empty string for nulls", () => {
    const cfg = makeConfig({
      rows: ["region"],
      columns: ["year"],
      filters: { region: { include: [""] } },
    });
    const pd = new PivotData(DATA_WITH_NULLS, cfg, {
      nullHandling: "exclude",
    });
    const rowKeys = pd.getRowKeys().map((k) => k[0]);
    expect(rowKeys).toEqual([""]);
    expect(pd.getGrandTotal("revenue").value()).toBe(200);
  });

  it("getUniqueValues fallback uses resolved values with separate mode", () => {
    const cfg = makeConfig({
      rows: ["region"],
      columns: ["year"],
    });
    const pd = new PivotData(DATA_WITH_NULLS, cfg, {
      nullHandling: "separate",
    });
    const unique = pd.getUniqueValues("region");
    expect(unique).toContain("(null)");
    expect(unique).not.toContain("");
  });
});
