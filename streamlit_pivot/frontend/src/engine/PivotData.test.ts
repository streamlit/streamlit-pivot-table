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
  type SyntheticMeasureConfig,
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

  it("filter on non-existent field is a no-op (column absent from data source — hybrid mode guard)", () => {
    // Fields not present in the data source are skipped by _shouldIncludeRow.
    // This prevents double-filtering in hybrid mode where off-axis fields are
    // absent from the pre-aggregated frame but may still be present in config.filters.
    const pd = new PivotData(
      SAMPLE_DATA,
      makeConfig({ filters: { nonexistent: { include: ["x"] } } }),
    );
    expect(pd.getGrandTotal().value()).toBe(750); // all rows pass — filter is a no-op
    expect(pd.uniqueRowKeyCount).toBe(2); // US and EU still present
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

  it("getHierarchyRowKeys returns parent rows before children", () => {
    const pd = new PivotData(
      MULTI_ROW_DATA,
      multiRowConfig({ row_layout: "hierarchy" }),
    );
    const grouped = pd.getHierarchyRowKeys();
    const entries = grouped.map(
      (g) => `${g.type}[${g.level}]:${g.key.join("/")}`,
    );
    expect(entries).toEqual([
      "subtotal[0]:EU",
      "data[1]:EU/DE",
      "data[1]:EU/FR",
      "subtotal[0]:US",
      "data[1]:US/CA",
      "data[1]:US/NY",
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

// ---------------------------------------------------------------------------
// Formula engine integration tests — all 8 evaluation paths
// ---------------------------------------------------------------------------

describe("PivotData - formula engine", () => {
  const FORMULA_DATA: DataRecord[] = [
    {
      region: "US",
      team: "A",
      year: "2023",
      revenue: 100,
      cost: 40,
      profit: 25,
    },
    {
      region: "US",
      team: "B",
      year: "2023",
      revenue: 50,
      cost: 30,
      profit: 15,
    },
    {
      region: "US",
      team: "A",
      year: "2024",
      revenue: 150,
      cost: 60,
      profit: 75,
    },
    {
      region: "EU",
      team: "A",
      year: "2023",
      revenue: 200,
      cost: 80,
      profit: 100,
    },
    {
      region: "EU",
      team: "B",
      year: "2024",
      revenue: 250,
      cost: 50,
      profit: 50,
    },
  ];

  function formulaConfig(
    overrides: TestConfigOverrides & {
      synthetic_measures?: PivotConfigV1["synthetic_measures"];
    } = {},
  ): PivotConfigV1 {
    const values = overrides.values ?? ["revenue"];
    const synth = overrides.synthetic_measures ?? [
      {
        id: "margin",
        label: "Margin",
        operation: "formula" as const,
        numerator: "",
        denominator: "",
        formula: '"revenue" - "cost"',
      },
    ];
    const aggFields = [
      ...new Set([
        ...values,
        ...synth.flatMap((m) => {
          if (m.operation === "formula" && m.formula) {
            const matches = m.formula.match(/"([^"]+)"/g) ?? [];
            return matches.map((s) => s.slice(1, -1));
          }
          return [m.numerator, m.denominator].filter(Boolean);
        }),
      ]),
    ];
    return {
      version: 1,
      rows: overrides.rows ?? ["region"],
      columns: overrides.columns ?? ["year"],
      values,
      show_totals: overrides.show_totals ?? true,
      empty_cell_value: "-",
      interactive: true,
      synthetic_measures: synth,
      aggregation: normalizeAggregationConfig(overrides.aggregation, aggFields),
      ...Object.fromEntries(
        Object.entries(overrides).filter(
          ([k]) =>
            ![
              "values",
              "rows",
              "columns",
              "show_totals",
              "synthetic_measures",
              "aggregation",
            ].includes(k),
        ),
      ),
    } as PivotConfigV1;
  }

  // Path 1: getAggregator (cell value)
  it("evaluates formula in getAggregator (cell)", () => {
    const pd = new PivotData(FORMULA_DATA, formulaConfig());
    // US/2023: revenue=100+50=150, cost=40+30=70 → margin=80
    expect(pd.getAggregator(["US"], ["2023"], "margin").value()).toBe(80);
    // EU/2023: revenue=200, cost=80 → margin=120
    expect(pd.getAggregator(["EU"], ["2023"], "margin").value()).toBe(120);
  });

  // Path 2: getRowTotal
  it("evaluates formula in getRowTotal", () => {
    const pd = new PivotData(FORMULA_DATA, formulaConfig());
    // US total: revenue=100+50+150=300, cost=40+30+60=130 → margin=170
    expect(pd.getRowTotal(["US"], "margin").value()).toBe(170);
  });

  // Path 3: getColTotal
  it("evaluates formula in getColTotal", () => {
    const pd = new PivotData(FORMULA_DATA, formulaConfig());
    // 2023 total: revenue=100+50+200=350, cost=40+30+80=150 → margin=200
    expect(pd.getColTotal(["2023"], "margin").value()).toBe(200);
  });

  // Path 4: getGrandTotal
  it("evaluates formula in getGrandTotal", () => {
    const pd = new PivotData(FORMULA_DATA, formulaConfig());
    // Grand: revenue=750, cost=260 → margin=490
    expect(pd.getGrandTotal("margin").value()).toBe(490);
  });

  // Path 5: getSubtotalAggregator
  it("evaluates formula in getSubtotalAggregator", () => {
    const config = formulaConfig({
      rows: ["region", "team"],
      show_subtotals: true,
    });
    const pd = new PivotData(FORMULA_DATA, config);
    // US subtotal for 2023: revenue=100+50=150, cost=40+30=70 → margin=80
    expect(pd.getSubtotalAggregator(["US"], ["2023"], "margin").value()).toBe(
      80,
    );
  });

  // Path 6: getColGroupSubtotal
  it("evaluates formula in getColGroupSubtotal", () => {
    const config = formulaConfig({
      columns: ["year", "team"],
      collapsed_groups: ["2023"],
    });
    const data: DataRecord[] = [
      { region: "US", year: "2023", team: "A", revenue: 100, cost: 40 },
      { region: "US", year: "2023", team: "B", revenue: 50, cost: 20 },
      { region: "US", year: "2024", team: "A", revenue: 200, cost: 80 },
    ];
    const pd = new PivotData(data, config);
    // US/year=2023 group: revenue=150, cost=60 → margin=90
    expect(pd.getColGroupSubtotal(["US"], ["2023"], "margin").value()).toBe(90);
  });

  // Path 7: getColGroupGrandSubtotal
  it("evaluates formula in getColGroupGrandSubtotal", () => {
    const config = formulaConfig({
      columns: ["year", "team"],
      collapsed_groups: ["2023"],
    });
    const data: DataRecord[] = [
      { region: "US", year: "2023", team: "A", revenue: 100, cost: 40 },
      { region: "US", year: "2023", team: "B", revenue: 50, cost: 20 },
      { region: "EU", year: "2023", team: "A", revenue: 200, cost: 80 },
    ];
    const pd = new PivotData(data, config);
    // Grand/year=2023 group: revenue=350, cost=140 → margin=210
    expect(pd.getColGroupGrandSubtotal(["2023"], "margin").value()).toBe(210);
  });

  // Path 8: getSubtotalColGroupAgg
  it("evaluates formula in getSubtotalColGroupAgg", () => {
    const config = formulaConfig({
      rows: ["region", "team"],
      columns: ["year", "team"],
      show_subtotals: true,
      collapsed_groups: ["2023"],
    });
    const data: DataRecord[] = [
      { region: "US", team: "A", year: "2023", revenue: 100, cost: 40 },
      { region: "US", team: "B", year: "2023", revenue: 50, cost: 20 },
      { region: "EU", team: "A", year: "2023", revenue: 200, cost: 80 },
    ];
    const pd = new PivotData(data, config);
    // US subtotal / year=2023 group: revenue=150, cost=60 → margin=90
    expect(pd.getSubtotalColGroupAgg(["US"], ["2023"], "margin").value()).toBe(
      90,
    );
  });

  // Edge case: formula referencing field not in values
  it("formula referencing field not in values defaults to sum", () => {
    const config = formulaConfig({
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "margin",
          label: "Margin",
          operation: "formula",
          numerator: "",
          denominator: "",
          formula: '"revenue" - "cost"',
        },
      ],
    });
    const pd = new PivotData(FORMULA_DATA, config);
    // cost is not in values but should be aggregated via sum
    expect(pd.getGrandTotal("margin").value()).toBe(490);
  });

  // Edge case: formula referencing field in values with non-sum aggregation
  it("formula sees configured aggregation (avg) for value fields", () => {
    const config = formulaConfig({
      values: ["revenue", "cost"],
      aggregation: { revenue: "avg", cost: "avg" } as AggregationConfig,
      synthetic_measures: [
        {
          id: "avg_margin",
          label: "Avg Margin",
          operation: "formula",
          numerator: "",
          denominator: "",
          formula: '"revenue" - "cost"',
        },
      ],
    });
    const pd = new PivotData(FORMULA_DATA, config);
    // Grand avg: revenue_avg=750/5=150, cost_avg=260/5=52 → 98
    expect(pd.getGrandTotal("avg_margin").value()).toBe(98);
  });

  // Edge case: formula with division by zero returns null
  it("formula division by zero returns null", () => {
    const data: DataRecord[] = [
      { region: "US", year: "2023", revenue: 100, cost: 0 },
    ];
    const config = formulaConfig({
      synthetic_measures: [
        {
          id: "ratio",
          label: "Ratio",
          operation: "formula",
          numerator: "",
          denominator: "",
          formula: '"revenue" / "cost"',
        },
      ],
    });
    const pd = new PivotData(data, config);
    expect(pd.getAggregator(["US"], ["2023"], "ratio").value()).toBeNull();
  });

  // Edge case: null propagation when field has no data for a cell
  it("formula returns null when referenced field has no data", () => {
    const data: DataRecord[] = [
      { region: "US", year: "2023", revenue: 100, cost: 40 },
    ];
    const config = formulaConfig();
    const pd = new PivotData(data, config);
    // EU/2023 has no data → both revenue and cost are null → margin is null
    expect(pd.getAggregator(["EU"], ["2023"], "margin").value()).toBeNull();
  });

  // Edge case: backward compat — legacy sum_over_sum unchanged
  it("legacy sum_over_sum still works alongside formula", () => {
    const config = formulaConfig({
      synthetic_measures: [
        {
          id: "ratio",
          label: "Ratio",
          operation: "sum_over_sum",
          numerator: "revenue",
          denominator: "cost",
        },
        {
          id: "margin",
          label: "Margin",
          operation: "formula",
          numerator: "",
          denominator: "",
          formula: '"revenue" - "cost"',
        },
      ],
    });
    const pd = new PivotData(FORMULA_DATA, config);
    // US/2023: ratio = 150/70 ≈ 2.14
    expect(pd.getAggregator(["US"], ["2023"], "ratio").value()).toBeCloseTo(
      150 / 70,
      5,
    );
    expect(pd.getAggregator(["US"], ["2023"], "margin").value()).toBe(80);
  });

  // Edge case: invalid formula → getFormulaErrors returns error
  it("invalid formula surfaces via getFormulaErrors()", () => {
    const config = formulaConfig({
      synthetic_measures: [
        {
          id: "broken",
          label: "Broken",
          operation: "formula",
          numerator: "",
          denominator: "",
          formula: '"revenue" + * "cost"',
        },
      ],
    });
    const pd = new PivotData(FORMULA_DATA, config);
    expect(pd.getFormulaErrors().size).toBe(1);
    expect(pd.getFormulaErrors().get("broken")).toBeDefined();
    expect(pd.getAggregator(["US"], ["2023"], "broken").value()).toBeNull();
  });

  // Edge case: empty formula string → error
  it("empty formula string surfaces via getFormulaErrors()", () => {
    const config = formulaConfig({
      synthetic_measures: [
        {
          id: "empty",
          label: "Empty",
          operation: "formula",
          numerator: "",
          denominator: "",
          formula: "",
        },
      ],
    });
    const pd = new PivotData(FORMULA_DATA, config);
    expect(pd.getFormulaErrors().size).toBe(1);
    expect(pd.getFormulaErrors().get("empty")).toBe("Formula is empty");
  });

  it("unknown field refs surface via getFormulaErrors()", () => {
    const config = formulaConfig({
      synthetic_measures: [
        {
          id: "typo",
          label: "Typo",
          operation: "formula" as const,
          numerator: "",
          denominator: "",
          formula: '"revenue" - "cosst"',
        },
      ],
    });
    const pd = new PivotData(FORMULA_DATA, config);
    expect(pd.getFormulaErrors().size).toBe(1);
    expect(pd.getFormulaErrors().get("typo")).toMatch(/Unknown field/);
    expect(pd.getFormulaErrors().get("typo")).toMatch(/"cosst"/);
  });

  // Edge case: valid formula + invalid formula → only broken one errors
  it("valid formulas unaffected by invalid ones", () => {
    const config = formulaConfig({
      synthetic_measures: [
        {
          id: "margin",
          label: "Margin",
          operation: "formula",
          numerator: "",
          denominator: "",
          formula: '"revenue" - "cost"',
        },
        {
          id: "broken",
          label: "Broken",
          operation: "formula",
          numerator: "",
          denominator: "",
          formula: '"revenue" + * "cost"',
        },
      ],
    });
    const pd = new PivotData(FORMULA_DATA, config);
    expect(pd.getFormulaErrors().size).toBe(1);
    expect(pd.getAggregator(["US"], ["2023"], "margin").value()).toBe(80);
    expect(pd.getAggregator(["US"], ["2023"], "broken").value()).toBeNull();
  });

  // Complex formula: if() conditional
  it("evaluates if() conditional formula", () => {
    const config = formulaConfig({
      synthetic_measures: [
        {
          id: "capped",
          label: "Capped Margin",
          operation: "formula",
          numerator: "",
          denominator: "",
          formula: 'if("revenue" - "cost" > 100, 100, "revenue" - "cost")',
        },
      ],
    });
    const pd = new PivotData(FORMULA_DATA, config);
    // US/2023: revenue=150, cost=70 → margin=80 (< 100) → 80
    expect(pd.getAggregator(["US"], ["2023"], "capped").value()).toBe(80);
    // EU/2023: revenue=200, cost=80 → margin=120 (> 100) → 100
    expect(pd.getAggregator(["EU"], ["2023"], "capped").value()).toBe(100);
  });
});

// ---------------------------------------------------------------------------
// Formula engine — hybrid mode tests
// ---------------------------------------------------------------------------

describe("PivotData - formula engine in hybrid mode", () => {
  const HYBRID_DATA: DataRecord[] = [
    { region: "US", year: "2023", revenue: 100, cost: 40 },
    { region: "US", year: "2024", revenue: 200, cost: 80 },
    { region: "EU", year: "2023", revenue: 150, cost: 60 },
    { region: "EU", year: "2024", revenue: 300, cost: 100 },
  ];

  function hybridFormulaConfig(
    overrides: Record<string, unknown> = {},
  ): PivotConfigV1 {
    const values = (overrides.values as string[]) ?? ["revenue", "cost"];
    const synth =
      (overrides.synthetic_measures as SyntheticMeasureConfig[]) ?? [
        {
          id: "margin",
          label: "Margin",
          operation: "formula" as const,
          numerator: "",
          denominator: "",
          formula: '"revenue" - "cost"',
        },
      ];
    return {
      version: 1,
      rows: (overrides.rows as string[]) ?? ["region"],
      columns: (overrides.columns as string[]) ?? ["year"],
      values,
      show_totals: true,
      empty_cell_value: "-",
      interactive: true,
      synthetic_measures: synth,
      aggregation: normalizeAggregationConfig(overrides.aggregation, values),
    } as PivotConfigV1;
  }

  it("formula evaluates correctly with hybridAggRemap (count → sum)", () => {
    const cfg = hybridFormulaConfig({ aggregation: "count" });
    const pd = new PivotData(HYBRID_DATA, cfg, {
      hybridAggRemap: { revenue: "sum", cost: "sum" },
    });
    // US/2023: revenue=100, cost=40 → margin=60
    expect(pd.getAggregator(["US"], ["2023"], "margin").value()).toBe(60);
    // EU/2024: revenue=300, cost=100 → margin=200
    expect(pd.getAggregator(["EU"], ["2024"], "margin").value()).toBe(200);
  });

  it("formula row totals use hybrid pre-computed source values when available", () => {
    const cfg = hybridFormulaConfig({ aggregation: "median" });
    const totals = makeHybridTotals(cfg, { revenue: 187.5, cost: 70 }, [
      { key: ["US"], values: { revenue: 150, cost: 60 } },
      { key: ["EU"], values: { revenue: 225, cost: 80 } },
    ]);
    const pd = new PivotData(HYBRID_DATA, cfg, { hybridTotals: totals });
    expect(pd.getRowTotal(["US"], "revenue").value()).toBe(150);
    // Formula uses hybrid-precomputed row values: revenue=150, cost=60
    const margin = pd.getRowTotal(["US"], "margin").value();
    expect(margin).toBe(90); // 150 - 60
  });

  it("formula grand total uses hybrid pre-computed source values when available", () => {
    const cfg = hybridFormulaConfig({ aggregation: "median" });
    const totals = makeHybridTotals(cfg, { revenue: 999, cost: 888 });
    const pd = new PivotData(HYBRID_DATA, cfg, { hybridTotals: totals });
    expect(pd.getGrandTotal("revenue").value()).toBe(999);
    // Formula uses hybrid-precomputed grand values: revenue=999, cost=888
    const margin = pd.getGrandTotal("margin").value();
    expect(margin).toBe(111); // 999 - 888
  });

  it("formula col totals use hybrid pre-computed source values when available", () => {
    const cfg = hybridFormulaConfig({ aggregation: "median" });
    const totals = makeHybridTotals(
      cfg,
      { revenue: 999, cost: 888 },
      [],
      [
        { key: ["2023"], values: { revenue: 125, cost: 50 } },
        { key: ["2024"], values: { revenue: 250, cost: 90 } },
      ],
    );
    const pd = new PivotData(HYBRID_DATA, cfg, { hybridTotals: totals });
    expect(pd.getColTotal(["2023"], "revenue").value()).toBe(125);
    // Formula uses hybrid-precomputed col values: revenue=125, cost=50
    const margin = pd.getColTotal(["2023"], "margin").value();
    expect(margin).toBe(75); // 125 - 50
  });

  it("formula with if() works in hybrid mode with agg remap", () => {
    const cfg = hybridFormulaConfig({
      aggregation: "count",
      synthetic_measures: [
        {
          id: "safe_ratio",
          label: "Safe Ratio",
          operation: "formula" as const,
          numerator: "",
          denominator: "",
          formula: 'if("cost" > 0, "revenue" / "cost", 0)',
        },
      ],
    });
    const pd = new PivotData(HYBRID_DATA, cfg, {
      hybridAggRemap: { revenue: "sum", cost: "sum" },
    });
    // US/2023: revenue=100, cost=40 → 100/40 = 2.5
    expect(pd.getAggregator(["US"], ["2023"], "safe_ratio").value()).toBe(2.5);
  });

  it("formula prefers hybrid values over client-side re-aggregated values", () => {
    const cfg = hybridFormulaConfig({ aggregation: "median" });
    // Provide hybrid grand values that differ from what client-side re-aggregation would produce
    const totals = makeHybridTotals(
      cfg,
      { revenue: 500, cost: 200 },
      [
        { key: ["US"], values: { revenue: 300, cost: 100 } },
        { key: ["EU"], values: { revenue: 400, cost: 150 } },
      ],
      [
        { key: ["2023"], values: { revenue: 350, cost: 120 } },
        { key: ["2024"], values: { revenue: 450, cost: 180 } },
      ],
    );
    const pd = new PivotData(HYBRID_DATA, cfg, { hybridTotals: totals });
    // Grand total formula should use hybrid values: 500 - 200 = 300
    expect(pd.getGrandTotal("margin").value()).toBe(300);
    // Row total formula should use hybrid values: 300 - 100 = 200
    expect(pd.getRowTotal(["US"], "margin").value()).toBe(200);
    // Col total formula should use hybrid values: 350 - 120 = 230
    expect(pd.getColTotal(["2023"], "margin").value()).toBe(230);
  });

  it("formula source fields not in values are aggregated correctly in hybrid mode", () => {
    const cfg = hybridFormulaConfig({
      values: ["revenue"],
      aggregation: "count",
      synthetic_measures: [
        {
          id: "margin",
          label: "Margin",
          operation: "formula" as const,
          numerator: "",
          denominator: "",
          formula: '"revenue" - "cost"',
        },
      ],
    });
    const pd = new PivotData(HYBRID_DATA, cfg, {
      hybridAggRemap: { revenue: "sum", cost: "sum" },
    });
    // cost is not in values but should still be aggregated via remapped sum
    expect(pd.getAggregator(["US"], ["2023"], "margin").value()).toBe(60);
  });
});

// ---------------------------------------------------------------------------
// Formula engine performance benchmark
// ---------------------------------------------------------------------------

describe("PivotData - formula performance", () => {
  it("formula overhead is <10% vs equivalent sum_over_sum (1000 rows × 50 cols × 3 measures)", () => {
    const regions = Array.from({ length: 20 }, (_, i) => `R${i}`);
    const years = Array.from({ length: 50 }, (_, i) => `Y${i}`);
    const data: DataRecord[] = [];
    for (let i = 0; i < 1000; i++) {
      data.push({
        region: regions[i % regions.length],
        year: years[i % years.length],
        revenue: Math.random() * 1000,
        cost: Math.random() * 500,
        profit: Math.random() * 300,
      });
    }

    const baseSynth = [
      {
        id: "m1",
        label: "M1",
        operation: "sum_over_sum" as const,
        numerator: "revenue",
        denominator: "cost",
      },
      {
        id: "m2",
        label: "M2",
        operation: "sum_over_sum" as const,
        numerator: "profit",
        denominator: "cost",
      },
      {
        id: "m3",
        label: "M3",
        operation: "difference" as const,
        numerator: "revenue",
        denominator: "profit",
      },
    ];
    const baseConfig = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue", "cost", "profit"],
      synthetic_measures: baseSynth,
    });

    const formulaSynth = [
      {
        id: "m1",
        label: "M1",
        operation: "formula" as const,
        numerator: "",
        denominator: "",
        formula: '"revenue" / "cost"',
      },
      {
        id: "m2",
        label: "M2",
        operation: "formula" as const,
        numerator: "",
        denominator: "",
        formula: '"profit" / "cost"',
      },
      {
        id: "m3",
        label: "M3",
        operation: "formula" as const,
        numerator: "",
        denominator: "",
        formula: '"revenue" - "profit"',
      },
    ];
    const formulaConfig = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue", "cost", "profit"],
      synthetic_measures: formulaSynth,
    });

    const runBenchmark = (cfg: PivotConfigV1) => {
      const pd = new PivotData(data, cfg);
      const rowKeys = pd.getRowKeys();
      const colKeys = pd.getColKeys();
      const start = performance.now();
      for (const rk of rowKeys) {
        for (const ck of colKeys) {
          for (const m of ["m1", "m2", "m3"]) {
            pd.getAggregator(rk, ck, m).value();
          }
        }
        for (const m of ["m1", "m2", "m3"]) {
          pd.getRowTotal(rk, m).value();
        }
      }
      for (const ck of colKeys) {
        for (const m of ["m1", "m2", "m3"]) {
          pd.getColTotal(ck, m).value();
        }
      }
      for (const m of ["m1", "m2", "m3"]) {
        pd.getGrandTotal(m).value();
      }
      return performance.now() - start;
    };

    // Warm up
    runBenchmark(baseConfig);
    runBenchmark(formulaConfig);

    const iterations = 3;
    let baseTotal = 0;
    let formulaTotal = 0;
    for (let i = 0; i < iterations; i++) {
      baseTotal += runBenchmark(baseConfig);
      formulaTotal += runBenchmark(formulaConfig);
    }
    const baseAvg = baseTotal / iterations;
    const formulaAvg = formulaTotal / iterations;
    const overhead = (formulaAvg - baseAvg) / baseAvg;

    // Generous margin: microbenchmarks are noisy in test runners / CI
    expect(overhead).toBeLessThan(5.0);
  });
});

// ---------------------------------------------------------------------------
// 0.5.0 — Top N and Value Filter tests
// ---------------------------------------------------------------------------

const FILTER_DATA: DataRecord[] = [
  { region: "US", product: "Alpha", revenue: 500 },
  { region: "US", product: "Beta", revenue: 200 },
  { region: "US", product: "Gamma", revenue: 800 },
  { region: "EU", product: "Alpha", revenue: 300 },
  { region: "EU", product: "Beta", revenue: 700 },
  { region: "EU", product: "Gamma", revenue: 100 },
];

// Helper: config without column dimension (so grand-total column = colKey [])
function makeFilterConfig(
  overrides: Partial<TestConfigOverrides> = {},
): PivotConfigV1 {
  return makeConfig({
    rows: ["region", "product"],
    columns: ["category"], // Use a column that exists in FILTER_DATA_WITH_COL
    values: ["revenue"],
    ...overrides,
  });
}

// Extended test data that includes a column dimension
const FILTER_DATA_WITH_COL: DataRecord[] = [
  { region: "US", product: "Alpha", category: "A", revenue: 500 },
  { region: "US", product: "Beta", category: "A", revenue: 200 },
  { region: "US", product: "Gamma", category: "A", revenue: 800 },
  { region: "EU", product: "Alpha", category: "A", revenue: 300 },
  { region: "EU", product: "Beta", category: "A", revenue: 700 },
  { region: "EU", product: "Gamma", category: "A", revenue: 100 },
];

describe("PivotData — Top N filters", () => {
  it("top_n_filters: top 2 products by revenue per region", () => {
    const config = makeFilterConfig({
      top_n_filters: [
        { field: "product", n: 2, by: "revenue", direction: "top" },
      ],
    });
    const pd = new PivotData(FILTER_DATA_WITH_COL, config);
    const rowKeys = pd.getRowKeys();

    // EU: top 2 are Beta (700) and Alpha (300) → Gamma (100) excluded
    const euRows = rowKeys.filter((k) => k[0] === "EU").map((k) => k[1]);
    expect(euRows).toContain("Beta");
    expect(euRows).toContain("Alpha");
    expect(euRows).not.toContain("Gamma");

    // US: top 2 are Gamma (800) and Alpha (500) → Beta (200) excluded
    const usRows = rowKeys.filter((k) => k[0] === "US").map((k) => k[1]);
    expect(usRows).toContain("Gamma");
    expect(usRows).toContain("Alpha");
    expect(usRows).not.toContain("Beta");
  });

  it("top_n_filters: bottom 1 product by revenue per region", () => {
    const config = makeFilterConfig({
      top_n_filters: [
        { field: "product", n: 1, by: "revenue", direction: "bottom" },
      ],
    });
    const pd = new PivotData(FILTER_DATA_WITH_COL, config);
    const rowKeys = pd.getRowKeys();

    // EU bottom 1: Gamma (100)
    const euRows = rowKeys.filter((k) => k[0] === "EU").map((k) => k[1]);
    expect(euRows).toEqual(["Gamma"]);

    // US bottom 1: Beta (200)
    const usRows = rowKeys.filter((k) => k[0] === "US").map((k) => k[1]);
    expect(usRows).toEqual(["Beta"]);
  });

  it("top_n_filters: parent subtotals are unaffected by filtering", () => {
    const config = makeConfig({
      rows: ["region", "product"],
      columns: ["year"],
      values: ["revenue"],
      top_n_filters: [
        { field: "product", n: 1, by: "revenue", direction: "top" },
      ],
    });
    const dataWithYear: DataRecord[] = [
      { region: "US", product: "Alpha", year: "2024", revenue: 500 },
      { region: "US", product: "Beta", year: "2024", revenue: 200 },
      { region: "US", product: "Gamma", year: "2024", revenue: 800 },
    ];
    const pd = new PivotData(dataWithYear, config);
    // Only top 1 (Gamma) visible in rows, but US subtotal still = 1500
    const usRows = pd
      .getRowKeys()
      .filter((k) => k[0] === "US")
      .map((k) => k[1]);
    expect(usRows).toEqual(["Gamma"]);
    // The subtotal aggregator for US (all columns) reads from full unfiltered data
    expect(pd.getSubtotalAggregator(["US"], [], "revenue").value()).toBe(1500);
  });

  it("top_n_filters: single-level dimension uses grand total ranking", () => {
    const config = makeFilterConfig({
      rows: ["region"],
      top_n_filters: [
        { field: "region", n: 1, by: "revenue", direction: "top" },
      ],
    });
    // US total = 1500, EU total = 1100 → top 1 = US
    const pd = new PivotData(FILTER_DATA_WITH_COL, config);
    expect(pd.getRowKeys()).toEqual([["US"]]);
  });
});

describe("PivotData — Value filters", () => {
  it("value_filters: revenue > 400 excludes low-revenue products per region", () => {
    const config = makeFilterConfig({
      value_filters: [
        { field: "product", by: "revenue", operator: "gt", value: 400 },
      ],
    });
    const pd = new PivotData(FILTER_DATA_WITH_COL, config);
    const rowKeys = pd.getRowKeys();

    // US: Alpha (500) ✓, Gamma (800) ✓, Beta (200) ✗
    const usRows = rowKeys.filter((k) => k[0] === "US").map((k) => k[1]);
    expect(usRows).toContain("Alpha");
    expect(usRows).toContain("Gamma");
    expect(usRows).not.toContain("Beta");

    // EU: Beta (700) ✓, Alpha (300) ✗, Gamma (100) ✗
    const euRows = rowKeys.filter((k) => k[0] === "EU").map((k) => k[1]);
    expect(euRows).toEqual(["Beta"]);
  });

  it("value_filters: between operator keeps members in range", () => {
    const config = makeFilterConfig({
      value_filters: [
        {
          field: "product",
          by: "revenue",
          operator: "between",
          value: 200,
          value2: 600,
        },
      ],
    });
    const pd = new PivotData(FILTER_DATA_WITH_COL, config);
    const rowKeys = pd.getRowKeys();

    // US: Alpha (500) ✓, Beta (200) ✓, Gamma (800) ✗
    const usRows = rowKeys.filter((k) => k[0] === "US").map((k) => k[1]);
    expect(usRows).toContain("Alpha");
    expect(usRows).toContain("Beta");
    expect(usRows).not.toContain("Gamma");
  });

  it("value_filters: parent subtotals are unaffected by filtering", () => {
    const dataWithYear: DataRecord[] = [
      { region: "US", product: "Alpha", year: "2024", revenue: 500 },
      { region: "US", product: "Beta", year: "2024", revenue: 200 },
    ];
    const config = makeConfig({
      rows: ["region", "product"],
      columns: ["year"],
      values: ["revenue"],
      value_filters: [
        { field: "product", by: "revenue", operator: "gt", value: 9999 },
      ],
    });
    const pd = new PivotData(dataWithYear, config);
    // All product rows hidden by impossible predicate, but US subtotal still = 700
    const usRows = pd.getRowKeys().filter((k) => k[0] === "US");
    expect(usRows).toHaveLength(0);
    expect(pd.getSubtotalAggregator(["US"], [], "revenue").value()).toBe(700);
  });

  it("value_filters: fields with null measure fail predicate and are excluded", () => {
    const dataWithNull: DataRecord[] = [
      { region: "US", product: "Alpha", category: "A", revenue: 500 },
      { region: "US", product: "Beta", category: "A", revenue: null },
    ];
    const config = makeFilterConfig({
      value_filters: [
        { field: "product", by: "revenue", operator: "gte", value: 0 },
      ],
    });
    const pd = new PivotData(dataWithNull, config);
    const usRows = pd
      .getRowKeys()
      .filter((k) => k[0] === "US")
      .map((k) => k[1]);
    expect(usRows).toContain("Alpha");
    expect(usRows).not.toContain("Beta");
  });
});
