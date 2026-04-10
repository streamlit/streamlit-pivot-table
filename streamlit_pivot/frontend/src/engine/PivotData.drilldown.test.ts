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
  type ColumnarDataSource,
  type PivotConfigV1,
} from "./types";

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

/** Columnar backing store built from row records (mirrors Arrow-style access). */
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

const DRILL_DATA: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100, profit: 40, extra: "x1" },
  { region: "US", year: "2024", revenue: 150, profit: 60, extra: "x2" },
  { region: "EU", year: "2023", revenue: 200, profit: 80, extra: "x3" },
  { region: "EU", year: "2024", revenue: 250, profit: 100, extra: "x4" },
  { region: "US", year: "2023", revenue: 50, profit: 20, extra: "x5" },
];

const SAMPLE_DATA: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100, profit: 40 },
  { region: "US", year: "2024", revenue: 150, profit: 60 },
  { region: "EU", year: "2023", revenue: 200, profit: 80 },
  { region: "EU", year: "2024", revenue: 250, profit: 100 },
  { region: "US", year: "2023", revenue: 50, profit: 20 },
];

describe("PivotData — columnar drill-down / getMatchingRecords", () => {
  it("columnar getMatchingRecords returns records with all columns", () => {
    const cfg = makeConfig();
    const pd = new PivotData(new TestColumnarSource(DRILL_DATA), cfg);
    const { records } = pd.getMatchingRecords({ region: "US" });
    expect(records).toHaveLength(3);
    const expectedCols = ["region", "year", "revenue", "profit", "extra"];
    for (const rec of records) {
      expect(Object.keys(rec).sort()).toEqual(expectedCols.sort());
    }
  });

  it("columnar getMatchingRecords measure values match source data", () => {
    const cfg = makeConfig();
    const pd = new PivotData(new TestColumnarSource(DRILL_DATA), cfg);
    const { records } = pd.getMatchingRecords({ region: "US", year: "2023" });
    expect(records).toHaveLength(2);
    const revenues = records
      .map((r) => Number(r.revenue))
      .sort((a, b) => a - b);
    expect(revenues).toEqual([50, 100]);
    const profits = records.map((r) => Number(r.profit)).sort((a, b) => a - b);
    expect(profits).toEqual([20, 40]);
    expect(new Set(records.map((r) => r.extra))).toEqual(new Set(["x1", "x5"]));
  });

  it("columnar getMatchingRecords with limit larger than totalCount returns all rows", () => {
    const cfg = makeConfig();
    const pd = new PivotData(new TestColumnarSource(SAMPLE_DATA), cfg);
    const { records, totalCount } = pd.getMatchingRecords({}, 999_999);
    expect(totalCount).toBe(5);
    expect(records).toHaveLength(5);
  });

  it("columnar getMatchingRecords with empty filter returns all rows", () => {
    const cfg = makeConfig();
    const pd = new PivotData(new TestColumnarSource(SAMPLE_DATA), cfg);
    const { records, totalCount } = pd.getMatchingRecords({});
    expect(totalCount).toBe(5);
    expect(records).toHaveLength(5);
  });

  it("columnar and record paths return identical drill-down results", () => {
    const cfg = makeConfig();
    const fromRecords = new PivotData(SAMPLE_DATA, cfg);
    const fromColumnar = new PivotData(
      new TestColumnarSource(SAMPLE_DATA),
      cfg,
    );
    const filters = { region: "US" as const, year: "2023" as const };
    const a = fromRecords.getMatchingRecords(filters);
    const b = fromColumnar.getMatchingRecords(filters);
    expect(a.totalCount).toBe(b.totalCount);
    expect(a.records).toEqual(b.records);
  });

  it("getColumnNames works without materialized records", () => {
    const cfg = makeConfig();
    const pd = new PivotData(new TestColumnarSource(SAMPLE_DATA), cfg);
    expect(pd.getColumnNames().sort()).toEqual(
      ["profit", "region", "revenue", "year"].sort(),
    );
  });
});

describe("PivotData — columnar empty dataset and getUniqueValues", () => {
  it("columnar source with empty dataset does not crash and yields empty keys", () => {
    const cfg = makeConfig();
    const pd = new PivotData(new TestColumnarSource([]), cfg);
    expect(pd.getRowKeys()).toEqual([]);
    expect(pd.getColKeys()).toEqual([]);
    expect(pd.recordCount).toBe(0);
  });

  it("getUniqueValues from columnar source matches record-backed PivotData", () => {
    const cfg = makeConfig();
    const fromRecords = new PivotData(SAMPLE_DATA, cfg);
    const fromColumnar = new PivotData(
      new TestColumnarSource(SAMPLE_DATA),
      cfg,
    );
    expect(fromColumnar.getUniqueValues("region")).toEqual(
      fromRecords.getUniqueValues("region"),
    );
    expect(fromColumnar.getUniqueValues("year")).toEqual(
      fromRecords.getUniqueValues("year"),
    );
  });
});
