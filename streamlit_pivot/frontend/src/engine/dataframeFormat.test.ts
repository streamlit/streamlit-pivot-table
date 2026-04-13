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
  normalizeToUTC,
  formatDateValue,
  formatDateTimeValue,
  formatIntegerLabel,
  formatDateWithPattern,
} from "./formatters";
import {
  normalizeAggregationConfig,
  type ColumnTypeMap,
  type PivotConfigV1,
} from "./types";

function makeConfig(overrides: Partial<PivotConfigV1> = {}): PivotConfigV1 {
  const values = overrides.values ?? ["revenue"];
  const config = {
    version: 1,
    rows: ["region"],
    columns: ["year"],
    values,
    auto_date_hierarchy: false,
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    ...overrides,
  } as PivotConfigV1;
  config.values = values;
  config.aggregation = normalizeAggregationConfig(undefined, values);
  return config;
}

// ---------------------------------------------------------------------------
// normalizeToUTC
// ---------------------------------------------------------------------------

describe("normalizeToUTC", () => {
  it("appends Z to naive datetime string with T separator", () => {
    expect(normalizeToUTC("2024-01-15T12:30:00")).toBe("2024-01-15T12:30:00Z");
  });

  it("appends Z to naive datetime string with space separator", () => {
    expect(normalizeToUTC("2024-01-15 12:30:00")).toBe("2024-01-15T12:30:00Z");
  });

  it("does NOT modify string already ending with Z", () => {
    expect(normalizeToUTC("2024-01-15T12:30:00Z")).toBe("2024-01-15T12:30:00Z");
  });

  it("does NOT modify date-only string", () => {
    expect(normalizeToUTC("2024-01-15")).toBe("2024-01-15");
  });

  it("does NOT modify string with timezone offset", () => {
    const input = "2024-01-15T12:30:00+05:00";
    expect(normalizeToUTC(input)).toBe(input);
  });

  it("does NOT modify non-ISO string", () => {
    expect(normalizeToUTC("hello")).toBe("hello");
  });
});

// ---------------------------------------------------------------------------
// formatDateValue / formatDateTimeValue / formatIntegerLabel
// ---------------------------------------------------------------------------

describe("formatDateValue", () => {
  it("formats epoch ms as locale date", () => {
    const result = formatDateValue(1705276800000);
    expect(typeof result).toBe("string");
    expect(result.length).toBeGreaterThan(0);
  });

  it("formats ISO string as locale date", () => {
    const result = formatDateValue("2024-01-15");
    expect(typeof result).toBe("string");
    expect(result).toContain("2024");
  });

  it("returns raw string for invalid input", () => {
    expect(formatDateValue("not-a-date")).toBe("not-a-date");
  });
});

describe("formatDateTimeValue", () => {
  it("formats epoch ms as locale datetime", () => {
    const result = formatDateTimeValue(1705276800000);
    expect(typeof result).toBe("string");
    expect(result.length).toBeGreaterThan(0);
  });

  it("formats ISO datetime string", () => {
    const result = formatDateTimeValue("2024-01-15T12:30:00.000Z");
    expect(typeof result).toBe("string");
    expect(result).toContain("2024");
  });
});

describe("formatIntegerLabel", () => {
  it("formats number without decimals", () => {
    const result = formatIntegerLabel(12345);
    expect(result).toContain("12");
    expect(result).toContain("345");
  });

  it("formats numeric string", () => {
    const result = formatIntegerLabel("12345");
    expect(result).toContain("12");
    expect(result).toContain("345");
  });

  it("returns raw string for non-numeric", () => {
    expect(formatIntegerLabel("hello")).toBe("hello");
  });
});

// ---------------------------------------------------------------------------
// formatDateWithPattern
// ---------------------------------------------------------------------------

describe("formatDateWithPattern", () => {
  it("respects YYYY-MM-DD token order exactly", () => {
    expect(
      formatDateWithPattern("2024-01-15T00:00:00.000Z", "YYYY-MM-DD"),
    ).toBe("2024-01-15");
  });

  it("applies YYYY-MM pattern", () => {
    expect(formatDateWithPattern("2024-01-15T00:00:00.000Z", "YYYY-MM")).toBe(
      "2024-01",
    );
  });

  it("applies DD/MM/YYYY pattern", () => {
    expect(
      formatDateWithPattern("2024-01-15T00:00:00.000Z", "DD/MM/YYYY"),
    ).toBe("15/01/2024");
  });

  it("applies MMM D, YYYY pattern", () => {
    expect(
      formatDateWithPattern("2024-01-15T00:00:00.000Z", "MMM D, YYYY"),
    ).toBe("Jan 15, 2024");
  });

  it("applies YYYY-MM-DD HH:mm pattern with time", () => {
    expect(
      formatDateWithPattern("2024-01-15T14:30:00.000Z", "YYYY-MM-DD HH:mm"),
    ).toBe("2024-01-15 14:30");
  });

  it("applies MMMM YYYY pattern", () => {
    expect(formatDateWithPattern("2024-01-15T00:00:00.000Z", "MMMM YYYY")).toBe(
      "January 2024",
    );
  });

  it("applies YY pattern", () => {
    expect(formatDateWithPattern("2024-01-15T00:00:00.000Z", "YY")).toBe("24");
  });

  it("preserves literal separators between tokens", () => {
    expect(
      formatDateWithPattern("2024-01-15T00:00:00.000Z", "YYYY/MM/DD"),
    ).toBe("2024/01/15");
  });

  it("returns raw string for invalid date", () => {
    expect(formatDateWithPattern("not-a-date", "YYYY")).toBe("not-a-date");
  });

  it("uses UTC so date-only values are not shifted", () => {
    expect(
      formatDateWithPattern("2024-01-01T00:00:00.000Z", "YYYY-MM-DD"),
    ).toBe("2024-01-01");
  });
});

// ---------------------------------------------------------------------------
// PivotData - canonical temporal key (_resolveDimKey)
// ---------------------------------------------------------------------------

describe("PivotData - canonical temporal keys", () => {
  it("datetime epoch ms -> ISO UTC string key", () => {
    const data: DataRecord[] = [
      { ts: 1705276800000, revenue: 100 },
      { ts: 1717203600000, revenue: 200 },
    ];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    expect(keys).toContain("2024-01-15T00:00:00.000Z");
    expect(keys).toContain("2024-06-01T01:00:00.000Z");
  });

  it("datetime ISO string -> UTC normalized key", () => {
    const data: DataRecord[] = [
      { ts: "2024-01-15T12:30:00", revenue: 100 },
      { ts: "2024-06-01T00:00:00", revenue: 200 },
    ];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    expect(keys).toContain("2024-01-15T12:30:00.000Z");
    expect(keys).toContain("2024-06-01T00:00:00.000Z");
  });

  it("date-only epoch ms -> YYYY-MM-DD key", () => {
    const data: DataRecord[] = [{ d: 1705276800000, revenue: 100 }];
    const columnTypes: ColumnTypeMap = new Map([["d", "date"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["d"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    expect(keys[0]).toBe("2024-01-15");
  });

  it("date-only ISO string -> YYYY-MM-DD key", () => {
    const data: DataRecord[] = [
      { d: "2024-01-15", revenue: 100 },
      { d: "2024-06-01", revenue: 200 },
    ];
    const columnTypes: ColumnTypeMap = new Map([["d", "date"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["d"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    expect(keys).toContain("2024-01-15");
    expect(keys).toContain("2024-06-01");
  });

  it("non-temporal dimensions produce String(raw) keys (backward compat)", () => {
    const data: DataRecord[] = [
      { region: "US", revenue: 100 },
      { region: "EU", revenue: 200 },
    ];
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["region"], columns: [], values: ["revenue"] }),
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    expect(keys).toEqual(["EU", "US"]);
  });

  it("integer dimensions produce String(raw) keys (backward compat)", () => {
    const data: DataRecord[] = [
      { year: 2023, revenue: 100 },
      { year: 2024, revenue: 200 },
    ];
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["year"], columns: [], values: ["revenue"] }),
    );
    const keys = pd.getRowKeys().map((k) => k[0]);
    expect(keys).toEqual(["2023", "2024"]);
  });
});

// ---------------------------------------------------------------------------
// PivotData - key/display separation
// ---------------------------------------------------------------------------

describe("PivotData - key/display separation", () => {
  it("two timestamps with same date but different times produce distinct keys", () => {
    const data: DataRecord[] = [
      { ts: "2024-01-15T00:00:00", revenue: 100 },
      { ts: "2024-01-15T12:30:00", revenue: 200 },
    ];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const keys = pd.getRowKeys();
    expect(keys).toHaveLength(2);
    expect(keys[0][0]).not.toBe(keys[1][0]);
    // Each key must be an ISO UTC string
    expect(keys[0][0]).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
    expect(keys[1][0]).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
  });

  it("aggregation uses canonical keys (no bucket collapse)", () => {
    const data: DataRecord[] = [
      { ts: "2024-01-15T00:00:00", revenue: 100 },
      { ts: "2024-01-15T12:30:00", revenue: 200 },
    ];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const midnight = pd.getAggregator(["2024-01-15T00:00:00.000Z"], []);
    const midday = pd.getAggregator(["2024-01-15T12:30:00.000Z"], []);
    expect(midnight.value()).toBe(100);
    expect(midday.value()).toBe(200);
  });

  it("formatDimLabel returns formatted display for datetime key", () => {
    const data: DataRecord[] = [{ ts: "2024-01-15T12:30:00", revenue: 100 }];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const key = pd.getRowKeys()[0][0];
    const label = pd.formatDimLabel("ts", key);
    // Label should be a locale-formatted string, not the raw ISO key
    expect(label).not.toBe(key);
    expect(label).toContain("2024");
  });

  it("formatDimLabel returns formatted display for date key", () => {
    const data: DataRecord[] = [{ d: "2024-01-15", revenue: 100 }];
    const columnTypes: ColumnTypeMap = new Map([["d", "date"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["d"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const key = pd.getRowKeys()[0][0];
    expect(key).toBe("2024-01-15");
    const label = pd.formatDimLabel("d", key);
    expect(label).toContain("2024");
    expect(label).not.toBe("2024-01-15");
  });

  it("formatDimLabel for integer type uses locale number formatting", () => {
    const data: DataRecord[] = [{ year: "12345", revenue: 100 }];
    const columnTypes: ColumnTypeMap = new Map([["year", "integer"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["year"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const label = pd.formatDimLabel("year", "12345");
    expect(label).toContain("12");
    expect(label).toContain("345");
  });

  it("formatDimLabel preserves raw 4-digit year labels", () => {
    const data: DataRecord[] = [{ year: "2022", revenue: 100 }];
    const columnTypes: ColumnTypeMap = new Map([["year", "integer"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["year"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    expect(pd.formatDimLabel("year", "2022")).toBe("2022");
  });

  it("formatDimLabel returns raw key for string type", () => {
    const data: DataRecord[] = [{ region: "US", revenue: 100 }];
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["region"], columns: [], values: ["revenue"] }),
    );
    expect(pd.formatDimLabel("region", "US")).toBe("US");
  });

  it("formatDimLabel applies dimension_format pattern for datetime", () => {
    const data: DataRecord[] = [{ ts: "2024-01-15T12:30:00", revenue: 100 }];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({
        rows: ["ts"],
        columns: [],
        values: ["revenue"],
        dimension_format: { ts: "YYYY" },
      }),
      { columnTypes },
    );
    const key = pd.getRowKeys()[0][0];
    const label = pd.formatDimLabel("ts", key);
    expect(label).toBe("2024");
  });

  it("formatDimLabel caches results", () => {
    const data: DataRecord[] = [{ ts: "2024-01-15T12:30:00", revenue: 100 }];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const key = pd.getRowKeys()[0][0];
    const label1 = pd.formatDimLabel("ts", key);
    const label2 = pd.formatDimLabel("ts", key);
    expect(label1).toBe(label2);
  });
});

// ---------------------------------------------------------------------------
// PivotData - filtering with temporal keys
// ---------------------------------------------------------------------------

describe("PivotData - filtering with temporal keys", () => {
  it("include filter uses canonical temporal key", () => {
    const data: DataRecord[] = [
      { ts: "2024-01-15T00:00:00", revenue: 100 },
      { ts: "2024-06-01T00:00:00", revenue: 200 },
    ];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({
        rows: ["ts"],
        columns: [],
        values: ["revenue"],
        filters: { ts: { include: ["2024-01-15T00:00:00.000Z"] } },
      }),
      { columnTypes },
    );
    expect(pd.getRowKeys()).toHaveLength(1);
    expect(pd.getGrandTotal().value()).toBe(100);
  });

  it("exclude filter uses canonical temporal key", () => {
    const data: DataRecord[] = [
      { ts: "2024-01-15T00:00:00", revenue: 100 },
      { ts: "2024-06-01T00:00:00", revenue: 200 },
    ];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({
        rows: ["ts"],
        columns: [],
        values: ["revenue"],
        filters: { ts: { exclude: ["2024-01-15T00:00:00.000Z"] } },
      }),
      { columnTypes },
    );
    expect(pd.getRowKeys()).toHaveLength(1);
    expect(pd.getGrandTotal().value()).toBe(200);
  });
});

// ---------------------------------------------------------------------------
// PivotData - RawValueMap / getRawDimValue / getColumnType
// ---------------------------------------------------------------------------

describe("PivotData - RawValueMap and export helpers", () => {
  it("getRawDimValue returns original typed value for temporal key", () => {
    const data: DataRecord[] = [{ ts: 1705276800000, revenue: 100 }];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const key = pd.getRowKeys()[0][0];
    expect(key).toBe("2024-01-15T00:00:00.000Z");
    const raw = pd.getRawDimValue("ts", key);
    expect(raw).toBe(1705276800000);
  });

  it("getColumnType returns the type for a field", () => {
    const data: DataRecord[] = [{ ts: 1705276800000, revenue: 100 }];
    const columnTypes: ColumnTypeMap = new Map([
      ["ts", "datetime"],
      ["revenue", "float"],
    ]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    expect(pd.getColumnType("ts")).toBe("datetime");
    expect(pd.getColumnType("revenue")).toBe("float");
    expect(pd.getColumnType("unknown")).toBeUndefined();
  });

  it("getColumnTypes returns the full type map", () => {
    const columnTypes: ColumnTypeMap = new Map([
      ["ts", "datetime"],
      ["revenue", "float"],
    ]);
    const data: DataRecord[] = [{ ts: 1705276800000, revenue: 100 }];
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const types = pd.getColumnTypes();
    expect(types).toBe(columnTypes);
  });
});

// ---------------------------------------------------------------------------
// PivotData - getUniqueValues with temporal keys
// ---------------------------------------------------------------------------

describe("PivotData - getUniqueValues with temporal types", () => {
  it("returns canonical keys for datetime dimensions", () => {
    const data: DataRecord[] = [
      { ts: "2024-01-15T00:00:00", revenue: 100 },
      { ts: "2024-01-15T00:00:00", revenue: 50 },
      { ts: "2024-06-01T00:00:00", revenue: 200 },
    ];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["revenue"] }),
      { columnTypes },
    );
    const unique = pd.getUniqueValues("ts");
    expect(unique).toContain("2024-01-15T00:00:00.000Z");
    expect(unique).toContain("2024-06-01T00:00:00.000Z");
    expect(unique).toHaveLength(2);
  });
});

// ---------------------------------------------------------------------------
// Cross-language canonical key parity (frontend side)
// ---------------------------------------------------------------------------

describe("Cross-language canonical key parity", () => {
  it("naive datetime string -> same ISO UTC key as Python", () => {
    const data: DataRecord[] = [{ ts: "2024-01-15T12:30:00", v: 1 }];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["v"] }),
      { columnTypes },
    );
    const key = pd.getRowKeys()[0][0];
    // Must match Python: _canonical_temporal_key("2024-01-15T12:30:00", "datetime")
    expect(key).toBe("2024-01-15T12:30:00.000Z");
  });

  it("date-only string -> same YYYY-MM-DD key as Python", () => {
    const data: DataRecord[] = [{ d: "2024-01-15", v: 1 }];
    const columnTypes: ColumnTypeMap = new Map([["d", "date"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["d"], columns: [], values: ["v"] }),
      { columnTypes },
    );
    const key = pd.getRowKeys()[0][0];
    // Must match Python: _canonical_temporal_key("2024-01-15", "date")
    expect(key).toBe("2024-01-15");
  });

  it("epoch ms datetime -> ISO UTC key", () => {
    const data: DataRecord[] = [{ ts: 1705276800000, v: 1 }];
    const columnTypes: ColumnTypeMap = new Map([["ts", "datetime"]]);
    const pd = new PivotData(
      data,
      makeConfig({ rows: ["ts"], columns: [], values: ["v"] }),
      { columnTypes },
    );
    const key = pd.getRowKeys()[0][0];
    // 1705276800000 ms = 2024-01-15T00:00:00.000Z
    expect(key).toBe("2024-01-15T00:00:00.000Z");
  });
});
