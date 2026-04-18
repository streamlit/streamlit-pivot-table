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
  AGGREGATION_TYPES,
  AGGREGATOR_CLASS,
  CONFIG_SCHEMA_VERSION,
  DEFAULT_CONFIG,
  getDimensionLabel,
  getDrilledDateGrain,
  getEffectiveDateGrain,
  getRenderedValueLabel,
  getTemporalGroupingMode,
  type AggregationConfig,
  getAggregationForField,
  normalizeAggregationConfig,
  getRenderedValueFields,
  isSyntheticMeasure,
  migrateSortDirection,
  normalizeToggleList,
  showColumnTotals,
  showRowTotals,
  showSubtotalForDim,
  showTotalForMeasure,
  validatePivotConfigRuntime,
  validatePivotConfigV1,
  type AggregatorClass,
  type AggregationType,
  type CellClickPayload,
  type PivotConfigV1,
  type SortConfig,
} from "./types";

type TestConfigOverrides = Partial<Omit<PivotConfigV1, "aggregation">> & {
  aggregation?: AggregationType | AggregationConfig;
};

function makeConfig(overrides: TestConfigOverrides = {}): PivotConfigV1 {
  const { aggregation: aggregationOverride, ...restOverrides } = overrides;
  const values = overrides.values ?? ["Revenue", "Profit"];
  const config = {
    version: 1,
    rows: ["Region", "Category"],
    columns: ["Year"],
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

describe("Config schema v1", () => {
  it("has version 1", () => {
    expect(CONFIG_SCHEMA_VERSION).toBe(1);
    expect(DEFAULT_CONFIG.version).toBe(1);
  });

  it("default config shape matches snapshot", () => {
    expect(DEFAULT_CONFIG).toMatchInlineSnapshot(`
      {
        "aggregation": {},
        "auto_date_hierarchy": true,
        "columns": [],
        "empty_cell_value": "-",
        "interactive": true,
        "row_layout": "table",
        "rows": [],
        "show_column_totals": true,
        "show_row_totals": true,
        "show_totals": true,
        "synthetic_measures": [],
        "values": [],
        "version": 1,
      }
    `);
  });

  it("default config has all required fields", () => {
    const required: (keyof PivotConfigV1)[] = [
      "version",
      "rows",
      "columns",
      "values",
      "aggregation",
      "show_totals",
      "empty_cell_value",
      "interactive",
    ];
    for (const key of required) {
      expect(DEFAULT_CONFIG).toHaveProperty(key);
    }
  });

  it("aggregation types are exhaustive", () => {
    expect(AGGREGATION_TYPES).toEqual([
      "sum",
      "avg",
      "count",
      "min",
      "max",
      "count_distinct",
      "median",
      "percentile_90",
      "first",
      "last",
    ]);
  });
});

describe("Aggregator classification", () => {
  it("every aggregation type has a class", () => {
    for (const agg of AGGREGATION_TYPES) {
      expect(AGGREGATOR_CLASS[agg]).toBeDefined();
    }
  });

  it("classes match expected taxonomy", () => {
    expect(AGGREGATOR_CLASS).toEqual({
      sum: "additive",
      count: "additive",
      min: "idempotent",
      max: "idempotent",
      avg: "non-additive",
      count_distinct: "non-additive",
      median: "non-additive",
      percentile_90: "non-additive",
      first: "non-additive",
      last: "non-additive",
    });
  });

  it("only uses valid class values", () => {
    const valid: AggregatorClass[] = ["additive", "idempotent", "non-additive"];
    for (const cls of Object.values(AGGREGATOR_CLASS)) {
      expect(valid).toContain(cls);
    }
  });
});

describe("CellClickPayload contract", () => {
  it("example payload matches expected shape", () => {
    const payload: CellClickPayload = {
      rowKey: ["US", "California"],
      colKey: ["2024", "Q1"],
      value: 1234.56,
      filters: { Region: "US", State: "California" },
    };

    expect(payload).toMatchInlineSnapshot(`
      {
        "colKey": [
          "2024",
          "Q1",
        ],
        "filters": {
          "Region": "US",
          "State": "California",
        },
        "rowKey": [
          "US",
          "California",
        ],
        "value": 1234.56,
      }
    `);
  });

  it("payload with null value is valid", () => {
    const payload: CellClickPayload = {
      rowKey: [],
      colKey: [],
      value: null,
      filters: {},
    };
    expect(payload.value).toBeNull();
  });
});

describe("validatePivotConfigV1", () => {
  it("accepts a valid config", () => {
    const result = validatePivotConfigV1({ ...DEFAULT_CONFIG });
    expect(result.version).toBe(1);
    expect(result.aggregation).toEqual({});
  });

  it("rejects non-object input", () => {
    expect(() => validatePivotConfigV1("string")).toThrow("JSON object");
    expect(() => validatePivotConfigV1(null)).toThrow("JSON object");
  });

  it("rejects wrong version", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, version: 2 }),
    ).toThrow("version must be 1");
  });

  it("rejects missing rows array", () => {
    const { rows: _, ...rest } = DEFAULT_CONFIG;
    expect(() => validatePivotConfigV1(rest)).toThrow(
      "'rows' must be an array",
    );
  });

  it("rejects invalid aggregation", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, aggregation: "not_real" }),
    ).toThrow("'aggregation' must be one of");
  });

  it("keeps config import validation shape-only for period comparison modes", () => {
    expect(() =>
      validatePivotConfigV1({
        version: 1,
        rows: ["Region"],
        columns: ["Year"],
        values: ["Revenue"],
        aggregation: "sum",
        auto_date_hierarchy: false,
        show_values_as: { Revenue: "diff_from_prev" },
      }),
    ).not.toThrow();
  });

  it("accepts null date_grains entries as explicit Original opt-outs", () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: ["Revenue"],
      aggregation: "sum",
      date_grains: { order_date: null },
    });
    expect(result.date_grains).toEqual({ order_date: null });
  });

  it("normalizes scalar aggregation input to a per-value map", () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: [],
      columns: [],
      values: ["Revenue", "Profit"],
      aggregation: "avg",
    });
    expect(result.aggregation).toEqual({ Revenue: "avg", Profit: "avg" });
  });

  it("fills missing map entries with the default aggregation", () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: [],
      columns: [],
      values: ["Revenue", "Profit"],
      aggregation: { Revenue: "count" },
    });
    expect(result.aggregation).toEqual({ Revenue: "count", Profit: "sum" });
  });

  it("rejects non-string array elements in rows", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, rows: [1, 2] }),
    ).toThrow("'rows' must contain only strings");
  });

  it("rejects non-string array elements in values", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, values: [true] }),
    ).toThrow("'values' must contain only strings");
  });

  it("rejects non-boolean show_totals", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, show_totals: "yes" }),
    ).toThrow("'show_totals' must be a boolean");
  });

  it("rejects non-boolean interactive", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, interactive: 1 }),
    ).toThrow("'interactive' must be a boolean");
  });

  it("rejects non-string empty_cell_value", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, empty_cell_value: 0 }),
    ).toThrow("'empty_cell_value' must be a string");
  });

  it("defaults optional fields when omitted", () => {
    const minimal = {
      version: 1,
      rows: [],
      columns: [],
      values: [],
      aggregation: "sum",
    };
    const result = validatePivotConfigV1(minimal);
    expect(result.show_totals).toBe(true);
    expect(result.interactive).toBe(true);
    expect(result.empty_cell_value).toBe("-");
    expect(result.aggregation).toEqual({});
  });

  it("accepts valid filters object", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      filters: { region: { include: ["US", "EU"] } },
    });
    expect(result.filters).toEqual({ region: { include: ["US", "EU"] } });
  });

  it("accepts row_layout when valid", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      row_layout: "hierarchy",
    });
    expect(result.row_layout).toBe("hierarchy");
  });

  it("rejects invalid row_layout", () => {
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        row_layout: "compact",
      }),
    ).toThrow('\'row_layout\' must be "table" or "hierarchy"');
  });

  it("rejects non-object filters", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, filters: "bad" }),
    ).toThrow("'filters' must be an object");
  });

  it("rejects array filters", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, filters: ["bad"] }),
    ).toThrow("'filters' must be an object");
  });

  it("accepts valid row_sort as SortConfig object", () => {
    const sc: SortConfig = { by: "key", direction: "desc" };
    const result = validatePivotConfigV1({ ...DEFAULT_CONFIG, row_sort: sc });
    expect(result.row_sort).toEqual(sc);
  });

  it("accepts valid col_sort as SortConfig object", () => {
    const sc: SortConfig = {
      by: "value",
      direction: "asc",
      value_field: "revenue",
    };
    const result = validatePivotConfigV1({ ...DEFAULT_CONFIG, col_sort: sc });
    expect(result.col_sort).toEqual(sc);
  });

  it("accepts legacy string row_sort and migrates to SortConfig", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      row_sort: "key_desc",
    });
    expect(result.row_sort).toEqual({ by: "key", direction: "desc" });
  });

  it("accepts legacy string col_sort and migrates to SortConfig", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      col_sort: "value_asc",
    });
    expect(result.col_sort).toEqual({ by: "value", direction: "asc" });
  });

  it("rejects invalid row_sort", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, row_sort: "invalid" }),
    ).toThrow("'row_sort'");
  });

  it("rejects invalid col_sort", () => {
    expect(() =>
      validatePivotConfigV1({ ...DEFAULT_CONFIG, col_sort: 42 }),
    ).toThrow("'col_sort'");
  });

  it("rejects SortConfig with invalid by field", () => {
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        row_sort: { by: "bad", direction: "asc" },
      }),
    ).toThrow("'row_sort.by'");
  });

  it("rejects SortConfig with invalid direction", () => {
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        col_sort: { by: "key", direction: "up" },
      }),
    ).toThrow("'col_sort.direction'");
  });

  it("omits Phase 2 optional fields when not provided", () => {
    const result = validatePivotConfigV1({ ...DEFAULT_CONFIG });
    expect(result.filters).toBeUndefined();
    expect(result.row_sort).toBeUndefined();
    expect(result.col_sort).toBeUndefined();
  });
});

describe("validatePivotConfigV1 — column_config display metadata", () => {
  it("accepts valid field_labels map", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      field_labels: { Revenue: "Total Revenue", Region: "Area" },
    });
    expect(result.field_labels).toEqual({
      Revenue: "Total Revenue",
      Region: "Area",
    });
  });

  it("accepts empty field_labels map", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      field_labels: {},
    });
    expect(result.field_labels).toEqual({});
  });

  it("rejects non-string field_labels values", () => {
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        field_labels: { Revenue: 123 },
      }),
    ).toThrow("field_labels");
  });

  it("accepts valid field_help map", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      field_help: { Revenue: "USD, pre-tax" },
    });
    expect(result.field_help).toEqual({ Revenue: "USD, pre-tax" });
  });

  it("rejects non-string field_help values", () => {
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        field_help: { Revenue: true },
      }),
    ).toThrow("field_help");
  });

  it("accepts field_widths preset strings", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      field_widths: { A: "small", B: "medium", C: "large" },
    });
    expect(result.field_widths).toEqual({
      A: "small",
      B: "medium",
      C: "large",
    });
  });

  it("accepts field_widths pixel numbers", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      field_widths: { A: 100, B: 220 },
    });
    expect(result.field_widths).toEqual({ A: 100, B: 220 });
  });

  it("rejects unknown preset string for field_widths", () => {
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        field_widths: { A: "huge" },
      }),
    ).toThrow("field_widths");
  });

  it("rejects non-positive field_widths values", () => {
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        field_widths: { A: 0 },
      }),
    ).toThrow("field_widths");
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        field_widths: { A: -10 },
      }),
    ).toThrow("field_widths");
  });

  it("rejects NaN / Infinity field_widths values", () => {
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        field_widths: { A: Number.NaN },
      }),
    ).toThrow("field_widths");
    expect(() =>
      validatePivotConfigV1({
        ...DEFAULT_CONFIG,
        field_widths: { A: Number.POSITIVE_INFINITY },
      }),
    ).toThrow("field_widths");
  });

  it("backward compat: omits new fields when absent", () => {
    const result = validatePivotConfigV1({ ...DEFAULT_CONFIG });
    expect(result.field_labels).toBeUndefined();
    expect(result.field_help).toBeUndefined();
    expect(result.field_widths).toBeUndefined();
  });

  it("accepts a config with only some of the new fields set", () => {
    const result = validatePivotConfigV1({
      ...DEFAULT_CONFIG,
      field_labels: { Revenue: "Rev" },
    });
    expect(result.field_labels).toEqual({ Revenue: "Rev" });
    expect(result.field_help).toBeUndefined();
    expect(result.field_widths).toBeUndefined();
  });
});

describe("migrateSortDirection", () => {
  it("converts all legacy string formats", () => {
    expect(migrateSortDirection("key_asc")).toEqual({
      by: "key",
      direction: "asc",
    });
    expect(migrateSortDirection("key_desc")).toEqual({
      by: "key",
      direction: "desc",
    });
    expect(migrateSortDirection("value_asc")).toEqual({
      by: "value",
      direction: "asc",
    });
    expect(migrateSortDirection("value_desc")).toEqual({
      by: "value",
      direction: "desc",
    });
  });
});

describe("showSubtotalForDim", () => {
  it("show_subtotals: true returns true for any dim", () => {
    const config = makeConfig({ show_subtotals: true });
    expect(showSubtotalForDim(config, "Region")).toBe(true);
    expect(showSubtotalForDim(config, "Category")).toBe(true);
    expect(showSubtotalForDim(config, "Other")).toBe(true);
  });

  it("show_subtotals: false returns false for any dim", () => {
    const config = makeConfig({ show_subtotals: false });
    expect(showSubtotalForDim(config, "Region")).toBe(false);
    expect(showSubtotalForDim(config, "Category")).toBe(false);
  });

  it("show_subtotals: undefined returns false", () => {
    const config = makeConfig();
    expect(showSubtotalForDim(config, "Region")).toBe(false);
  });

  it('show_subtotals: ["Region"] with dimName="Region" returns true', () => {
    const config = makeConfig({ show_subtotals: ["Region"] });
    expect(showSubtotalForDim(config, "Region")).toBe(true);
  });

  it('show_subtotals: ["Region"] with dimName="Category" returns false', () => {
    const config = makeConfig({ show_subtotals: ["Region"] });
    expect(showSubtotalForDim(config, "Category")).toBe(false);
  });

  it("show_subtotals: [] returns false (defensive edge case)", () => {
    const config = makeConfig({ show_subtotals: [] });
    expect(showSubtotalForDim(config, "Region")).toBe(false);
  });
});

describe("showTotalForMeasure", () => {
  it('show_row_totals: true, axis="row" returns true', () => {
    const config = makeConfig({ show_row_totals: true });
    expect(showTotalForMeasure(config, "Revenue", "row")).toBe(true);
    expect(showTotalForMeasure(config, "Profit", "row")).toBe(true);
  });

  it('show_row_totals: false, axis="row" returns false', () => {
    const config = makeConfig({ show_row_totals: false });
    expect(showTotalForMeasure(config, "Revenue", "row")).toBe(false);
  });

  it('show_row_totals: ["Revenue"], measure="Revenue", axis="row" returns true', () => {
    const config = makeConfig({ show_row_totals: ["Revenue"] });
    expect(showTotalForMeasure(config, "Revenue", "row")).toBe(true);
  });

  it('show_row_totals: ["Revenue"], measure="Profit", axis="row" returns false', () => {
    const config = makeConfig({ show_row_totals: ["Revenue"] });
    expect(showTotalForMeasure(config, "Profit", "row")).toBe(false);
  });

  it('show_column_totals: ["Revenue"], measure="Revenue", axis="col" returns true', () => {
    const config = makeConfig({ show_column_totals: ["Revenue"] });
    expect(showTotalForMeasure(config, "Revenue", "col")).toBe(true);
  });

  it('show_column_totals: ["Revenue"], measure="Profit", axis="col" returns false', () => {
    const config = makeConfig({ show_column_totals: ["Revenue"] });
    expect(showTotalForMeasure(config, "Profit", "col")).toBe(false);
  });

  it('axis="grand": both show_row_totals and show_column_totals must include the measure', () => {
    const configBoth = makeConfig({
      show_row_totals: ["Revenue"],
      show_column_totals: ["Revenue"],
    });
    expect(showTotalForMeasure(configBoth, "Revenue", "grand")).toBe(true);

    const configRowOnly = makeConfig({
      show_row_totals: ["Revenue"],
      show_column_totals: ["Profit"],
    });
    expect(showTotalForMeasure(configRowOnly, "Revenue", "grand")).toBe(false);

    const configColOnly = makeConfig({
      show_row_totals: ["Profit"],
      show_column_totals: ["Revenue"],
    });
    expect(showTotalForMeasure(configColOnly, "Revenue", "grand")).toBe(false);
  });

  it("fallback: show_row_totals undefined, show_totals true returns true", () => {
    const config = makeConfig({ show_totals: true });
    expect(showTotalForMeasure(config, "Revenue", "row")).toBe(true);
  });
});

describe("showRowTotals", () => {
  it("returns true for non-empty string[]", () => {
    const config = makeConfig({ show_row_totals: ["Revenue"] });
    expect(showRowTotals(config)).toBe(true);
  });

  it("returns false for empty string[] (edge case)", () => {
    const config = makeConfig({ show_row_totals: [] });
    expect(showRowTotals(config)).toBe(false);
  });

  it("returns false when columns.length === 0", () => {
    const config = makeConfig({ columns: [] });
    expect(showRowTotals(config)).toBe(false);
  });
});

describe("showColumnTotals", () => {
  it("returns true for non-empty string[]", () => {
    const config = makeConfig({ show_column_totals: ["Revenue"] });
    expect(showColumnTotals(config)).toBe(true);
  });

  it("returns false for empty string[] (edge case)", () => {
    const config = makeConfig({ show_column_totals: [] });
    expect(showColumnTotals(config)).toBe(false);
  });

  it("returns false when rows.length === 0", () => {
    const config = makeConfig({ rows: [] });
    expect(showColumnTotals(config)).toBe(false);
  });
});

describe("normalizeToggleList", () => {
  it('current=true, toggle "A" with allItems=["A","B","C"] returns ["B","C"]', () => {
    expect(normalizeToggleList(true, ["A", "B", "C"], "A")).toEqual(["B", "C"]);
  });

  it('current=false, toggle "A" returns ["A"]', () => {
    expect(normalizeToggleList(false, ["A", "B", "C"], "A")).toEqual(["A"]);
  });

  it('current=["A","B"], toggle "A" with allItems=["A","B","C"] returns ["B"]', () => {
    expect(normalizeToggleList(["A", "B"], ["A", "B", "C"], "A")).toEqual([
      "B",
    ]);
  });

  it('current=["A","B"], toggle "C" with allItems=["A","B","C"] returns true (all items)', () => {
    expect(normalizeToggleList(["A", "B"], ["A", "B", "C"], "C")).toBe(true);
  });

  it('current=["A"], toggle "A" returns false (empty)', () => {
    expect(normalizeToggleList(["A"], ["A", "B", "C"], "A")).toBe(false);
  });
});

describe("validatePivotConfigV1 with bool | string[]", () => {
  it("accepts show_subtotals: true and validates to true", () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: ["Region", "Category"],
      columns: ["Year"],
      values: ["Revenue", "Profit"],
      aggregation: "sum",
      show_subtotals: true,
    });
    expect(result.show_subtotals).toBe(true);
  });

  it('show_subtotals: ["Region"] with rows=["Region","Category"] normalizes to true (only non-leaf dim)', () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: ["Region", "Category"],
      columns: ["Year"],
      values: ["Revenue", "Profit"],
      aggregation: "sum",
      show_subtotals: ["Region"],
    });
    expect(result.show_subtotals).toBe(true);
  });

  it('show_subtotals: ["Region"] with rows=["Region","Category","Product"] keeps partial list', () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: ["Region", "Category", "Product"],
      columns: ["Year"],
      values: ["Revenue", "Profit"],
      aggregation: "sum",
      show_subtotals: ["Region"],
    });
    expect(result.show_subtotals).toEqual(["Region"]);
  });

  it('show_subtotals: ["Region","Category"] with rows=["Region","Category","Product"] normalizes to true', () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: ["Region", "Category", "Product"],
      columns: ["Year"],
      values: ["Revenue", "Profit"],
      aggregation: "sum",
      show_subtotals: ["Region", "Category"],
    });
    expect(result.show_subtotals).toBe(true);
  });

  it('show_subtotals: ["Product"] (leaf dim) normalizes to false (field omitted)', () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: ["Region", "Category", "Product"],
      columns: ["Year"],
      values: ["Revenue", "Profit"],
      aggregation: "sum",
      show_subtotals: ["Product"],
    });
    expect(result.show_subtotals).toBeUndefined();
  });

  it('show_subtotals: ["Unknown"] with rows=["Region","Category"] normalizes to false (field omitted)', () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: ["Region", "Category"],
      columns: ["Year"],
      values: ["Revenue", "Profit"],
      aggregation: "sum",
      show_subtotals: ["Unknown"],
    });
    expect(result.show_subtotals).toBeUndefined();
    expect(showSubtotalForDim(result, "Region")).toBe(false);
  });

  it('show_row_totals: ["Revenue"] with values=["Revenue","Profit"] keeps ["Revenue"]', () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: ["Region", "Category"],
      columns: ["Year"],
      values: ["Revenue", "Profit"],
      aggregation: "sum",
      show_row_totals: ["Revenue"],
    });
    expect(result.show_row_totals).toEqual(["Revenue"]);
  });

  it('show_row_totals: ["Revenue","Profit"] normalizes to true', () => {
    const result = validatePivotConfigV1({
      version: 1,
      rows: ["Region", "Category"],
      columns: ["Year"],
      values: ["Revenue", "Profit"],
      aggregation: "sum",
      show_row_totals: ["Revenue", "Profit"],
    });
    expect(result.show_row_totals).toBe(true);
  });
});

describe("Synthetic measures", () => {
  it("validates and preserves synthetic measures", () => {
    const result = validatePivotConfigV1({
      ...makeConfig(),
      synthetic_measures: [
        {
          id: "prs_per_person",
          label: "PRs / Person",
          operation: "sum_over_sum",
          numerator: "Revenue",
          denominator: "Profit",
        },
      ],
    });
    expect(result.synthetic_measures).toHaveLength(1);
    expect(result.synthetic_measures?.[0].id).toBe("prs_per_person");
    expect(getRenderedValueFields(result)).toEqual([
      "Revenue",
      "Profit",
      "prs_per_person",
    ]);
    expect(isSyntheticMeasure(result, "prs_per_person")).toBe(true);
  });

  it("drops show_values_as entries for synthetic measures", () => {
    const result = validatePivotConfigV1({
      ...makeConfig(),
      synthetic_measures: [
        {
          id: "prs_per_person",
          label: "PRs / Person",
          operation: "sum_over_sum",
          numerator: "Revenue",
          denominator: "Profit",
        },
      ],
      show_values_as: {
        Revenue: "pct_of_total",
        prs_per_person: "pct_of_total",
      },
    });
    expect(result.show_values_as).toEqual({ Revenue: "pct_of_total" });
  });
});

describe("date hierarchy helpers", () => {
  it("auto-defaults temporal axis fields to month", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: [],
      aggregation: {},
    });
    expect(getEffectiveDateGrain(config, "order_date", "date")).toBe("month");
    expect(getTemporalGroupingMode(config, "order_date", "date")).toBe("auto");
  });

  it("supports explicit Original opt-out", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: [],
      aggregation: {},
      date_grains: { order_date: null },
    });
    expect(getEffectiveDateGrain(config, "order_date", "date")).toBeUndefined();
    expect(getTemporalGroupingMode(config, "order_date", "date")).toBe(
      "original",
    );
  });

  it("disables auto hierarchy when requested", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: [],
      aggregation: {},
      auto_date_hierarchy: false,
    });
    expect(getEffectiveDateGrain(config, "order_date", "date")).toBeUndefined();
    expect(getTemporalGroupingMode(config, "order_date", "date")).toBe("none");
  });

  it("still honors explicit grain overrides when auto hierarchy is off", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: [],
      aggregation: {},
      auto_date_hierarchy: false,
      date_grains: { order_date: "quarter" },
    });
    expect(getEffectiveDateGrain(config, "order_date", "date")).toBe("quarter");
    expect(getTemporalGroupingMode(config, "order_date", "date")).toBe(
      "explicit",
    );
  });

  it("uses the default drill ladder and excludes week", () => {
    expect(getDrilledDateGrain("month", "up")).toBe("quarter");
    expect(getDrilledDateGrain("month", "down")).toBe("day");
    expect(getDrilledDateGrain("week", "up")).toBeUndefined();
    expect(getDrilledDateGrain("week", "down")).toBeUndefined();
  });

  it("runtime-validates period comparisons against temporal column types", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["Region"],
      columns: ["Year"],
      values: ["Revenue"],
      aggregation: "sum",
      show_values_as: { Revenue: "diff_from_prev" },
    });
    expect(() =>
      validatePivotConfigRuntime(
        config,
        new Map([
          ["Region", "string"],
          ["Year", "integer"],
        ]),
      ),
    ).toThrow(
      "period comparison show_values_as modes require a grouped date/datetime field on rows or columns",
    );
  });

  it("runtime-allows period comparisons when auto hierarchy targets a temporal axis", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["Region"],
      columns: ["order_date"],
      values: ["Revenue"],
      aggregation: "sum",
      show_values_as: { Revenue: "diff_from_prev" },
    });
    expect(() =>
      validatePivotConfigRuntime(
        config,
        new Map([
          ["Region", "string"],
          ["order_date", "date"],
        ]),
      ),
    ).not.toThrow();
  });
});

describe("getAggregationForField", () => {
  it("returns the configured aggregation for a raw value field", () => {
    const config = makeConfig({
      aggregation: { Revenue: "avg", Profit: "count" },
    });
    expect(getAggregationForField("Revenue", config)).toBe("avg");
    expect(getAggregationForField("Profit", config)).toBe("count");
  });

  it("defaults synthetic-only source fields to sum", () => {
    const config = makeConfig({
      values: [],
      synthetic_measures: [
        {
          id: "margin_ratio",
          label: "Margin Ratio",
          operation: "sum_over_sum",
          numerator: "Revenue",
          denominator: "Cost",
        },
      ],
      aggregation: {},
    });
    expect(getAggregationForField("Revenue", config)).toBe("sum");
    expect(getAggregationForField("Cost", config)).toBe("sum");
  });
});

describe("getEffectiveDateGrain with adaptiveGrain", () => {
  it("returns adaptive grain instead of default when auto-hierarchy applies", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: [],
      aggregation: {},
    });
    expect(getEffectiveDateGrain(config, "order_date", "date", "year")).toBe(
      "year",
    );
    expect(getEffectiveDateGrain(config, "order_date", "date", "quarter")).toBe(
      "quarter",
    );
  });

  it("explicit date_grains override still takes precedence", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: [],
      aggregation: {},
      date_grains: { order_date: "week" },
    });
    expect(getEffectiveDateGrain(config, "order_date", "date", "year")).toBe(
      "week",
    );
  });

  it("null opt-out still returns undefined regardless of adaptive grain", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: [],
      aggregation: {},
      date_grains: { order_date: null },
    });
    expect(
      getEffectiveDateGrain(config, "order_date", "date", "year"),
    ).toBeUndefined();
  });

  it("auto_date_hierarchy=false ignores adaptive grain", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: [],
      aggregation: {},
      auto_date_hierarchy: false,
    });
    expect(
      getEffectiveDateGrain(config, "order_date", "date", "year"),
    ).toBeUndefined();
  });

  it("falls back to default when adaptiveGrain is undefined", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["order_date"],
      columns: [],
      values: [],
      aggregation: {},
    });
    expect(getEffectiveDateGrain(config, "order_date", "date", undefined)).toBe(
      "month",
    );
  });
});

describe("validatePivotConfigV1 — formula synthetic measures", () => {
  it("accepts formula operation with valid formula", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: ["region"],
      columns: [],
      values: ["revenue"],
      aggregation: {},
      synthetic_measures: [
        {
          id: "margin",
          label: "Margin",
          operation: "formula",
          formula: '"revenue" - "cost"',
        },
      ],
    });
    expect(config.synthetic_measures).toHaveLength(1);
    expect(config.synthetic_measures![0].operation).toBe("formula");
    expect(config.synthetic_measures![0].formula).toBe('"revenue" - "cost"');
  });

  it("rejects formula operation with empty formula", () => {
    expect(() =>
      validatePivotConfigV1({
        version: 1,
        rows: [],
        columns: [],
        values: ["revenue"],
        aggregation: {},
        synthetic_measures: [
          {
            id: "m",
            label: "M",
            operation: "formula",
            formula: "",
          },
        ],
      }),
    ).toThrow(/must be a non-empty string/);
  });

  it("rejects formula with no field references", () => {
    expect(() =>
      validatePivotConfigV1({
        version: 1,
        rows: [],
        columns: [],
        values: ["revenue"],
        aggregation: {},
        synthetic_measures: [
          {
            id: "m",
            label: "M",
            operation: "formula",
            formula: "42 + 1",
          },
        ],
      }),
    ).toThrow(/must reference at least one field/);
  });

  it("rejects syntactically invalid formulas", () => {
    expect(() =>
      validatePivotConfigV1({
        version: 1,
        rows: [],
        columns: [],
        values: ["revenue"],
        aggregation: {},
        synthetic_measures: [
          {
            id: "m",
            label: "M",
            operation: "formula",
            formula: '"Revenue" + * "Cost"',
          },
        ],
      }),
    ).toThrow(/is invalid/);
  });

  it("rejects formulas with wrong function arity", () => {
    expect(() =>
      validatePivotConfigV1({
        version: 1,
        rows: [],
        columns: [],
        values: ["revenue"],
        aggregation: {},
        synthetic_measures: [
          {
            id: "m",
            label: "M",
            operation: "formula",
            formula: 'min("A", "B", "C")',
          },
        ],
      }),
    ).toThrow(/expects exactly 2/);
  });

  it("merges formula source fields into aggregation normalization", () => {
    const config = validatePivotConfigV1({
      version: 1,
      rows: [],
      columns: [],
      values: ["revenue"],
      aggregation: { cost: "avg" },
      synthetic_measures: [
        {
          id: "margin",
          label: "Margin",
          operation: "formula",
          formula: '"revenue" - "cost"',
        },
      ],
    });
    expect(config.aggregation["cost"]).toBe("avg");
  });
});

describe("getRenderedValueLabel with field_labels", () => {
  it("returns field id when no override is set", () => {
    const config = makeConfig({ values: ["Revenue"] });
    expect(getRenderedValueLabel(config, "Revenue")).toBe("Revenue");
  });

  it("returns column_config.label when set", () => {
    const config = makeConfig({
      values: ["Revenue"],
      field_labels: { Revenue: "Total Revenue" },
    });
    expect(getRenderedValueLabel(config, "Revenue")).toBe("Total Revenue");
  });

  it("falls back to field id when label is empty string", () => {
    const config = makeConfig({
      values: ["Revenue"],
      field_labels: { Revenue: "" },
    });
    expect(getRenderedValueLabel(config, "Revenue")).toBe("Revenue");
  });

  it("falls back to field id when label is whitespace-only", () => {
    const config = makeConfig({
      values: ["Revenue"],
      field_labels: { Revenue: "   " },
    });
    expect(getRenderedValueLabel(config, "Revenue")).toBe("Revenue");
  });

  it("trims whitespace around a real label", () => {
    const config = makeConfig({
      values: ["Revenue"],
      field_labels: { Revenue: "  Rev  " },
    });
    expect(getRenderedValueLabel(config, "Revenue")).toBe("Rev");
  });

  it("override takes precedence over synthetic measure label", () => {
    const config = makeConfig({
      values: [],
      synthetic_measures: [
        {
          id: "margin",
          label: "Margin",
          operation: "difference",
          numerator: "Revenue",
          denominator: "Cost",
        },
      ],
      field_labels: { margin: "Gross Margin" },
    });
    expect(getRenderedValueLabel(config, "margin")).toBe("Gross Margin");
  });

  it("uses synthetic measure label when no override", () => {
    const config = makeConfig({
      values: [],
      synthetic_measures: [
        {
          id: "margin",
          label: "Margin",
          operation: "difference",
          numerator: "Revenue",
          denominator: "Cost",
        },
      ],
    });
    expect(getRenderedValueLabel(config, "margin")).toBe("Margin");
  });
});

describe("getDimensionLabel with field_labels", () => {
  it("returns field id when no override and no grain", () => {
    const config = makeConfig({ rows: ["Region"] });
    expect(getDimensionLabel(config, "Region")).toBe("Region");
  });

  it("returns column_config.label when set (no grain)", () => {
    const config = makeConfig({
      rows: ["Region"],
      field_labels: { Region: "Area" },
    });
    expect(getDimensionLabel(config, "Region")).toBe("Area");
  });

  it("falls back to field id for empty label (no grain)", () => {
    const config = makeConfig({
      rows: ["Region"],
      field_labels: { Region: "" },
    });
    expect(getDimensionLabel(config, "Region")).toBe("Region");
  });

  it("falls back to field id for whitespace-only label (no grain)", () => {
    const config = makeConfig({
      rows: ["Region"],
      field_labels: { Region: "   " },
    });
    expect(getDimensionLabel(config, "Region")).toBe("Region");
  });

  it("appends temporal grain suffix to the override when set", () => {
    const config = makeConfig({
      rows: ["order_date"],
      columns: [],
      field_labels: { order_date: "Order" },
      auto_date_hierarchy: true,
    });
    expect(getDimensionLabel(config, "order_date", "date", "month")).toBe(
      "Order (Month)",
    );
  });

  it("appends temporal grain suffix to the field id when no override", () => {
    const config = makeConfig({
      rows: ["order_date"],
      columns: [],
      auto_date_hierarchy: true,
    });
    expect(getDimensionLabel(config, "order_date", "date", "month")).toBe(
      "order_date (Month)",
    );
  });

  it("empty label still appends grain suffix to field id fallback", () => {
    const config = makeConfig({
      rows: ["order_date"],
      columns: [],
      field_labels: { order_date: "" },
      auto_date_hierarchy: true,
    });
    expect(getDimensionLabel(config, "order_date", "date", "year")).toBe(
      "order_date (Year)",
    );
  });

  it("identity contract: label override does not change field id in config", () => {
    // Labels are display-only: the canonical ids remain untouched, so
    // sort/filter/CF keyed on the id continue to work.
    const config = makeConfig({
      rows: ["Region"],
      values: ["Revenue"],
      field_labels: { Region: "Area", Revenue: "Rev" },
    });
    expect(config.rows).toEqual(["Region"]);
    expect(config.values).toEqual(["Revenue"]);
  });
});
