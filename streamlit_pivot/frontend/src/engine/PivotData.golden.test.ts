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
 * Golden verification tests: pandas-computed reference values vs PivotData engine.
 *
 * Every expected value in golden_expected.json was computed entirely by pandas.
 * The TS PivotData engine is the system under test. These tests verify the
 * engine produces results that match the independent oracle.
 *
 * Each config is tested with BOTH the record path (DataRecord[]) and the
 * columnar path (ColumnarDataSource) to ensure S6 parity.
 */

import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { PivotData, type DataRecord, makeKeyString } from "./PivotData";
import { DataRecordSource } from "./parseArrow";
import {
  normalizeAggregationConfig,
  type AggregationConfig,
  type AggregationType,
  type ColumnarDataSource,
  type PivotConfigV1,
} from "./types";

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

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

function makePair(
  records: DataRecord[],
  config: PivotConfigV1,
): { record: PivotData; columnar: PivotData } {
  return {
    record: new PivotData(records, config),
    columnar: new PivotData(new TestColumnarSource(records), config),
  };
}

const TOLERANCE = 0.02;

function expectClose(actual: number | null, expected: number, label: string) {
  expect(
    actual,
    `${label}: expected ${expected}, got ${actual}`,
  ).not.toBeNull();
  expect(actual).toBeCloseTo(expected, 1);
}

/**
 * Verify all cells, row totals, col totals, and grand total for a single-measure
 * PivotData against the golden reference.
 */
function verifyAllValues(
  pd: PivotData,
  g: {
    cells: Record<string, Record<string, number>>;
    row_totals: Record<string, number>;
    col_totals: Record<string, number>;
    grand_total: number;
  },
  valField: string,
  label: string,
) {
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

// ---------------------------------------------------------------------------
// Golden tests
// ---------------------------------------------------------------------------

describe("Golden Verification Suite", () => {
  describe("Config A — Basic sum", () => {
    const g = golden.A;
    const config = makeConfig({ aggregation: "sum" });
    const { record, columnar } = makePair(smallRecords, config);

    it("record path matches pandas golden values", () => {
      verifyAllValues(record, g, "Revenue", "record");
    });

    it("columnar path matches pandas golden values", () => {
      verifyAllValues(columnar, g, "Revenue", "columnar");
    });

    it("record and columnar paths produce identical results", () => {
      for (const rowKey of record.getRowKeys()) {
        for (const colKey of record.getColKeys()) {
          expect(record.getAggregator(rowKey, colKey).value()).toBe(
            columnar.getAggregator(rowKey, colKey).value(),
          );
        }
      }
    });
  });

  describe("Config B — Multi-measure", () => {
    const g = golden.B;
    const config = makeConfig({
      values: ["Revenue", "Profit"],
      aggregation: { Revenue: "sum", Profit: "sum" },
    });
    const { record, columnar } = makePair(smallRecords, config);

    for (const measure of ["Revenue", "Profit"] as const) {
      it(`record path matches pandas for ${measure}`, () => {
        verifyAllValues(
          record,
          g.measures[measure],
          measure,
          `record ${measure}`,
        );
      });

      it(`columnar path matches pandas for ${measure}`, () => {
        verifyAllValues(
          columnar,
          g.measures[measure],
          measure,
          `columnar ${measure}`,
        );
      });
    }
  });

  describe("Config C — Per-measure agg (Revenue=sum, Units=avg)", () => {
    const g = golden.C;
    const config = makeConfig({
      values: ["Revenue", "Units"],
      aggregation: { Revenue: "sum", Units: "avg" },
    });
    const { record, columnar } = makePair(smallRecords, config);

    it("record Revenue cells match pandas sum", () => {
      verifyAllValues(record, g.measures.Revenue, "Revenue", "record Revenue");
    });

    it("record Units cells match pandas avg", () => {
      verifyAllValues(record, g.measures.Units, "Units", "record Units");
    });

    it("columnar Revenue cells match pandas sum", () => {
      verifyAllValues(
        columnar,
        g.measures.Revenue,
        "Revenue",
        "columnar Revenue",
      );
    });

    it("columnar Units cells match pandas avg", () => {
      verifyAllValues(columnar, g.measures.Units, "Units", "columnar Units");
    });

    it("Revenue sum and Units avg are numerically different for same cells", () => {
      for (const rowKey of record.getRowKeys()) {
        for (const colKey of record.getColKeys()) {
          const rev = record.getAggregator(rowKey, colKey, "Revenue").value();
          const units = record.getAggregator(rowKey, colKey, "Units").value();
          if (rev !== null && units !== null) {
            expect(rev).not.toBe(units);
          }
        }
      }
    });
  });

  describe("Config D — Count", () => {
    const g = golden.D;
    const config = makeConfig({ aggregation: "count" });
    const { record, columnar } = makePair(smallRecords, config);

    it("record path matches pandas count", () => {
      verifyAllValues(record, g, "Revenue", "record");
    });

    it("columnar path matches pandas count", () => {
      verifyAllValues(columnar, g, "Revenue", "columnar");
    });
  });

  describe("Config E — Subtotals", () => {
    const g = golden.E;
    const config = makeConfig({
      rows: ["Region", "Category"],
      aggregation: "sum",
      show_subtotals: true,
    });
    const { record, columnar } = makePair(smallRecords, config);

    it("record leaf cells match pandas", () => {
      for (const [rowKeyStr, colVals] of Object.entries(g.cells)) {
        const rowKey = rowKeyStr.split("|");
        for (const [colKeyStr, expected] of Object.entries(
          colVals as Record<string, number>,
        )) {
          const colKey = colKeyStr.split("|");
          const actual = record
            .getAggregator(rowKey, colKey, "Revenue")
            .value();
          expectClose(
            actual,
            expected,
            `record cell [${rowKeyStr}][${colKeyStr}]`,
          );
        }
      }
    });

    it("record subtotals by Region match pandas groupby(Region)", () => {
      const subtotals = g.subtotals.by_region;
      for (const [regionStr, colVals] of Object.entries(subtotals.cells)) {
        const parentKey = regionStr.split("|");
        for (const [colKeyStr, expected] of Object.entries(
          colVals as Record<string, number>,
        )) {
          const colKey = colKeyStr.split("|");
          const actual = record
            .getSubtotalAggregator(parentKey, colKey, "Revenue")
            .value();
          expectClose(
            actual,
            expected,
            `record subtotal [${regionStr}][${colKeyStr}]`,
          );
        }
      }
    });

    it("columnar subtotals match record subtotals", () => {
      const subtotals = g.subtotals.by_region;
      for (const [regionStr, colVals] of Object.entries(subtotals.cells)) {
        const parentKey = regionStr.split("|");
        for (const colKeyStr of Object.keys(
          colVals as Record<string, number>,
        )) {
          const colKey = colKeyStr.split("|");
          expect(
            record.getSubtotalAggregator(parentKey, colKey, "Revenue").value(),
          ).toBe(
            columnar
              .getSubtotalAggregator(parentKey, colKey, "Revenue")
              .value(),
          );
        }
      }
    });

    it("subtotal row total = sum of children row totals (mathematical invariant)", () => {
      const regions = [...new Set(smallRecords.map((r) => String(r.Region)))];
      for (const region of regions) {
        const childKeys = record.getRowKeys().filter((k) => k[0] === region);
        const childSum = childKeys.reduce(
          (sum, k) => sum + (record.getRowTotal(k, "Revenue").value() ?? 0),
          0,
        );
        const subtotalRowTotal = record
          .getSubtotalAggregator([region], [], "Revenue")
          .value();
        expectClose(
          subtotalRowTotal,
          childSum,
          `subtotal row total [${region}]`,
        );
      }
    });
  });

  describe("Config F — Pct of total", () => {
    const g = golden.F;
    const config = makeConfig({ aggregation: "sum" });
    const pd = new PivotData(smallRecords, config);

    it("raw cell values match pandas (pre-pct computation)", () => {
      for (const [rowKeyStr, colVals] of Object.entries(g.cells_raw)) {
        const rowKey = rowKeyStr.split("|");
        for (const [colKeyStr, expected] of Object.entries(
          colVals as Record<string, number>,
        )) {
          const colKey = colKeyStr.split("|");
          const actual = pd.getAggregator(rowKey, colKey, "Revenue").value();
          expectClose(
            actual,
            expected,
            `raw cell [${rowKeyStr}][${colKeyStr}]`,
          );
        }
      }
    });

    it("pct_of_total = cell / grand_total * 100 matches pandas", () => {
      const grand = pd.getGrandTotal("Revenue").value()!;
      expectClose(grand, g.grand_total, "grand total");

      for (const [rowKeyStr, colVals] of Object.entries(g.pct_cells)) {
        const rowKey = rowKeyStr.split("|");
        for (const [colKeyStr, expectedPct] of Object.entries(
          colVals as Record<string, number>,
        )) {
          const colKey = colKeyStr.split("|");
          const rawVal = pd.getAggregator(rowKey, colKey, "Revenue").value()!;
          const computedPct = (rawVal / grand) * 100;
          expectClose(
            computedPct,
            expectedPct,
            `pct_of_total [${rowKeyStr}][${colKeyStr}]`,
          );
        }
      }
    });
  });

  describe("Config F2 — Pct of row", () => {
    const g = golden.F2;
    const config = makeConfig({ aggregation: "sum" });
    const pd = new PivotData(smallRecords, config);

    it("pct_of_row = cell / row_total * 100 matches pandas", () => {
      for (const [rowKeyStr, colVals] of Object.entries(g.pct_cells)) {
        const rowKey = rowKeyStr.split("|");
        const rowTotal = pd.getRowTotal(rowKey, "Revenue").value()!;
        for (const [colKeyStr, expectedPct] of Object.entries(
          colVals as Record<string, number>,
        )) {
          const colKey = colKeyStr.split("|");
          const rawVal = pd.getAggregator(rowKey, colKey, "Revenue").value()!;
          const computedPct = (rawVal / rowTotal) * 100;
          expectClose(
            computedPct,
            expectedPct,
            `pct_of_row [${rowKeyStr}][${colKeyStr}]`,
          );
        }
      }
    });

    it("pct_of_row values differ from pct_of_total values", () => {
      const gf = golden.F;
      let foundDifference = false;
      for (const rowKeyStr of Object.keys(g.pct_cells)) {
        for (const colKeyStr of Object.keys(
          (g.pct_cells as Record<string, Record<string, number>>)[rowKeyStr]!,
        )) {
          const pctRow = (
            g.pct_cells as Record<string, Record<string, number>>
          )[rowKeyStr]![colKeyStr]!;
          const pctTotal = (
            gf.pct_cells as Record<string, Record<string, number>>
          )[rowKeyStr]?.[colKeyStr];
          if (pctTotal !== undefined && Math.abs(pctRow - pctTotal) > 0.01) {
            foundDifference = true;
          }
        }
      }
      expect(foundDifference).toBe(true);
    });
  });

  describe("Config F3 — Pct of col", () => {
    const g = golden.F3;
    const config = makeConfig({ aggregation: "sum" });
    const pd = new PivotData(smallRecords, config);

    it("pct_of_col = cell / col_total * 100 matches pandas", () => {
      for (const [rowKeyStr, colVals] of Object.entries(g.pct_cells)) {
        const rowKey = rowKeyStr.split("|");
        for (const [colKeyStr, expectedPct] of Object.entries(
          colVals as Record<string, number>,
        )) {
          const colKey = colKeyStr.split("|");
          const colTotal = pd.getColTotal(colKey, "Revenue").value()!;
          const rawVal = pd.getAggregator(rowKey, colKey, "Revenue").value()!;
          const computedPct = (rawVal / colTotal) * 100;
          expectClose(
            computedPct,
            expectedPct,
            `pct_of_col [${rowKeyStr}][${colKeyStr}]`,
          );
        }
      }
    });

    it("pct_of_col values differ from pct_of_row values", () => {
      const gf2 = golden.F2;
      let foundDifference = false;
      for (const rowKeyStr of Object.keys(g.pct_cells)) {
        for (const colKeyStr of Object.keys(
          (g.pct_cells as Record<string, Record<string, number>>)[rowKeyStr]!,
        )) {
          const pctCol = (
            g.pct_cells as Record<string, Record<string, number>>
          )[rowKeyStr]![colKeyStr]!;
          const pctRow = (
            gf2.pct_cells as Record<string, Record<string, number>>
          )[rowKeyStr]?.[colKeyStr];
          if (pctRow !== undefined && Math.abs(pctCol - pctRow) > 0.01) {
            foundDifference = true;
          }
        }
      }
      expect(foundDifference).toBe(true);
    });
  });

  describe("Config G — Filtering", () => {
    const g = golden.G;
    const config = makeConfig({
      aggregation: "sum",
      filters: { Region: { include: ["North", "South"] } },
    });
    const { record, columnar } = makePair(smallRecords, config);

    it("record path matches pandas filtered values", () => {
      verifyAllValues(record, g, "Revenue", "record filtered");
    });

    it("columnar path matches pandas filtered values", () => {
      verifyAllValues(columnar, g, "Revenue", "columnar filtered");
    });

    it("only North and South rows are present", () => {
      const rowKeys = record.getRowKeys();
      expect(rowKeys.every((k) => ["North", "South"].includes(k[0]!))).toBe(
        true,
      );
    });
  });

  describe("Config H — Synthetic sum_over_sum", () => {
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
    });
    const { record, columnar } = makePair(smallRecords, config);

    it("record raw Revenue matches pandas", () => {
      for (const [region, expected] of Object.entries(
        g.raw_revenue as Record<string, number>,
      )) {
        const actual = record.getRowTotal([region], "Revenue").value();
        expectClose(actual, expected, `Revenue [${region}]`);
      }
    });

    it("record synthetic ratio matches pandas", () => {
      for (const [region, expected] of Object.entries(
        g.synthetic_ratios as Record<string, number>,
      )) {
        const actual = record
          .getAggregator([region], [], "rev_per_unit")
          .value();
        expect(actual).not.toBeNull();
        expect(Math.abs(actual! - expected)).toBeLessThan(0.01);
      }
    });

    it("record grand total ratio matches pandas", () => {
      const actual = record.getGrandTotal("rev_per_unit").value();
      expect(actual).not.toBeNull();
      expect(Math.abs(actual! - g.grand_total_ratio)).toBeLessThan(0.01);
    });

    it("columnar matches record for synthetic", () => {
      for (const rowKey of record.getRowKeys()) {
        expect(record.getAggregator(rowKey, [], "rev_per_unit").value()).toBe(
          columnar.getAggregator(rowKey, [], "rev_per_unit").value(),
        );
      }
    });
  });

  describe("Config H2 — Synthetic difference", () => {
    const g = golden.H2;
    const config = makeConfig({
      rows: ["Region"],
      columns: [],
      values: ["Revenue", "Profit"],
      aggregation: { Revenue: "sum", Profit: "sum" },
      synthetic_measures: [
        {
          id: "rev_minus_profit",
          label: "Rev-Profit",
          operation: "difference",
          numerator: "Revenue",
          denominator: "Profit",
        },
      ],
    });
    const { record, columnar } = makePair(smallRecords, config);

    it("record synthetic difference matches pandas", () => {
      for (const [region, expected] of Object.entries(
        g.synthetic_diffs as Record<string, number>,
      )) {
        const actual = record
          .getAggregator([region], [], "rev_minus_profit")
          .value();
        expectClose(actual, expected, `difference [${region}]`);
      }
    });

    it("record grand total difference matches pandas", () => {
      const actual = record.getGrandTotal("rev_minus_profit").value();
      expectClose(actual, g.grand_total_diff, "grand total difference");
    });

    it("columnar matches record for difference", () => {
      for (const rowKey of record.getRowKeys()) {
        expect(
          record.getAggregator(rowKey, [], "rev_minus_profit").value(),
        ).toBe(columnar.getAggregator(rowKey, [], "rev_minus_profit").value());
      }
    });
  });

  describe("Config I — Value sort desc", () => {
    const g = golden.I;
    const config = makeConfig({
      aggregation: "sum",
      row_sort: { by: "value", direction: "desc", value_field: "Revenue" },
    });
    const { record, columnar } = makePair(smallRecords, config);

    it("record row order matches pandas sort_values(ascending=False)", () => {
      const actualOrder = record.getRowKeys().map((k) => k[0]);
      expect(actualOrder).toEqual(g.expected_row_order);
    });

    it("columnar row order matches pandas sort_values(ascending=False)", () => {
      const actualOrder = columnar.getRowKeys().map((k) => k[0]);
      expect(actualOrder).toEqual(g.expected_row_order);
    });

    it("cell values match pandas (order-independent)", () => {
      verifyAllValues(record, g, "Revenue", "record sorted");
    });
  });
});
