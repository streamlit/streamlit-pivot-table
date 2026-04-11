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
 * Scaled correctness + path parity tests.
 *
 * Verifies PivotData correctness at production-scale data sizes (10K, 200K rows)
 * using pandas-computed golden reference values. Also verifies record/columnar
 * path parity at scale and performance budgets.
 */

import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { PivotData, type DataRecord } from "./PivotData";
import {
  normalizeAggregationConfig,
  type AggregationConfig,
  type AggregationType,
  type ColumnarDataSource,
  type PivotConfigV1,
} from "./types";
import { measureSync, DEFAULT_BUDGETS } from "./perf";

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
const scaled = golden.scaled ?? {};

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

function verifyAllValues(
  pd: PivotData,
  g: {
    cells: Record<string, Record<string, number>>;
    row_totals: Record<string, number>;
    col_totals?: Record<string, number>;
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
  if (g.col_totals) {
    for (const [colKeyStr, expected] of Object.entries(g.col_totals)) {
      const colKey = colKeyStr.split("|");
      const actual = pd.getColTotal(colKey, valField).value();
      expectClose(actual, expected, `${label} col total [${colKeyStr}]`);
    }
  }
  const actualGrand = pd.getGrandTotal(valField).value();
  expectClose(actualGrand, g.grand_total, `${label} grand total`);
}

// ---------------------------------------------------------------------------
// Medium dataset (10K rows)
// ---------------------------------------------------------------------------

describe("Scaled Correctness — Medium (10K rows)", () => {
  const mediumRecords = loadCSV("medium.csv");
  const skip = mediumRecords.length === 0;

  it.skipIf(skip)("A_medium — basic sum matches pandas", () => {
    const g = scaled.A_medium;
    const config = makeConfig({ aggregation: "sum" });
    const pdRec = new PivotData(mediumRecords, config);
    const pdCol = new PivotData(new TestColumnarSource(mediumRecords), config);

    verifyAllValues(pdRec, g, "Revenue", "record 10K");
    verifyAllValues(pdCol, g, "Revenue", "columnar 10K");
  });

  it.skipIf(skip)("C_medium — per-measure agg matches pandas", () => {
    const g = scaled.C_medium;
    const config = makeConfig({
      values: ["Revenue", "Units"],
      aggregation: { Revenue: "sum", Units: "avg" },
    });
    const pdRec = new PivotData(mediumRecords, config);
    verifyAllValues(pdRec, g.measures.Revenue, "Revenue", "record 10K Revenue");
    verifyAllValues(pdRec, g.measures.Units, "Units", "record 10K Units");
  });

  it.skipIf(skip)("E_medium — subtotals match pandas", () => {
    const g = scaled.E_medium;
    const config = makeConfig({
      rows: ["Region", "Category"],
      aggregation: "sum",
      show_subtotals: true,
    });
    const pdRec = new PivotData(mediumRecords, config);

    const subtotals = g.subtotals.by_region;
    for (const [regionStr, colVals] of Object.entries(subtotals.cells)) {
      const parentKey = regionStr.split("|");
      for (const [colKeyStr, expected] of Object.entries(
        colVals as Record<string, number>,
      )) {
        const colKey = colKeyStr.split("|");
        const actual = pdRec
          .getSubtotalAggregator(parentKey, colKey, "Revenue")
          .value();
        expectClose(
          actual,
          expected,
          `subtotal 10K [${regionStr}][${colKeyStr}]`,
        );
      }
    }
  });

  it.skipIf(skip)("record and columnar paths identical at 10K rows", () => {
    const config = makeConfig({ aggregation: "sum" });
    const pdRec = new PivotData(mediumRecords, config);
    const pdCol = new PivotData(new TestColumnarSource(mediumRecords), config);

    for (const rowKey of pdRec.getRowKeys()) {
      for (const colKey of pdRec.getColKeys()) {
        const recVal = pdRec.getAggregator(rowKey, colKey, "Revenue").value();
        const colVal = pdCol.getAggregator(rowKey, colKey, "Revenue").value();
        expect(recVal).toBe(colVal);
      }
      expect(pdRec.getRowTotal(rowKey, "Revenue").value()).toBe(
        pdCol.getRowTotal(rowKey, "Revenue").value(),
      );
    }
    expect(pdRec.getGrandTotal("Revenue").value()).toBe(
      pdCol.getGrandTotal("Revenue").value(),
    );
  });

  it.skipIf(skip)("10K compute ≤ 100ms", () => {
    const config = makeConfig({ aggregation: "sum" });
    const { elapsedMs } = measureSync(
      () => new PivotData(mediumRecords, config),
    );
    expect(elapsedMs).toBeLessThan(100);
  });
});

// ---------------------------------------------------------------------------
// Large dataset (200K rows)
// ---------------------------------------------------------------------------

describe("Scaled Correctness — Large (200K rows)", () => {
  const largeRecords = loadCSV("large.csv");
  const skip = largeRecords.length === 0;

  it.skipIf(skip)("A_large — basic sum matches pandas", () => {
    const g = scaled.A_large;
    const config = makeConfig({ aggregation: "sum" });
    const pdRec = new PivotData(largeRecords, config);
    const pdCol = new PivotData(new TestColumnarSource(largeRecords), config);

    verifyAllValues(pdRec, g, "Revenue", "record 200K");
    verifyAllValues(pdCol, g, "Revenue", "columnar 200K");
  });

  it.skipIf(skip)("E_large — subtotals match pandas at 200K", () => {
    const g = scaled.E_large;
    const config = makeConfig({
      rows: ["Region", "Category"],
      aggregation: "sum",
      show_subtotals: true,
    });
    const pdRec = new PivotData(largeRecords, config);

    const subtotals = g.subtotals.by_region;
    for (const [regionStr, colVals] of Object.entries(subtotals.cells)) {
      const parentKey = regionStr.split("|");
      for (const [colKeyStr, expected] of Object.entries(
        colVals as Record<string, number>,
      )) {
        const colKey = colKeyStr.split("|");
        const actual = pdRec
          .getSubtotalAggregator(parentKey, colKey, "Revenue")
          .value();
        expectClose(
          actual,
          expected,
          `subtotal 200K [${regionStr}][${colKeyStr}]`,
        );
      }
    }
  });

  it.skipIf(skip)("record and columnar paths identical at 200K rows", () => {
    const config = makeConfig({ aggregation: "sum" });
    const pdRec = new PivotData(largeRecords, config);
    const pdCol = new PivotData(new TestColumnarSource(largeRecords), config);

    for (const rowKey of pdRec.getRowKeys()) {
      for (const colKey of pdRec.getColKeys()) {
        const recVal = pdRec.getAggregator(rowKey, colKey, "Revenue").value();
        const colVal = pdCol.getAggregator(rowKey, colKey, "Revenue").value();
        expect(recVal).toBe(colVal);
      }
    }
    expect(pdRec.getGrandTotal("Revenue").value()).toBe(
      pdCol.getGrandTotal("Revenue").value(),
    );
  });

  it.skipIf(skip)("200K compute ≤ maxComputeMs budget", () => {
    const config = makeConfig({ aggregation: "sum" });
    const { elapsedMs } = measureSync(
      () => new PivotData(largeRecords, config),
    );
    // CI runners are ~2x slower than dev machines; apply headroom multiplier
    // so the test validates the right order-of-magnitude without flaking.
    const CI_HEADROOM = 2;
    expect(elapsedMs).toBeLessThan(DEFAULT_BUDGETS.maxComputeMs * CI_HEADROOM);
  });

  it.skipIf(skip)(
    "TestColumnarSource creation at 200K completes in reasonable time",
    () => {
      const { elapsedMs } = measureSync(
        () => new TestColumnarSource(largeRecords),
      );
      // TestColumnarSource copies data to column arrays — real ArrowDataSource is ~0ms.
      // This just ensures no pathological slowdown in the test utility itself.
      expect(elapsedMs).toBeLessThan(2000);
    },
  );
});
