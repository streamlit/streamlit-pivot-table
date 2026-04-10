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
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Export grid values vs pandas golden_expected.json (Config A, F).
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
import { buildExportGrid, gridToCSV, gridToTSV } from "./exportData";

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
) as {
  A: {
    cells: Record<string, Record<string, number>>;
    row_totals: Record<string, number>;
    col_totals: Record<string, number>;
    grand_total: number;
  };
  F: {
    pct_cells: Record<string, Record<string, number>>;
    pct_row_totals: Record<string, number>;
    pct_col_totals: Record<string, number>;
  };
};

const smallRecords = loadCSV("small.csv");

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

function expectClose(actual: number, expected: number, label: string) {
  expect(actual, label).toBeCloseTo(expected, 1);
}

/** Parse raw export grid and assert data + totals match golden A. */
function expectRawGridMatchesGoldenA(
  grid: string[][],
  pd: PivotData,
  g: typeof golden.A,
) {
  const rowKeys = pd.getRowKeys();
  const colKeys = pd.getColKeys();
  const numRowDimCols = Math.max(pd.config.rows.length, 1);

  let dataRowIdx = 1;
  for (const rk of rowKeys) {
    const row = grid[dataRowIdx++]!;
    const region = rk[0]!;
    for (let ci = 0; ci < colKeys.length; ci++) {
      const year = colKeys[ci]![0]!;
      const expected = g.cells[region]![year]!;
      const actual = parseFloat(row[numRowDimCols + ci]!);
      expectClose(actual, expected, `cell ${region}/${year}`);
    }
    const expectedRt = g.row_totals[region]!;
    const actualRt = parseFloat(row[numRowDimCols + colKeys.length]!);
    expectClose(actualRt, expectedRt, `row total ${region}`);
  }

  const totalRow = grid[grid.length - 1]!;
  for (let ci = 0; ci < colKeys.length; ci++) {
    const year = colKeys[ci]![0]!;
    const expected = g.col_totals[year]!;
    const actual = parseFloat(totalRow[numRowDimCols + ci]!);
    expectClose(actual, expected, `col total ${year}`);
  }
  const expectedGrand = g.grand_total;
  const actualGrand = parseFloat(totalRow[numRowDimCols + colKeys.length]!);
  expectClose(actualGrand, expectedGrand, "grand total");
}

/** Strip % and locale grouping; return displayed percent as a number (e.g. 6.3 for 6.3%). */
function parseDisplayedPercent(cell: string): number {
  const cleaned = cell.replace(/%/g, "").replace(/,/g, "").trim();
  return parseFloat(cleaned);
}

describe("exportData — golden value verification", () => {
  it("raw export grid matches golden expected values for Config A", () => {
    const config = makeConfig({ aggregation: "sum" });
    const pd = new PivotData(smallRecords, config);
    const grid = buildExportGrid(pd, config, "raw");
    expectRawGridMatchesGoldenA(grid, pd, golden.A);

    const csv = gridToCSV(grid);
    const reparsed = csv
      .trim()
      .split("\n")
      .map((line) => line.split(","));
    expect(reparsed).toEqual(grid);
    expectRawGridMatchesGoldenA(reparsed, pd, golden.A);
  });

  it("TSV export contains the same numeric values as CSV export", () => {
    const config = makeConfig({ aggregation: "sum" });
    const pd = new PivotData(smallRecords, config);
    const grid = buildExportGrid(pd, config, "raw");
    const csv = gridToCSV(grid);
    const tsv = gridToTSV(grid);

    const csvRows = csv
      .trim()
      .split("\n")
      .map((line) => line.split(","));
    const tsvRows = tsv
      .trim()
      .split("\n")
      .map((line) => line.split("\t"));
    expect(tsvRows.length).toBe(csvRows.length);
    for (let r = 0; r < csvRows.length; r++) {
      expect(tsvRows[r]!.length).toBe(csvRows[r]!.length);
      for (let c = 0; c < csvRows[r]!.length; c++) {
        const a = csvRows[r]![c]!;
        const b = tsvRows[r]![c]!;
        const na = parseFloat(a);
        const nb = parseFloat(b);
        if (Number.isFinite(na) && Number.isFinite(nb)) {
          expect(nb).toBe(na);
        } else {
          expect(b).toBe(a);
        }
      }
    }
  });

  it("formatted export with show_values_as pct_of_total contains percentage values", () => {
    const g = golden.F;
    const config = makeConfig({
      aggregation: "sum",
      show_values_as: { Revenue: "pct_of_total" },
    });
    const pd = new PivotData(smallRecords, config);
    const grid = buildExportGrid(pd, config, "formatted");

    const rowKeys = pd.getRowKeys();
    const colKeys = pd.getColKeys();
    const numRowDimCols = Math.max(config.rows.length, 1);

    let dataRowIdx = 1;
    for (const rk of rowKeys) {
      const row = grid[dataRowIdx++]!;
      const region = rk[0]!;
      for (let ci = 0; ci < colKeys.length; ci++) {
        const cell = row[numRowDimCols + ci]!;
        expect(cell).toMatch(/%/);
        const year = colKeys[ci]![0]!;
        const expectedPct = g.pct_cells[region]![year]!;
        expect(parseDisplayedPercent(cell)).toBeCloseTo(expectedPct, 0);
      }
      const rtCell = row[numRowDimCols + colKeys.length]!;
      expect(rtCell).toMatch(/%/);
      expect(parseDisplayedPercent(rtCell)).toBeCloseTo(
        g.pct_row_totals[region]!,
        0,
      );
    }

    const totalRow = grid[grid.length - 1]!;
    for (let ci = 0; ci < colKeys.length; ci++) {
      const cell = totalRow[numRowDimCols + ci]!;
      expect(cell).toMatch(/%/);
      const year = colKeys[ci]![0]!;
      expect(parseDisplayedPercent(cell)).toBeCloseTo(
        g.pct_col_totals[year]!,
        0,
      );
    }
    const grandCell = totalRow[numRowDimCols + colKeys.length]!;
    expect(grandCell).toMatch(/100/);
    expect(grandCell).toMatch(/%/);
  });
});
