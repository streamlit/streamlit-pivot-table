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
import { DataRecordSource } from "./parseArrow";
import {
  normalizeAggregationConfig,
  type AggregationType,
  type AggregationConfig,
  type PivotConfigV1,
} from "./types";

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

const RECORDS: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100, profit: 40 },
  { region: "US", year: "2024", revenue: 150, profit: 60 },
  { region: "EU", year: "2023", revenue: 200, profit: 80 },
];

describe("DataRecordSource (columnar adapter for in-memory records)", () => {
  const columns = ["region", "year", "revenue", "profit"] as const;
  const source = new DataRecordSource(RECORDS, [...columns]);

  it("getValue returns the correct value per row and field", () => {
    expect(source.getValue(0, "region")).toBe("US");
    expect(source.getValue(0, "year")).toBe("2023");
    expect(source.getValue(0, "revenue")).toBe(100);
    expect(source.getValue(2, "profit")).toBe(80);
  });

  it("getColumnNames returns all configured column names in order", () => {
    expect(source.getColumnNames()).toEqual([...columns]);
  });

  it("numRows matches the backing array length", () => {
    expect(source.numRows).toBe(3);
  });

  it("PivotData from DataRecord[] matches PivotData from DataRecordSource", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const fromArray = new PivotData(RECORDS, config);
    const fromSource = new PivotData(
      new DataRecordSource(RECORDS, Object.keys(RECORDS[0]!)),
      config,
    );

    expect(fromSource.getRowKeys()).toEqual(fromArray.getRowKeys());
    expect(fromSource.getColKeys()).toEqual(fromArray.getColKeys());

    for (const rowKey of fromArray.getRowKeys()) {
      for (const colKey of fromArray.getColKeys()) {
        for (const field of ["revenue", "profit"] as const) {
          expect(fromSource.getAggregator(rowKey, colKey, field).value()).toBe(
            fromArray.getAggregator(rowKey, colKey, field).value(),
          );
        }
      }
    }
    expect(fromSource.getGrandTotal("revenue").value()).toBe(
      fromArray.getGrandTotal("revenue").value(),
    );
    expect(fromSource.getGrandTotal("profit").value()).toBe(
      fromArray.getGrandTotal("profit").value(),
    );
  });
});
