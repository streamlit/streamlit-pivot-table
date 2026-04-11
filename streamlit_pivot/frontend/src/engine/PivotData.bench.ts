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
 * Performance benchmarks for the PivotData engine.
 *
 * Validates that pivot computation stays within the budget thresholds
 * defined in perf.ts for small, medium, and stress-test datasets.
 *
 * Run with: npx vitest bench --run
 */
import { bench, describe } from "vitest";
import { PivotData, type DataRecord } from "./PivotData";
import {
  normalizeAggregationConfig,
  type AggregationConfig,
  type AggregationType,
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

function generateRecords(
  numRows: number,
  numRegions: number,
  numYears: number,
  numValues: number = 1,
): { records: DataRecord[]; config: PivotConfigV1 } {
  const records: DataRecord[] = [];
  const regions = Array.from({ length: numRegions }, (_, i) => `Region_${i}`);
  const years = Array.from({ length: numYears }, (_, i) => `${2000 + i}`);
  const valueFields = Array.from({ length: numValues }, (_, i) => `val_${i}`);

  for (let i = 0; i < numRows; i++) {
    const record: DataRecord = {
      region: regions[i % numRegions],
      year: years[i % numYears],
    };
    for (const vf of valueFields) {
      record[vf] = Math.random() * 10000;
    }
    records.push(record);
  }

  return {
    records,
    config: makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: valueFields,
    }),
  };
}

describe("PivotData computation benchmarks", () => {
  // Small: 1,000 rows, 10 regions, 5 years -> 50 unique cells
  const small = generateRecords(1000, 10, 5);
  bench("small dataset (1K rows, 10x5 grid)", () => {
    new PivotData(small.records, small.config);
  });

  // Medium: 50,000 rows, 100 regions, 20 years -> 2,000 unique cells
  const medium = generateRecords(50000, 100, 20);
  bench("medium dataset (50K rows, 100x20 grid)", () => {
    new PivotData(medium.records, medium.config);
  });

  // Multi-value medium: 50,000 rows, 50 regions, 10 years, 3 values
  const multiVal = generateRecords(50000, 50, 10, 3);
  bench("multi-value medium (50K rows, 50x10x3 grid)", () => {
    new PivotData(multiVal.records, multiVal.config);
  });

  // Large: 200,000 rows, 500 regions, 20 years -> 10,000 unique cells
  const large = generateRecords(200000, 500, 20);
  bench("large dataset (200K rows, 500x20 grid)", () => {
    new PivotData(large.records, large.config);
  });

  // Wide columns: 50,000 rows, 20 regions, 1,000 years -> 20,000 unique cells
  const wide = generateRecords(50000, 20, 1000);
  bench("wide columns (50K rows, 20x1000 grid)", () => {
    new PivotData(wide.records, wide.config);
  });
});
