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
import { PivotData, type DataRecord } from "../engine/PivotData";
import {
  normalizeAggregationConfig,
  type PivotConfigV1,
} from "../engine/types";
import {
  COLUMN_VIRTUALIZATION_THRESHOLD,
  DEFAULT_BUDGETS,
  FEATURE_FLAGS,
} from "../engine/perf";
import { checkRenderBudget } from "./budgetCheck";

type TestConfigOverrides = Partial<Omit<PivotConfigV1, "aggregation">> & {
  aggregation?: unknown;
};

function makeConfig(overrides: TestConfigOverrides = {}): PivotConfigV1 {
  const { aggregation: aggregationOverride, ...restOverrides } = overrides;
  const values = overrides.values ?? ["v"];
  const config = {
    version: 1 as const,
    rows: ["r"],
    columns: ["c"],
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

function wideRecords(numCols: number): DataRecord[] {
  return Array.from({ length: numCols }, (_, i) => ({
    r: "only-row",
    c: `col-${i}`,
    v: i,
  }));
}

describe("wide column mode (S1) + virtualization budgets", () => {
  it("returns every column key for 500+ unique column values (engine not truncated)", () => {
    const n = 520;
    const pd = new PivotData(
      wideRecords(n),
      makeConfig({ aggregation: "sum" }),
    );
    expect(pd.uniqueColKeyCount).toBe(n);
    expect(pd.getColKeys()).toHaveLength(n);
  });

  it("PivotData keeps full 1001 column keys; render budget caps display at 1000 when wideColumnMode", () => {
    expect(FEATURE_FLAGS.wideColumnMode).toBe(true);
    const n = 1001;
    const pd = new PivotData(
      wideRecords(n),
      makeConfig({ aggregation: "sum" }),
    );
    expect(pd.uniqueColKeyCount).toBe(n);
    expect(pd.getColKeys()).toHaveLength(n);

    const budget = checkRenderBudget(
      pd.uniqueRowKeyCount,
      pd.uniqueColKeyCount,
      pd.config.values.length,
    );
    expect(budget.columnsTruncated).toBe(true);
    expect(budget.truncatedColumnCount).toBe(
      DEFAULT_BUDGETS.maxColumnCardinality,
    );
    expect(DEFAULT_BUDGETS.maxColumnCardinality).toBe(1000);
  });

  it("FEATURE_FLAGS.wideColumnMode is enabled", () => {
    expect(FEATURE_FLAGS.wideColumnMode).toBe(true);
  });

  it("checkRenderBudget enables virtualization when column count exceeds COLUMN_VIRTUALIZATION_THRESHOLD", () => {
    const colCount = COLUMN_VIRTUALIZATION_THRESHOLD + 1;
    const result = checkRenderBudget(5, colCount, 1);
    expect(result.needsColumnVirtualization).toBe(true);
    expect(result.needsVirtualization).toBe(true);
  });

  it("checkRenderBudget flags cell-based virtualization when cells exceed budget", () => {
    const result = checkRenderBudget(600, 10, 1);
    expect(600 * 10).toBeGreaterThan(DEFAULT_BUDGETS.maxVisibleCells);
    expect(result.needsVirtualization).toBe(true);
    expect(result.warnings.some((w) => w.includes("DOM budget"))).toBe(true);
  });
});
