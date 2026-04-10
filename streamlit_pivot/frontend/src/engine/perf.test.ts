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
  checkBudgets,
  DEFAULT_BUDGETS,
  measureSync,
  type PerfMetrics,
} from "./perf";

describe("measureSync", () => {
  it("returns the function result and elapsed time", () => {
    const { result, elapsedMs } = measureSync(() => 42);
    expect(result).toBe(42);
    expect(elapsedMs).toBeGreaterThanOrEqual(0);
    expect(elapsedMs).toBeLessThan(100);
  });
});

describe("DEFAULT_BUDGETS", () => {
  it("matches expected values", () => {
    expect(DEFAULT_BUDGETS).toMatchInlineSnapshot(`
      {
        "maxColumnCardinality": 1000,
        "maxComputeMs": 500,
        "maxRenderMs": 200,
        "maxVisibleCells": 5000,
      }
    `);
  });
});

describe("checkBudgets", () => {
  const okMetrics: PerfMetrics = {
    pivotComputeMs: 50,
    renderMs: 30,
    totalRows: 100,
    totalCols: 10,
    totalCells: 1000,
  };

  it("returns no warnings when within budget", () => {
    expect(checkBudgets(okMetrics)).toEqual([]);
  });

  it("warns when compute time exceeds budget", () => {
    const warnings = checkBudgets({ ...okMetrics, pivotComputeMs: 600 });
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("Pivot computation took");
    expect(warnings[0]).toContain("600");
  });

  it("warns when render time exceeds budget", () => {
    const warnings = checkBudgets({ ...okMetrics, renderMs: 300 });
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("Render took");
  });

  it("warns when cell count exceeds DOM budget", () => {
    const warnings = checkBudgets({ ...okMetrics, totalCells: 6000 });
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("cells exceed DOM budget");
  });

  it("warns when column cardinality exceeds cap", () => {
    const warnings = checkBudgets({ ...okMetrics, totalCols: 1200 });
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain("column values exceed cardinality cap");
  });

  it("returns multiple warnings when multiple budgets exceeded", () => {
    const badMetrics: PerfMetrics = {
      pivotComputeMs: 800,
      renderMs: 400,
      totalRows: 5000,
      totalCols: 1200,
      totalCells: 10000,
    };
    const warnings = checkBudgets(badMetrics);
    expect(warnings).toHaveLength(4);
  });
});
