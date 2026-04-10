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
import { checkRenderBudget } from "./budgetCheck";
import { DEFAULT_BUDGETS } from "../engine/perf";

describe("checkRenderBudget", () => {
  it("returns no warnings for small pivot", () => {
    const result = checkRenderBudget(10, 5, 1);
    expect(result.needsVirtualization).toBe(false);
    expect(result.columnsTruncated).toBe(false);
    expect(result.needsColumnVirtualization).toBe(false);
    expect(result.warnings).toHaveLength(0);
  });

  it("enables virtualization when cells exceed budget", () => {
    const rowCount = 1000;
    const colCount = 10;
    const result = checkRenderBudget(rowCount, colCount, 1);
    expect(rowCount * colCount).toBeGreaterThan(
      DEFAULT_BUDGETS.maxVisibleCells,
    );
    expect(result.needsVirtualization).toBe(true);
    expect(result.warnings.length).toBeGreaterThan(0);
  });

  it("truncates columns when cardinality exceeds limit", () => {
    const result = checkRenderBudget(10, 1500, 1);
    expect(result.columnsTruncated).toBe(true);
    expect(result.truncatedColumnCount).toBe(
      DEFAULT_BUDGETS.maxColumnCardinality,
    );
    expect(result.warnings.some((w) => w.includes("Column cardinality"))).toBe(
      true,
    );
  });

  it("does not truncate at 500 columns", () => {
    const result = checkRenderBudget(10, 500, 1);
    expect(result.columnsTruncated).toBe(false);
    expect(result.truncatedColumnCount).toBe(500);
    expect(result.needsColumnVirtualization).toBe(true);
  });

  it("accounts for multiple values in cell count", () => {
    const result = checkRenderBudget(100, 10, 5);
    expect(result.needsVirtualization).toBe(false);
    const result2 = checkRenderBudget(500, 10, 5);
    expect(500 * 10 * 5).toBeGreaterThan(DEFAULT_BUDGETS.maxVisibleCells);
    expect(result2.needsVirtualization).toBe(true);
  });
});
