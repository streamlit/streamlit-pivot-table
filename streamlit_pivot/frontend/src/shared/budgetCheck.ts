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

import { DEFAULT_BUDGETS } from "../engine/perf";

export interface BudgetResult {
  needsVirtualization: boolean;
  columnsTruncated: boolean;
  truncatedColumnCount: number;
  warnings: string[];
}

/**
 * Check whether the pivot dimensions exceed rendering budgets.
 * Returns whether virtualization is needed and any warning messages.
 */
export function checkRenderBudget(
  rowCount: number,
  colCount: number,
  valueCount: number,
): BudgetResult {
  const warnings: string[] = [];
  let needsVirtualization = false;
  let columnsTruncated = false;
  let truncatedColumnCount = colCount;

  if (colCount > DEFAULT_BUDGETS.maxColumnCardinality) {
    columnsTruncated = true;
    truncatedColumnCount = DEFAULT_BUDGETS.maxColumnCardinality;
    warnings.push(
      `Column cardinality (${colCount}) exceeds limit (${DEFAULT_BUDGETS.maxColumnCardinality}). ` +
        `Showing first ${DEFAULT_BUDGETS.maxColumnCardinality} columns.`,
    );
  }

  const effectiveCells =
    rowCount * truncatedColumnCount * Math.max(valueCount, 1);

  if (effectiveCells > DEFAULT_BUDGETS.maxVisibleCells) {
    needsVirtualization = true;
    warnings.push(
      `Total cells (${effectiveCells.toLocaleString()}) exceeds DOM budget ` +
        `(${DEFAULT_BUDGETS.maxVisibleCells.toLocaleString()}). Virtualization enabled.`,
    );
  }

  return {
    needsVirtualization,
    columnsTruncated,
    truncatedColumnCount,
    warnings,
  };
}
