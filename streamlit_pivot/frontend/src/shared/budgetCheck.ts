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

import {
  COLUMN_VIRTUALIZATION_THRESHOLD,
  DEFAULT_BUDGETS,
} from "../engine/perf";

export interface BudgetResult {
  needsVirtualization: boolean;
  /** True when column count exceeds the threshold where horizontal windowing is recommended. */
  needsColumnVirtualization: boolean;
  columnsTruncated: boolean;
  truncatedColumnCount: number;
  warnings: string[];
}

function exceedsCellBudget(
  rowCount: number,
  colCount: number,
  valueCount: number,
): boolean {
  return (
    rowCount * colCount * Math.max(valueCount, 1) >
    DEFAULT_BUDGETS.maxVisibleCells
  );
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
  const needsColumnVirtualization = colCount > COLUMN_VIRTUALIZATION_THRESHOLD;

  const maxColCap = DEFAULT_BUDGETS.maxColumnCardinality;
  const colsAfterHardCap = Math.min(colCount, maxColCap);
  const exceedsHardCap = colCount > maxColCap;

  const needsVirtualizationFromCells = exceedsCellBudget(
    rowCount,
    colsAfterHardCap,
    valueCount,
  );
  const needsVirtualization =
    needsVirtualizationFromCells || needsColumnVirtualization;

  let columnsTruncated = false;
  let truncatedColumnCount = colCount;

  if (needsVirtualization) {
    truncatedColumnCount = colsAfterHardCap;
    columnsTruncated = exceedsHardCap;

    if (exceedsHardCap) {
      warnings.push(
        `Column cardinality (${colCount}) exceeds limit (${maxColCap}). ` +
          `Showing first ${maxColCap} columns.`,
      );
    }

    if (needsVirtualizationFromCells) {
      warnings.push(
        `Total cells (${(rowCount * colsAfterHardCap * Math.max(valueCount, 1)).toLocaleString()}) exceeds DOM budget ` +
          `(${DEFAULT_BUDGETS.maxVisibleCells.toLocaleString()}). Virtualization enabled.`,
      );
    }
  } else {
    truncatedColumnCount = colCount;
    columnsTruncated = false;
  }

  return {
    needsVirtualization,
    needsColumnVirtualization,
    columnsTruncated,
    truncatedColumnCount,
    warnings,
  };
}
