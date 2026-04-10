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
 * Performance instrumentation and budget enforcement.
 *
 * Used by the pivot engine and renderers to measure timing and warn when
 * approaching budget limits.
 */

export interface PerfMetrics {
  pivotComputeMs: number;
  renderMs: number;
  totalRows: number;
  totalCols: number;
  totalCells: number;
}

export type PerfActionKind = "initial_mount" | "sort" | "filter" | "drilldown";

export interface PerfActionMeasurement {
  kind: PerfActionKind;
  elapsedMs: number;
  axis?: "row" | "col";
  field?: string;
  totalCount?: number;
}

export interface PivotPerfMetrics extends PerfMetrics {
  parseMs: number;
  sourceRows: number;
  sourceCols: number;
  firstMountMs: number;
  executionMode: "client_only" | "threshold_hybrid";
  needsVirtualization: boolean;
  columnsTruncated: boolean;
  truncatedColumnCount?: number;
  warnings: string[];
  lastAction?: PerfActionMeasurement;
}

export interface PerfBudgets {
  /** Max compute time in ms before warning (medium dataset target) */
  maxComputeMs: number;
  /** Max render time in ms before warning */
  maxRenderMs: number;
  /** Max visible cells before virtualization activates */
  maxVisibleCells: number;
  /** Max unique column values before truncation (wide-column mode cap) */
  maxColumnCardinality: number;
}

/** Column count above which horizontal windowing is preferred (legacy cap when wideColumnMode is off). */
export const COLUMN_VIRTUALIZATION_THRESHOLD = 200;

/** Hard cap when {@link FEATURE_FLAGS.wideColumnMode} is false (backward compatible). */
export const LEGACY_MAX_COLUMN_CARDINALITY = 200;

export const FEATURE_FLAGS = {
  /** When false, column cardinality uses {@link LEGACY_MAX_COLUMN_CARDINALITY} (200). */
  wideColumnMode: true,
};

export const DEFAULT_BUDGETS: PerfBudgets = {
  maxComputeMs: 500,
  maxRenderMs: 200,
  maxVisibleCells: 5000,
  maxColumnCardinality: 1000,
};

/**
 * Measure execution time of a synchronous function.
 */
export function measureSync<T>(fn: () => T): { result: T; elapsedMs: number } {
  const start = performance.now();
  const result = fn();
  const elapsedMs = performance.now() - start;
  return { result, elapsedMs };
}

/**
 * Check metrics against budgets and return warnings.
 */
export function checkBudgets(
  metrics: PerfMetrics,
  budgets: PerfBudgets = DEFAULT_BUDGETS,
): string[] {
  const warnings: string[] = [];

  if (metrics.pivotComputeMs > budgets.maxComputeMs) {
    warnings.push(
      `Pivot computation took ${metrics.pivotComputeMs.toFixed(0)}ms ` +
        `(budget: ${budgets.maxComputeMs}ms). Consider reducing data size.`,
    );
  }

  if (metrics.renderMs > budgets.maxRenderMs) {
    warnings.push(
      `Render took ${metrics.renderMs.toFixed(0)}ms ` +
        `(budget: ${budgets.maxRenderMs}ms).`,
    );
  }

  if (metrics.totalCells > budgets.maxVisibleCells) {
    warnings.push(
      `${metrics.totalCells.toLocaleString()} cells exceed DOM budget ` +
        `(${budgets.maxVisibleCells.toLocaleString()}). Virtualization recommended.`,
    );
  }

  if (metrics.totalCols > budgets.maxColumnCardinality) {
    warnings.push(
      `${metrics.totalCols} column values exceed cardinality cap ` +
        `(${budgets.maxColumnCardinality}). Showing top ${budgets.maxColumnCardinality}.`,
    );
  }

  return warnings;
}

/**
 * Log metrics to console in debug mode.
 */
export function logMetrics(
  metrics: PerfMetrics | PivotPerfMetrics,
  label = "PivotTable",
): void {
  if (process.env.NODE_ENV !== "production") {
    const extras =
      "parseMs" in metrics
        ? ` parse=${metrics.parseMs.toFixed(1)}ms mode=${metrics.executionMode}`
        : "";
    const action =
      "lastAction" in metrics && metrics.lastAction
        ? ` action=${metrics.lastAction.kind}:${metrics.lastAction.elapsedMs.toFixed(1)}ms`
        : "";
    console.log(
      `[${label}]${extras} compute=${metrics.pivotComputeMs.toFixed(1)}ms ` +
        `render=${metrics.renderMs.toFixed(1)}ms ` +
        `rows=${metrics.totalRows} cols=${metrics.totalCols} ` +
        `cells=${metrics.totalCells}${action}`,
    );
  }
}
