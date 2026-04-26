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
 * Shared transform layer for 0.5.0 "Show Values As" analytical modes.
 *
 * All functions are **pure** — they take a `PivotData` instance (read-only)
 * and return a value or a Map. Results are cached on the first call per
 * (pivotData, colKey, valField) combination via a module-level WeakMap, so
 * repeated calls from the renderer are O(1) after the first call.
 *
 * ## Mode semantics
 *
 * ### `running_total`
 * Accumulates raw values along the row axis in display order (the order
 * returned by `pivotData.getSortedLeafRowKeys()`). Resets at each distinct
 * parent group — rows sharing the same `rowKey.slice(0, -1)` parent prefix
 * form one group. For a single-level pivot (no parent), the running total
 * accumulates across all rows without reset.
 * - Total/subtotal rows: display the raw aggregate (not a running total).
 * - Column axis running totals: out of scope for 0.5.0.
 *
 * ### `pct_running_total`
 * Running total divided by the parent-group total for the same column.
 * - Denominator: `getParentSubtotal(rowKey, colKey, valField)`, which for a
 *   top-level row equals `getColTotal(colKey, valField)`.
 * - For a single-level pivot (no parent group), the denominator is the
 *   column grand total.
 * - Zero/null denominator → returns `null` (displayed as `empty_cell_value`).
 * - Total rows: display the raw aggregate (not a percentage — 100% would be
 *   misleading and the running total equals the subtotal anyway).
 *
 * ### `rank`
 * Competition rank (`RANK.EQ` in Excel: ties share the same rank, next rank
 * skips — 1, 1, 3, not 1, 1, 2). Rank resets per column; each column gets
 * independent 1-N ranks within each parent group (same grouping as
 * `running_total`). Null values are excluded from ranking and receive
 * `null` instead of a numeric rank.
 *
 * ### `pct_of_parent`
 * For a leaf cell, denominator is the immediate parent subtotal for the same
 * column (`getParentSubtotal`). For a subtotal cell, denominator is its own
 * parent (one level up). For grand total, returns `null`. For column-only
 * pivots (no row subtotals), denominator is the column grand total.
 *
 * ### `index`
 * `(cellValue / grandTotal) / ((rowTotal / grandTotal) × (colTotal / grandTotal))`
 * Simplifies to `cellValue × grandTotal / (rowTotal × colTotal)`. Returns
 * `null` when any denominator is zero or null.
 */

import type { PivotData } from "./PivotData";

// ---------------------------------------------------------------------------
// Internal serialisation helpers
// ---------------------------------------------------------------------------

function serializeKey(key: string[]): string {
  return key.join("\x01");
}

// ---------------------------------------------------------------------------
// Module-level WeakMap cache keyed on PivotData instance
// ---------------------------------------------------------------------------

type CacheKey = string; // `${colKeyStr}\x02${valField}`
type RunningTotalMap = Map<string, number | null>; // rowKeyStr → value
type RankMap = Map<string, number | null>; // rowKeyStr → rank (null for null-valued rows)

const _runningTotalCache = new WeakMap<
  PivotData,
  Map<CacheKey, RunningTotalMap>
>();
const _rankCache = new WeakMap<PivotData, Map<CacheKey, RankMap>>();

function getOrCreate<V>(
  weakMap: WeakMap<PivotData, Map<CacheKey, V>>,
  pd: PivotData,
  cacheKey: CacheKey,
  compute: () => V,
): V {
  let byField = weakMap.get(pd);
  if (!byField) {
    byField = new Map();
    weakMap.set(pd, byField);
  }
  if (!byField.has(cacheKey)) {
    byField.set(cacheKey, compute());
  }
  return byField.get(cacheKey)!;
}

// ---------------------------------------------------------------------------
// Running total
// ---------------------------------------------------------------------------

/**
 * Compute running totals for all sorted leaf rows at the given column.
 * Returns a Map from serialized rowKey → cumulative sum (or null if the
 * raw value for that cell is null).
 */
export function computeRunningTotals(
  pivotData: PivotData,
  colKey: string[],
  valField: string,
): RunningTotalMap {
  const cacheKey: CacheKey = `${serializeKey(colKey)}\x02${valField}`;
  return getOrCreate(_runningTotalCache, pivotData, cacheKey, () => {
    const rowKeys = pivotData.getSortedLeafRowKeys();
    const map: RunningTotalMap = new Map();
    // Group by parent prefix (rowKey.slice(0, -1))
    let currentParent: string | null = null;
    let accumulator: number = 0;

    for (const rowKey of rowKeys) {
      const parentStr = serializeKey(rowKey.slice(0, -1));
      if (parentStr !== currentParent) {
        // New parent group — reset accumulator
        currentParent = parentStr;
        accumulator = 0;
      }
      const rawValue = pivotData
        .getAggregator(rowKey, colKey, valField)
        .value();
      const rowKeyStr = serializeKey(rowKey);
      if (rawValue === null) {
        map.set(rowKeyStr, null);
      } else {
        accumulator += rawValue;
        map.set(rowKeyStr, accumulator);
      }
    }
    return map;
  });
}

/**
 * Lookup the running total for a single cell. Returns `null` if the raw
 * value is null or the rowKey is not a leaf key.
 */
export function getRunningTotal(
  pivotData: PivotData,
  rowKey: string[],
  colKey: string[],
  valField: string,
): number | null {
  const map = computeRunningTotals(pivotData, colKey, valField);
  return map.get(serializeKey(rowKey)) ?? null;
}

// ---------------------------------------------------------------------------
// % Running total
// ---------------------------------------------------------------------------

/**
 * Return `running_total / parent_group_total` for a single leaf cell.
 *
 * Denominator is `pivotData.getParentSubtotal(rowKey, colKey, valField)`.
 * Returns `null` on null raw value, null running total, or zero/null denom.
 */
export function getPctRunningTotal(
  pivotData: PivotData,
  rowKey: string[],
  colKey: string[],
  valField: string,
): number | null {
  const rt = getRunningTotal(pivotData, rowKey, colKey, valField);
  if (rt === null) return null;
  const denom = pivotData.getParentSubtotal(rowKey, colKey, valField);
  if (denom === null || denom === 0) return null;
  return rt / denom;
}

// ---------------------------------------------------------------------------
// Rank
// ---------------------------------------------------------------------------

/**
 * Compute competition ranks for all sorted leaf rows at the given column.
 * Returns a Map from serialized rowKey → rank (null for null-valued rows).
 * Rank resets per parent group (same grouping as running_total).
 * Competition rank: ties share the lowest rank, next rank skips (1, 1, 3).
 */
export function computeRanks(
  pivotData: PivotData,
  colKey: string[],
  valField: string,
): RankMap {
  const cacheKey: CacheKey = `${serializeKey(colKey)}\x02rank:${valField}`;
  return getOrCreate(_rankCache, pivotData, cacheKey, () => {
    const rowKeys = pivotData.getSortedLeafRowKeys();
    const map: RankMap = new Map();

    // Group rows by parent prefix
    const groups = new Map<string, string[][]>();
    for (const rowKey of rowKeys) {
      const parentStr = serializeKey(rowKey.slice(0, -1));
      if (!groups.has(parentStr)) groups.set(parentStr, []);
      groups.get(parentStr)!.push(rowKey);
    }

    for (const groupRowKeys of groups.values()) {
      // Collect (rowKey, value) pairs, excluding null values
      const pairs: Array<{ rowKey: string[]; value: number }> = [];
      for (const rowKey of groupRowKeys) {
        const v = pivotData.getAggregator(rowKey, colKey, valField).value();
        if (v !== null) {
          pairs.push({ rowKey, value: v });
        } else {
          map.set(serializeKey(rowKey), null);
        }
      }

      // Sort by value descending (largest = rank 1)
      pairs.sort((a, b) => b.value - a.value);

      // Assign competition ranks (1, 1, 3, ...)
      let rank = 1;
      for (let i = 0; i < pairs.length; i++) {
        if (i > 0 && pairs[i]!.value !== pairs[i - 1]!.value) {
          rank = i + 1;
        }
        map.set(serializeKey(pairs[i]!.rowKey), rank);
      }
    }

    return map;
  });
}

/**
 * Lookup the rank for a single cell. Returns `null` for null raw values.
 */
export function getRank(
  pivotData: PivotData,
  rowKey: string[],
  colKey: string[],
  valField: string,
): number | null {
  const map = computeRanks(pivotData, colKey, valField);
  return map.get(serializeKey(rowKey)) ?? null;
}

// ---------------------------------------------------------------------------
// % of parent
// ---------------------------------------------------------------------------

/**
 * Return `rawValue / parentSubtotal` for a leaf cell, or
 * `rawValue / grandparentSubtotal` for a subtotal cell.
 *
 * - For leaf rows: denominator = `getParentSubtotal(rowKey, colKey, valField)`
 * - For subtotal rows (where rowKey is the parent prefix of a group):
 *   denominator = `getParentSubtotal(rowKey, colKey, valField)` (its parent)
 * - Returns `null` when rawValue is null, or denominator is null/zero.
 */
export function getPctOfParent(
  rawValue: number | null,
  pivotData: PivotData,
  rowKey: string[],
  colKey: string[],
  valField: string,
): number | null {
  if (rawValue === null) return null;
  const denom = pivotData.getParentSubtotal(rowKey, colKey, valField);
  if (denom === null || denom === 0) return null;
  return rawValue / denom;
}

// ---------------------------------------------------------------------------
// Index
// ---------------------------------------------------------------------------

/**
 * Excel's INDEX formula:
 * `(cell / grandTotal) / ((rowTotal / grandTotal) × (colTotal / grandTotal))`
 * Simplifies to: `cell × grandTotal / (rowTotal × colTotal)`.
 *
 * Returns `null` when rawValue is null, or any denominator is zero/null.
 */
export function getIndex(
  rawValue: number | null,
  pivotData: PivotData,
  rowKey: string[],
  colKey: string[],
  valField: string,
): number | null {
  if (rawValue === null) return null;
  const grandTotal = pivotData.getGrandTotal(valField).value();
  const rowTotal = pivotData.getRowTotal(rowKey, valField).value();
  const colTotal = pivotData.getColTotal(colKey, valField).value();
  if (!grandTotal || !rowTotal || !colTotal) return null;
  return (rawValue * grandTotal) / (rowTotal * colTotal);
}
