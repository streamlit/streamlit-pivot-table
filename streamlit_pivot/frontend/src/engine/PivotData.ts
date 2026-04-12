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
  type Aggregator,
  type AggregatorFactory,
  createAggregator,
  getAggregatorFactory,
} from "./aggregators";
import {
  type AggregationType,
  type ColumnarDataSource,
  getAggregationForField,
  type DimensionFilter,
  type HybridTotals,
  type NullHandlingConfig,
  type NullHandlingMode,
  type PivotConfigV1,
  type SyntheticMeasureConfig,
  type SortConfig,
} from "./types";
import { DataRecordSource } from "./parseArrow";
import { formatNumber } from "./formatters";

export type DataRecord = Record<string, unknown>;

/** Represents a row in the grouped output (data row or subtotal row). */
export interface GroupedRow {
  type: "data" | "subtotal";
  /** The full row key for data rows, or the parent prefix for subtotal rows. */
  key: string[];
  /** Nesting level (0 = top-level group). Only meaningful for subtotals. */
  level: number;
}

export interface PivotDataOptions {
  nullHandling?: NullHandlingConfig;
  sorters?: Record<string, string[]>;
  hybridTotals?: HybridTotals;
  hybridAggRemap?: Record<string, AggregationType>;
}

export function makeKeyString(parts: string[]): string {
  return parts.join("\x00");
}

function getNullMode(
  field: string,
  config: NullHandlingConfig | undefined,
): NullHandlingMode {
  if (!config) return "exclude";
  if (typeof config === "string") return config;
  return config[field] ?? "exclude";
}

function normalizeNullHandling(
  nh: NullHandlingConfig | undefined,
): NullHandlingConfig | null {
  if (nh == null) return null;
  if (typeof nh === "string") return nh;
  return Object.fromEntries(
    Object.entries(nh).sort(([a], [b]) => a.localeCompare(b)),
  );
}

function normalizeShowSubtotals(
  st: boolean | string[] | undefined,
): boolean | string[] {
  if (st == null) return false;
  if (Array.isArray(st)) return [...st].sort();
  return st;
}

export function buildSidecarFingerprint(
  config: PivotConfigV1,
  nullHandling: NullHandlingConfig | undefined,
): string {
  const agg = config.aggregation ?? {};
  const filters = config.filters ?? {};
  const obj = {
    aggregation: Object.fromEntries(
      Object.entries(agg).sort(([a], [b]) => a.localeCompare(b)),
    ),
    columns: config.columns,
    filters: Object.fromEntries(
      Object.entries(filters)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([k, v]) => [
          k,
          {
            exclude: [...(v.exclude ?? [])].sort(),
            include: [...(v.include ?? [])].sort(),
          },
        ]),
    ),
    null_handling: normalizeNullHandling(nullHandling),
    rows: config.rows,
    show_subtotals: normalizeShowSubtotals(config.show_subtotals),
    values: config.values,
  };
  return JSON.stringify(obj);
}

/**
 * Compare two string cell values with deterministic ordering:
 * empty/null last, numbers before strings, then locale string comparison.
 */
export function mixedCompare(a: string, b: string): number {
  if (a === b) return 0;
  if (a === "") return 1;
  if (b === "") return -1;
  const na = Number(a);
  const nb = Number(b);
  const aIsNum = !isNaN(na) && a.trim() !== "";
  const bIsNum = !isNaN(nb) && b.trim() !== "";
  if (aIsNum && bIsNum) return na - nb;
  if (aIsNum) return -1;
  if (bIsNum) return 1;
  return a.localeCompare(b);
}

/**
 * Core pivot computation engine.
 *
 * Takes parsed records + config, produces row/column keys and aggregated values.
 * All totals are recomputed from raw data (not from partial aggregates) to
 * ensure correctness across all aggregator classes.
 */
export class PivotData {
  private readonly _config: PivotConfigV1;
  private readonly _dataSource: ColumnarDataSource;
  /** Set when constructed from `DataRecord[]`; null for pure columnar (Arrow) input. */
  private readonly _materializedRecords: DataRecord[] | null;
  private readonly _options: PivotDataOptions;
  private _valueFloatArrays: Map<string, Float64Array | null> | null = null;

  /** All value-like source fields that must be aggregated from records. */
  private readonly _allAggregatedFields: readonly string[];
  /** Synthetic measure lookup by id. */
  private readonly _syntheticById: Map<string, SyntheticMeasureConfig>;
  private readonly _hasSyntheticMeasures: boolean;
  private readonly _factoriesByField: Map<string, AggregatorFactory>;

  private readonly _rowKeySet: Map<string, string[]> = new Map();
  private readonly _colKeySet: Map<string, string[]> = new Map();

  private readonly _cellAggs: Map<string, Aggregator> = new Map();
  private readonly _rowTotalAggs: Map<string, Aggregator> = new Map();
  private readonly _colTotalAggs: Map<string, Aggregator> = new Map();
  private readonly _grandTotalAggs: Map<string, Aggregator> = new Map();
  /** Sum-based aggregates used to evaluate synthetic measures. */
  private readonly _cellSumAggs: Map<string, Aggregator> = new Map();
  private readonly _rowTotalSumAggs: Map<string, Aggregator> = new Map();
  private readonly _colTotalSumAggs: Map<string, Aggregator> = new Map();
  private readonly _grandTotalSumAggs: Map<string, Aggregator> = new Map();
  private _subtotalAggs: Map<string, Aggregator> | null = null;
  private _subtotalSumAggs: Map<string, Aggregator> | null = null;

  private _sortedRowKeys: string[][] | null = null;
  private _sortedColKeys: string[][] | null = null;

  /** Unique string values per column, built lazily by getUniqueValues(). */
  private _uniqueValuesCache: Map<string, string[]> | null = null;
  /** Precomputed record indexes by field/value for common interaction fields. */
  private readonly _recordIndexesByFieldValue: Map<
    string,
    Map<string, number[]>
  > = new Map();
  /** Fields whose distinct values/indexes are worth precomputing. */
  private readonly _indexedFields: ReadonlySet<string>;

  /** Sidecar lookup maps (null if stale or absent). */
  private _hybridGrand: Map<string, number | null> | null = null;
  private _hybridRowTotals: Map<string, number | null> | null = null;
  private _hybridColTotals: Map<string, number | null> | null = null;
  private _hybridColPrefix: Map<string, number | null> | null = null;
  private _hybridColPrefixGrand: Map<string, number | null> | null = null;
  private _hybridSubtotals: Map<string, number | null> | null = null;
  private _hybridCrossSubtotals: Map<string, number | null> | null = null;

  constructor(
    input: DataRecord[] | ColumnarDataSource,
    config: PivotConfigV1,
    options?: PivotDataOptions,
  ) {
    this._config = config;
    this._options = options ?? {};
    if (Array.isArray(input)) {
      const columns =
        input.length > 0 ? Object.keys(input[0] as DataRecord) : [];
      this._dataSource = new DataRecordSource(input, columns);
      this._materializedRecords = input;
    } else {
      this._dataSource = input;
      this._materializedRecords = null;
    }
    this._syntheticById = new Map(
      (config.synthetic_measures ?? []).map((m) => [m.id, m]),
    );
    this._hasSyntheticMeasures = this._syntheticById.size > 0;
    const syntheticSourceFields = (config.synthetic_measures ?? []).flatMap(
      (m) => [m.numerator, m.denominator],
    );
    this._allAggregatedFields = [
      ...new Set([...config.values, ...syntheticSourceFields]),
    ];
    const hybridAggRemap = options?.hybridAggRemap;
    this._factoriesByField = new Map(
      this._allAggregatedFields.map((field) => {
        const configAgg = getAggregationForField(field, config);
        const effectiveAgg = hybridAggRemap?.[field] ?? configAgg;
        return [field, getAggregatorFactory(effectiveAgg)];
      }),
    );
    this._indexedFields = new Set([
      ...config.rows,
      ...config.columns,
      ...Object.keys(config.filters ?? {}),
    ]);

    const hybridTotals = options?.hybridTotals;
    if (hybridTotals) {
      const localFp = buildSidecarFingerprint(config, options?.nullHandling);
      if (localFp === hybridTotals.sidecar_fingerprint) {
        this._hybridGrand = new Map(Object.entries(hybridTotals.grand));

        this._hybridRowTotals = new Map();
        for (const entry of hybridTotals.row) {
          const keyStr = makeKeyString(entry.key);
          for (const [f, v] of Object.entries(entry.values)) {
            this._hybridRowTotals.set(`${keyStr}\x01${f}`, v);
          }
        }

        this._hybridColTotals = new Map();
        for (const entry of hybridTotals.col) {
          const keyStr = makeKeyString(entry.key);
          for (const [f, v] of Object.entries(entry.values)) {
            this._hybridColTotals.set(`${keyStr}\x01${f}`, v);
          }
        }

        if (hybridTotals.col_prefix) {
          this._hybridColPrefix = new Map();
          for (const entry of hybridTotals.col_prefix) {
            const rowKeyStr = makeKeyString(entry.row ?? []);
            const cpStr = makeKeyString(entry.key);
            for (const [f, v] of Object.entries(entry.values)) {
              this._hybridColPrefix.set(`${rowKeyStr}\x01${cpStr}\x01${f}`, v);
            }
          }
        }

        if (hybridTotals.col_prefix_grand) {
          this._hybridColPrefixGrand = new Map();
          for (const entry of hybridTotals.col_prefix_grand) {
            const cpStr = makeKeyString(entry.key);
            for (const [f, v] of Object.entries(entry.values)) {
              this._hybridColPrefixGrand.set(`${cpStr}\x01${f}`, v);
            }
          }
        }

        if (hybridTotals.subtotals) {
          this._hybridSubtotals = new Map();
          for (const entry of hybridTotals.subtotals) {
            const parentStr = makeKeyString(entry.key);
            const colStr = makeKeyString(entry.col ?? []);
            for (const [f, v] of Object.entries(entry.values)) {
              this._hybridSubtotals.set(`${parentStr}\x01${colStr}\x01${f}`, v);
            }
          }
        }

        if (hybridTotals.cross_subtotals) {
          this._hybridCrossSubtotals = new Map();
          for (const entry of hybridTotals.cross_subtotals) {
            const parentStr = makeKeyString(entry.key);
            const cpStr = makeKeyString(entry.col_prefix ?? []);
            for (const [f, v] of Object.entries(entry.values)) {
              this._hybridCrossSubtotals.set(
                `${parentStr}\x01${cpStr}\x01${f}`,
                v,
              );
            }
          }
        }
      }
    }

    for (const valField of this._allAggregatedFields) {
      this._grandTotalAggs.set(
        valField,
        this._factoryForField(valField).create(),
      );
      if (this._hasSyntheticMeasures) {
        this._grandTotalSumAggs.set(valField, createAggregator("sum"));
      }
    }

    this._processRecords();
  }

  private _defaultValueField(): string {
    return (
      this._config.values[0] ?? this._config.synthetic_measures?.[0]?.id ?? ""
    );
  }

  private _factoryForField(valField: string): AggregatorFactory {
    return (
      this._factoriesByField.get(valField) ??
      getAggregatorFactory(getAggregationForField(valField, this._config))
    );
  }

  // ---------------------------------------------------------------------------
  // Filtering
  // ---------------------------------------------------------------------------

  private _shouldIncludeRow(rowIndex: number): boolean {
    const filters = this._config.filters;
    if (!filters) return true;
    for (const [field, filter] of Object.entries(filters)) {
      const raw = this._dataSource.getValue(rowIndex, field);
      const val = this._resolveDimValue(field, raw);
      if (filter.include && filter.include.length > 0) {
        if (!filter.include.includes(val)) return false;
      } else if (filter.exclude && filter.exclude.length > 0) {
        if (filter.exclude.includes(val)) return false;
      }
    }
    return true;
  }

  // ---------------------------------------------------------------------------
  // Null handling
  // ---------------------------------------------------------------------------

  private _resolveDimValue(field: string, raw: unknown): string {
    if (raw == null || raw === "") {
      const mode = getNullMode(field, this._options.nullHandling);
      return mode === "separate" ? "(null)" : "";
    }
    return String(raw);
  }

  private _resolveAggValue(field: string, raw: unknown): unknown {
    if (raw == null) {
      const mode = getNullMode(field, this._options.nullHandling);
      return mode === "zero" ? 0 : raw;
    }
    return raw;
  }

  // ---------------------------------------------------------------------------
  // Record processing
  // ---------------------------------------------------------------------------

  private _pushAggValue(agg: Aggregator, valField: string, raw: unknown): void {
    agg.push(this._resolveAggValue(valField, raw));
  }

  private _getValueArrays(): Map<string, Float64Array | null> {
    if (!this._valueFloatArrays) {
      this._valueFloatArrays = new Map();
      for (const vf of this._allAggregatedFields) {
        this._valueFloatArrays.set(
          vf,
          this._dataSource.getFloat64Column?.(vf) ?? null,
        );
      }
    }
    return this._valueFloatArrays;
  }

  private _rawAggValue(
    rowIndex: number,
    valField: string,
    valueArrays: Map<string, Float64Array | null>,
  ): unknown {
    const arr = valueArrays.get(valField);
    if (arr) return arr[rowIndex];
    return this._dataSource.getValue(rowIndex, valField);
  }

  private _materializeRow(rowIndex: number): DataRecord {
    const record: DataRecord = {};
    for (const col of this._dataSource.getColumnNames()) {
      record[col] = this._dataSource.getValue(rowIndex, col);
    }
    return record;
  }

  private _evaluateSynthetic(
    synthetic: SyntheticMeasureConfig,
    getSum: (field: string) => number | null,
  ): number | null {
    const numerator = getSum(synthetic.numerator);
    const denominator = getSum(synthetic.denominator);
    if (numerator == null || denominator == null) return null;
    if (synthetic.operation === "sum_over_sum") {
      return denominator === 0 ? null : numerator / denominator;
    }
    return numerator - denominator;
  }

  private _fixedAggregator(value: number | null): Aggregator {
    return {
      push: () => undefined,
      value: () => value,
      count: () => (value == null ? 0 : 1),
      format: (emptyCellValue: string) =>
        value == null ? emptyCellValue : formatNumber(value),
    };
  }

  private _processRecords(): void {
    const allAggregatedFields = this._allAggregatedFields;
    const valueArrays = this._getValueArrays();
    const numRows = this._dataSource.numRows;
    const uniqueValueSets = new Map<string, Set<string>>();
    for (const field of this._indexedFields) {
      uniqueValueSets.set(field, new Set());
      this._recordIndexesByFieldValue.set(field, new Map());
    }

    for (let recordIndex = 0; recordIndex < numRows; recordIndex++) {
      for (const field of this._indexedFields) {
        const resolved = this._resolveDimValue(
          field,
          this._dataSource.getValue(recordIndex, field),
        );
        uniqueValueSets.get(field)?.add(resolved);
        const perField = this._recordIndexesByFieldValue.get(field);
        if (!perField) continue;
        const existing = perField.get(resolved);
        if (existing) {
          existing.push(recordIndex);
        } else {
          perField.set(resolved, [recordIndex]);
        }
      }

      if (!this._shouldIncludeRow(recordIndex)) continue;

      const rowKey = this._config.rows.map((r) =>
        this._resolveDimValue(r, this._dataSource.getValue(recordIndex, r)),
      );
      const colKey = this._config.columns.map((c) =>
        this._resolveDimValue(c, this._dataSource.getValue(recordIndex, c)),
      );
      const rowKeyStr = makeKeyString(rowKey);
      const colKeyStr = makeKeyString(colKey);

      if (!this._rowKeySet.has(rowKeyStr)) {
        this._rowKeySet.set(rowKeyStr, rowKey);
      }
      if (!this._colKeySet.has(colKeyStr)) {
        this._colKeySet.set(colKeyStr, colKey);
      }

      for (const valField of allAggregatedFields) {
        const rawVal = this._rawAggValue(recordIndex, valField, valueArrays);
        const cellKeyStr = `${rowKeyStr}\x01${colKeyStr}\x01${valField}`;
        let cellAgg = this._cellAggs.get(cellKeyStr);
        if (!cellAgg) {
          cellAgg = this._factoryForField(valField).create();
          this._cellAggs.set(cellKeyStr, cellAgg);
        }
        this._pushAggValue(cellAgg, valField, rawVal);
        if (this._hasSyntheticMeasures) {
          let cellSumAgg = this._cellSumAggs.get(cellKeyStr);
          if (!cellSumAgg) {
            cellSumAgg = createAggregator("sum");
            this._cellSumAggs.set(cellKeyStr, cellSumAgg);
          }
          this._pushAggValue(cellSumAgg, valField, rawVal);
        }

        const rowTotalKeyStr = `${rowKeyStr}\x01${valField}`;
        let rowTotalAgg = this._rowTotalAggs.get(rowTotalKeyStr);
        if (!rowTotalAgg) {
          rowTotalAgg = this._factoryForField(valField).create();
          this._rowTotalAggs.set(rowTotalKeyStr, rowTotalAgg);
        }
        this._pushAggValue(rowTotalAgg, valField, rawVal);
        if (this._hasSyntheticMeasures) {
          let rowSumAgg = this._rowTotalSumAggs.get(rowTotalKeyStr);
          if (!rowSumAgg) {
            rowSumAgg = createAggregator("sum");
            this._rowTotalSumAggs.set(rowTotalKeyStr, rowSumAgg);
          }
          this._pushAggValue(rowSumAgg, valField, rawVal);
        }

        const colTotalKeyStr = `${colKeyStr}\x01${valField}`;
        let colTotalAgg = this._colTotalAggs.get(colTotalKeyStr);
        if (!colTotalAgg) {
          colTotalAgg = this._factoryForField(valField).create();
          this._colTotalAggs.set(colTotalKeyStr, colTotalAgg);
        }
        this._pushAggValue(colTotalAgg, valField, rawVal);
        if (this._hasSyntheticMeasures) {
          let colSumAgg = this._colTotalSumAggs.get(colTotalKeyStr);
          if (!colSumAgg) {
            colSumAgg = createAggregator("sum");
            this._colTotalSumAggs.set(colTotalKeyStr, colSumAgg);
          }
          this._pushAggValue(colSumAgg, valField, rawVal);
        }

        const grandAgg = this._grandTotalAggs.get(valField)!;
        this._pushAggValue(grandAgg, valField, rawVal);
        if (this._hasSyntheticMeasures) {
          const grandSumAgg = this._grandTotalSumAggs.get(valField)!;
          this._pushAggValue(grandSumAgg, valField, rawVal);
        }
      }
    }

    if (uniqueValueSets.size > 0) {
      this._uniqueValuesCache = new Map(
        [...uniqueValueSets.entries()].map(([field, values]) => [
          field,
          [...values].sort((a, b) => a.localeCompare(b)),
        ]),
      );
    }
  }

  // ---------------------------------------------------------------------------
  // Sorting
  // ---------------------------------------------------------------------------

  private _buildKeyComparator(
    sortConfig: SortConfig | undefined,
    dimensions: string[],
    getValueForSort: (key: string[]) => number | null,
    getSubtotalForSort?: (prefix: string[]) => number | null,
  ): (a: string[], b: string[]) => number {
    const by = sortConfig?.by ?? "key";
    const direction = sortConfig?.direction ?? "asc";
    const sorters = this._options.sorters;
    const sign = direction === "desc" ? -1 : 1;

    if (by === "value") {
      const numDims = dimensions.length;
      // When dimension is set, only apply the requested direction at and
      // below that level; parent levels use ascending (sign=1) so their
      // existing group order is preserved.  This gives BI-standard scoped
      // sorting: sorting from a specific dimension header only reorders
      // that level and its children.
      const targetDimIdx = sortConfig?.dimension
        ? dimensions.indexOf(sortConfig.dimension)
        : -1;
      if (numDims >= 2 && getSubtotalForSort) {
        return (a, b) => {
          for (let level = 0; level < numDims - 1; level++) {
            if (a[level] === b[level]) continue;
            const levelSign =
              targetDimIdx === -1 || level >= targetDimIdx ? sign : 1;
            const prefixA = a.slice(0, level + 1);
            const prefixB = b.slice(0, level + 1);
            const va = getSubtotalForSort(prefixA);
            const vb = getSubtotalForSort(prefixB);
            if (va == null && vb == null)
              return mixedCompare(a[level], b[level]) * levelSign;
            if (va == null) return 1;
            if (vb == null) return -1;
            if (va !== vb) return (va - vb) * levelSign;
            return mixedCompare(a[level], b[level]) * levelSign;
          }
          const leafSign =
            targetDimIdx === -1 || numDims - 1 >= targetDimIdx ? sign : 1;
          const va = getValueForSort(a);
          const vb = getValueForSort(b);
          if (va == null && vb == null) return 0;
          if (va == null) return 1;
          if (vb == null) return -1;
          return (va - vb) * leafSign;
        };
      }
      return (a, b) => {
        const va = getValueForSort(a);
        const vb = getValueForSort(b);
        if (va == null && vb == null) return 0;
        if (va == null) return 1;
        if (vb == null) return -1;
        return (va - vb) * sign;
      };
    }

    const targetDimIdx = sortConfig?.dimension
      ? dimensions.indexOf(sortConfig.dimension)
      : -1;

    return (a, b) => {
      for (let i = 0; i < Math.max(a.length, b.length); i++) {
        const ai = a[i] ?? "";
        const bi = b[i] ?? "";
        const dim = dimensions[i];
        const customOrder = dim && sorters?.[dim];
        let cmp: number;
        if (customOrder) {
          const ia = customOrder.indexOf(ai);
          const ib = customOrder.indexOf(bi);
          const oa = ia === -1 ? customOrder.length : ia;
          const ob = ib === -1 ? customOrder.length : ib;
          cmp = oa - ob;
          if (cmp === 0) cmp = mixedCompare(ai, bi);
        } else {
          cmp = mixedCompare(ai, bi);
        }
        const levelSign = targetDimIdx === -1 || i === targetDimIdx ? sign : 1;
        if (cmp !== 0) return cmp * levelSign;
      }
      return 0;
    };
  }

  /**
   * Build the value-getter for row sorting. When the sort config specifies
   * a col_key, sort by the cell value at that intersection; otherwise use
   * the row total.
   */
  private _rowValueGetter(
    sortConfig: SortConfig | undefined,
  ): (key: string[]) => number | null {
    const valField = sortConfig?.value_field ?? this._defaultValueField();
    if (sortConfig?.col_key) {
      const colKey = sortConfig.col_key;
      return (rowKey) => this.getAggregator(rowKey, colKey, valField).value();
    }
    return (rowKey) => this.getRowTotal(rowKey, valField).value();
  }

  /**
   * Build the value-getter for column sorting. Uses the column total for
   * the specified (or first) value field.
   */
  private _colValueGetter(
    sortConfig: SortConfig | undefined,
  ): (key: string[]) => number | null {
    const valField = sortConfig?.value_field ?? this._defaultValueField();
    return (colKey) => this.getColTotal(colKey, valField).value();
  }

  getRowKeys(): string[][] {
    if (!this._sortedRowKeys) {
      const subtotalGetter =
        this._config.rows.length >= 2
          ? (prefix: string[]) => {
              const valField =
                this._config.row_sort?.value_field ?? this._defaultValueField();
              const colKey = this._config.row_sort?.col_key;
              return this.getSubtotalAggregator(
                prefix,
                colKey ?? [],
                valField,
              ).value();
            }
          : undefined;
      const cmp = this._buildKeyComparator(
        this._config.row_sort,
        this._config.rows,
        this._rowValueGetter(this._config.row_sort),
        subtotalGetter,
      );
      this._sortedRowKeys = [...this._rowKeySet.values()].sort(cmp);
    }
    return this._sortedRowKeys;
  }

  getColKeys(): string[][] {
    if (!this._sortedColKeys) {
      const cmp = this._buildKeyComparator(
        this._config.col_sort,
        this._config.columns,
        this._colValueGetter(this._config.col_sort),
      );
      this._sortedColKeys = [...this._colKeySet.values()].sort(cmp);
    }
    return this._sortedColKeys;
  }

  // ---------------------------------------------------------------------------
  // Aggregator access
  // ---------------------------------------------------------------------------

  getAggregator(
    rowKey: string[],
    colKey: string[],
    valField?: string,
  ): Aggregator {
    const field = valField ?? this._defaultValueField();
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const value = this._evaluateSynthetic(synthetic, (sourceField) => {
        const sourceKey = `${makeKeyString(rowKey)}\x01${makeKeyString(colKey)}\x01${sourceField}`;
        return this._cellSumAggs.get(sourceKey)?.value() ?? null;
      });
      return this._fixedAggregator(value);
    }
    const keyStr = `${makeKeyString(rowKey)}\x01${makeKeyString(colKey)}\x01${field}`;
    const agg = this._cellAggs.get(keyStr);
    if (!agg) {
      return this._factoryForField(field).create();
    }
    return agg;
  }

  getRowTotal(rowKey: string[], valField?: string): Aggregator {
    const field = valField ?? this._defaultValueField();
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const value = this._evaluateSynthetic(synthetic, (sourceField) => {
        const sourceKey = `${makeKeyString(rowKey)}\x01${sourceField}`;
        return this._rowTotalSumAggs.get(sourceKey)?.value() ?? null;
      });
      return this._fixedAggregator(value);
    }
    const keyStr = `${makeKeyString(rowKey)}\x01${field}`;
    if (this._hybridRowTotals?.has(keyStr)) {
      return this._fixedAggregator(this._hybridRowTotals.get(keyStr)!);
    }
    const agg = this._rowTotalAggs.get(keyStr);
    if (!agg) {
      return this._factoryForField(field).create();
    }
    return agg;
  }

  getColTotal(colKey: string[], valField?: string): Aggregator {
    const field = valField ?? this._defaultValueField();
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const value = this._evaluateSynthetic(synthetic, (sourceField) => {
        const sourceKey = `${makeKeyString(colKey)}\x01${sourceField}`;
        return this._colTotalSumAggs.get(sourceKey)?.value() ?? null;
      });
      return this._fixedAggregator(value);
    }
    const keyStr = `${makeKeyString(colKey)}\x01${field}`;
    if (this._hybridColTotals?.has(keyStr)) {
      return this._fixedAggregator(this._hybridColTotals.get(keyStr)!);
    }
    const agg = this._colTotalAggs.get(keyStr);
    if (!agg) {
      return this._factoryForField(field).create();
    }
    return agg;
  }

  getGrandTotal(valField?: string): Aggregator {
    const field = valField ?? this._defaultValueField();
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const value = this._evaluateSynthetic(
        synthetic,
        (sourceField) =>
          this._grandTotalSumAggs.get(sourceField)?.value() ?? null,
      );
      return this._fixedAggregator(value);
    }
    if (this._hybridGrand?.has(field)) {
      return this._fixedAggregator(this._hybridGrand.get(field)!);
    }
    const agg = this._grandTotalAggs.get(field);
    if (!agg) {
      return this._factoryForField(field).create();
    }
    return agg;
  }

  // ---------------------------------------------------------------------------
  // Subtotals (Phase 3a)
  // ---------------------------------------------------------------------------

  /**
   * Build subtotal aggregators lazily. Each subtotal aggregator is keyed by
   * `parentKeyStr + "\x01" + colKeyStr + "\x01" + valField` and recomputes
   * from raw records to ensure correctness for non-additive aggregators.
   */
  private _buildSubtotalAggs(): Map<string, Aggregator> {
    if (this._subtotalAggs) return this._subtotalAggs;
    this._subtotalAggs = new Map();
    this._subtotalSumAggs = new Map();

    const rowDims = this._config.rows;
    if (rowDims.length < 2) return this._subtotalAggs;

    const valueArrays = this._getValueArrays();
    const numRows = this._dataSource.numRows;
    for (let recordIndex = 0; recordIndex < numRows; recordIndex++) {
      if (!this._shouldIncludeRow(recordIndex)) continue;

      const fullRowKey = rowDims.map((r) =>
        this._resolveDimValue(r, this._dataSource.getValue(recordIndex, r)),
      );
      const colKey = this._config.columns.map((c) =>
        this._resolveDimValue(c, this._dataSource.getValue(recordIndex, c)),
      );
      const colKeyStr = makeKeyString(colKey);

      for (let level = 0; level < rowDims.length - 1; level++) {
        const parentKey = fullRowKey.slice(0, level + 1);
        const parentKeyStr = makeKeyString(parentKey);

        for (const valField of this._allAggregatedFields) {
          const rawVal = this._rawAggValue(recordIndex, valField, valueArrays);
          const cellKey = `${parentKeyStr}\x01${colKeyStr}\x01${valField}`;
          let cellAgg = this._subtotalAggs.get(cellKey);
          if (!cellAgg) {
            cellAgg = this._factoryForField(valField).create();
            this._subtotalAggs.set(cellKey, cellAgg);
          }
          this._pushAggValue(cellAgg, valField, rawVal);
          if (this._hasSyntheticMeasures) {
            let cellSumAgg = this._subtotalSumAggs.get(cellKey);
            if (!cellSumAgg) {
              cellSumAgg = createAggregator("sum");
              this._subtotalSumAggs.set(cellKey, cellSumAgg);
            }
            this._pushAggValue(cellSumAgg, valField, rawVal);
          }

          if (colKeyStr !== "") {
            const rowTotalKey = `${parentKeyStr}\x01\x01${valField}`;
            let rowAgg = this._subtotalAggs.get(rowTotalKey);
            if (!rowAgg) {
              rowAgg = this._factoryForField(valField).create();
              this._subtotalAggs.set(rowTotalKey, rowAgg);
            }
            this._pushAggValue(rowAgg, valField, rawVal);
            if (this._hasSyntheticMeasures) {
              let rowSumAgg = this._subtotalSumAggs.get(rowTotalKey);
              if (!rowSumAgg) {
                rowSumAgg = createAggregator("sum");
                this._subtotalSumAggs.set(rowTotalKey, rowSumAgg);
              }
              this._pushAggValue(rowSumAgg, valField, rawVal);
            }
          }
        }
      }
    }
    return this._subtotalAggs;
  }

  /**
   * Get the subtotal aggregator for a parent group at a given level and column.
   * @param parentKey - The prefix key for this group (length = level + 1).
   * @param colKey - The column key. Pass [] for the row total of this subtotal.
   * @param valField - The value field name.
   */
  getSubtotalAggregator(
    parentKey: string[],
    colKey: string[],
    valField: string,
  ): Aggregator {
    const field = valField;
    const synthetic = this._syntheticById.get(field);
    const parentKeyStr = makeKeyString(parentKey);
    const colKeyStr = makeKeyString(colKey);
    if (synthetic) {
      const aggs = this._buildSubtotalAggs();
      void aggs;
      const value = this._evaluateSynthetic(synthetic, (sourceField) => {
        const sourceKey = `${parentKeyStr}\x01${colKeyStr}\x01${sourceField}`;
        return this._subtotalSumAggs?.get(sourceKey)?.value() ?? null;
      });
      return this._fixedAggregator(value);
    }
    const lookupKey = `${parentKeyStr}\x01${colKeyStr}\x01${field}`;
    if (this._hybridSubtotals?.has(lookupKey)) {
      return this._fixedAggregator(this._hybridSubtotals.get(lookupKey)!);
    }
    const aggs = this._buildSubtotalAggs();
    return aggs.get(lookupKey) ?? this._factoryForField(field).create();
  }

  /**
   * Get a flat list of row entries interleaving data rows and subtotal rows.
   * Collapsed groups have their children hidden but the subtotal row remains.
   * Subtotal rows appear BELOW their group (Excel convention).
   */
  getGroupedRowKeys(ignoreCollapsed?: boolean): GroupedRow[] {
    const rowDims = this._config.rows;
    const rowKeys = this.getRowKeys();

    let collapsed: Set<string>;
    if (ignoreCollapsed) {
      collapsed = new Set<string>();
    } else {
      const rawCollapsed = this._config.collapsed_groups ?? [];
      if (rawCollapsed.includes("__ALL__")) {
        collapsed = new Set<string>();
        const seen = new Set<string>();
        for (const rk of rowKeys) {
          const topKey = makeKeyString(rk.slice(0, 1));
          if (!seen.has(topKey)) {
            seen.add(topKey);
            collapsed.add(topKey);
          }
        }
      } else {
        collapsed = new Set(rawCollapsed);
      }
    }

    const subtotalsVal = this._config.show_subtotals;
    if (rowDims.length < 2 || !subtotalsVal) {
      return rowKeys.map((key) => ({
        type: "data" as const,
        key,
        level: rowDims.length - 1,
      }));
    }

    const subtotalEnabledAtLevel = (lvl: number): boolean => {
      if (subtotalsVal === true) return true;
      if (Array.isArray(subtotalsVal))
        return subtotalsVal.includes(rowDims[lvl]);
      return false;
    };

    const result: GroupedRow[] = [];
    const numLevels = rowDims.length;

    for (let i = 0; i < rowKeys.length; i++) {
      const currentKey = rowKeys[i];
      const nextKey: string[] | undefined = rowKeys[i + 1];

      // Check if any ancestor group is collapsed — if so, skip this data row
      let hidden = false;
      for (let lvl = 0; lvl < numLevels - 1; lvl++) {
        const prefix = currentKey.slice(0, lvl + 1);
        if (collapsed.has(makeKeyString(prefix))) {
          hidden = true;
          break;
        }
      }

      if (!hidden) {
        result.push({ type: "data", key: currentKey, level: numLevels - 1 });
      }

      // After processing this row, check if any group just ended.
      // A group at level `lvl` ends when the next row's prefix differs.
      // Insert subtotal rows from deepest level to shallowest.
      for (let lvl = numLevels - 2; lvl >= 0; lvl--) {
        const currentPrefix = currentKey.slice(0, lvl + 1);
        const nextPrefix = nextKey?.slice(0, lvl + 1);
        const groupEnded =
          !nextKey ||
          makeKeyString(currentPrefix) !== makeKeyString(nextPrefix!);

        if (groupEnded) {
          // Check if any ancestor is collapsed — subtotal should still show
          // if this specific group is collapsed, but not if a parent is collapsed
          let ancestorCollapsed = false;
          for (let parentLvl = 0; parentLvl < lvl; parentLvl++) {
            const parentPrefix = currentKey.slice(0, parentLvl + 1);
            if (collapsed.has(makeKeyString(parentPrefix))) {
              ancestorCollapsed = true;
              break;
            }
          }

          if (!ancestorCollapsed && subtotalEnabledAtLevel(lvl)) {
            result.push({ type: "subtotal", key: currentPrefix, level: lvl });
          }
        }
      }
    }

    return result;
  }

  // ---------------------------------------------------------------------------
  // Column group subtotals (Phase 3a — column collapse/expand)
  // ---------------------------------------------------------------------------

  private _colSubtotalAggs: Map<string, Aggregator> | null = null;
  private _colSubtotalSumAggs: Map<string, Aggregator> | null = null;
  /** Row-group × col-group cross-section subtotals (for subtotal rows with collapsed columns). */
  private _crossSubtotalAggs: Map<string, Aggregator> | null = null;
  private _crossSubtotalSumAggs: Map<string, Aggregator> | null = null;

  /**
   * Build column-group subtotal aggregators lazily from raw records.
   * Also builds grand-row entries (empty row key) and cross-section entries
   * (row-group prefix × column prefix) in a single pass for correctness
   * with non-additive aggregators.
   */
  private _buildColSubtotalAggs(): Map<string, Aggregator> {
    if (this._colSubtotalAggs) return this._colSubtotalAggs;
    this._colSubtotalAggs = new Map();
    this._colSubtotalSumAggs = new Map();
    this._crossSubtotalAggs = new Map();
    this._crossSubtotalSumAggs = new Map();

    const colDims = this._config.columns;
    const rowDims = this._config.rows;
    if (colDims.length < 2) return this._colSubtotalAggs;

    const emptyRowKeyStr = makeKeyString([]);
    const buildCross = rowDims.length >= 2;

    const valueArrays = this._getValueArrays();
    const numRows = this._dataSource.numRows;
    for (let recordIndex = 0; recordIndex < numRows; recordIndex++) {
      if (!this._shouldIncludeRow(recordIndex)) continue;

      const rowKey = rowDims.map((r) =>
        this._resolveDimValue(r, this._dataSource.getValue(recordIndex, r)),
      );
      const fullColKey = colDims.map((c) =>
        this._resolveDimValue(c, this._dataSource.getValue(recordIndex, c)),
      );
      const rowKeyStr = makeKeyString(rowKey);

      for (let level = 0; level < colDims.length - 1; level++) {
        const colPrefix = fullColKey.slice(0, level + 1);
        const colPrefixStr = makeKeyString(colPrefix);

        for (const valField of this._allAggregatedFields) {
          const rawVal = this._rawAggValue(recordIndex, valField, valueArrays);
          const cellKey = `${rowKeyStr}\x01${colPrefixStr}\x01${valField}`;
          let cellAgg = this._colSubtotalAggs.get(cellKey);
          if (!cellAgg) {
            cellAgg = this._factoryForField(valField).create();
            this._colSubtotalAggs.set(cellKey, cellAgg);
          }
          this._pushAggValue(cellAgg, valField, rawVal);
          if (this._hasSyntheticMeasures) {
            let cellSumAgg = this._colSubtotalSumAggs.get(cellKey);
            if (!cellSumAgg) {
              cellSumAgg = createAggregator("sum");
              this._colSubtotalSumAggs.set(cellKey, cellSumAgg);
            }
            this._pushAggValue(cellSumAgg, valField, rawVal);
          }

          const grandKey = `${emptyRowKeyStr}\x01${colPrefixStr}\x01${valField}`;
          let grandAgg = this._colSubtotalAggs.get(grandKey);
          if (!grandAgg) {
            grandAgg = this._factoryForField(valField).create();
            this._colSubtotalAggs.set(grandKey, grandAgg);
          }
          this._pushAggValue(grandAgg, valField, rawVal);
          if (this._hasSyntheticMeasures) {
            let grandSumAgg = this._colSubtotalSumAggs.get(grandKey);
            if (!grandSumAgg) {
              grandSumAgg = createAggregator("sum");
              this._colSubtotalSumAggs.set(grandKey, grandSumAgg);
            }
            this._pushAggValue(grandSumAgg, valField, rawVal);
          }

          if (buildCross) {
            for (let rowLevel = 0; rowLevel < rowDims.length - 1; rowLevel++) {
              const parentKey = rowKey.slice(0, rowLevel + 1);
              const parentKeyStr = makeKeyString(parentKey);
              const crossKey = `${parentKeyStr}\x01${colPrefixStr}\x01${valField}`;
              let crossAgg = this._crossSubtotalAggs!.get(crossKey);
              if (!crossAgg) {
                crossAgg = this._factoryForField(valField).create();
                this._crossSubtotalAggs!.set(crossKey, crossAgg);
              }
              this._pushAggValue(crossAgg, valField, rawVal);
              if (this._hasSyntheticMeasures) {
                let crossSumAgg = this._crossSubtotalSumAggs!.get(crossKey);
                if (!crossSumAgg) {
                  crossSumAgg = createAggregator("sum");
                  this._crossSubtotalSumAggs!.set(crossKey, crossSumAgg);
                }
                this._pushAggValue(crossSumAgg, valField, rawVal);
              }
            }
          }
        }
      }
    }
    return this._colSubtotalAggs;
  }

  /**
   * Get the column group subtotal for a given row and column prefix.
   * Used when column groups are collapsed.
   */
  getColGroupSubtotal(
    rowKey: string[],
    colPrefix: string[],
    valField: string,
  ): Aggregator {
    const rowKeyStr = makeKeyString(rowKey);
    const colPrefixStr = makeKeyString(colPrefix);
    const field = valField;
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const aggs = this._buildColSubtotalAggs();
      void aggs;
      const value = this._evaluateSynthetic(synthetic, (sourceField) => {
        const sourceKey = `${rowKeyStr}\x01${colPrefixStr}\x01${sourceField}`;
        return this._colSubtotalSumAggs?.get(sourceKey)?.value() ?? null;
      });
      return this._fixedAggregator(value);
    }
    const lookupKey = `${rowKeyStr}\x01${colPrefixStr}\x01${field}`;
    if (this._hybridColPrefix?.has(lookupKey)) {
      return this._fixedAggregator(this._hybridColPrefix.get(lookupKey)!);
    }
    const aggs = this._buildColSubtotalAggs();
    return aggs.get(lookupKey) ?? this._factoryForField(field).create();
  }

  /**
   * Get the grand-total-row column group subtotal (all rows for a column prefix).
   * Pre-computed from raw records to ensure correctness for non-additive aggregators.
   */
  getColGroupGrandSubtotal(colPrefix: string[], valField: string): Aggregator {
    const colPrefixStr = makeKeyString(colPrefix);
    const field = valField;
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const aggs = this._buildColSubtotalAggs();
      void aggs;
      const value = this._evaluateSynthetic(synthetic, (sourceField) => {
        const sourceKey = `${makeKeyString([])}\x01${colPrefixStr}\x01${sourceField}`;
        return this._colSubtotalSumAggs?.get(sourceKey)?.value() ?? null;
      });
      return this._fixedAggregator(value);
    }
    const lookupKey = `${colPrefixStr}\x01${field}`;
    if (this._hybridColPrefixGrand?.has(lookupKey)) {
      return this._fixedAggregator(this._hybridColPrefixGrand.get(lookupKey)!);
    }
    const aggs = this._buildColSubtotalAggs();
    const key = `${makeKeyString([])}\x01${colPrefixStr}\x01${field}`;
    return aggs.get(key) ?? this._factoryForField(field).create();
  }

  /**
   * Get the cross-section subtotal: a row-group (parent key prefix) at a
   * collapsed column-group prefix. Used for subtotal rows with collapsed columns.
   */
  getSubtotalColGroupAgg(
    parentKey: string[],
    colPrefix: string[],
    valField: string,
  ): Aggregator {
    const parentKeyStr = makeKeyString(parentKey);
    const colPrefixStr = makeKeyString(colPrefix);
    const field = valField;
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      this._buildColSubtotalAggs();
      const value = this._evaluateSynthetic(synthetic, (sourceField) => {
        const sourceKey = `${parentKeyStr}\x01${colPrefixStr}\x01${sourceField}`;
        return this._crossSubtotalSumAggs?.get(sourceKey)?.value() ?? null;
      });
      return this._fixedAggregator(value);
    }
    const lookupKey = `${parentKeyStr}\x01${colPrefixStr}\x01${field}`;
    if (this._hybridCrossSubtotals?.has(lookupKey)) {
      return this._fixedAggregator(this._hybridCrossSubtotals.get(lookupKey)!);
    }
    this._buildColSubtotalAggs();
    return (
      this._crossSubtotalAggs!.get(lookupKey) ??
      this._factoryForField(field).create()
    );
  }

  // ---------------------------------------------------------------------------
  // Unique values (for filter menus)
  // ---------------------------------------------------------------------------

  getUniqueValues(field: string): string[] {
    if (!this._uniqueValuesCache) {
      this._uniqueValuesCache = new Map();
    }
    let cached = this._uniqueValuesCache.get(field);
    if (!cached) {
      const set = new Set<string>();
      const n = this._dataSource.numRows;
      for (let i = 0; i < n; i++) {
        set.add(
          this._resolveDimValue(field, this._dataSource.getValue(i, field)),
        );
      }
      cached = [...set].sort((a, b) => a.localeCompare(b));
      this._uniqueValuesCache.set(field, cached);
    }
    return cached;
  }

  // ---------------------------------------------------------------------------
  // Metadata
  // ---------------------------------------------------------------------------

  get config(): PivotConfigV1 {
    return this._config;
  }

  get recordCount(): number {
    return this._dataSource.numRows;
  }

  get uniqueRowKeyCount(): number {
    return this._rowKeySet.size;
  }

  get uniqueColKeyCount(): number {
    return this._colKeySet.size;
  }

  get totalCellCount(): number {
    const renderedMeasureCount =
      this._config.values.length +
      (this._config.synthetic_measures?.length ?? 0);
    return (
      this._rowKeySet.size *
      this._colKeySet.size *
      Math.max(1, renderedMeasureCount)
    );
  }

  /**
   * All column names from the source records (union of keys across all records).
   * Cached lazily.
   */
  private _columnNamesCache: string[] | null = null;

  getColumnNames(): string[] {
    if (!this._columnNamesCache) {
      if (this._materializedRecords) {
        const cols = new Set<string>();
        for (const record of this._materializedRecords) {
          for (const key of Object.keys(record)) {
            cols.add(key);
          }
        }
        this._columnNamesCache = [...cols];
      } else {
        this._columnNamesCache = [...this._dataSource.getColumnNames()];
      }
    }
    return this._columnNamesCache;
  }

  /**
   * Return source records matching the given dimension filters (from a cell click).
   * Applies both the pivot's own config filters and the cell-click filters.
   */
  getMatchingRecords(
    filters: Record<string, string>,
    limit: number = 500,
  ): { records: DataRecord[]; totalCount: number } {
    const matched: DataRecord[] = [];
    let totalCount = 0;
    const numRows = this._dataSource.numRows;
    let candidateIndexes: number[] | null = null;
    for (const [field, value] of Object.entries(filters)) {
      const indexed = this._recordIndexesByFieldValue.get(field)?.get(value);
      if (!indexed) {
        candidateIndexes = [];
        break;
      }
      if (
        candidateIndexes === null ||
        indexed.length < candidateIndexes.length
      ) {
        candidateIndexes = indexed;
      }
    }

    const useFullScan = candidateIndexes === null;
    const scanLength = useFullScan ? numRows : candidateIndexes!.length;

    for (let i = 0; i < scanLength; i++) {
      const recordIndex = useFullScan ? i : candidateIndexes![i]!;
      if (!this._shouldIncludeRow(recordIndex)) continue;

      let matchesClick = true;
      for (const [field, value] of Object.entries(filters)) {
        const recordVal = this._resolveDimValue(
          field,
          this._dataSource.getValue(recordIndex, field),
        );
        if (recordVal !== value) {
          matchesClick = false;
          break;
        }
      }
      if (!matchesClick) continue;

      totalCount++;
      if (matched.length < limit) {
        matched.push(
          this._materializedRecords
            ? this._materializedRecords[recordIndex]!
            : this._materializeRow(recordIndex),
        );
      }
    }

    return { records: matched, totalCount };
  }
}
