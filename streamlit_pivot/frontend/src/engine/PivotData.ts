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
  type ColumnType,
  type ColumnTypeMap,
  type DateGrain,
  getEffectiveDateGrain,
  getAggregationForField,
  getSyntheticSourceFields,
  type DimensionFilter,
  type HybridTotals,
  type NullHandlingConfig,
  type NullHandlingMode,
  type PivotConfigV1,
  type SyntheticMeasureConfig,
  type SortConfig,
  type TopNFilter,
  type ValueFilter,
  getRenderedValueFields,
} from "./types";
import {
  compileFormula,
  type CompiledFormula,
  FormulaParseError,
} from "./formulaParser";
import { DataRecordSource } from "./parseArrow";
import {
  formatNumber,
  formatDateValue,
  formatDateTimeValue,
  formatIntegerLabel,
  formatDateWithPattern,
} from "./formatters";
import {
  bucketTemporalKey,
  buildModifiedColKey,
  buildModifiedRowKey,
  extractParentBuckets,
  formatTemporalBucketLabel,
  isTemporalColumnType,
  shiftTemporalBucketKey,
} from "./dateGrouping";

export type DataRecord = Record<string, unknown>;

// ---------------------------------------------------------------------------
// Values-axis row-key encoding helpers
// ---------------------------------------------------------------------------

/** The reserved prefix used to encode a value field as a row-key pseudo-segment. */
const VALUES_AXIS_PREFIX = "__vf__:";

/**
 * Encode a value field id as the trailing pseudo-segment of a values-on-rows row key.
 * e.g. encodeValueFieldSegment("revenue") === "__vf__:revenue"
 */
export function encodeValueFieldSegment(fieldId: string): string {
  return VALUES_AXIS_PREFIX + fieldId;
}

/**
 * Decode a value field id from a row-key pseudo-segment.
 * Returns the field id string when the segment starts with the reserved prefix,
 * null otherwise.
 */
export function decodeValueFieldSegment(segment: string): string | null {
  return segment.startsWith(VALUES_AXIS_PREFIX)
    ? segment.slice(VALUES_AXIS_PREFIX.length)
    : null;
}

/**
 * Extract the encoded value field from a values-on-rows row key.
 * Works at any depth: leaf `["East","Widget","__vf__:revenue"]`, subtotal
 * `["East","__vf__:revenue"]`, and grand-total `["__vf__:revenue"]`.
 * Returns null when the last segment is not an encoded value field.
 * No dependency on config — callers must guard with `config.values_axis === "rows"`.
 */
export function getValueFieldForRowKey(
  rowKey: readonly string[],
): string | null {
  if (rowKey.length === 0) return null;
  return decodeValueFieldSegment(rowKey[rowKey.length - 1]);
}

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
  columnTypes?: ColumnTypeMap;
  adaptiveDateGrains?: Record<string, DateGrain>;
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
  adaptiveDateGrains?: Record<string, DateGrain>,
): string {
  const agg = config.aggregation ?? {};
  const filters = config.filters ?? {};
  const obj = {
    adaptive_date_grains: adaptiveDateGrains
      ? Object.fromEntries(
          Object.entries(adaptiveDateGrains).sort(([a], [b]) =>
            a.localeCompare(b),
          ),
        )
      : {},
    aggregation: Object.fromEntries(
      Object.entries(agg).sort(([a], [b]) => a.localeCompare(b)),
    ),
    auto_date_hierarchy: config.auto_date_hierarchy !== false,
    columns: config.columns,
    date_grains: config.date_grains
      ? Object.fromEntries(
          Object.entries(config.date_grains).sort(([a], [b]) =>
            a.localeCompare(b),
          ),
        )
      : {},
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

// ---------------------------------------------------------------------------
// 0.5.0 Filtering helpers — Top N / Bottom N and Value Filters
// ---------------------------------------------------------------------------

/**
 * Retrieve the aggregated measure value for a given key at grand-total context.
 *
 * Row axis:
 *   - Full leaf keys (length === numRowDims): getRowTotal (sum across all cols)
 *   - Partial prefix keys: getSubtotalAggregator(key, [], byField)
 *
 * Column axis:
 *   - Full leaf keys (length === numColDims): getColTotal (sum across all rows)
 *   - Partial prefix keys: sum all matching full leaf col keys from _colTotalAggs
 *     (equivalent to a column subtotal at grand-total row context)
 *
 * Returns null when the key is not found or the value is non-numeric / missing.
 */
function _getMeasureValue(
  data: PivotData,
  key: string[],
  byField: string,
  axis: "rows" | "columns",
): number | null {
  let rawVal: unknown;

  if (axis === "rows") {
    const numRowDims = data["_config"].rows.length;
    const agg =
      key.length >= numRowDims
        ? data.getRowTotal(key, byField)
        : data.getSubtotalAggregator(key, [], byField);
    rawVal = agg.value();
  } else {
    const numColDims = data["_config"].columns.length;
    if (key.length >= numColDims) {
      // Full leaf column key — use the pre-built column total aggregator.
      rawVal = data.getColTotal(key, byField).value();
    } else {
      // Partial column key (parent level in multi-level column dimension).
      // Use getColGroupGrandSubtotal() which is pre-computed from raw records
      // and is correct for non-additive aggregators (e.g. distinct count, median).
      rawVal = data.getColGroupGrandSubtotal(key, byField).value();
    }
  }

  if (
    rawVal === null ||
    rawVal === undefined ||
    typeof rawVal !== "number" ||
    isNaN(rawVal)
  )
    return null;
  return rawVal;
}

/**
 * Apply Top N / Bottom N filters to a sorted key list.
 *
 * Filtering is **per-parent**: keys are grouped by their parent prefix
 * (segments 0..dimIdx-1). Within each group the **unique members** at
 * dimension `dimIdx` are scored independently, so a winning member keeps
 * ALL of its descendant leaf rows (not just the first N leaf rows).
 *
 * Members with null measure values are excluded from the ranking and never
 * consume Top N / Bottom N slots (they are always dropped).
 *
 * The relative sort order of kept leaf keys is preserved.
 */
function _applyTopNFilters(
  data: PivotData,
  keys: string[][],
  filters: TopNFilter[],
  dimList: string[],
  axis: "rows" | "columns",
): string[][] {
  if (!filters.length) return keys;

  let result = keys;
  for (const f of filters) {
    const dimIdx = dimList.indexOf(f.field);
    if (dimIdx < 0) continue; // field not in this axis — skip

    // Group all leaf keys by their parent prefix (segments 0..dimIdx-1).
    const groups = new Map<string, string[][]>();
    for (const key of result) {
      const parentKey = makeKeyString(key.slice(0, dimIdx));
      if (!groups.has(parentKey)) groups.set(parentKey, []);
      groups.get(parentKey)!.push(key);
    }

    const kept: string[][] = [];
    for (const group of groups.values()) {
      // Build a map of unique member values at dimIdx → score.
      // We score the member once (using any leaf key that shares that member)
      // rather than once per leaf row, fixing the "multi-slot consumption" bug.
      const memberScores = new Map<string, number | null>();
      for (const key of group) {
        const member = key[dimIdx]!;
        if (!memberScores.has(member)) {
          memberScores.set(
            member,
            _getMeasureValue(data, key.slice(0, dimIdx + 1), f.by, axis),
          );
        }
      }

      // Keep only members with non-null scores, then rank.
      const nonNull = [...memberScores.entries()]
        .filter((entry): entry is [string, number] => entry[1] !== null)
        .sort(([, a], [, b]) => b - a); // descending (highest first)

      const topSlice =
        f.direction === "top"
          ? nonNull.slice(0, f.n) // top N highest
          : nonNull.slice(-f.n); // bottom N lowest (nulls already excluded)

      const winnerMembers = new Set(topSlice.map(([member]) => member));

      // Re-emit leaf keys in original sort order for winning members only.
      for (const key of group) {
        if (winnerMembers.has(key[dimIdx]!)) kept.push(key);
      }
    }
    result = kept;
  }
  return result;
}

/**
 * Apply value-predicate filters to a sorted key list.
 *
 * Filtering is **per-parent**: for each member the predicate is evaluated
 * against its aggregated `by` measure at grand-total column context.
 * Members with null measure values fail the predicate and are excluded.
 */
function _applyValueFilters(
  data: PivotData,
  keys: string[][],
  filters: ValueFilter[],
  dimList: string[],
  axis: "rows" | "columns",
): string[][] {
  if (!filters.length) return keys;

  let result = keys;
  for (const f of filters) {
    const dimIdx = dimList.indexOf(f.field);
    if (dimIdx < 0) continue;

    result = result.filter((key) => {
      const val = _getMeasureValue(data, key.slice(0, dimIdx + 1), f.by, axis);
      if (val === null) return false; // null fails all predicates
      switch (f.operator) {
        case "gt":
          return val > f.value;
        case "gte":
          return val >= f.value;
        case "lt":
          return val < f.value;
        case "lte":
          return val <= f.value;
        case "eq":
          return val === f.value;
        case "neq":
          return val !== f.value;
        case "between":
          return val >= f.value && val <= (f.value2 ?? f.value);
        default:
          return true;
      }
    });
  }
  return result;
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

  /** Sorted, filtered dimension-only row keys (no values-axis encoding). */
  private _sortedDimRowKeys: string[][] | null = null;
  /** Public-facing expanded row keys (with __vf__ encoding when values_axis="rows"). */
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
  /** Columns present in the data source — used to guard _shouldIncludeRow in hybrid mode. */
  private readonly _columnSet: ReadonlySet<string>;

  /** Merged column types (Arrow + Python supplement). */
  private readonly _columnTypes: ColumnTypeMap | undefined;
  /** Cached display labels: field -> canonicalKey -> formatted label. */
  private readonly _displayLabelMap: Map<string, Map<string, string>> =
    new Map();
  /** Original typed raw values: field -> canonicalKey -> original Arrow value. */
  private readonly _rawValueMap: Map<string, Map<string, unknown>> = new Map();

  /** Sidecar lookup maps (null if stale or absent). */
  private _hybridGrand: Map<string, number | null> | null = null;
  private _hybridRowTotals: Map<string, number | null> | null = null;
  private _hybridColTotals: Map<string, number | null> | null = null;
  private _hybridColPrefix: Map<string, number | null> | null = null;
  private _hybridColPrefixGrand: Map<string, number | null> | null = null;
  private _hybridSubtotals: Map<string, number | null> | null = null;
  private _hybridCrossSubtotals: Map<string, number | null> | null = null;
  private _hybridTemporalParent: Map<string, number | null> | null = null;
  private _hybridTemporalParentGrand: Map<string, number | null> | null = null;
  private _hybridTemporalRowParent: Map<string, number | null> | null = null;
  private _hybridTemporalRowParentGrand: Map<string, number | null> | null =
    null;

  private readonly _compiledFormulas: Map<string, CompiledFormula>;
  private readonly _formulaErrors: Map<string, string>;
  private readonly _formulaScope: Record<string, number | null> = {};

  private static readonly NULL_AGGREGATOR: Aggregator = Object.freeze({
    push: () => undefined,
    value: () => null,
    count: () => 0,
    format: (emptyCellValue: string) => emptyCellValue,
  });

  constructor(
    input: DataRecord[] | ColumnarDataSource,
    config: PivotConfigV1,
    options?: PivotDataOptions,
  ) {
    this._config = config;
    this._options = options ?? {};
    this._columnTypes = options?.columnTypes;
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

    this._compiledFormulas = new Map();
    this._formulaErrors = new Map();
    for (const m of config.synthetic_measures ?? []) {
      if (m.operation === "formula" && m.formula) {
        try {
          this._compiledFormulas.set(m.id, compileFormula(m.formula));
        } catch (e) {
          this._formulaErrors.set(
            m.id,
            e instanceof FormulaParseError ? e.message : String(e),
          );
        }
      } else if (m.operation === "formula") {
        this._formulaErrors.set(m.id, "Formula is empty");
      }
    }

    const syntheticSourceFields = getSyntheticSourceFields(
      config.synthetic_measures,
    );
    this._allAggregatedFields = [
      ...new Set([...config.values, ...syntheticSourceFields]),
    ];

    const dataColumnSet = new Set(this._dataSource.getColumnNames());
    for (const [id, compiled] of this._compiledFormulas) {
      if (this._formulaErrors.has(id)) continue;
      const unknownRefs = compiled.fieldRefs.filter(
        (ref) => !dataColumnSet.has(ref),
      );
      if (unknownRefs.length > 0) {
        this._formulaErrors.set(
          id,
          `Unknown field(s): ${unknownRefs.map((r) => `"${r}"`).join(", ")}`,
        );
      }
    }

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
      ...(config.filter_fields ?? []),
      ...Object.keys(config.filters ?? {}),
    ]);
    this._columnSet = new Set(this._dataSource.getColumnNames());

    const hybridTotals = options?.hybridTotals;
    if (hybridTotals) {
      const localFp = buildSidecarFingerprint(
        config,
        options?.nullHandling,
        options?.adaptiveDateGrains,
      );
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

        if (hybridTotals.temporal_parent) {
          this._hybridTemporalParent = new Map();
          for (const entry of hybridTotals.temporal_parent) {
            const rowKeyStr = makeKeyString(entry.row ?? []);
            const colStr = makeKeyString(entry.col);
            for (const [f, v] of Object.entries(entry.values)) {
              this._hybridTemporalParent.set(
                `${rowKeyStr}\x01${colStr}\x01${f}`,
                v,
              );
            }
          }
        }

        if (hybridTotals.temporal_parent_grand) {
          this._hybridTemporalParentGrand = new Map();
          for (const entry of hybridTotals.temporal_parent_grand) {
            const colStr = makeKeyString(entry.col);
            for (const [f, v] of Object.entries(entry.values)) {
              this._hybridTemporalParentGrand.set(`${colStr}\x01${f}`, v);
            }
          }
        }
        if (hybridTotals.temporal_row_parent) {
          this._hybridTemporalRowParent = new Map();
          for (const entry of hybridTotals.temporal_row_parent) {
            const rowStr = makeKeyString(entry.row);
            const colStr = makeKeyString(entry.col ?? []);
            for (const [f, v] of Object.entries(entry.values)) {
              this._hybridTemporalRowParent.set(
                `${rowStr}\x01${colStr}\x01${f}`,
                v,
              );
            }
          }
        }
        if (hybridTotals.temporal_row_parent_grand) {
          this._hybridTemporalRowParentGrand = new Map();
          for (const entry of hybridTotals.temporal_row_parent_grand) {
            const rowStr = makeKeyString(entry.row);
            for (const [f, v] of Object.entries(entry.values)) {
              this._hybridTemporalRowParentGrand.set(`${rowStr}\x01${f}`, v);
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

  private _emptyAggregatorForField(valField: string): Aggregator {
    return getAggregatorFactory(
      getAggregationForField(valField, this._config),
    ).create();
  }

  // ---------------------------------------------------------------------------
  // Filtering
  // ---------------------------------------------------------------------------

  private _shouldIncludeRow(rowIndex: number): boolean {
    const filters = this._config.filters;
    if (!filters) return true;
    for (const [field, filter] of Object.entries(filters)) {
      // In hybrid mode, off-axis filter fields are absent from the pre-aggregated
      // frame (they were applied server-side). Skip them to prevent double-filtering
      // which would otherwise exclude all rows.
      if (!this._columnSet.has(field)) continue;
      const raw = this._dataSource.getValue(rowIndex, field);
      const val = this._resolveDimKey(field, raw);
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

  private _dateGrainForField(field: string): DateGrain | undefined {
    return getEffectiveDateGrain(
      this._config,
      field,
      this._columnTypes?.get(field),
      this._options.adaptiveDateGrains?.[field],
    );
  }

  private _compareDimKeys(_field: string, a: string, b: string): number {
    return mixedCompare(a, b);
  }

  private _resolveDimKey(field: string, raw: unknown): string {
    if (raw == null || raw === "") {
      const mode = getNullMode(field, this._options.nullHandling);
      return mode === "separate" ? "(null)" : "";
    }
    const colType = this._columnTypes?.get(field);
    if (isTemporalColumnType(colType)) {
      const grain = this._dateGrainForField(field);
      const canonical = bucketTemporalKey(raw, colType, grain);
      if (canonical !== null) {
        // Cache the original raw value for export
        if (!this._rawValueMap.has(field)) {
          this._rawValueMap.set(field, new Map());
        }
        const fieldRawMap = this._rawValueMap.get(field)!;
        if (!fieldRawMap.has(canonical)) {
          fieldRawMap.set(canonical, raw);
        }
        return canonical;
      }
    }
    return String(raw);
  }

  /**
   * Compute a formatted display label for a dimension value.
   * Uses the config's dimension_format if available, otherwise falls back
   * to auto-formatting based on column type.
   */
  formatDimLabel(field: string, key: string): string {
    if (!key || key === "(null)") return key;

    // Check cache first
    const fieldCache = this._displayLabelMap.get(field);
    if (fieldCache) {
      const cached = fieldCache.get(key);
      if (cached !== undefined) return cached;
    }

    const colType = this._columnTypes?.get(field);
    let label = key;

    // Check for explicit dimension_format pattern
    const pattern =
      this._config.dimension_format?.[field] ??
      this._config.dimension_format?.["__all__"];
    const grain = this._dateGrainForField(field);

    if (grain && isTemporalColumnType(colType)) {
      label = formatTemporalBucketLabel(key, grain, pattern);
    } else if (pattern && (colType === "datetime" || colType === "date")) {
      label = formatDateWithPattern(key, pattern);
    } else if (colType === "datetime") {
      const rawVal = this._rawValueMap.get(field)?.get(key);
      label = formatDateTimeValue(rawVal ?? key);
    } else if (colType === "date") {
      const rawVal = this._rawValueMap.get(field)?.get(key);
      label = formatDateValue(rawVal ?? key);
    } else if (colType === "integer") {
      const looksLikeYearField = /(^|[_\s])years?($|[_\s])/i.test(field);
      const looksLikeYearValue = /^\d{4}$/.test(key);
      label =
        looksLikeYearField && looksLikeYearValue
          ? key
          : formatIntegerLabel(key);
    }

    // Cache the label
    if (!this._displayLabelMap.has(field)) {
      this._displayLabelMap.set(field, new Map());
    }
    this._displayLabelMap.get(field)!.set(key, label);
    return label;
  }

  /**
   * Get the original typed raw value for a dimension key (e.g. epoch ms for timestamps).
   * Returns undefined if not available.
   */
  getRawDimValue(field: string, key: string): unknown {
    return this._rawValueMap.get(field)?.get(key);
  }

  /** Get the semantic column type for a field, or undefined if unknown. */
  getColumnType(field: string): ColumnType | undefined {
    return this._columnTypes?.get(field);
  }

  /** Get the full column type map. */
  getColumnTypes(): ColumnTypeMap | undefined {
    return this._columnTypes;
  }

  private _resolveAggValue(field: string, raw: unknown): unknown {
    if (raw == null || (typeof raw === "number" && Number.isNaN(raw))) {
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
    getAgg: (field: string) => number | null,
  ): number | null {
    if (synthetic.operation === "formula") {
      const compiled = this._compiledFormulas.get(synthetic.id);
      if (!compiled) return null;
      const scope = this._formulaScope;
      for (const key in scope) delete scope[key];
      for (const ref of compiled.fieldRefs) {
        scope[ref] = getAgg(ref);
      }
      return compiled.evaluate(scope);
    }
    const numerator = getSum(synthetic.numerator);
    const denominator = getSum(synthetic.denominator);
    if (numerator == null || denominator == null) return null;
    if (synthetic.operation === "sum_over_sum") {
      return denominator === 0 ? null : numerator / denominator;
    }
    return numerator - denominator;
  }

  getFormulaErrors(): ReadonlyMap<string, string> {
    return this._formulaErrors;
  }

  private _fixedAggregator(value: number | null): Aggregator {
    if (value == null) return PivotData.NULL_AGGREGATOR;
    return {
      push: () => undefined,
      value: () => value,
      count: () => 1,
      format: () => formatNumber(value),
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
        const resolved = this._resolveDimKey(
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
        this._resolveDimKey(r, this._dataSource.getValue(recordIndex, r)),
      );
      const colKey = this._config.columns.map((c) =>
        this._resolveDimKey(c, this._dataSource.getValue(recordIndex, c)),
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
          [...values].sort((a, b) => this._compareDimKeys(field, a, b)),
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
    /**
     * When true, the value-sort path returns 0 (instead of falling back to key
     * order) when two parent-level subtotals are equal or both null. Set to true
     * for all non-final comparators in a chained sort so that ties propagate to
     * the next comparator rather than being resolved by this one's key fallback.
     */
    suppressKeyFallback?: boolean,
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
              return suppressKeyFallback
                ? 0
                : mixedCompare(a[level], b[level]) * levelSign;
            if (va == null) return 1;
            if (vb == null) return -1;
            if (va !== vb) return (va - vb) * levelSign;
            // Equal numeric subtotals: let secondary sort decide if suppressed.
            if (suppressKeyFallback) return 0;
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
          if (cmp === 0) cmp = this._compareDimKeys(dim ?? "", ai, bi);
        } else {
          cmp = this._compareDimKeys(dim ?? "", ai, bi);
        }
        const levelSign = targetDimIdx === -1 || i === targetDimIdx ? sign : 1;
        if (cmp !== 0) return cmp * levelSign;
      }
      return 0;
    };
  }

  /**
   * Normalize a row_sort or col_sort value (single SortConfig or array) to a
   * SortConfig array. Returns an empty array when the config is absent.
   */
  private _normalizeSortConfigs(
    sc: SortConfig | SortConfig[] | undefined,
  ): SortConfig[] {
    if (!sc) return [];
    return Array.isArray(sc) ? sc : [sc];
  }

  /**
   * Build a single comparator that chains the supplied sort configs in priority
   * order: the first config is primary, subsequent configs break ties. Passing
   * an empty array returns a stable no-op comparator (keys are left in
   * insertion order, i.e. whatever getRowKeys() / getColKeys() produces after
   * applying the default `undefined` comparator).
   *
   * For non-final configs in the chain, `suppressKeyFallback=true` is passed to
   * `_buildKeyComparator` so that equal parent-level subtotals return 0 rather
   * than being resolved by an implicit key comparison. This ensures that the
   * secondary (or tertiary, …) comparator actually runs when the primary produces
   * a tie, rather than the primary's own key fallback silently resolving it.
   */
  private _buildChainedComparator(
    sortConfigs: SortConfig[],
    dimensions: string[],
    isRowAxis: boolean,
  ): (a: string[], b: string[]) => number {
    if (sortConfigs.length === 0) {
      return this._buildKeyComparator(undefined, dimensions, () => null);
    }
    const lastIdx = sortConfigs.length - 1;
    const comparators = sortConfigs.map((sc, idx) => {
      const isLast = idx === lastIdx;
      const valueGetter = isRowAxis
        ? this._rowValueGetter(sc)
        : this._colValueGetter(sc);
      const subtotalGetter =
        isRowAxis && dimensions.length >= 2 && sc.by === "value"
          ? (prefix: string[]) => {
              const valField = sc.value_field ?? this._defaultValueField();
              return this.getSubtotalAggregator(
                prefix,
                sc.col_key ?? [],
                valField,
              ).value();
            }
          : undefined;
      return this._buildKeyComparator(
        sc,
        dimensions,
        valueGetter,
        subtotalGetter,
        !isLast, // suppressKeyFallback for all non-final comparators
      );
    });
    if (comparators.length === 1) return comparators[0];
    return (a, b) => {
      for (const cmp of comparators) {
        const result = cmp(a, b);
        if (result !== 0) return result;
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

  /**
   * Internal: sorted + filtered dimension-only row keys (no values-axis encoding).
   * Used by getGroupedRowKeys() for grouping/collapsing logic, and by the public
   * getRowKeys() which may expand with value field segments on top.
   */
  private _getDimRowKeys(): string[][] {
    if (!this._sortedDimRowKeys) {
      const rowSortConfigs = this._normalizeSortConfigs(this._config.row_sort);
      const cmp = this._buildChainedComparator(
        rowSortConfigs,
        this._config.rows,
        true,
      );
      let sorted = [...this._rowKeySet.values()].sort(cmp);

      // Apply Top N and value filters (display-only; totals are unaffected).
      const rowTopN = (this._config.top_n_filters ?? []).filter(
        (f) => (f.axis ?? "rows") === "rows",
      );
      const rowValueFilters = (this._config.value_filters ?? []).filter(
        (f) => (f.axis ?? "rows") === "rows",
      );
      if (rowTopN.length > 0)
        sorted = _applyTopNFilters(
          this,
          sorted,
          rowTopN,
          this._config.rows,
          "rows",
        );
      if (rowValueFilters.length > 0)
        sorted = _applyValueFilters(
          this,
          sorted,
          rowValueFilters,
          this._config.rows,
          "rows",
        );

      this._sortedDimRowKeys = sorted;
    }
    return this._sortedDimRowKeys;
  }

  getRowKeys(): string[][] {
    if (!this._sortedRowKeys) {
      const dimKeys = this._getDimRowKeys();
      if (this._config.values_axis !== "rows") {
        this._sortedRowKeys = dimKeys;
      } else {
        const valueFields = getRenderedValueFields(this._config);
        this._sortedRowKeys = dimKeys.flatMap((key) =>
          valueFields.map((vf) => [...key, encodeValueFieldSegment(vf)]),
        );
      }
    }
    return this._sortedRowKeys;
  }

  getColKeys(): string[][] {
    if (!this._sortedColKeys) {
      const colSortConfigs = this._normalizeSortConfigs(this._config.col_sort);
      const cmp = this._buildChainedComparator(
        colSortConfigs,
        this._config.columns,
        false,
      );
      let sorted = [...this._colKeySet.values()].sort(cmp);

      // Apply Top N and value filters on the column axis.
      const colTopN = (this._config.top_n_filters ?? []).filter(
        (f) => f.axis === "columns",
      );
      const colValueFilters = (this._config.value_filters ?? []).filter(
        (f) => f.axis === "columns",
      );
      if (colTopN.length > 0)
        sorted = _applyTopNFilters(
          this,
          sorted,
          colTopN,
          this._config.columns,
          "columns",
        );
      if (colValueFilters.length > 0)
        sorted = _applyValueFilters(
          this,
          sorted,
          colValueFilters,
          this._config.columns,
          "columns",
        );

      this._sortedColKeys = sorted;
    }
    return this._sortedColKeys;
  }

  getPeriodComparisonAxis(): {
    axis: "row" | "col";
    field: string;
    index: number;
    grain: DateGrain;
  } | null {
    const columnCandidate = this._config.columns.findIndex((field) => {
      const colType = this._columnTypes?.get(field);
      return isTemporalColumnType(colType) && !!this._dateGrainForField(field);
    });
    if (columnCandidate !== -1) {
      const field = this._config.columns[columnCandidate]!;
      return {
        axis: "col",
        field,
        index: columnCandidate,
        grain: this._dateGrainForField(field)!,
      };
    }
    const rowCandidate = this._config.rows.findIndex((field) => {
      const colType = this._columnTypes?.get(field);
      return isTemporalColumnType(colType) && !!this._dateGrainForField(field);
    });
    if (rowCandidate !== -1) {
      const field = this._config.rows[rowCandidate]!;
      return {
        axis: "row",
        field,
        index: rowCandidate,
        grain: this._dateGrainForField(field)!,
      };
    }
    return null;
  }

  getPreviousComparableKey(
    axis: "row" | "col",
    key: string[],
    mode:
      | "diff_from_prev"
      | "pct_diff_from_prev"
      | "diff_from_prev_year"
      | "pct_diff_from_prev_year",
  ): string[] | null {
    const ctx = this.getPeriodComparisonAxis();
    if (!ctx || ctx.axis !== axis) return null;
    const currentBucket = key[ctx.index];
    if (!currentBucket) return null;
    const strategy =
      mode === "diff_from_prev_year" || mode === "pct_diff_from_prev_year"
        ? "previous_year"
        : "previous";
    const previousBucket = shiftTemporalBucketKey(
      currentBucket,
      ctx.grain,
      strategy,
    );
    if (!previousBucket) return null;
    const nextKey = [...key];
    nextKey[ctx.index] = previousBucket;
    return nextKey;
  }

  computePeriodComparisonValue(
    current: number | null,
    previous: number | null,
    mode:
      | "diff_from_prev"
      | "pct_diff_from_prev"
      | "diff_from_prev_year"
      | "pct_diff_from_prev_year",
  ): number | null {
    if (current == null || previous == null) return null;
    if (mode === "diff_from_prev" || mode === "diff_from_prev_year") {
      return current - previous;
    }
    if (previous === 0) return null;
    return (current - previous) / previous;
  }

  getCellComparisonValue(
    rowKey: string[],
    colKey: string[],
    valField: string,
    mode:
      | "diff_from_prev"
      | "pct_diff_from_prev"
      | "diff_from_prev_year"
      | "pct_diff_from_prev_year",
  ): number | null {
    const ctx = this.getPeriodComparisonAxis();
    if (!ctx) return null;
    const current = this.getAggregator(rowKey, colKey, valField).value();
    if (ctx.axis === "col") {
      const prevColKey = this.getPreviousComparableKey("col", colKey, mode);
      if (!prevColKey) return null;
      const previous = this.getAggregator(rowKey, prevColKey, valField).value();
      return this.computePeriodComparisonValue(current, previous, mode);
    }
    const prevRowKey = this.getPreviousComparableKey("row", rowKey, mode);
    if (!prevRowKey) return null;
    const previous = this.getAggregator(prevRowKey, colKey, valField).value();
    return this.computePeriodComparisonValue(current, previous, mode);
  }

  getRowTotalComparisonValue(
    rowKey: string[],
    valField: string,
    mode:
      | "diff_from_prev"
      | "pct_diff_from_prev"
      | "diff_from_prev_year"
      | "pct_diff_from_prev_year",
  ): number | null {
    const ctx = this.getPeriodComparisonAxis();
    if (!ctx || ctx.axis !== "row") return null;
    const prevRowKey = this.getPreviousComparableKey("row", rowKey, mode);
    if (!prevRowKey) return null;
    const current = this.getRowTotal(rowKey, valField).value();
    const previous = this.getRowTotal(prevRowKey, valField).value();
    return this.computePeriodComparisonValue(current, previous, mode);
  }

  getColTotalComparisonValue(
    colKey: string[],
    valField: string,
    mode:
      | "diff_from_prev"
      | "pct_diff_from_prev"
      | "diff_from_prev_year"
      | "pct_diff_from_prev_year",
  ): number | null {
    const ctx = this.getPeriodComparisonAxis();
    if (!ctx || ctx.axis !== "col") return null;
    const prevColKey = this.getPreviousComparableKey("col", colKey, mode);
    if (!prevColKey) return null;
    const current = this.getColTotal(colKey, valField).value();
    const previous = this.getColTotal(prevColKey, valField).value();
    return this.computePeriodComparisonValue(current, previous, mode);
  }

  getSubtotalComparisonValue(
    parentKey: string[],
    colKey: string[],
    valField: string,
    mode:
      | "diff_from_prev"
      | "pct_diff_from_prev"
      | "diff_from_prev_year"
      | "pct_diff_from_prev_year",
  ): number | null {
    const ctx = this.getPeriodComparisonAxis();
    if (!ctx) return null;
    const current = this.getSubtotalAggregator(
      parentKey,
      colKey,
      valField,
    ).value();
    if (ctx.axis === "col") {
      if (ctx.index >= colKey.length) return null;
      const prevColKey = this.getPreviousComparableKey("col", colKey, mode);
      if (!prevColKey) return null;
      const previous = this.getSubtotalAggregator(
        parentKey,
        prevColKey,
        valField,
      ).value();
      return this.computePeriodComparisonValue(current, previous, mode);
    }
    if (ctx.index >= parentKey.length) return null;
    const prevParentKey = this.getPreviousComparableKey("row", parentKey, mode);
    if (!prevParentKey) return null;
    const previous = this.getSubtotalAggregator(
      prevParentKey,
      colKey,
      valField,
    ).value();
    return this.computePeriodComparisonValue(current, previous, mode);
  }

  getColGroupComparisonValue(
    rowKey: string[],
    colPrefix: string[],
    valField: string,
    mode:
      | "diff_from_prev"
      | "pct_diff_from_prev"
      | "diff_from_prev_year"
      | "pct_diff_from_prev_year",
  ): number | null {
    const ctx = this.getPeriodComparisonAxis();
    if (!ctx || ctx.axis !== "col" || ctx.index >= colPrefix.length)
      return null;
    const prevPrefix = this.getPreviousComparableKey("col", colPrefix, mode);
    if (!prevPrefix) return null;
    const current =
      rowKey.length === 0
        ? this.getColGroupGrandSubtotal(colPrefix, valField).value()
        : this.getSubtotalColGroupAgg(rowKey, colPrefix, valField).value();
    const previous =
      rowKey.length === 0
        ? this.getColGroupGrandSubtotal(prevPrefix, valField).value()
        : this.getSubtotalColGroupAgg(rowKey, prevPrefix, valField).value();
    return this.computePeriodComparisonValue(current, previous, mode);
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
      const prefix = `${makeKeyString(rowKey)}\x01${makeKeyString(colKey)}\x01`;
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => this._cellSumAggs.get(prefix + sf)?.value() ?? null,
        (sf) => this._cellAggs.get(prefix + sf)?.value() ?? null,
      );
      return this._fixedAggregator(value);
    }
    const keyStr = `${makeKeyString(rowKey)}\x01${makeKeyString(colKey)}\x01${field}`;
    const agg = this._cellAggs.get(keyStr);
    if (!agg) {
      return this._emptyAggregatorForField(field);
    }
    return agg;
  }

  getRowTotal(rowKey: string[], valField?: string): Aggregator {
    const field = valField ?? this._defaultValueField();
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const prefix = `${makeKeyString(rowKey)}\x01`;
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => this._rowTotalSumAggs.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridRowTotals?.get(prefix + sf) ??
          this._rowTotalAggs.get(prefix + sf)?.value() ??
          null,
      );
      return this._fixedAggregator(value);
    }
    const keyStr = `${makeKeyString(rowKey)}\x01${field}`;
    if (this._hybridRowTotals?.has(keyStr)) {
      return this._fixedAggregator(this._hybridRowTotals.get(keyStr)!);
    }
    const agg = this._rowTotalAggs.get(keyStr);
    if (!agg) {
      return this._emptyAggregatorForField(field);
    }
    return agg;
  }

  getColTotal(colKey: string[], valField?: string): Aggregator {
    const field = valField ?? this._defaultValueField();
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const prefix = `${makeKeyString(colKey)}\x01`;
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => this._colTotalSumAggs.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridColTotals?.get(prefix + sf) ??
          this._colTotalAggs.get(prefix + sf)?.value() ??
          null,
      );
      return this._fixedAggregator(value);
    }
    const keyStr = `${makeKeyString(colKey)}\x01${field}`;
    if (this._hybridColTotals?.has(keyStr)) {
      return this._fixedAggregator(this._hybridColTotals.get(keyStr)!);
    }
    const agg = this._colTotalAggs.get(keyStr);
    if (!agg) {
      return this._emptyAggregatorForField(field);
    }
    return agg;
  }

  /**
   * Return the sorted leaf row keys in current display order (after sort, excluding total rows).
   *
   * When `values_axis="rows"`, returns `[...dimSegments, "__vf__:<fieldId>"]` encoded keys
   * so that running total / rank transforms operate on the same encoded keys that appear in
   * the table. Callers responsible for aggregate lookups must strip via `getValueFieldForRowKey`
   * and `rowKey.slice(0, -1)` before calling engine aggregate methods.
   */
  getSortedLeafRowKeys(): string[][] {
    return this.getRowKeys();
  }

  /**
   * Return the subtotal for the immediate parent of `rowKey` at the given `colKey`
   * and `valField`. The "parent" is `rowKey.slice(0, -1)`.
   *
   * - If `rowKey` is a top-level key (length 1), the parent is the root — return the
   *   column total (`getColTotal(colKey, valField)`), i.e. the same as a single-level
   *   pivot's denominator for `pct_running_total`.
   * - If `rowKey` is empty, return null.
   */
  getParentSubtotal(
    rowKey: string[],
    colKey: string[],
    valField: string,
  ): number | null {
    if (rowKey.length === 0) return null;
    const parentKey = rowKey.slice(0, -1);
    if (parentKey.length === 0) {
      // Top-level row: parent is the column grand total
      return this.getColTotal(colKey, valField).value();
    }
    return this.getSubtotalAggregator(parentKey, colKey, valField).value();
  }

  getGrandTotal(valField?: string): Aggregator {
    const field = valField ?? this._defaultValueField();
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => this._grandTotalSumAggs.get(sf)?.value() ?? null,
        (sf) =>
          this._hybridGrand?.get(sf) ??
          this._grandTotalAggs.get(sf)?.value() ??
          null,
      );
      return this._fixedAggregator(value);
    }
    if (this._hybridGrand?.has(field)) {
      return this._fixedAggregator(this._hybridGrand.get(field)!);
    }
    const agg = this._grandTotalAggs.get(field);
    if (!agg) {
      return this._emptyAggregatorForField(field);
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
        this._resolveDimKey(r, this._dataSource.getValue(recordIndex, r)),
      );
      const colKey = this._config.columns.map((c) =>
        this._resolveDimKey(c, this._dataSource.getValue(recordIndex, c)),
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
      const prefix = `${parentKeyStr}\x01${colKeyStr}\x01`;
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => this._subtotalSumAggs?.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridSubtotals?.get(prefix + sf) ??
          this._subtotalAggs?.get(prefix + sf)?.value() ??
          null,
      );
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
    // Use dimension-only keys for grouping/collapsing logic; value-field expansion
    // happens at the END of this method so subtotal prefix keys are computed correctly.
    const rowKeys = this._getDimRowKeys();

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

    const subtotalsVal =
      this._config.row_layout === "hierarchy" && rowDims.length >= 2
        ? true
        : this._config.show_subtotals;
    if (rowDims.length < 2 || !subtotalsVal) {
      const plain = rowKeys.map((key) => ({
        type: "data" as const,
        key,
        level: rowDims.length - 1,
      }));
      if (this._config.values_axis === "rows") {
        const valueFields = getRenderedValueFields(this._config);
        return plain.flatMap((entry) =>
          valueFields.map((vf) => ({
            ...entry,
            key: [...entry.key, encodeValueFieldSegment(vf)],
          })),
        );
      }
      return plain;
    }

    const subtotalEnabledAtLevel = (lvl: number): boolean => {
      if (subtotalsVal === true) return true;
      if (Array.isArray(subtotalsVal))
        return subtotalsVal.includes(rowDims[lvl]);
      return false;
    };

    const result: GroupedRow[] = [];
    const numLevels = rowDims.length;
    const subtotalAtTop =
      (this._config.subtotal_position ?? "bottom") === "top";

    if (subtotalAtTop) {
      // "top" mode: emit the subtotal row BEFORE the group members it summarises.
      // Detect group starts by comparing the current key's prefix at each non-leaf
      // level against the previous key's prefix. On the very first key every level
      // is a "new group", so all applicable subtotal headers are emitted first.
      let prevKey: string[] = [];
      for (let i = 0; i < rowKeys.length; i++) {
        const currentKey = rowKeys[i]!;

        // Check if any ancestor group is collapsed — hides this data row.
        let hidden = false;
        for (let lvl = 0; lvl < numLevels - 1; lvl++) {
          if (collapsed.has(makeKeyString(currentKey.slice(0, lvl + 1)))) {
            hidden = true;
            break;
          }
        }

        // Detect new groups starting at each non-leaf level (shallowest first).
        for (let lvl = 0; lvl < numLevels - 1; lvl++) {
          const currentPrefix = currentKey.slice(0, lvl + 1);
          const prevPrefix = prevKey.slice(0, lvl + 1);
          const isNewGroup =
            i === 0 ||
            makeKeyString(currentPrefix) !== makeKeyString(prevPrefix);
          if (!isNewGroup) continue;

          // Emit the subtotal header unless an ancestor group is collapsed.
          let ancestorCollapsed = false;
          for (let parentLvl = 0; parentLvl < lvl; parentLvl++) {
            if (
              collapsed.has(makeKeyString(currentKey.slice(0, parentLvl + 1)))
            ) {
              ancestorCollapsed = true;
              break;
            }
          }
          if (!ancestorCollapsed && subtotalEnabledAtLevel(lvl)) {
            result.push({ type: "subtotal", key: currentPrefix, level: lvl });
          }
        }

        if (!hidden) {
          result.push({ type: "data", key: currentKey, level: numLevels - 1 });
        }

        prevKey = currentKey;
      }
    } else {
      // "bottom" mode (default): emit data rows first, subtotals after each group.
      for (let i = 0; i < rowKeys.length; i++) {
        const currentKey = rowKeys[i]!;
        const nextKey: string[] | undefined = rowKeys[i + 1];

        // Check if any ancestor group is collapsed — if so, skip this data row.
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
            // if this specific group is collapsed, but not if a parent is collapsed.
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
    }

    // Expand with value field pseudo-segments for values_axis="rows".
    if (this._config.values_axis === "rows") {
      const valueFields = getRenderedValueFields(this._config);
      return result.flatMap((entry) =>
        valueFields.map((vf) => ({
          ...entry,
          key: [...entry.key, encodeValueFieldSegment(vf)],
        })),
      );
    }

    return result;
  }

  /**
   * Get hierarchy-style row entries with parent groups rendered BEFORE children.
   * Collapsed groups keep the parent row visible and hide descendants.
   */
  getHierarchyRowKeys(ignoreCollapsed?: boolean): GroupedRow[] {
    const rowDims = this._config.rows;
    // Use dimension-only keys for grouping; expand at end like getGroupedRowKeys.
    const rowKeys = this._getDimRowKeys();

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

    if (rowDims.length === 0) {
      return [{ type: "data", key: [], level: 0 }];
    }

    const result: GroupedRow[] = [];
    const numLevels = rowDims.length;

    const visit = (
      startIdx: number,
      endIdx: number,
      level: number,
      prefix: string[],
    ): void => {
      if (startIdx >= endIdx) return;
      if (level >= numLevels - 1) {
        for (let i = startIdx; i < endIdx; i++) {
          result.push({ type: "data", key: rowKeys[i]!, level: numLevels - 1 });
        }
        return;
      }

      let idx = startIdx;
      while (idx < endIdx) {
        const currentValue = rowKeys[idx]![level] ?? "";
        const groupPrefix = [...prefix, currentValue];
        let end = idx + 1;
        while (end < endIdx && rowKeys[end]![level] === currentValue) {
          end++;
        }

        result.push({ type: "subtotal", key: groupPrefix, level });
        if (!collapsed.has(makeKeyString(groupPrefix))) {
          visit(idx, end, level + 1, groupPrefix);
        }
        idx = end;
      }
    };

    visit(0, rowKeys.length, 0, []);

    // Expand with value field pseudo-segments for values_axis="rows".
    if (this._config.values_axis === "rows") {
      const valueFields = getRenderedValueFields(this._config);
      return result.flatMap((entry) =>
        valueFields.map((vf) => ({
          ...entry,
          key: [...entry.key, encodeValueFieldSegment(vf)],
        })),
      );
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
  /** Temporal parent aggregators (rowKey × modifiedColKey × valField). */
  private _temporalSubtotalAggs: Map<string, Aggregator> | null = null;
  /** Temporal parent aggregators on the row axis (modifiedRowKey × colKey × valField). */
  private _temporalRowSubtotalAggs: Map<string, Aggregator> | null = null;

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
        this._resolveDimKey(r, this._dataSource.getValue(recordIndex, r)),
      );
      const fullColKey = colDims.map((c) =>
        this._resolveDimKey(c, this._dataSource.getValue(recordIndex, c)),
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
      const prefix = `${rowKeyStr}\x01${colPrefixStr}\x01`;
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => this._colSubtotalSumAggs?.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridColPrefix?.get(prefix + sf) ??
          this._colSubtotalAggs?.get(prefix + sf)?.value() ??
          null,
      );
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
    const emptyKeyStr = makeKeyString([]);
    const field = valField;
    const synthetic = this._syntheticById.get(field);
    if (synthetic) {
      const aggs = this._buildColSubtotalAggs();
      void aggs;
      const prefix = `${emptyKeyStr}\x01${colPrefixStr}\x01`;
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => this._colSubtotalSumAggs?.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridColPrefixGrand?.get(`${colPrefixStr}\x01${sf}`) ??
          this._colSubtotalAggs?.get(prefix + sf)?.value() ??
          null,
      );
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
      const prefix = `${parentKeyStr}\x01${colPrefixStr}\x01`;
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => this._crossSubtotalSumAggs?.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridCrossSubtotals?.get(prefix + sf) ??
          this._crossSubtotalAggs?.get(prefix + sf)?.value() ??
          null,
      );
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
  // Temporal hierarchy subtotals (collapsed parent aggregation)
  // ---------------------------------------------------------------------------

  /**
   * Build temporal subtotal aggregators lazily from raw records.
   * Uses modified column keys (sibling segments preserved, temporal segment
   * replaced with "tp:{field}:{parentBucket}") for correct per-instance
   * aggregation in multi-dimension column layouts.
   */
  private _buildTemporalSubtotalAggs(): Map<string, Aggregator> {
    if (this._temporalSubtotalAggs) return this._temporalSubtotalAggs;
    this._temporalSubtotalAggs = new Map();

    const colDims = this._config.columns;
    const rowDims = this._config.rows;
    if (colDims.length === 0) return this._temporalSubtotalAggs;

    const temporalCols: {
      index: number;
      field: string;
      grain: DateGrain;
    }[] = [];
    for (let i = 0; i < colDims.length; i++) {
      const f = colDims[i]!;
      const ct = this._columnTypes?.get(f);
      if (!ct || !isTemporalColumnType(ct)) continue;
      const grain = getEffectiveDateGrain(
        this._config,
        f,
        ct,
        this._options.adaptiveDateGrains?.[f],
      );
      if (!grain || grain === "year") continue;
      temporalCols.push({ index: i, field: f, grain });
    }
    if (temporalCols.length === 0) return this._temporalSubtotalAggs;

    const emptyRowKeyStr = makeKeyString([]);
    const valueArrays = this._getValueArrays();
    const numRows = this._dataSource.numRows;

    for (let recordIndex = 0; recordIndex < numRows; recordIndex++) {
      if (!this._shouldIncludeRow(recordIndex)) continue;

      const rowKey = rowDims.map((r) =>
        this._resolveDimKey(r, this._dataSource.getValue(recordIndex, r)),
      );
      const fullColKey = colDims.map((c) =>
        this._resolveDimKey(c, this._dataSource.getValue(recordIndex, c)),
      );
      const rowKeyStr = makeKeyString(rowKey);

      for (const tc of temporalCols) {
        const leafKey = fullColKey[tc.index]!;
        const parentBuckets = extractParentBuckets(leafKey, tc.grain);

        for (const parentBucket of parentBuckets) {
          const modifiedColKey = buildModifiedColKey(
            fullColKey,
            tc.index,
            tc.field,
            parentBucket,
          );
          const modifiedColKeyStr = makeKeyString(modifiedColKey);

          for (const valField of this._allAggregatedFields) {
            const rawVal = this._rawAggValue(
              recordIndex,
              valField,
              valueArrays,
            );

            const cellKey = `${rowKeyStr}\x01${modifiedColKeyStr}\x01${valField}`;
            let cellAgg = this._temporalSubtotalAggs.get(cellKey);
            if (!cellAgg) {
              cellAgg = this._factoryForField(valField).create();
              this._temporalSubtotalAggs.set(cellKey, cellAgg);
            }
            this._pushAggValue(cellAgg, valField, rawVal);

            const grandKey = `${emptyRowKeyStr}\x01${modifiedColKeyStr}\x01${valField}`;
            let grandAgg = this._temporalSubtotalAggs.get(grandKey);
            if (!grandAgg) {
              grandAgg = this._factoryForField(valField).create();
              this._temporalSubtotalAggs.set(grandKey, grandAgg);
            }
            this._pushAggValue(grandAgg, valField, rawVal);
          }
        }
      }
    }
    return this._temporalSubtotalAggs;
  }

  /**
   * Get the temporal parent aggregate for a specific row and modified column key.
   * Prioritizes hybrid (Python-precomputed) values, falls back to client-side.
   */
  getTemporalColSubtotal(
    rowKey: string[],
    modifiedColKey: string[],
    valField: string,
  ): Aggregator {
    const synthetic = this._syntheticById.get(valField);
    if (synthetic) {
      const rowKeyStr = makeKeyString(rowKey);
      const modifiedColKeyStr = makeKeyString(modifiedColKey);
      const prefix = `${rowKeyStr}\x01${modifiedColKeyStr}\x01`;
      const aggs = this._buildTemporalSubtotalAggs();
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => aggs.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridTemporalParent?.get(prefix + sf) ??
          aggs.get(prefix + sf)?.value() ??
          null,
      );
      return this._fixedAggregator(value);
    }
    const rowKeyStr = makeKeyString(rowKey);
    const modifiedColKeyStr = makeKeyString(modifiedColKey);
    const lookupKey = `${rowKeyStr}\x01${modifiedColKeyStr}\x01${valField}`;

    if (this._hybridTemporalParent?.has(lookupKey)) {
      return this._fixedAggregator(this._hybridTemporalParent.get(lookupKey)!);
    }
    const aggs = this._buildTemporalSubtotalAggs();
    return aggs.get(lookupKey) ?? this._factoryForField(valField).create();
  }

  /**
   * Get the temporal parent aggregate for the grand total row.
   */
  getTemporalColSubtotalGrand(
    modifiedColKey: string[],
    valField: string,
  ): Aggregator {
    const synthetic = this._syntheticById.get(valField);
    if (synthetic) {
      const modifiedColKeyStr = makeKeyString(modifiedColKey);
      const prefix = `${makeKeyString([])}\x01${modifiedColKeyStr}\x01`;
      const aggs = this._buildTemporalSubtotalAggs();
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => aggs.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridTemporalParentGrand?.get(
            `${modifiedColKeyStr}\x01${sf}`,
          ) ??
          aggs.get(prefix + sf)?.value() ??
          null,
      );
      return this._fixedAggregator(value);
    }
    const modifiedColKeyStr = makeKeyString(modifiedColKey);
    const lookupKey = `${modifiedColKeyStr}\x01${valField}`;

    if (this._hybridTemporalParentGrand?.has(lookupKey)) {
      return this._fixedAggregator(
        this._hybridTemporalParentGrand.get(lookupKey)!,
      );
    }
    const aggs = this._buildTemporalSubtotalAggs();
    const key = `${makeKeyString([])}\x01${modifiedColKeyStr}\x01${valField}`;
    return aggs.get(key) ?? this._factoryForField(valField).create();
  }

  private _buildTemporalRowSubtotalAggs(): Map<string, Aggregator> {
    if (this._temporalRowSubtotalAggs) return this._temporalRowSubtotalAggs;
    this._temporalRowSubtotalAggs = new Map();

    const rowDims = this._config.rows;
    const colDims = this._config.columns;
    if (rowDims.length === 0) return this._temporalRowSubtotalAggs;

    const temporalCols: {
      index: number;
      field: string;
      grain: DateGrain;
    }[] = [];
    for (let i = 0; i < colDims.length; i++) {
      const f = colDims[i]!;
      const ct = this._columnTypes?.get(f);
      if (!ct || !isTemporalColumnType(ct)) continue;
      const grain = getEffectiveDateGrain(
        this._config,
        f,
        ct,
        this._options.adaptiveDateGrains?.[f],
      );
      if (!grain || grain === "year") continue;
      temporalCols.push({ index: i, field: f, grain });
    }

    const temporalRows: {
      index: number;
      field: string;
      grain: DateGrain;
    }[] = [];
    for (let i = 0; i < rowDims.length; i++) {
      const f = rowDims[i]!;
      const ct = this._columnTypes?.get(f);
      if (!ct || !isTemporalColumnType(ct)) continue;
      const grain = getEffectiveDateGrain(
        this._config,
        f,
        ct,
        this._options.adaptiveDateGrains?.[f],
      );
      if (!grain || grain === "year") continue;
      temporalRows.push({ index: i, field: f, grain });
    }
    if (temporalRows.length === 0) return this._temporalRowSubtotalAggs;

    const valueArrays = this._getValueArrays();
    const numRows = this._dataSource.numRows;
    for (let recordIndex = 0; recordIndex < numRows; recordIndex++) {
      if (!this._shouldIncludeRow(recordIndex)) continue;

      const fullRowKey = rowDims.map((r) =>
        this._resolveDimKey(r, this._dataSource.getValue(recordIndex, r)),
      );
      const colKey = colDims.map((c) =>
        this._resolveDimKey(c, this._dataSource.getValue(recordIndex, c)),
      );
      const colKeyStr = makeKeyString(colKey);

      for (const tr of temporalRows) {
        const leafKey = fullRowKey[tr.index]!;
        const parentBuckets = extractParentBuckets(leafKey, tr.grain);

        for (const parentBucket of parentBuckets) {
          const modifiedRowKey = buildModifiedRowKey(
            fullRowKey,
            tr.index,
            tr.field,
            parentBucket,
          ).slice(0, tr.index + 1);
          const modifiedRowKeyStr = makeKeyString(modifiedRowKey);

          for (const valField of this._allAggregatedFields) {
            const rawVal = this._rawAggValue(
              recordIndex,
              valField,
              valueArrays,
            );

            const cellKey = `${modifiedRowKeyStr}\x01${colKeyStr}\x01${valField}`;
            let cellAgg = this._temporalRowSubtotalAggs.get(cellKey);
            if (!cellAgg) {
              cellAgg = this._factoryForField(valField).create();
              this._temporalRowSubtotalAggs.set(cellKey, cellAgg);
            }
            this._pushAggValue(cellAgg, valField, rawVal);

            const totalKey = `${modifiedRowKeyStr}\x01\x01${valField}`;
            let totalAgg = this._temporalRowSubtotalAggs.get(totalKey);
            if (!totalAgg) {
              totalAgg = this._factoryForField(valField).create();
              this._temporalRowSubtotalAggs.set(totalKey, totalAgg);
            }
            this._pushAggValue(totalAgg, valField, rawVal);

            for (let level = 0; level < colDims.length - 1; level++) {
              const colPrefix = colKey.slice(0, level + 1);
              const colPrefixKey = `${modifiedRowKeyStr}\x01${makeKeyString(colPrefix)}\x01${valField}`;
              let prefixAgg = this._temporalRowSubtotalAggs.get(colPrefixKey);
              if (!prefixAgg) {
                prefixAgg = this._factoryForField(valField).create();
                this._temporalRowSubtotalAggs.set(colPrefixKey, prefixAgg);
              }
              this._pushAggValue(prefixAgg, valField, rawVal);
            }

            for (const tc of temporalCols) {
              const leafColKey = colKey[tc.index]!;
              const parentBuckets = extractParentBuckets(leafColKey, tc.grain);
              for (const parentBucket of parentBuckets) {
                const modifiedColKey = buildModifiedColKey(
                  colKey,
                  tc.index,
                  tc.field,
                  parentBucket,
                );
                const modifiedColLookupKey = `${modifiedRowKeyStr}\x01${makeKeyString(modifiedColKey)}\x01${valField}`;
                let modifiedColAgg =
                  this._temporalRowSubtotalAggs.get(modifiedColLookupKey);
                if (!modifiedColAgg) {
                  modifiedColAgg = this._factoryForField(valField).create();
                  this._temporalRowSubtotalAggs.set(
                    modifiedColLookupKey,
                    modifiedColAgg,
                  );
                }
                this._pushAggValue(modifiedColAgg, valField, rawVal);
              }
            }
          }
        }
      }
    }
    return this._temporalRowSubtotalAggs;
  }

  getTemporalRowSubtotal(
    modifiedRowKey: string[],
    colKey: string[],
    valField: string,
  ): Aggregator {
    const synthetic = this._syntheticById.get(valField);
    if (synthetic) {
      const modifiedRowKeyStr = makeKeyString(modifiedRowKey);
      const colKeyStr = makeKeyString(colKey);
      const prefix = `${modifiedRowKeyStr}\x01${colKeyStr}\x01`;
      const aggs = this._buildTemporalRowSubtotalAggs();
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => aggs.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridTemporalRowParent?.get(prefix + sf) ??
          aggs.get(prefix + sf)?.value() ??
          null,
      );
      return this._fixedAggregator(value);
    }
    const modifiedRowKeyStr = makeKeyString(modifiedRowKey);
    const colKeyStr = makeKeyString(colKey);
    const lookupKey = `${modifiedRowKeyStr}\x01${colKeyStr}\x01${valField}`;

    if (this._hybridTemporalRowParent?.has(lookupKey)) {
      return this._fixedAggregator(
        this._hybridTemporalRowParent.get(lookupKey)!,
      );
    }
    const aggs = this._buildTemporalRowSubtotalAggs();
    return aggs.get(lookupKey) ?? this._factoryForField(valField).create();
  }

  getTemporalRowSubtotalGrand(
    modifiedRowKey: string[],
    valField: string,
  ): Aggregator {
    const synthetic = this._syntheticById.get(valField);
    if (synthetic) {
      const modifiedRowKeyStr = makeKeyString(modifiedRowKey);
      const prefix = `${modifiedRowKeyStr}\x01\x01`;
      const aggs = this._buildTemporalRowSubtotalAggs();
      const value = this._evaluateSynthetic(
        synthetic,
        (sf) => aggs.get(prefix + sf)?.value() ?? null,
        (sf) =>
          this._hybridTemporalRowParentGrand?.get(
            `${modifiedRowKeyStr}\x01${sf}`,
          ) ??
          aggs.get(prefix + sf)?.value() ??
          null,
      );
      return this._fixedAggregator(value);
    }
    const modifiedRowKeyStr = makeKeyString(modifiedRowKey);
    const lookupKey = `${modifiedRowKeyStr}\x01${valField}`;

    if (this._hybridTemporalRowParentGrand?.has(lookupKey)) {
      return this._fixedAggregator(
        this._hybridTemporalRowParentGrand.get(lookupKey)!,
      );
    }
    const aggs = this._buildTemporalRowSubtotalAggs();
    const key = `${modifiedRowKeyStr}\x01\x01${valField}`;
    return aggs.get(key) ?? this._factoryForField(valField).create();
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
          this._resolveDimKey(field, this._dataSource.getValue(i, field)),
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
        const recordVal = this._resolveDimKey(
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
