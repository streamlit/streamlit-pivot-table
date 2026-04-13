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
 * Pivot Table -- Config Schema v1 & Event Payload Contracts
 *
 * This file is the single source of truth for the data shapes that cross
 * the Python ↔ TypeScript boundary.  Both sides must agree on these types.
 *
 * COMPATIBILITY POLICY:
 *   - Adding an optional field is NOT a breaking change.
 *   - Removing a field, renaming a field, or changing a field's type IS breaking.
 *   - Breaking changes require a version bump (v1 → v2) and a migration
 *     function (migrateV1toV2).
 *   - Snapshot tests lock the serialised form of both PivotConfig and
 *     CellClickPayload.  Any structural change will fail CI until the
 *     snapshots are intentionally updated.
 */

// ---------------------------------------------------------------------------
// Config schema v1
// ---------------------------------------------------------------------------

export const CONFIG_SCHEMA_VERSION = 1;

export interface PivotConfigV1 {
  version: 1;
  rows: string[];
  columns: string[];
  values: string[];
  /** Auto-apply a default hierarchy to temporal fields placed on rows/columns. */
  auto_date_hierarchy?: boolean;
  /** Per-field temporal grain override. Null explicitly opts out to Original/raw values. */
  date_grains?: Record<string, DateGrain | null>;
  synthetic_measures?: SyntheticMeasureConfig[];
  aggregation: AggregationConfig;
  show_totals: boolean;
  /** Independent row totals toggle. bool = all/none, string[] = only listed measures. */
  show_row_totals?: boolean | string[];
  /** Independent column totals toggle. bool = all/none, string[] = only listed measures. */
  show_column_totals?: boolean | string[];
  empty_cell_value: string;
  interactive: boolean;
  /** Phase 2: per-dimension value filters (include/exclude lists). */
  filters?: Record<string, DimensionFilter>;
  /** Phase 2: row sort config. */
  row_sort?: SortConfig;
  /** Phase 2: column sort config. */
  col_sort?: SortConfig;
  /** Phase 3a: show subtotal rows. bool = all/none dims, string[] = only listed dims. */
  show_subtotals?: boolean | string[];
  /** Phase 3a: repeat row labels on every row instead of spanning. */
  repeat_row_labels?: boolean;
  /** Phase 3a: serialized group keys currently collapsed (managed by frontend). */
  collapsed_groups?: string[];
  /** Phase 3b: display mode per value field (raw, % of total/row/col). */
  show_values_as?: Record<string, ShowValuesAs>;
  /** Phase 3c: conditional formatting rules applied to value cells. */
  conditional_formatting?: ConditionalFormatRule[];
  /** Phase 3a: serialized column group keys currently collapsed. */
  collapsed_col_groups?: string[];
  /** Per-field collapsed temporal parent groups (stringified modified column keys). */
  collapsed_temporal_groups?: Record<string, string[]>;
  /** Phase 3d: toggle sticky (fixed) headers on/off. Default true. */
  sticky_headers?: boolean;
  /** Phase 3d: per-field number format patterns (e.g. {"Revenue": "$,.0f"}). */
  number_format?: Record<string, string>;
  /** Per-field dimension format patterns for date/datetime labels. */
  dimension_format?: Record<string, string>;
  /** Phase 3d: per-field alignment override. */
  column_alignment?: Record<string, "left" | "center" | "right">;
}

export type SyntheticOperation = "sum_over_sum" | "difference";

export interface SyntheticMeasureConfig {
  id: string;
  label: string;
  operation: SyntheticOperation;
  numerator: string;
  denominator: string;
  format?: string;
}

export interface DimensionFilter {
  include?: string[];
  exclude?: string[];
}

// ---------------------------------------------------------------------------
// Sort configuration
// ---------------------------------------------------------------------------

export interface SortConfig {
  /** "key" = alphabetical on labels, "value" = by aggregated measure */
  by: "key" | "value";
  direction: "asc" | "desc";
  /** Required when by="value": which value field to sort by */
  value_field?: string;
  /** Required when by="value" for row_sort: which column key to sort within */
  col_key?: string[];
  /** Which dimension the sort was triggered from (key sort only applies direction at this level). */
  dimension?: string;
}

/**
 * Legacy string type kept for backward compatibility with old configs.
 * `migrateSortDirection()` converts these to `SortConfig`.
 */
export type LegacySortDirection =
  | "key_asc"
  | "key_desc"
  | "value_asc"
  | "value_desc";

/** Convert a legacy SortDirection string to the new SortConfig object. */
export function migrateSortDirection(dir: LegacySortDirection): SortConfig {
  switch (dir) {
    case "key_asc":
      return { by: "key", direction: "asc" };
    case "key_desc":
      return { by: "key", direction: "desc" };
    case "value_asc":
      return { by: "value", direction: "asc" };
    case "value_desc":
      return { by: "value", direction: "desc" };
  }
}

/** Type guard: is the value a legacy string sort direction? */
function isLegacySortDirection(v: unknown): v is LegacySortDirection {
  return (
    typeof v === "string" &&
    ["key_asc", "key_desc", "value_asc", "value_desc"].includes(v)
  );
}

/** Validate and normalize a raw sort config value (handles both legacy strings and objects). */
function validateSortConfig(
  raw: unknown,
  fieldName: string,
): SortConfig | undefined {
  if (raw === undefined) return undefined;
  if (isLegacySortDirection(raw)) return migrateSortDirection(raw);
  if (typeof raw === "object" && raw !== null && !Array.isArray(raw)) {
    const o = raw as Record<string, unknown>;
    if (o.by !== "key" && o.by !== "value") {
      throw new Error(`'${fieldName}.by' must be "key" or "value"`);
    }
    if (o.direction !== "asc" && o.direction !== "desc") {
      throw new Error(`'${fieldName}.direction' must be "asc" or "desc"`);
    }
    const sc: SortConfig = { by: o.by, direction: o.direction };
    if (o.value_field !== undefined) {
      if (typeof o.value_field !== "string")
        throw new Error(`'${fieldName}.value_field' must be a string`);
      sc.value_field = o.value_field;
    }
    if (o.col_key !== undefined) {
      if (
        !Array.isArray(o.col_key) ||
        !o.col_key.every((v: unknown) => typeof v === "string")
      ) {
        throw new Error(`'${fieldName}.col_key' must be an array of strings`);
      }
      sc.col_key = o.col_key as string[];
    }
    if (o.dimension !== undefined) {
      if (typeof o.dimension !== "string")
        throw new Error(`'${fieldName}.dimension' must be a string`);
      sc.dimension = o.dimension;
    }
    return sc;
  }
  throw new Error(
    `'${fieldName}' must be a SortConfig object or a legacy sort direction string`,
  );
}

export type AggregationType =
  | "sum"
  | "avg"
  | "count"
  | "min"
  | "max"
  | "count_distinct"
  | "median"
  | "percentile_90"
  | "first"
  | "last";

export const AGGREGATION_TYPES: readonly AggregationType[] = [
  "sum",
  "avg",
  "count",
  "min",
  "max",
  "count_distinct",
  "median",
  "percentile_90",
  "first",
  "last",
] as const;

export type AggregationConfig = Record<string, AggregationType>;

export const DEFAULT_AGGREGATION: AggregationType = "sum";

function isAggregationType(value: unknown): value is AggregationType {
  return AGGREGATION_TYPES.includes(value as AggregationType);
}

export function normalizeAggregationConfig(
  raw: unknown,
  values: string[],
): AggregationConfig {
  if (raw === undefined || raw === null) {
    return Object.fromEntries(
      values.map((field) => [field, DEFAULT_AGGREGATION]),
    ) as AggregationConfig;
  }

  if (typeof raw === "string") {
    if (!isAggregationType(raw)) {
      throw new Error(
        `'aggregation' must be one of: ${AGGREGATION_TYPES.join(", ")}`,
      );
    }
    return Object.fromEntries(
      values.map((field) => [field, raw]),
    ) as AggregationConfig;
  }

  if (typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error(
      "'aggregation' must be an aggregation type or an object keyed by raw value fields",
    );
  }

  const provided = raw as Record<string, unknown>;
  return Object.fromEntries(
    values.map((field) => {
      const entry = provided[field];
      if (entry === undefined) return [field, DEFAULT_AGGREGATION];
      if (!isAggregationType(entry)) {
        throw new Error(
          `'aggregation["${field}"]' must be one of: ${AGGREGATION_TYPES.join(", ")}`,
        );
      }
      return [field, entry];
    }),
  ) as AggregationConfig;
}

export function getAggregationForField(
  valField: string,
  config: PivotConfigV1,
): AggregationType {
  return config.aggregation[valField] ?? DEFAULT_AGGREGATION;
}

export function stringifyPivotConfig(config: PivotConfigV1): string {
  return JSON.stringify({
    ...config,
    auto_date_hierarchy: config.auto_date_hierarchy !== false,
    aggregation: normalizeAggregationConfig(config.aggregation, config.values),
    date_grains: config.date_grains
      ? Object.fromEntries(
          Object.entries(config.date_grains).sort(([a], [b]) =>
            a.localeCompare(b),
          ),
        )
      : undefined,
    collapsed_temporal_groups: config.collapsed_temporal_groups
      ? Object.fromEntries(
          Object.entries(config.collapsed_temporal_groups)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([k, v]) => [k, [...v].sort()]),
        )
      : undefined,
  });
}

// ---------------------------------------------------------------------------
// Show-values-as display mode (Phase 3b)
// ---------------------------------------------------------------------------

export type ShowValuesAs =
  | "raw"
  | "pct_of_total"
  | "pct_of_row"
  | "pct_of_col"
  | "diff_from_prev"
  | "pct_diff_from_prev"
  | "diff_from_prev_year"
  | "pct_diff_from_prev_year";

export type PeriodComparisonMode =
  | "diff_from_prev"
  | "pct_diff_from_prev"
  | "diff_from_prev_year"
  | "pct_diff_from_prev_year";

export type DateGrain = "year" | "quarter" | "month" | "week" | "day";
export type DateGrainOverride = DateGrain | null;
export type TemporalGroupingMode = "auto" | "explicit" | "original" | "none";

export const DATE_GRAINS: readonly DateGrain[] = [
  "year",
  "quarter",
  "month",
  "week",
  "day",
] as const;

export const DATE_GRAIN_LABELS: Record<DateGrain, string> = {
  year: "Year",
  quarter: "Quarter",
  month: "Month",
  week: "Week",
  day: "Day",
};

export const AUTO_DATE_HIERARCHY_DEFAULT = true;
export const DEFAULT_AUTO_DATE_GRAIN: DateGrain = "month";
export const DEFAULT_DATE_DRILL_SEQUENCE: readonly DateGrain[] = [
  "year",
  "quarter",
  "month",
  "day",
] as const;

/**
 * Returns the hierarchy levels (outermost to leaf) for a given effective grain.
 * Year grain produces a flat [year] (no hierarchy).
 * Quarter skipped for day grain to cap at 3 levels.
 */
export function getTemporalHierarchyLevels(grain: DateGrain): DateGrain[] {
  switch (grain) {
    case "year":
      return ["year"];
    case "quarter":
      return ["year", "quarter"];
    case "month":
      return ["year", "quarter", "month"];
    case "day":
      return ["year", "month", "day"];
    case "week":
      return ["year", "week"];
    default:
      return [grain];
  }
}

// ---------------------------------------------------------------------------
// Conditional formatting (Phase 3c)
// ---------------------------------------------------------------------------

export interface ConditionalFormatRule {
  type: "color_scale" | "data_bars" | "threshold";
  /** Which value fields this rule applies to. Empty array = all fields. */
  apply_to: string[];
  /** Also apply to subtotal/grand total cells? Default false. */
  include_totals?: boolean;
}

export interface ColorScaleRule extends ConditionalFormatRule {
  type: "color_scale";
  min_color: string;
  max_color: string;
  mid_color?: string;
}

export interface DataBarsRule extends ConditionalFormatRule {
  type: "data_bars";
  color?: string;
  fill: "gradient" | "solid";
}

export interface ThresholdCondition {
  operator: "gt" | "gte" | "lt" | "lte" | "eq" | "between";
  value: number;
  value2?: number;
  background?: string;
  color?: string;
  bold?: boolean;
}

export interface ThresholdRule extends ConditionalFormatRule {
  type: "threshold";
  conditions: ThresholdCondition[];
}

export const DEFAULT_CONFIG: PivotConfigV1 = {
  version: CONFIG_SCHEMA_VERSION,
  rows: [],
  columns: [],
  values: [],
  auto_date_hierarchy: AUTO_DATE_HIERARCHY_DEFAULT,
  synthetic_measures: [],
  aggregation: {},
  show_totals: true,
  show_row_totals: true,
  show_column_totals: true,
  empty_cell_value: "-",
  interactive: true,
};

/**
 * Normalize a `boolean | string[]` config field. Filters string[] to only
 * valid members, then normalizes: empty -> false, all items -> true.
 */
function normalizeBoolOrList(
  raw: unknown,
  validItems: string[],
  fallback: boolean | string[],
): boolean | string[] {
  if (typeof raw === "boolean") return raw;
  if (Array.isArray(raw)) {
    const filtered = (raw as unknown[]).filter(
      (v): v is string => typeof v === "string" && validItems.includes(v),
    );
    if (filtered.length === 0) return false;
    if (filtered.length === validItems.length) return true;
    return filtered;
  }
  return fallback;
}

function validateSyntheticMeasures(
  raw: unknown,
  allColumns?: string[],
): SyntheticMeasureConfig[] {
  if (raw === undefined) return [];
  if (!Array.isArray(raw)) {
    throw new Error("'synthetic_measures' must be an array");
  }
  const seenIds = new Set<string>();
  const seenLabels = new Set<string>();
  const validOps = new Set<SyntheticOperation>(["sum_over_sum", "difference"]);
  const measures: SyntheticMeasureConfig[] = [];
  for (let i = 0; i < raw.length; i++) {
    const m = raw[i];
    if (typeof m !== "object" || m === null || Array.isArray(m)) {
      throw new Error(`'synthetic_measures[${i}]' must be an object`);
    }
    const obj = m as Record<string, unknown>;
    const id = obj.id;
    const label = obj.label;
    const operation = obj.operation;
    const numerator = obj.numerator;
    const denominator = obj.denominator;
    if (typeof id !== "string" || id.trim() === "") {
      throw new Error(
        `'synthetic_measures[${i}].id' must be a non-empty string`,
      );
    }
    if (typeof label !== "string" || label.trim() === "") {
      throw new Error(
        `'synthetic_measures[${i}].label' must be a non-empty string`,
      );
    }
    if (
      typeof operation !== "string" ||
      !validOps.has(operation as SyntheticOperation)
    ) {
      throw new Error(
        `'synthetic_measures[${i}].operation' must be "sum_over_sum" or "difference"`,
      );
    }
    if (typeof numerator !== "string" || numerator.trim() === "") {
      throw new Error(
        `'synthetic_measures[${i}].numerator' must be a non-empty field name`,
      );
    }
    if (typeof denominator !== "string" || denominator.trim() === "") {
      throw new Error(
        `'synthetic_measures[${i}].denominator' must be a non-empty field name`,
      );
    }
    if (allColumns && allColumns.length > 0) {
      if (!allColumns.includes(numerator)) {
        throw new Error(
          `'synthetic_measures[${i}].numerator' must be a valid source field`,
        );
      }
      if (!allColumns.includes(denominator)) {
        throw new Error(
          `'synthetic_measures[${i}].denominator' must be a valid source field`,
        );
      }
    }
    if (seenIds.has(id)) {
      throw new Error(`Duplicate synthetic measure id: "${id}"`);
    }
    if (seenLabels.has(label)) {
      throw new Error(`Duplicate synthetic measure label: "${label}"`);
    }
    seenIds.add(id);
    seenLabels.add(label);
    measures.push({
      id,
      label,
      operation: operation as SyntheticOperation,
      numerator,
      denominator,
      format: typeof obj.format === "string" ? obj.format : undefined,
    });
  }
  return measures;
}

export function getSyntheticMeasureMap(
  config: PivotConfigV1,
): Map<string, SyntheticMeasureConfig> {
  return new Map((config.synthetic_measures ?? []).map((m) => [m.id, m]));
}

export function getRenderedValueFields(config: PivotConfigV1): string[] {
  const syntheticIds = (config.synthetic_measures ?? []).map((m) => m.id);
  return [...config.values, ...syntheticIds];
}

export function getRenderedValueLabel(
  config: PivotConfigV1,
  valueField: string,
): string {
  const synthetic = getSyntheticMeasureMap(config).get(valueField);
  return synthetic?.label ?? valueField;
}

export function isSyntheticMeasure(
  config: PivotConfigV1,
  valueField: string,
): boolean {
  return getSyntheticMeasureMap(config).has(valueField);
}

export function getDimensionLabel(
  config: PivotConfigV1,
  field: string,
  columnType?: ColumnType,
  adaptiveGrain?: DateGrain,
): string {
  const grain = getEffectiveDateGrain(config, field, columnType, adaptiveGrain);
  if (!grain) return field;
  return `${field} (${DATE_GRAIN_LABELS[grain]})`;
}

export function getExplicitDateGrain(
  config: PivotConfigV1,
  field: string,
): DateGrainOverride | undefined {
  return config.date_grains?.[field];
}

export function isTemporalAxisField(
  config: PivotConfigV1,
  field: string,
): boolean {
  return config.rows.includes(field) || config.columns.includes(field);
}

export function getTemporalGroupingMode(
  config: PivotConfigV1,
  field: string,
  columnType?: ColumnType,
): TemporalGroupingMode {
  const explicit = getExplicitDateGrain(config, field);
  if (explicit === null) return "original";
  if (explicit !== undefined) return "explicit";
  if (
    config.auto_date_hierarchy !== false &&
    isTemporalAxisField(config, field) &&
    (columnType === "date" || columnType === "datetime")
  ) {
    return "auto";
  }
  return "none";
}

export function getEffectiveDateGrain(
  config: PivotConfigV1,
  field: string,
  columnType?: ColumnType,
  adaptiveGrain?: DateGrain,
): DateGrain | undefined {
  const explicit = getExplicitDateGrain(config, field);
  if (explicit === null) return undefined;
  if (explicit !== undefined) return explicit;
  if (
    config.auto_date_hierarchy !== false &&
    isTemporalAxisField(config, field) &&
    (columnType === "date" || columnType === "datetime")
  ) {
    return adaptiveGrain ?? DEFAULT_AUTO_DATE_GRAIN;
  }
  return undefined;
}

export function getDateDrillSequence(
  grain: DateGrain | undefined,
): readonly DateGrain[] | undefined {
  if (!grain) return DEFAULT_DATE_DRILL_SEQUENCE;
  return DEFAULT_DATE_DRILL_SEQUENCE.includes(grain)
    ? DEFAULT_DATE_DRILL_SEQUENCE
    : undefined;
}

export function getDrilledDateGrain(
  grain: DateGrain | undefined,
  direction: "up" | "down",
): DateGrain | undefined {
  const sequence = getDateDrillSequence(grain);
  if (!sequence) return undefined;
  const current = grain ?? DEFAULT_AUTO_DATE_GRAIN;
  const idx = sequence.indexOf(current);
  if (idx === -1) return undefined;
  const nextIdx = direction === "up" ? idx - 1 : idx + 1;
  return nextIdx >= 0 && nextIdx < sequence.length
    ? sequence[nextIdx]
    : undefined;
}

export function isPeriodComparisonMode(
  mode: ShowValuesAs | undefined,
): mode is PeriodComparisonMode {
  return (
    mode === "diff_from_prev" ||
    mode === "pct_diff_from_prev" ||
    mode === "diff_from_prev_year" ||
    mode === "pct_diff_from_prev_year"
  );
}

export function getPeriodComparisonMode(
  config: PivotConfigV1,
  valueField: string,
): PeriodComparisonMode | undefined {
  const mode = config.show_values_as?.[valueField];
  return isPeriodComparisonMode(mode) ? mode : undefined;
}

function hasGroupedTemporalComparisonAxis(
  config: PivotConfigV1,
  columnTypes?: ColumnTypeMap,
  adaptiveDateGrains?: Record<string, DateGrain>,
): boolean {
  return [...config.rows, ...config.columns].some((field) => {
    const columnType = columnTypes?.get(field);
    return (
      (columnType === "date" || columnType === "datetime") &&
      !!getEffectiveDateGrain(
        config,
        field,
        columnType,
        adaptiveDateGrains?.[field],
      )
    );
  });
}

export function validatePivotConfigRuntime(
  config: PivotConfigV1,
  columnTypes?: ColumnTypeMap,
  adaptiveDateGrains?: Record<string, DateGrain>,
): PivotConfigV1 {
  const hasPeriodMode = Object.values(config.show_values_as ?? {}).some(
    (mode) => isPeriodComparisonMode(mode),
  );
  if (
    hasPeriodMode &&
    !hasGroupedTemporalComparisonAxis(config, columnTypes, adaptiveDateGrains)
  ) {
    throw new Error(
      "period comparison show_values_as modes require a grouped date/datetime field on rows or columns",
    );
  }
  return config;
}

export function getSyntheticMeasureFormat(
  config: PivotConfigV1,
  valueField: string,
): string | undefined {
  return getSyntheticMeasureMap(config).get(valueField)?.format;
}

/**
 * Runtime validation for imported JSON configs.
 * Returns the config if valid, or throws with a descriptive message.
 */
export function validatePivotConfigV1(obj: unknown): PivotConfigV1 {
  if (typeof obj !== "object" || obj === null) {
    throw new Error("Config must be a JSON object");
  }
  const o = obj as Record<string, unknown>;
  if (o.version !== 1) throw new Error("Config version must be 1");

  for (const field of ["rows", "columns", "values"] as const) {
    if (!Array.isArray(o[field]))
      throw new Error(`'${field}' must be an array`);
    if (!(o[field] as unknown[]).every((v) => typeof v === "string")) {
      throw new Error(`'${field}' must contain only strings`);
    }
  }

  if (o.show_totals !== undefined && typeof o.show_totals !== "boolean") {
    throw new Error("'show_totals' must be a boolean");
  }
  if (o.interactive !== undefined && typeof o.interactive !== "boolean") {
    throw new Error("'interactive' must be a boolean");
  }
  if (
    o.auto_date_hierarchy !== undefined &&
    typeof o.auto_date_hierarchy !== "boolean"
  ) {
    throw new Error("'auto_date_hierarchy' must be a boolean");
  }
  if (
    o.empty_cell_value !== undefined &&
    typeof o.empty_cell_value !== "string"
  ) {
    throw new Error("'empty_cell_value' must be a string");
  }

  if (
    o.filters !== undefined &&
    (typeof o.filters !== "object" ||
      o.filters === null ||
      Array.isArray(o.filters))
  ) {
    throw new Error(
      "'filters' must be an object mapping field names to {include?, exclude?}",
    );
  }

  const rowSort = validateSortConfig(o.row_sort, "row_sort");
  const colSort = validateSortConfig(o.col_sort, "col_sort");

  const showTotals =
    typeof o.show_totals === "boolean"
      ? o.show_totals
      : DEFAULT_CONFIG.show_totals;
  const rows = o.rows as string[];
  const columns = o.columns as string[];
  const values = o.values as string[];
  const syntheticMeasures = validateSyntheticMeasures(o.synthetic_measures);

  const result: PivotConfigV1 = {
    version: 1,
    rows,
    columns,
    values,
    auto_date_hierarchy:
      typeof o.auto_date_hierarchy === "boolean"
        ? o.auto_date_hierarchy
        : AUTO_DATE_HIERARCHY_DEFAULT,
    synthetic_measures: syntheticMeasures,
    aggregation: normalizeAggregationConfig(o.aggregation, values),
    show_totals: showTotals,
    show_row_totals: normalizeBoolOrList(o.show_row_totals, values, showTotals),
    show_column_totals: normalizeBoolOrList(
      o.show_column_totals,
      values,
      showTotals,
    ),
    empty_cell_value:
      typeof o.empty_cell_value === "string"
        ? o.empty_cell_value
        : DEFAULT_CONFIG.empty_cell_value,
    interactive:
      typeof o.interactive === "boolean"
        ? o.interactive
        : DEFAULT_CONFIG.interactive,
  };
  if (
    o.date_grains !== undefined &&
    typeof o.date_grains === "object" &&
    o.date_grains !== null &&
    !Array.isArray(o.date_grains)
  ) {
    const rawDateGrains = o.date_grains as Record<string, unknown>;
    const normalizedDateGrains = Object.fromEntries(
      Object.entries(rawDateGrains).sort(([a], [b]) => a.localeCompare(b)),
    );
    for (const [field, grain] of Object.entries(normalizedDateGrains)) {
      if (typeof field !== "string") {
        throw new Error("'date_grains' keys must be strings");
      }
      if (grain !== null && !DATE_GRAINS.includes(grain as DateGrain)) {
        throw new Error(
          `'date_grains["${field}"]' must be one of: ${DATE_GRAINS.join(", ")}, or null`,
        );
      }
    }
    result.date_grains = normalizedDateGrains as Record<
      string,
      DateGrain | null
    >;
  }
  if (o.filters !== undefined)
    result.filters = o.filters as Record<string, DimensionFilter>;
  if (rowSort) result.row_sort = rowSort;
  if (colSort) result.col_sort = colSort;
  const subtotals = normalizeBoolOrList(
    o.show_subtotals,
    rows.slice(0, -1),
    false,
  );
  if (subtotals !== false) result.show_subtotals = subtotals;
  if (typeof o.repeat_row_labels === "boolean")
    result.repeat_row_labels = o.repeat_row_labels;
  if (Array.isArray(o.collapsed_groups))
    result.collapsed_groups = o.collapsed_groups as string[];
  if (Array.isArray(o.collapsed_col_groups))
    result.collapsed_col_groups = o.collapsed_col_groups as string[];
  if (
    o.collapsed_temporal_groups &&
    typeof o.collapsed_temporal_groups === "object" &&
    !Array.isArray(o.collapsed_temporal_groups)
  )
    result.collapsed_temporal_groups = o.collapsed_temporal_groups as Record<
      string,
      string[]
    >;
  if (typeof o.sticky_headers === "boolean")
    result.sticky_headers = o.sticky_headers;
  if (
    o.show_values_as !== undefined &&
    typeof o.show_values_as === "object" &&
    !Array.isArray(o.show_values_as)
  ) {
    const VALID_SHOW_AS = new Set<string>([
      "raw",
      "pct_of_total",
      "pct_of_row",
      "pct_of_col",
      "diff_from_prev",
      "pct_diff_from_prev",
      "diff_from_prev_year",
      "pct_diff_from_prev_year",
    ]);
    const syntheticIds = new Set(syntheticMeasures.map((m) => m.id));
    const sva = o.show_values_as as Record<string, unknown>;
    for (const [k, v] of Object.entries(sva)) {
      if (syntheticIds.has(k)) {
        continue;
      }
      if (typeof v !== "string" || !VALID_SHOW_AS.has(v)) {
        throw new Error(
          `'show_values_as["${k}"]' must be one of: ${[...VALID_SHOW_AS].join(", ")}`,
        );
      }
    }
    result.show_values_as = Object.fromEntries(
      Object.entries(sva).filter(([k]) => !syntheticIds.has(k)),
    ) as Record<string, ShowValuesAs>;
  }
  if (Array.isArray(o.conditional_formatting)) {
    const VALID_CF_TYPES = new Set<string>([
      "color_scale",
      "data_bars",
      "threshold",
    ]);
    const cfRules = o.conditional_formatting as Record<string, unknown>[];
    for (let i = 0; i < cfRules.length; i++) {
      const rule = cfRules[i];
      if (typeof rule !== "object" || rule === null || Array.isArray(rule)) {
        throw new Error(`'conditional_formatting[${i}]' must be an object`);
      }
      const ruleType = (rule as Record<string, unknown>).type;
      if (typeof ruleType !== "string" || !VALID_CF_TYPES.has(ruleType)) {
        throw new Error(
          `'conditional_formatting[${i}].type' must be one of: ${[...VALID_CF_TYPES].join(", ")}`,
        );
      }
      const applyTo = (rule as Record<string, unknown>).apply_to;
      if (
        applyTo !== undefined &&
        (!Array.isArray(applyTo) ||
          !applyTo.every((v: unknown) => typeof v === "string"))
      ) {
        throw new Error(
          `'conditional_formatting[${i}].apply_to' must be an array of strings`,
        );
      }
    }
    result.conditional_formatting =
      cfRules as unknown as ConditionalFormatRule[];
  }
  if (
    o.number_format !== undefined &&
    typeof o.number_format === "object" &&
    !Array.isArray(o.number_format)
  ) {
    const nf = o.number_format as Record<string, unknown>;
    for (const [k, v] of Object.entries(nf)) {
      if (typeof v !== "string") {
        throw new Error(`'number_format["${k}"]' must be a string`);
      }
    }
    result.number_format = nf as Record<string, string>;
  }
  if (
    o.dimension_format !== undefined &&
    typeof o.dimension_format === "object" &&
    !Array.isArray(o.dimension_format)
  ) {
    const df = o.dimension_format as Record<string, unknown>;
    for (const [k, v] of Object.entries(df)) {
      if (typeof v !== "string") {
        throw new Error(`'dimension_format["${k}"]' must be a string`);
      }
    }
    result.dimension_format = df as Record<string, string>;
  }
  if (
    o.column_alignment !== undefined &&
    typeof o.column_alignment === "object" &&
    !Array.isArray(o.column_alignment)
  ) {
    const VALID_ALIGNMENTS = new Set<string>(["left", "center", "right"]);
    const ca = o.column_alignment as Record<string, unknown>;
    for (const [k, v] of Object.entries(ca)) {
      if (typeof v !== "string" || !VALID_ALIGNMENTS.has(v)) {
        throw new Error(
          `'column_alignment["${k}"]' must be one of: ${[...VALID_ALIGNMENTS].join(", ")}`,
        );
      }
    }
    result.column_alignment = ca as Record<string, "left" | "center" | "right">;
  }
  return result;
}

// ---------------------------------------------------------------------------
// Aggregator classification
// ---------------------------------------------------------------------------

/**
 * Each aggregator declares its class for correct totals/subtotals computation
 * and test dispatch:
 *
 * - additive:     grand_total == sum(row_totals) == sum(col_totals)
 * - idempotent:   grand_total == agg(all_records);
 *                 Min: grand_total <= min(row_totals)
 *                 Max: grand_total >= max(row_totals)
 * - non-additive: grand_total == agg(all_records), recomputed from raw data
 */
export type AggregatorClass = "additive" | "idempotent" | "non-additive";

export const AGGREGATOR_CLASS: Record<AggregationType, AggregatorClass> = {
  sum: "additive",
  count: "additive",
  min: "idempotent",
  max: "idempotent",
  avg: "non-additive",
  count_distinct: "non-additive",
  median: "non-additive",
  percentile_90: "non-additive",
  first: "non-additive",
  last: "non-additive",
};

// ---------------------------------------------------------------------------
// Totals-visibility helpers (shared by Toolbar + renderers)
// ---------------------------------------------------------------------------

export function showRowTotals(config: PivotConfigV1): boolean {
  if (config.columns.length === 0) return false;
  const v = config.show_row_totals ?? config.show_totals;
  if (Array.isArray(v)) return v.length > 0;
  return v;
}

export function showColumnTotals(config: PivotConfigV1): boolean {
  if (config.rows.length === 0) return false;
  const v = config.show_column_totals ?? config.show_totals;
  if (Array.isArray(v)) return v.length > 0;
  return v;
}

/** Check if subtotals should show for a specific row dimension. */
export function showSubtotalForDim(
  config: PivotConfigV1,
  dimName: string,
): boolean {
  const v = config.show_subtotals;
  if (v === undefined || v === false) return false;
  if (v === true) return true;
  return v.includes(dimName);
}

/** Check if a specific measure should appear in total cells on a given axis. */
export function showTotalForMeasure(
  config: PivotConfigV1,
  measure: string,
  axis: "row" | "col" | "grand",
): boolean {
  if (axis === "row" || axis === "grand") {
    const v = config.show_row_totals ?? config.show_totals;
    if (Array.isArray(v)) {
      if (!v.includes(measure)) return false;
    } else if (!v) return false;
  }
  if (axis === "col" || axis === "grand") {
    const v = config.show_column_totals ?? config.show_totals;
    if (Array.isArray(v)) {
      if (!v.includes(measure)) return false;
    } else if (!v) return false;
  }
  return true;
}

/**
 * Toggle a single item in a `boolean | string[]` field.
 * Returns the normalized next value.
 */
export function normalizeToggleList(
  current: boolean | string[] | undefined,
  allItems: string[],
  toggleItem: string,
): boolean | string[] {
  let list: string[];
  if (current === true || current === undefined) {
    list = allItems.filter((item) => item !== toggleItem);
  } else if (current === false) {
    list = [toggleItem];
  } else {
    list = current.includes(toggleItem)
      ? current.filter((item) => item !== toggleItem)
      : [...current, toggleItem];
  }
  if (list.length === 0) return false;
  if (list.length === allItems.length) return true;
  return list;
}

// ---------------------------------------------------------------------------
// Event payloads
// ---------------------------------------------------------------------------

/**
 * Payload fired by setTriggerValue("cell_click", payload).
 *
 * This is the canonical shape -- Python's CellClickPayload TypedDict must
 * mirror it exactly.
 */
export interface CellClickPayload {
  rowKey: string[];
  colKey: string[];
  value: number | null;
  filters: Record<string, string>;
  valueField?: string;
}

export interface PivotPerfAction {
  kind: "initial_mount" | "sort" | "filter" | "drilldown";
  elapsedMs: number;
  axis?: "row" | "col";
  field?: string;
  totalCount?: number;
}

export interface PivotPerfMetrics {
  parseMs: number;
  pivotComputeMs: number;
  renderMs: number;
  firstMountMs: number;
  sourceRows: number;
  sourceCols: number;
  totalRows: number;
  totalCols: number;
  totalCells: number;
  executionMode: "client_only" | "threshold_hybrid";
  needsVirtualization: boolean;
  columnsTruncated: boolean;
  truncatedColumnCount?: number;
  warnings: string[];
  lastAction?: PivotPerfAction;
}

// ---------------------------------------------------------------------------
// Column type detection (Arrow schema + Python supplement)
// ---------------------------------------------------------------------------

export type ColumnType =
  | "datetime"
  | "date"
  | "integer"
  | "float"
  | "string"
  | "boolean";

export type ColumnTypeMap = Map<string, ColumnType>;

// ---------------------------------------------------------------------------
// Null handling (developer constraint, passed via data channel)
// ---------------------------------------------------------------------------

export type NullHandlingMode = "exclude" | "zero" | "separate";

export type NullHandlingConfig =
  | NullHandlingMode
  | Record<string, NullHandlingMode>;

// ---------------------------------------------------------------------------
// Hybrid mode: server-computed totals sidecar
// ---------------------------------------------------------------------------

export interface HybridTotalEntry {
  key: string[];
  row?: string[];
  col?: string[];
  col_prefix?: string[];
  values: Record<string, number | null>;
}

export interface TemporalParentEntry {
  row?: string[];
  col: string[];
  field: string;
  grain: string;
  values: Record<string, number | null>;
}

export interface HybridTotals {
  sidecar_fingerprint: string;
  grand: Record<string, number | null>;
  row: HybridTotalEntry[];
  col: HybridTotalEntry[];
  col_prefix?: HybridTotalEntry[];
  col_prefix_grand?: HybridTotalEntry[];
  subtotals?: HybridTotalEntry[];
  cross_subtotals?: HybridTotalEntry[];
  temporal_parent?: TemporalParentEntry[];
  temporal_parent_grand?: TemporalParentEntry[];
}

// ---------------------------------------------------------------------------
// Data shape passed from Python via `data={...}`
// ---------------------------------------------------------------------------

export interface PivotTableData {
  /** Arrow-serialized DataFrame (handled by CCv2 framework) */
  dataframe: unknown;
  /** Optional fixed height in pixels */
  height: number | null;
  /** Max height when height is null. Table scrolls once content exceeds this. */
  max_height?: number;
  /** Initial config from Python (also sent via default for state round-trip) */
  config: PivotConfigV1;
  /** Phase 2: null handling policy. Defaults to "exclude". */
  null_handling?: NullHandlingConfig;
  /** Phase 2: columns hidden from all UI. */
  hidden_attributes?: string[];
  /** Phase 2: columns hidden from the values/aggregators dropdown only. */
  hidden_from_aggregators?: string[];
  /** Phase 2: columns that cannot be reassigned between zones. */
  hidden_from_drag_drop?: string[];
  /** Phase 2: custom sort orderings per dimension. */
  sorters?: Record<string, string[]>;
  /** Phase 2: lock config controls while keeping header-menu exploration enabled. */
  locked?: boolean;
  /** Phase 2: max items shown in header-menu filter checklist. */
  menu_limit?: number;
  /** Phase 4: enable drill-down detail panel on cell click. */
  enable_drilldown?: boolean;
  /** Base filename (without extension) for exported files. */
  export_filename?: string;
  /** Performance execution mode used for this render path. */
  execution_mode?: "client_only" | "threshold_hybrid";
  /** Human-readable reason why the threshold_hybrid path was selected. */
  server_mode_reason?: string;
  /** Server-computed totals sidecar for non-decomposable aggs in hybrid mode. */
  hybrid_totals?: HybridTotals;
  /** Aggregation type remap for count/count_distinct leaf cells in hybrid mode. */
  hybrid_agg_remap?: Record<string, AggregationType>;
  /** Server-provided drill-down rows (hybrid mode round-trip). */
  drilldown_records?: Record<string, unknown>[];
  /** Column names for server drill-down rows, in display order. */
  drilldown_columns?: string[];
  /** Total matching rows across all pages (hybrid drill-down). */
  drilldown_total_count?: number;
  /** Zero-based page index for the current drill-down slice. */
  drilldown_page?: number;
  /** Number of rows per page (set by the server). */
  drilldown_page_size?: number;
  /** Original row count before hybrid pre-aggregation. */
  source_row_count?: number;
  /** Python-detected column types (covers drilldown-only and object-dtype temporal columns). */
  original_column_types?: Record<string, ColumnType>;
  /** Per-field adaptive date grain computed from the source-filtered DataFrame's date range. */
  adaptive_date_grains?: Record<string, DateGrain>;
}

// ---------------------------------------------------------------------------
// State shape (keys in default={...})
// ---------------------------------------------------------------------------

export interface PivotTableState {
  config: PivotConfigV1;
  perf_metrics?: PivotPerfMetrics;
}

/** Abstraction over row-oriented `DataRecord[]` and columnar Arrow table access. */
export interface ColumnarDataSource {
  readonly numRows: number;
  getValue(rowIndex: number, fieldName: string): unknown;
  getColumnNames(): string[];
  getFloat64Column?(fieldName: string): Float64Array | null;
  getColumnTypes?(): ColumnTypeMap;
}
