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

import { FrontendRendererArgs } from "@streamlit/component-v2-lib";
import {
  FC,
  ReactElement,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  type CellClickPayload,
  type ColumnType,
  type ColumnTypeMap,
  type DateGrain,
  type DimensionFilter,
  DEFAULT_CONFIG,
  getEffectiveDateGrain,
  getExplicitDateGrain,
  getRenderedValueFields,
  isPeriodComparisonMode,
  isSyntheticMeasure,
  type PivotPerfMetrics,
  stringifyPivotConfig,
  validatePivotConfigV1,
  validatePivotConfigRuntime,
  type PivotConfigV1,
  type PivotTableData,
  type ShowValuesAs,
  type SortConfig,
} from "./engine/types";
import { PivotData, type PivotDataOptions } from "./engine/PivotData";
import {
  createArrowDataSource,
  getArrowColumnNames,
  getNumericColumns,
} from "./engine/parseArrow";
import {
  measureSync,
  logMetrics,
  checkBudgets,
  DEFAULT_BUDGETS,
  type PerfActionMeasurement,
  type PivotPerfMetrics as PivotPerfMetricsModel,
} from "./engine/perf";
import { checkRenderBudget } from "./shared/budgetCheck";
import ErrorBoundary from "./shared/ErrorBoundary";
import TableRenderer from "./renderers/TableRenderer";
import VirtualizedTableRenderer from "./renderers/VirtualizedTableRenderer";
import Toolbar from "./config/Toolbar";
import { FilterBar } from "./config/FilterBar";
import DrilldownPanel from "./renderers/DrilldownPanel";
import WarningBanner from "./shared/WarningBanner";

export type DrilldownSortDirection = "asc" | "desc";

export type DrilldownRequest = CellClickPayload & {
  page?: number;
  sortColumn?: string;
  sortDirection?: DrilldownSortDirection;
  requestId?: string;
};

export type PivotRootState = {
  config: PivotConfigV1;
  cell_click: CellClickPayload;
  perf_metrics?: PivotPerfMetrics;
  drilldown_request?: DrilldownRequest | null;
};

export type PivotRootProps = Pick<
  FrontendRendererArgs<PivotRootState, PivotTableData>,
  "setStateValue" | "setTriggerValue"
> &
  PivotTableData & {
    /** Stable per-instance key from the component framework; used for localStorage namespacing. */
    instanceKey?: string;
  };

const PivotRoot: FC<PivotRootProps> = ({
  instanceKey,
  config: configProp,
  dataframe,
  height,
  max_height,
  null_handling,
  hidden_attributes,
  hidden_from_aggregators,
  hidden_from_drag_drop,
  sorters,
  locked,
  menu_limit,
  enable_drilldown,
  export_filename,
  execution_mode,
  server_mode_reason,
  drilldown_records,
  drilldown_columns,
  drilldown_total_count,
  drilldown_page,
  drilldown_page_size,
  drilldown_request_id,
  hybrid_totals,
  hybrid_agg_remap,
  filter_field_values,
  source_row_count,
  original_column_types,
  adaptive_date_grains,
  setStateValue,
  setTriggerValue,
}): ReactElement => {
  // Local override for user-initiated config changes. Without this, calling
  // setStateValue("config", ...) triggers a Python rerun that re-sends the
  // original config prop, immediately overwriting the user's change (especially
  // when no `key` is provided, so Python can't hydrate persisted state).
  const [localConfig, setLocalConfig] = useState<PivotConfigV1 | null>(null);

  // Detect when Python intentionally sends a *new* config (as opposed to
  // re-sending the same config on a rerun). When the prop changes, discard
  // local overrides so the new Python config takes effect.
  const normalizedPropConfig = useMemo(
    () => validatePivotConfigV1(configProp ?? DEFAULT_CONFIG),
    [configProp],
  );
  const propConfigJson = useMemo(
    () => stringifyPivotConfig(normalizedPropConfig),
    [normalizedPropConfig],
  );
  const prevPropJsonRef = useRef(propConfigJson);
  if (propConfigJson !== prevPropJsonRef.current) {
    prevPropJsonRef.current = propConfigJson;
    if (localConfig !== null) {
      setLocalConfig(null);
    }
  }

  const rawCurrentConfig = localConfig ?? normalizedPropConfig;
  const [isTableScrollable, setIsTableScrollable] = useState(false);
  const [drilldownPayload, setDrilldownPayload] =
    useState<CellClickPayload | null>(null);
  const [drilldownSortColumn, setDrilldownSortColumn] = useState<
    string | undefined
  >(undefined);
  const [drilldownSortDirection, setDrilldownSortDirection] = useState<
    DrilldownSortDirection | undefined
  >(undefined);
  const [pendingDrilldownRequest, setPendingDrilldownRequest] =
    useState<DrilldownRequest | null>(null);
  const drilldownRequestSeqRef = useRef(0);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [windowHeight, setWindowHeight] = useState(
    typeof window !== "undefined" ? window.innerHeight : 800,
  );

  const { pivotInput, parseMs } = useMemo(() => {
    const measured = measureSync(() => createArrowDataSource(dataframe));
    const ds = measured.result;
    return {
      pivotInput: ds && ds.numRows > 0 ? ds : null,
      parseMs: measured.elapsedMs,
    };
  }, [dataframe]);

  const rawAllColumns = useMemo(
    () => getArrowColumnNames(dataframe),
    [dataframe],
  );

  const rawNumericColumns = useMemo(
    () => getNumericColumns(dataframe),
    [dataframe],
  );

  const hiddenSet = useMemo(
    () => new Set(hidden_attributes ?? []),
    [hidden_attributes],
  );

  const hiddenAggSet = useMemo(
    () => new Set(hidden_from_aggregators ?? []),
    [hidden_from_aggregators],
  );

  const frozenColumns = useMemo(
    () => new Set(hidden_from_drag_drop ?? []),
    [hidden_from_drag_drop],
  );

  const allColumns = useMemo(
    () => rawAllColumns.filter((c) => !hiddenSet.has(c)),
    [rawAllColumns, hiddenSet],
  );

  const numericColumns = useMemo(
    () =>
      rawNumericColumns.filter(
        (c) => !hiddenSet.has(c) && !hiddenAggSet.has(c),
      ),
    [rawNumericColumns, hiddenSet, hiddenAggSet],
  );

  const syntheticSourceColumns = useMemo(
    () => rawNumericColumns.filter((c) => !hiddenSet.has(c)),
    [rawNumericColumns, hiddenSet],
  );

  const mergedColumnTypes: ColumnTypeMap = useMemo(() => {
    const result: ColumnTypeMap = new Map<string, ColumnType>();
    // Start with Python supplement (covers all columns including drilldown-only)
    if (original_column_types) {
      for (const [col, ct] of Object.entries(original_column_types)) {
        result.set(col, ct);
      }
    }
    // Arrow-derived types take precedence, except: if Arrow says "string"
    // and Python says temporal, prefer Python (object-typed date columns)
    if (pivotInput?.getColumnTypes) {
      const arrowTypes = pivotInput.getColumnTypes();
      for (const [col, arrowType] of arrowTypes) {
        const pythonType = result.get(col);
        if (
          arrowType === "string" &&
          (pythonType === "datetime" || pythonType === "date")
        ) {
          continue; // Python detected temporal in object column
        }
        result.set(col, arrowType);
      }
    }
    return result;
  }, [pivotInput, original_column_types]);

  const typedAdaptiveGrains = adaptive_date_grains as
    | Record<string, DateGrain>
    | undefined;

  const currentConfig = useMemo((): PivotConfigV1 => {
    try {
      return validatePivotConfigRuntime(
        rawCurrentConfig,
        mergedColumnTypes,
        typedAdaptiveGrains,
      );
    } catch {
      // Strip stale analytical filter refs and retry. If the original error was
      // caused by a dangling field/by reference the retry succeeds. If the error
      // was unrelated (e.g. period comparison on a non-temporal axis) the retry
      // throws again and the exception propagates naturally to the error boundary.
      const safe: PivotConfigV1 = { ...rawCurrentConfig };
      delete safe.top_n_filters;
      delete safe.value_filters;
      return validatePivotConfigRuntime(
        safe,
        mergedColumnTypes,
        typedAdaptiveGrains,
      );
    }
  }, [rawCurrentConfig, mergedColumnTypes, typedAdaptiveGrains]);

  const initialConfigRef = useRef<PivotConfigV1>(currentConfig);
  const initialConfig = initialConfigRef.current;

  // Invalidate key-derived state when adaptive grains shift between renders.
  // Runs as an effect (not during render) to avoid duplicate state writes and
  // render-loop issues in Strict Mode / concurrent rendering.
  const prevAdaptiveGrainsRef = useRef(typedAdaptiveGrains);
  useEffect(() => {
    const prev = prevAdaptiveGrainsRef.current;
    if (typedAdaptiveGrains === prev) return;
    prevAdaptiveGrainsRef.current = typedAdaptiveGrains;

    const cfg = rawCurrentConfig;
    const affectedRows: string[] = [];
    const affectedCols: string[] = [];
    for (const field of [...cfg.rows, ...cfg.columns]) {
      const explicit = getExplicitDateGrain(cfg, field);
      if (explicit !== undefined) continue;
      const colType = mergedColumnTypes.get(field);
      const prevGrain = getEffectiveDateGrain(
        cfg,
        field,
        colType,
        prev?.[field],
      );
      const nextGrain = getEffectiveDateGrain(
        cfg,
        field,
        colType,
        typedAdaptiveGrains?.[field],
      );
      if (prevGrain !== nextGrain) {
        if (cfg.rows.includes(field)) affectedRows.push(field);
        if (cfg.columns.includes(field)) affectedCols.push(field);
      }
    }

    if (affectedRows.length > 0 || affectedCols.length > 0) {
      const allAffected = new Set([...affectedRows, ...affectedCols]);
      const nextFilters = cfg.filters
        ? Object.fromEntries(
            Object.entries(cfg.filters).filter(([f]) => !allAffected.has(f)),
          )
        : undefined;

      // Only strip period-comparison show_values_as modes when the field
      // that serves as the period comparison axis is itself affected.
      // The comparison axis is the first grouped temporal field on columns,
      // falling back to the first grouped temporal field on rows.
      let comparisonAxisAffected = false;
      const compField =
        cfg.columns.find(
          (f) =>
            (mergedColumnTypes.get(f) === "date" ||
              mergedColumnTypes.get(f) === "datetime") &&
            getEffectiveDateGrain(
              cfg,
              f,
              mergedColumnTypes.get(f),
              typedAdaptiveGrains?.[f],
            ),
        ) ??
        cfg.rows.find(
          (f) =>
            (mergedColumnTypes.get(f) === "date" ||
              mergedColumnTypes.get(f) === "datetime") &&
            getEffectiveDateGrain(
              cfg,
              f,
              mergedColumnTypes.get(f),
              typedAdaptiveGrains?.[f],
            ),
        );
      if (compField && allAffected.has(compField)) {
        comparisonAxisAffected = true;
      }
      const nextSVA =
        comparisonAxisAffected && cfg.show_values_as
          ? Object.fromEntries(
              Object.entries(cfg.show_values_as).filter(
                ([, mode]) => !isPeriodComparisonMode(mode),
              ),
            )
          : cfg.show_values_as;

      const clearTemporalGroups = (
        groups: Record<string, string[]> | undefined,
      ): Record<string, string[]> | undefined => {
        if (!groups) return undefined;
        const cleaned = Object.fromEntries(
          Object.entries(groups).filter(([f]) => !allAffected.has(f)),
        );
        return Object.keys(cleaned).length > 0 ? cleaned : undefined;
      };

      const patched: PivotConfigV1 = {
        ...cfg,
        filters:
          nextFilters && Object.keys(nextFilters).length > 0
            ? nextFilters
            : undefined,
        show_values_as:
          nextSVA && Object.keys(nextSVA).length > 0 ? nextSVA : undefined,
        collapsed_groups:
          affectedRows.length > 0 ? undefined : cfg.collapsed_groups,
        collapsed_col_groups:
          affectedCols.length > 0 ? undefined : cfg.collapsed_col_groups,
        collapsed_temporal_groups: clearTemporalGroups(
          cfg.collapsed_temporal_groups,
        ),
        collapsed_temporal_row_groups: clearTemporalGroups(
          cfg.collapsed_temporal_row_groups,
        ),
      };
      setLocalConfig(patched);
      setStateValue("config", patched);
    }
  }, [typedAdaptiveGrains, rawCurrentConfig, mergedColumnTypes, setStateValue]);

  const currentConfigJson = useMemo(
    () => stringifyPivotConfig(currentConfig),
    [currentConfig],
  );

  const pivotOptions: PivotDataOptions = useMemo(
    () => ({
      nullHandling: null_handling,
      sorters,
      hybridTotals: hybrid_totals,
      hybridAggRemap: hybrid_agg_remap,
      columnTypes: mergedColumnTypes,
      adaptiveDateGrains: typedAdaptiveGrains,
    }),
    [
      null_handling,
      sorters,
      hybrid_totals,
      hybrid_agg_remap,
      mergedColumnTypes,
      typedAdaptiveGrains,
    ],
  );

  const { pivotData, computeMs } = useMemo(() => {
    if (!pivotInput) return { pivotData: null, computeMs: 0 };
    const measured = measureSync(
      () => new PivotData(pivotInput, currentConfig, pivotOptions),
    );
    return { pivotData: measured.result, computeMs: measured.elapsedMs };
  }, [pivotInput, currentConfig, pivotOptions]);

  const budget = useMemo(() => {
    if (!pivotData) return null;
    return checkRenderBudget(
      pivotData.uniqueRowKeyCount,
      pivotData.uniqueColKeyCount,
      getRenderedValueFields(currentConfig).length,
    );
  }, [pivotData, currentConfig]);

  useEffect(() => {
    if (budget?.needsVirtualization) {
      setIsTableScrollable(true);
    }
  }, [budget?.needsVirtualization]);

  const renderStartRef = useRef(0);
  const [debugMetrics, setDebugMetrics] =
    useState<PivotPerfMetricsModel | null>(null);
  const renderCountRef = useRef(0);
  const prevPivotDataRef = useRef<PivotData | null>(null);
  const prevConfigJsonRef = useRef<string>(currentConfigJson);
  const initialMetricsPublishedRef = useRef(false);
  const pendingActionRef = useRef<{
    kind: PerfActionMeasurement["kind"];
    startedAt: number;
    axis?: "row" | "col";
    field?: string;
  } | null>(null);

  if (pivotData !== prevPivotDataRef.current) {
    prevPivotDataRef.current = pivotData;
    renderCountRef.current = 0;
    if (!pivotData) setDebugMetrics(null);
  }

  // Only clear drilldown when the config content actually changes (not on
  // trivial re-renders caused by Python reruns after setTriggerValue).
  if (currentConfigJson !== prevConfigJsonRef.current) {
    prevConfigJsonRef.current = currentConfigJson;
    setDrilldownPayload(null);
    setDrilldownSortColumn(undefined);
    setDrilldownSortDirection(undefined);
    setPendingDrilldownRequest(null);
  }

  renderStartRef.current = performance.now();

  useEffect(() => {
    if (!pivotData) return;
    const renderMs =
      Math.round((performance.now() - renderStartRef.current) * 100) / 100;
    renderCountRef.current += 1;
    let lastAction = debugMetrics?.lastAction;
    if (!initialMetricsPublishedRef.current) {
      initialMetricsPublishedRef.current = true;
      lastAction = {
        kind: "initial_mount",
        elapsedMs: Math.round((parseMs + computeMs + renderMs) * 100) / 100,
      };
    } else if (pendingActionRef.current) {
      lastAction = {
        kind: pendingActionRef.current.kind,
        elapsedMs:
          Math.round(
            (performance.now() - pendingActionRef.current.startedAt) * 100,
          ) / 100,
        axis: pendingActionRef.current.axis,
        field: pendingActionRef.current.field,
      };
      pendingActionRef.current = null;
    }

    const metrics: PivotPerfMetricsModel = {
      parseMs,
      pivotComputeMs: computeMs,
      renderMs,
      firstMountMs:
        debugMetrics?.firstMountMs ??
        Math.round((parseMs + computeMs + renderMs) * 100) / 100,
      sourceRows: source_row_count ?? pivotData.recordCount,
      sourceCols: rawAllColumns.length,
      totalRows: pivotData.uniqueRowKeyCount,
      totalCols: pivotData.uniqueColKeyCount,
      totalCells: pivotData.totalCellCount,
      executionMode: execution_mode ?? "client_only",
      needsVirtualization: Boolean(budget?.needsVirtualization),
      columnsTruncated: Boolean(budget?.columnsTruncated),
      truncatedColumnCount: budget?.truncatedColumnCount,
      warnings: [],
      lastAction,
    };
    const warnings = [...(budget?.warnings ?? [])];
    for (const warning of checkBudgets(metrics)) {
      if (!warnings.includes(warning)) warnings.push(warning);
    }
    metrics.warnings = warnings;
    if (containerRef.current) {
      containerRef.current.setAttribute(
        "data-perf-metrics",
        JSON.stringify(metrics),
      );
    }
    if (process.env.NODE_ENV === "development") {
      logMetrics(metrics);
    }
    if (renderCountRef.current <= 2 || !debugMetrics) {
      setDebugMetrics(metrics);
    }
  }, [
    budget,
    computeMs,
    execution_mode,
    parseMs,
    pivotData,
    rawAllColumns.length,
    pivotInput,
  ]);

  // perf_metrics are exposed via the data-perf-metrics DOM attribute (set
  // above) for debugging and E2E tests.  We intentionally do NOT push them
  // back through setStateValue because the resulting Streamlit rerun creates
  // a timing window that can swallow concurrent trigger events (cell clicks,
  // toolbar changes) — especially on slower browsers like WebKit.

  const perfWarnings = useMemo(() => {
    if (!debugMetrics) {
      if (!pivotData) return [];
      return checkBudgets({
        pivotComputeMs: computeMs,
        renderMs: 0,
        totalRows: pivotData.uniqueRowKeyCount,
        totalCols: pivotData.uniqueColKeyCount,
        totalCells: pivotData.totalCellCount,
      });
    }
    return checkBudgets(debugMetrics);
  }, [pivotData, computeMs, debugMetrics]);

  const allWarnings = useMemo(() => {
    const w = [...(budget?.warnings ?? [])];
    for (const pw of perfWarnings) {
      if (!w.includes(pw)) w.push(pw);
    }
    if (execution_mode === "threshold_hybrid") {
      const hybridInfo =
        server_mode_reason?.trim() ||
        "This table uses server pre-aggregated data for performance.";
      if (!w.includes(hybridInfo)) w.push(hybridInfo);
    }
    return w;
  }, [budget, perfWarnings, execution_mode, server_mode_reason]);

  const pendingFlushRef = useRef<PivotConfigV1 | null>(null);

  const handleConfigChange = useCallback(
    (newConfig: PivotConfigV1) => {
      if (stringifyPivotConfig(newConfig) !== currentConfigJson) {
        setLocalConfig(newConfig);
        if (isFullscreen) {
          pendingFlushRef.current = newConfig;
        } else {
          pendingFlushRef.current = null;
          setStateValue("config", newConfig);
        }
      }
    },
    [setStateValue, currentConfigJson, isFullscreen],
  );

  const drilldownEnabled = enable_drilldown == null || !!enable_drilldown;
  const isHybridMode = execution_mode === "threshold_hybrid";

  const sendHybridDrilldownRequest = useCallback(
    (
      payload: CellClickPayload,
      requestState: Pick<
        DrilldownRequest,
        "page" | "sortColumn" | "sortDirection"
      >,
    ) => {
      const request: DrilldownRequest = {
        ...payload,
        ...requestState,
        requestId: `drilldown-${++drilldownRequestSeqRef.current}`,
      };
      setPendingDrilldownRequest(request);
      setStateValue("drilldown_request", request);
    },
    [setStateValue],
  );

  useEffect(() => {
    if (!isHybridMode || !pendingDrilldownRequest) return;
    if (pendingDrilldownRequest.requestId === drilldown_request_id) {
      setPendingDrilldownRequest(null);
    }
  }, [drilldown_request_id, isHybridMode, pendingDrilldownRequest]);

  useEffect(() => {
    if (drilldownPayload || !isHybridMode) return;
    setPendingDrilldownRequest(null);
  }, [drilldownPayload, isHybridMode]);

  const handleCellClick = useCallback(
    (payload: CellClickPayload) => {
      setTriggerValue("cell_click", payload);
      if (drilldownEnabled) {
        setDrilldownPayload(payload);
        setDrilldownSortColumn(undefined);
        setDrilldownSortDirection(undefined);
        if (isHybridMode) {
          sendHybridDrilldownRequest(payload, {
            page: 0,
            sortColumn: undefined,
            sortDirection: undefined,
          });
        }
      }
    },
    [
      setTriggerValue,
      drilldownEnabled,
      isHybridMode,
      sendHybridDrilldownRequest,
    ],
  );

  const handleDrilldownSortChange = useCallback(
    (
      sortColumn: string | undefined,
      sortDirection: DrilldownSortDirection | undefined,
    ) => {
      setDrilldownSortColumn(sortColumn);
      setDrilldownSortDirection(sortDirection);
      if (isHybridMode && drilldownPayload) {
        sendHybridDrilldownRequest(drilldownPayload, {
          page: 0,
          sortColumn,
          sortDirection,
        });
      }
    },
    [drilldownPayload, isHybridMode, sendHybridDrilldownRequest],
  );

  const handleSortChange = useCallback(
    (axis: "row" | "col", sort: SortConfig | undefined) => {
      pendingActionRef.current = {
        kind: "sort",
        startedAt: performance.now(),
        axis,
      };
      const key = axis === "row" ? "row_sort" : "col_sort";
      handleConfigChange({ ...currentConfig, [key]: sort });
    },
    [currentConfig, handleConfigChange],
  );

  const handleFilterChange = useCallback(
    (field: string, filter: DimensionFilter | undefined) => {
      pendingActionRef.current = {
        kind: "filter",
        startedAt: performance.now(),
        field,
      };
      const filters = { ...(currentConfig.filters ?? {}) };
      if (filter) {
        filters[field] = filter;
      } else {
        delete filters[field];
      }
      const hasFilters = Object.keys(filters).length > 0;
      handleConfigChange({
        ...currentConfig,
        filters: hasFilters ? filters : undefined,
      });
    },
    [currentConfig, handleConfigChange],
  );

  const handleRemoveFilterField = useCallback(
    (field: string) => {
      const newFilterFields = (currentConfig.filter_fields ?? []).filter(
        (f) => f !== field,
      );
      const filters = { ...(currentConfig.filters ?? {}) };
      // Dual-role guard: only clear the filter entry if the field is NOT also
      // in rows or columns. If it is, the header-menu indicator must stay active.
      const rowColSet = new Set([
        ...currentConfig.rows,
        ...currentConfig.columns,
      ]);
      if (!rowColSet.has(field)) {
        delete filters[field];
      }
      const hasFilters = Object.keys(filters).length > 0;
      handleConfigChange({
        ...currentConfig,
        filter_fields: newFilterFields.length ? newFilterFields : undefined,
        filters: hasFilters ? filters : undefined,
      });
    },
    [currentConfig, handleConfigChange],
  );

  const handleDrilldownMeasured = useCallback(
    (action: PerfActionMeasurement) => {
      setDebugMetrics((prev) =>
        prev ? { ...prev, lastAction: action } : prev,
      );
    },
    [],
  );

  const handleTopNFilterChange = useCallback(
    (
      filter: import("./engine/types").TopNFilter | undefined,
      field: string,
      axis: "rows" | "columns",
    ) => {
      const existing = currentConfig.top_n_filters ?? [];
      const rest = existing.filter(
        (f) => !(f.field === field && (f.axis ?? "rows") === axis),
      );
      const updated = filter ? [...rest, filter] : rest;
      handleConfigChange({
        ...currentConfig,
        top_n_filters: updated.length > 0 ? updated : undefined,
      });
    },
    [currentConfig, handleConfigChange],
  );

  const handleValueFilterChange = useCallback(
    (
      filter: import("./engine/types").ValueFilter | undefined,
      field: string,
      axis: "rows" | "columns",
    ) => {
      const existing = currentConfig.value_filters ?? [];
      const rest = existing.filter(
        (f) => !(f.field === field && (f.axis ?? "rows") === axis),
      );
      const updated = filter ? [...rest, filter] : rest;
      handleConfigChange({
        ...currentConfig,
        value_filters: updated.length > 0 ? updated : undefined,
      });
    },
    [currentConfig, handleConfigChange],
  );

  const handleShowValuesAsChange = useCallback(
    (field: string, mode: ShowValuesAs) => {
      if (isSyntheticMeasure(currentConfig, field)) return;
      const showValuesAs = { ...(currentConfig.show_values_as ?? {}) };
      if (mode === "raw") {
        delete showValuesAs[field];
      } else {
        showValuesAs[field] = mode;
      }
      const hasEntries = Object.keys(showValuesAs).length > 0;
      handleConfigChange({
        ...currentConfig,
        show_values_as: hasEntries ? showValuesAs : undefined,
      });
    },
    [currentConfig, handleConfigChange],
  );

  const handleCollapseChange = useCallback(
    (axis: "row" | "col", collapsed: string[]) => {
      const key = axis === "row" ? "collapsed_groups" : "collapsed_col_groups";
      handleConfigChange({ ...currentConfig, [key]: collapsed });
    },
    [currentConfig, handleConfigChange],
  );

  const safeMaxRows = useMemo(() => {
    if (!pivotData || !budget || budget.needsVirtualization) return undefined;
    // Non-virtual path: cap rows from DOM budget using displayed column count
    // (no legacy 200-cap when wide mode + virtualization; that path returns above).
    const effectiveCols = budget.columnsTruncated
      ? budget.truncatedColumnCount
      : pivotData.uniqueColKeyCount;
    const colsPerCell = Math.max(
      getRenderedValueFields(currentConfig).length,
      1,
    );
    const cellsPerRow = effectiveCols * colsPerCell;
    if (cellsPerRow === 0) return undefined;
    const maxRows = Math.floor(DEFAULT_BUDGETS.maxVisibleCells / cellsPerRow);
    return maxRows < pivotData.uniqueRowKeyCount ? maxRows : undefined;
  }, [pivotData, budget, currentConfig]);

  const containerRef = useRef<HTMLDivElement>(null);

  const handleToggleFullscreen = useCallback(() => {
    setIsFullscreen((prev) => {
      if (!prev) {
        setWindowHeight(window.innerHeight);
      } else if (pendingFlushRef.current) {
        setStateValue("config", pendingFlushRef.current);
        pendingFlushRef.current = null;
      }
      return !prev;
    });
  }, [setStateValue]);

  useEffect(() => {
    if (!isFullscreen) return;
    const onResize = () => setWindowHeight(window.innerHeight);
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        if (pendingFlushRef.current) {
          setStateValue("config", pendingFlushRef.current);
          pendingFlushRef.current = null;
        }
        setIsFullscreen(false);
      }
    };
    window.addEventListener("resize", onResize);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("resize", onResize);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isFullscreen, setStateValue]);

  useLayoutEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const bg = getComputedStyle(el).backgroundColor;
    const match = bg.match(/(\d+),\s*(\d+),\s*(\d+)/);
    if (!match) return;
    const [r, g, b] = [+match[1], +match[2], +match[3]].map((c) => {
      const s = c / 255;
      return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
    });
    const lum = 0.2126 * r + 0.7152 * g + 0.0722 * b;
    const isDark = lum <= 0.4;
    el.style.setProperty(
      "--pivot-popover-shadow",
      isDark
        ? "0px 4px 16px rgba(0, 0, 0, 0.7)"
        : "0px 4px 16px rgba(0, 0, 0, 0.16)",
    );
    el.style.setProperty(
      "--pivot-dropdown-shadow",
      isDark
        ? "0 6px 18px rgba(0, 0, 0, 0.4)"
        : "0 6px 18px rgba(0, 0, 0, 0.08)",
    );
  });

  const containerStyle: React.CSSProperties = {
    fontFamily: "var(--st-font)",
    fontSize: "var(--st-base-font-size)",
    color: "var(--st-text-color)",
    backgroundColor: "var(--st-background-color)",
    ...(isFullscreen
      ? {
          position: "fixed" as const,
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          zIndex: 1000050,
          display: "flex",
          flexDirection: "column" as const,
          overflow: "hidden",
          padding: "2.875rem 1.5rem 1.5rem",
        }
      : {}),
  };

  return (
    <ErrorBoundary>
      <div
        ref={containerRef}
        data-testid="pivot-container"
        style={containerStyle}
      >
        <WarningBanner messages={allWarnings} />

        {currentConfig.interactive && (
          <Toolbar
            instanceKey={instanceKey}
            config={currentConfig}
            allColumns={allColumns}
            numericColumns={numericColumns}
            syntheticSourceColumns={syntheticSourceColumns}
            columnTypes={mergedColumnTypes}
            adaptiveDateGrains={typedAdaptiveGrains}
            onConfigChange={!locked ? handleConfigChange : undefined}
            initialConfig={initialConfig}
            locked={locked}
            frozenColumns={frozenColumns.size > 0 ? frozenColumns : undefined}
            showStickyToggle={isTableScrollable}
            pivotData={pivotData ?? undefined}
            filterFieldValues={filter_field_values}
            pickerPortalTarget={containerRef.current}
            exportFilename={export_filename}
            onCollapseChange={
              currentConfig.interactive ? handleCollapseChange : undefined
            }
            isFullscreen={isFullscreen}
            onToggleFullscreen={handleToggleFullscreen}
          />
        )}

        {(currentConfig.filter_fields?.length ?? 0) > 0 &&
          currentConfig.show_sections !== false &&
          pivotData && (
            <FilterBar
              filterFields={currentConfig.filter_fields!}
              filters={currentConfig.filters}
              pivotData={pivotData}
              config={currentConfig}
              adaptiveDateGrains={typedAdaptiveGrains}
              filterFieldValues={filter_field_values}
              onFilterChange={handleFilterChange}
              onRemoveField={handleRemoveFilterField}
              menuLimit={menu_limit}
              portalTarget={containerRef.current}
            />
          )}

        <div
          style={
            isFullscreen
              ? {
                  flex: 1,
                  minHeight: 0,
                  display: "flex",
                  flexDirection: "column" as const,
                  overflow: "hidden",
                }
              : undefined
          }
        >
          {pivotData ? (
            budget?.needsVirtualization ? (
              <VirtualizedTableRenderer
                pivotData={pivotData}
                config={currentConfig}
                adaptiveDateGrains={typedAdaptiveGrains}
                onCellClick={handleCellClick}
                maxColumns={
                  budget.columnsTruncated
                    ? budget.truncatedColumnCount
                    : undefined
                }
                containerHeight={
                  isFullscreen ? windowHeight : (max_height ?? 500)
                }
                onSortChange={
                  currentConfig.interactive ? handleSortChange : undefined
                }
                onFilterChange={
                  currentConfig.interactive ? handleFilterChange : undefined
                }
                onConfigChange={
                  currentConfig.interactive && !locked
                    ? handleConfigChange
                    : undefined
                }
                onShowValuesAsChange={
                  currentConfig.interactive
                    ? handleShowValuesAsChange
                    : undefined
                }
                onCollapseChange={
                  currentConfig.interactive ? handleCollapseChange : undefined
                }
                menuLimit={menu_limit}
                scrollable={isFullscreen}
              />
            ) : (
              <TableRenderer
                pivotData={pivotData}
                config={currentConfig}
                adaptiveDateGrains={typedAdaptiveGrains}
                onCellClick={handleCellClick}
                maxColumns={
                  budget?.columnsTruncated
                    ? budget.truncatedColumnCount
                    : undefined
                }
                maxRows={safeMaxRows}
                onSortChange={
                  currentConfig.interactive ? handleSortChange : undefined
                }
                onFilterChange={
                  currentConfig.interactive ? handleFilterChange : undefined
                }
                onConfigChange={
                  currentConfig.interactive && !locked
                    ? handleConfigChange
                    : undefined
                }
                onShowValuesAsChange={
                  currentConfig.interactive
                    ? handleShowValuesAsChange
                    : undefined
                }
                onTopNFilterChange={
                  currentConfig.interactive ? handleTopNFilterChange : undefined
                }
                onValueFilterChange={
                  currentConfig.interactive
                    ? handleValueFilterChange
                    : undefined
                }
                onCollapseChange={
                  currentConfig.interactive ? handleCollapseChange : undefined
                }
                menuLimit={menu_limit}
                scrollable={isFullscreen}
                maxHeight={isFullscreen ? undefined : (max_height ?? 500)}
                onOverflowChange={setIsTableScrollable}
              />
            )
          ) : (
            <div
              data-testid="pivot-table-empty"
              style={{
                padding: "24px",
                textAlign: "center",
                opacity: 0.6,
                fontStyle: "italic",
              }}
            >
              No data to display. Provide a DataFrame with rows, columns, and
              values configured.
            </div>
          )}
        </div>

        {drilldownPayload && pivotData && drilldownEnabled && (
          <DrilldownPanel
            pivotData={pivotData}
            payload={drilldownPayload}
            sortColumn={drilldownSortColumn}
            sortDirection={drilldownSortDirection}
            onSortChange={handleDrilldownSortChange}
            numberFormat={currentConfig.number_format}
            columnAlignment={currentConfig.column_alignment}
            columnTypes={mergedColumnTypes}
            onClose={() => {
              setDrilldownPayload(null);
              setDrilldownSortColumn(undefined);
              setDrilldownSortDirection(undefined);
              setPendingDrilldownRequest(null);
              if (isHybridMode) {
                setStateValue("drilldown_request", null);
              }
            }}
            onMeasured={handleDrilldownMeasured}
            serverRecords={isHybridMode ? drilldown_records : undefined}
            serverColumns={isHybridMode ? drilldown_columns : undefined}
            serverTotalCount={isHybridMode ? drilldown_total_count : undefined}
            isLoading={
              isHybridMode &&
              (pendingDrilldownRequest != null || !drilldown_records)
            }
            serverPage={isHybridMode ? drilldown_page : undefined}
            serverPageSize={isHybridMode ? drilldown_page_size : undefined}
            onPageChange={
              isHybridMode
                ? (page: number) => {
                    sendHybridDrilldownRequest(drilldownPayload, {
                      page,
                      sortColumn: drilldownSortColumn,
                      sortDirection: drilldownSortDirection,
                    });
                  }
                : undefined
            }
          />
        )}

        {process.env.NODE_ENV === "development" && debugMetrics && (
          <div
            data-testid="pivot-debug-overlay"
            style={{
              fontFamily: "monospace",
              fontSize: "10px",
              opacity: 0.6,
              padding: "4px 8px",
              borderTop: "1px solid var(--st-dataframe-border-color)",
              display: "flex",
              gap: "12px",
            }}
          >
            <span>compute: {debugMetrics.pivotComputeMs.toFixed(1)}ms</span>
            <span>parse: {debugMetrics.parseMs.toFixed(1)}ms</span>
            <span>render: {debugMetrics.renderMs.toFixed(1)}ms</span>
            <span>rows: {debugMetrics.totalRows}</span>
            <span>cols: {debugMetrics.totalCols}</span>
            <span>cells: {debugMetrics.totalCells}</span>
          </div>
        )}
      </div>
    </ErrorBoundary>
  );
};

export default PivotRoot;
