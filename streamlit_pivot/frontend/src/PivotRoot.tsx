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
  type DimensionFilter,
  DEFAULT_CONFIG,
  getRenderedValueFields,
  isSyntheticMeasure,
  type PivotPerfMetrics,
  stringifyPivotConfig,
  validatePivotConfigV1,
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
import DrilldownPanel from "./renderers/DrilldownPanel";
import WarningBanner from "./shared/WarningBanner";

export type DrilldownRequest = CellClickPayload & { page?: number };

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
  PivotTableData;

const PivotRoot: FC<PivotRootProps> = ({
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

  const currentConfig = localConfig ?? normalizedPropConfig;
  const [isTableScrollable, setIsTableScrollable] = useState(false);
  const [drilldownPayload, setDrilldownPayload] =
    useState<CellClickPayload | null>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [windowHeight, setWindowHeight] = useState(
    typeof window !== "undefined" ? window.innerHeight : 800,
  );

  const initialConfigRef = useRef<PivotConfigV1>(currentConfig);
  const initialConfig = initialConfigRef.current;

  const currentConfigJson = useMemo(
    () => stringifyPivotConfig(currentConfig),
    [currentConfig],
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

  const pivotOptions: PivotDataOptions = useMemo(
    () => ({ nullHandling: null_handling, sorters }),
    [null_handling, sorters],
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
    setIsTableScrollable(
      Boolean(budget?.needsVirtualization || height != null),
    );
  }, [budget?.needsVirtualization, height]);

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
      sourceRows: pivotData.recordCount,
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
        "This table uses server pre-aggregated data. Drill-down to raw rows is not available.";
      if (!w.includes(hybridInfo)) w.push(hybridInfo);
    }
    return w;
  }, [budget, perfWarnings, execution_mode, server_mode_reason]);

  const handleConfigChange = useCallback(
    (newConfig: PivotConfigV1) => {
      if (stringifyPivotConfig(newConfig) !== currentConfigJson) {
        setLocalConfig(newConfig);
        setStateValue("config", newConfig);
      }
    },
    [setStateValue, currentConfigJson],
  );

  const drilldownEnabled = enable_drilldown == null || !!enable_drilldown;
  const isHybridMode = execution_mode === "threshold_hybrid";

  const handleCellClick = useCallback(
    (payload: CellClickPayload) => {
      setTriggerValue("cell_click", payload);
      if (drilldownEnabled) {
        setDrilldownPayload(payload);
        if (isHybridMode) {
          setStateValue("drilldown_request", { ...payload, page: 0 });
        }
      }
    },
    [setTriggerValue, setStateValue, drilldownEnabled, isHybridMode],
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

  const handleDrilldownMeasured = useCallback(
    (action: PerfActionMeasurement) => {
      setDebugMetrics((prev) =>
        prev ? { ...prev, lastAction: action } : prev,
      );
    },
    [],
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
      if (!prev) setWindowHeight(window.innerHeight);
      return !prev;
    });
  }, []);

  useEffect(() => {
    if (!isFullscreen) return;
    const onResize = () => setWindowHeight(window.innerHeight);
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsFullscreen(false);
    };
    window.addEventListener("resize", onResize);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("resize", onResize);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [isFullscreen]);

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
      : height != null
        ? {
            height: `${height}px`,
            display: "flex",
            flexDirection: "column" as const,
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
            config={currentConfig}
            allColumns={allColumns}
            numericColumns={numericColumns}
            syntheticSourceColumns={syntheticSourceColumns}
            onConfigChange={!locked ? handleConfigChange : undefined}
            initialConfig={initialConfig}
            locked={locked}
            frozenColumns={frozenColumns.size > 0 ? frozenColumns : undefined}
            showStickyToggle={isTableScrollable}
            pivotData={pivotData ?? undefined}
            exportFilename={export_filename}
            onCollapseChange={
              currentConfig.interactive ? handleCollapseChange : undefined
            }
            isFullscreen={isFullscreen}
            onToggleFullscreen={handleToggleFullscreen}
          />
        )}

        <div
          style={
            isFullscreen || height != null
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
                onCellClick={handleCellClick}
                maxColumns={
                  budget.columnsTruncated
                    ? budget.truncatedColumnCount
                    : undefined
                }
                containerHeight={
                  isFullscreen ? windowHeight : (height ?? max_height ?? 500)
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
                scrollable={isFullscreen || height != null}
              />
            ) : (
              <TableRenderer
                pivotData={pivotData}
                config={currentConfig}
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
                onCollapseChange={
                  currentConfig.interactive ? handleCollapseChange : undefined
                }
                menuLimit={menu_limit}
                scrollable={isFullscreen || height != null}
                maxHeight={
                  isFullscreen
                    ? undefined
                    : height == null
                      ? (max_height ?? 500)
                      : undefined
                }
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
            onClose={() => {
              setDrilldownPayload(null);
              if (isHybridMode) {
                setStateValue("drilldown_request", null);
              }
            }}
            onMeasured={handleDrilldownMeasured}
            serverRecords={isHybridMode ? drilldown_records : undefined}
            serverColumns={isHybridMode ? drilldown_columns : undefined}
            serverTotalCount={isHybridMode ? drilldown_total_count : undefined}
            isLoading={isHybridMode && !drilldown_records}
            serverPage={isHybridMode ? drilldown_page : undefined}
            serverPageSize={isHybridMode ? drilldown_page_size : undefined}
            onPageChange={
              isHybridMode
                ? (page: number) => {
                    setStateValue("drilldown_request", {
                      ...drilldownPayload,
                      page,
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
