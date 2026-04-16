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
  DndContext,
  DragOverlay,
  PointerSensor,
  KeyboardSensor,
  useSensor,
  useSensors,
  pointerWithin,
  type DragStartEvent,
  type DragOverEvent,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  SortableContext,
  horizontalListSortingStrategy,
  arrayMove,
} from "@dnd-kit/sortable";
import { useDroppable } from "@dnd-kit/core";
import type { PivotData } from "../engine/PivotData";
import {
  type ExportContent,
  type ExportFormat,
  exportPivotData,
} from "../engine/exportData";
import {
  getDimensionLabel,
  normalizeAggregationConfig,
  stringifyPivotConfig,
  type AggregationConfig,
  type AggregationType,
  type ColumnTypeMap,
  type DateGrain,
  type DimensionFilter,
  type PivotConfigV1,
  type ShowValuesAs,
  type SortConfig,
  type SyntheticMeasureConfig,
  validatePivotConfigRuntime,
  validatePivotConfigV1,
} from "../engine/types";
import { useClickOutside } from "../shared/useClickOutside";
import {
  SortableFieldChip,
  DragOverlayChip,
  makeItemId,
  parseItemId,
} from "./DndFieldChip";
import SettingsPanel, {
  canDropFieldToZone,
  InlineAggPicker,
  cleanupConfigAfterFieldChanges,
} from "./SettingsPanel";
import styles from "./Toolbar.module.css";

export interface ToolbarProps {
  config: PivotConfigV1;
  allColumns: string[];
  numericColumns: string[];
  /** Numeric columns allowed as synthetic source fields (can include hidden-from-aggregators). */
  syntheticSourceColumns?: string[];
  columnTypes?: ColumnTypeMap;
  adaptiveDateGrains?: Record<string, DateGrain>;
  onConfigChange?: (config: PivotConfigV1) => void;
  /** Original config from Python for reset. If omitted, reset is hidden. */
  initialConfig?: PivotConfigV1;
  /** Lock config controls while keeping header-menu exploration enabled. */
  locked?: boolean;
  /** Columns that cannot be reassigned between zones. */
  frozenColumns?: Set<string>;
  /** Show the sticky headers toggle (only when table content is scrollable). */
  showStickyToggle?: boolean;
  /** PivotData instance for data export. */
  pivotData?: PivotData;
  /** Base filename (without extension) for exported files. */
  exportFilename?: string;
  /** Viewer-safe collapse updates, enabled whenever interactive is true. */
  onCollapseChange?: (axis: "row" | "col", collapsed: string[]) => void;
  /** Whether the table is currently in fullscreen mode. */
  isFullscreen?: boolean;
  /** Toggle fullscreen mode on/off. */
  onToggleFullscreen?: () => void;
}

function configsEqual(a: PivotConfigV1, b: PivotConfigV1): boolean {
  return stringifyPivotConfig(a) === stringifyPivotConfig(b);
}

function stringArraysEqual(
  a: readonly string[] | undefined,
  b: readonly string[],
): boolean {
  if ((a?.length ?? 0) !== b.length) return false;
  return b.every((value, idx) => a?.[idx] === value);
}

const AGG_LABELS: Record<AggregationType, string> = {
  sum: "Sum",
  avg: "Avg",
  count: "Count",
  min: "Min",
  max: "Max",
  count_distinct: "Count Distinct",
  median: "Median",
  percentile_90: "90th Percentile",
  first: "First",
  last: "Last",
};

const AGG_CHIP_LABELS: Record<AggregationType, string> = {
  ...AGG_LABELS,
  count_distinct: "Distinct",
  percentile_90: "P90",
};

type ZoneKey = "rows" | "columns" | "values";

export interface DragMoveParams {
  sourceZone: ZoneKey;
  targetZone: ZoneKey;
  field: string;
  /** For same-zone reorder: the field being hovered over. */
  overField?: string;
  config: PivotConfigV1;
  numericColumns: string[];
}

/**
 * Pure function that computes the updated PivotConfigV1 after a drag-and-drop
 * move. Returns null when the move is invalid or a no-op.
 */
export function applyDragMove({
  sourceZone,
  targetZone,
  field,
  overField,
  config,
  numericColumns,
}: DragMoveParams): PivotConfigV1 | null {
  if (sourceZone === targetZone) {
    const arr = config[sourceZone];
    const oldIndex = arr.indexOf(field);
    const newIndex = overField ? arr.indexOf(overField) : -1;
    if (oldIndex === -1 || newIndex === -1 || oldIndex === newIndex)
      return null;

    const reordered = arrayMove(arr, oldIndex, newIndex);
    const updated: PivotConfigV1 = { ...config, [sourceZone]: reordered };
    if (sourceZone === "rows") {
      delete updated.collapsed_groups;
    } else if (sourceZone === "columns") {
      delete updated.collapsed_col_groups;
    }
    return updated;
  }

  // Cross-zone move
  if (
    !canDropFieldToZone({
      field,
      fromZone: sourceZone,
      toZone: targetZone,
      numericFields: new Set(numericColumns),
      rowFields: config.rows,
      columnFields: config.columns,
    })
  ) {
    return null;
  }

  const updated: PivotConfigV1 = {
    ...config,
    [sourceZone]: config[sourceZone].filter((f: string) => f !== field),
    [targetZone]: [...config[targetZone], field],
  };

  const syncAgg = (values: string[], agg: AggregationConfig) =>
    normalizeAggregationConfig(agg, values);

  if (targetZone === "values") {
    updated.aggregation = syncAgg(updated.values, updated.aggregation);
  }

  if (sourceZone === "values") {
    updated.aggregation = syncAgg(updated.values, updated.aggregation);
    if (updated.show_values_as?.[field]) {
      updated.show_values_as = { ...updated.show_values_as };
      delete updated.show_values_as[field];
    }
    if (Array.isArray(updated.show_row_totals)) {
      const pruned = updated.show_row_totals.filter((f) => f !== field);
      updated.show_row_totals = pruned.length > 0 ? pruned : false;
    }
    if (Array.isArray(updated.show_column_totals)) {
      const pruned = updated.show_column_totals.filter((f) => f !== field);
      updated.show_column_totals = pruned.length > 0 ? pruned : false;
    }
    if (
      updated.row_sort?.by === "value" &&
      updated.row_sort.value_field === field
    ) {
      delete updated.row_sort;
    }
    if (
      updated.col_sort?.by === "value" &&
      updated.col_sort.value_field === field
    ) {
      delete updated.col_sort;
    }
    if (updated.conditional_formatting) {
      updated.conditional_formatting = updated.conditional_formatting
        .map((rule) => {
          if (!rule.apply_to.includes(field)) return rule;
          const pruned = rule.apply_to.filter((f) => f !== field);
          return { ...rule, apply_to: pruned };
        })
        .filter((rule) => rule.apply_to.length > 0);
    }
  }

  if (sourceZone === "rows") {
    if (updated.row_sort?.dimension === field) {
      delete updated.row_sort;
    }
    delete updated.collapsed_groups;
    if (Array.isArray(updated.show_subtotals)) {
      const pruned = (updated.show_subtotals as string[]).filter(
        (f) => f !== field,
      );
      updated.show_subtotals = pruned.length > 0 ? pruned : false;
    }
  }

  if (sourceZone === "columns") {
    if (updated.col_sort?.dimension === field) {
      delete updated.col_sort;
    }
    delete updated.collapsed_col_groups;
  }

  return updated;
}

const Toolbar: FC<ToolbarProps> = ({
  config,
  allColumns,
  numericColumns,
  syntheticSourceColumns,
  columnTypes,
  adaptiveDateGrains,
  onConfigChange,
  initialConfig,
  locked,
  frozenColumns,
  showStickyToggle,
  pivotData,
  exportFilename,
  onCollapseChange,
  isFullscreen,
  onToggleFullscreen,
}): ReactElement => {
  const emitIfChanged = useCallback(
    (updated: PivotConfigV1) => {
      if (onConfigChange && !configsEqual(updated, config)) {
        onConfigChange(updated);
      }
    },
    [config, onConfigChange],
  );

  const handleValueAggChange = useCallback(
    (field: string, agg: AggregationType) => {
      emitIfChanged({
        ...config,
        aggregation: normalizeAggregationConfig(
          { ...config.aggregation, [field]: agg },
          config.values,
        ),
      });
    },
    [config, emitIfChanged],
  );

  const handleRemoveField = useCallback(
    (zone: ZoneKey, field: string) => {
      if (frozenColumns?.has(field)) return;
      const nextConfig =
        zone === "rows"
          ? { ...config, rows: config.rows.filter((item) => item !== field) }
          : zone === "columns"
            ? {
                ...config,
                columns: config.columns.filter((item) => item !== field),
              }
            : {
                ...config,
                values: config.values.filter((item) => item !== field),
              };
      emitIfChanged(cleanupConfigAfterFieldChanges(config, nextConfig));
    },
    [config, emitIfChanged, frozenColumns],
  );

  const expandAllGroups = useCallback(() => {
    if (!onCollapseChange) return;
    if (!stringArraysEqual(config.collapsed_groups, [])) {
      onCollapseChange("row", []);
    }
  }, [config, onCollapseChange]);

  const collapseAllGroups = useCallback(() => {
    if (config.rows.length < 2) return;
    const next = ["__ALL__"];
    if (!onCollapseChange) return;
    if (!stringArraysEqual(config.collapsed_groups, next)) {
      onCollapseChange("row", next);
    }
  }, [config, onCollapseChange]);

  const expandAllColGroups = useCallback(() => {
    if (!onCollapseChange) return;
    if (!stringArraysEqual(config.collapsed_col_groups, [])) {
      onCollapseChange("col", []);
    }
  }, [config, onCollapseChange]);

  const collapseAllColGroups = useCallback(() => {
    if (config.columns.length < 2) return;
    const next = ["__ALL__"];
    if (!onCollapseChange) return;
    if (!stringArraysEqual(config.collapsed_col_groups, next)) {
      onCollapseChange("col", next);
    }
  }, [config, onCollapseChange]);

  const handleSwapRowsCols = useCallback(() => {
    emitIfChanged({
      ...config,
      rows: config.columns,
      columns: config.rows,
      row_sort: config.col_sort,
      col_sort: config.row_sort,
    });
  }, [config, emitIfChanged]);

  // -------------------------------------------------------------------------
  // Drag-and-drop state & handlers
  // -------------------------------------------------------------------------
  const [activeId, setActiveId] = useState<string | null>(null);
  const [activeOverZone, setActiveOverZone] = useState<string | null>(null);

  const pointerSensor = useSensor(PointerSensor, {
    activationConstraint: { distance: 5 },
  });
  const keyboardSensor = useSensor(KeyboardSensor);
  const sensors = useSensors(pointerSensor, keyboardSensor);

  const activeField = useMemo(() => {
    if (!activeId) return null;
    return parseItemId(activeId);
  }, [activeId]);
  const toolbarRef = useRef<HTMLDivElement>(null);
  const settingsWrapperRef = useRef<HTMLDivElement>(null);

  const handleDragStart = useCallback((event: DragStartEvent) => {
    setActiveId(String(event.active.id));
  }, []);

  const handleDragOver = useCallback(
    (event: DragOverEvent) => {
      const { active, over } = event;
      if (!over) {
        setActiveOverZone(null);
        return;
      }

      const sourceZone = (active.data.current as { zone: string })?.zone;
      const field = (active.data.current as { field: string })?.field;
      let targetZone: string | undefined;
      if (over.data.current?.type === "container") {
        targetZone = String(over.id);
      } else {
        targetZone = (over.data.current as { zone?: string })?.zone;
      }

      if (!targetZone || targetZone === sourceZone) {
        setActiveOverZone(null);
        return;
      }

      if (targetZone === "values" && field && !numericColumns.includes(field)) {
        setActiveOverZone(null);
        return;
      }

      setActiveOverZone(targetZone);
    },
    [numericColumns],
  );

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { active, over } = event;
      setActiveId(null);
      setActiveOverZone(null);

      if (!over) return;

      const sourceZone = (active.data.current as { zone: string })?.zone;
      const field = (active.data.current as { field: string })?.field;
      if (!sourceZone || !field) return;

      let targetZone: string | undefined;
      if (over.data.current?.type === "container") {
        targetZone = String(over.id);
      } else {
        targetZone = (over.data.current as { zone?: string })?.zone;
      }
      if (!targetZone) return;

      const overField = (over.data.current as { field?: string })?.field;
      const updated = applyDragMove({
        sourceZone: sourceZone as ZoneKey,
        targetZone: targetZone as ZoneKey,
        field,
        overField,
        config,
        numericColumns,
      });
      if (updated) emitIfChanged(updated);
    },
    [config, emitIfChanged, numericColumns],
  );

  const utilGroupRef = useRef<HTMLDivElement>(null);
  const activeToolbarIdx = useRef(0);

  // Reconcile roving tabIndex after every render (buttons may appear/disappear)
  useLayoutEffect(() => {
    const group = utilGroupRef.current;
    if (!group) return;
    const buttons = Array.from(
      group.querySelectorAll<HTMLElement>("button[data-toolbar-btn]"),
    );
    if (buttons.length === 0) return;
    const idx = Math.min(activeToolbarIdx.current, buttons.length - 1);
    activeToolbarIdx.current = idx;
    buttons.forEach((b, i) => {
      b.tabIndex = i === idx ? 0 : -1;
    });
  });

  // Settings panel open state
  const [settingsOpen, setSettingsOpen] = useState(false);
  const handleSettingsClose = useCallback(() => setSettingsOpen(false), []);
  const settingsCloseRef = useRef(handleSettingsClose);
  settingsCloseRef.current = handleSettingsClose;

  // Close settings panel on drag start (avoids stale-state conflicts)
  const handleDragStartWithSettingsClose = useCallback(
    (event: DragStartEvent) => {
      handleDragStart(event);
      settingsCloseRef.current();
    },
    [handleDragStart],
  );

  // Group expand/collapse state
  const showRowGroups =
    !!config.show_subtotals && config.rows.length >= 2 && !!onCollapseChange;
  const showColGroups = config.columns.length >= 2 && !!onCollapseChange;

  // Zone card content helpers
  const syntheticMeasures = config.synthetic_measures ?? [];
  const emptyHint = locked ? undefined : "Apply fields in settings menu";

  return (
    <div
      ref={toolbarRef}
      data-testid="pivot-toolbar"
      className={`${styles.toolbar} ${locked ? styles.toolbarLocked : ""}`}
    >
      <DndContext
        sensors={sensors}
        collisionDetection={pointerWithin}
        onDragStart={handleDragStartWithSettingsClose}
        onDragOver={handleDragOver}
        onDragEnd={handleDragEnd}
      >
        <ZoneCard
          label="Rows"
          testId="toolbar-rows"
          zoneId="rows"
          selected={config.rows}
          disabled={locked}
          frozenColumns={frozenColumns}
          sortConfig={config.row_sort}
          filters={config.filters}
          isDropHighlighted={activeOverZone === "rows"}
          activeId={activeId}
          emptyHint={emptyHint}
          onRemove={(field) => handleRemoveField("rows", field)}
          displayLabelForField={(field) =>
            getDimensionLabel(
              config,
              field,
              columnTypes?.get(field),
              adaptiveDateGrains?.[field],
            )
          }
        />
        <ZoneCard
          label="Columns"
          testId="toolbar-columns"
          zoneId="columns"
          selected={config.columns}
          disabled={locked}
          frozenColumns={frozenColumns}
          sortConfig={config.col_sort}
          filters={config.filters}
          isDropHighlighted={activeOverZone === "columns"}
          activeId={activeId}
          emptyHint={emptyHint}
          onRemove={(field) => handleRemoveField("columns", field)}
          displayLabelForField={(field) =>
            getDimensionLabel(
              config,
              field,
              columnTypes?.get(field),
              adaptiveDateGrains?.[field],
            )
          }
        />
        <ZoneCard
          label="Values"
          testId="toolbar-values"
          zoneId="values"
          selected={config.values}
          disabled={locked}
          frozenColumns={frozenColumns}
          isDropHighlighted={activeOverZone === "values"}
          activeId={activeId}
          emptyHint={emptyHint}
          isValues
          aggregation={config.aggregation}
          showValuesAs={config.show_values_as}
          syntheticMeasures={syntheticMeasures}
          onAggregationChange={handleValueAggChange}
          onRemove={(field) => handleRemoveField("values", field)}
          aggPortalTarget={toolbarRef.current}
          aggTestIdPrefix="toolbar-values-agg"
        />
        <DragOverlay dropAnimation={{ duration: 200 }}>
          {activeField ? (
            <DragOverlayChip
              field={activeField.field}
              displayLabel={
                activeField.zone === "rows" || activeField.zone === "columns"
                  ? getDimensionLabel(
                      config,
                      activeField.field,
                      columnTypes?.get(activeField.field),
                      adaptiveDateGrains?.[activeField.field],
                    )
                  : undefined
              }
              testId={`toolbar-${activeField.zone}`}
              isValuesField={activeField.zone === "values"}
              aggregationLabel={
                activeField.zone === "values"
                  ? AGG_CHIP_LABELS[
                      config.aggregation?.[activeField.field] ?? "sum"
                    ]
                  : undefined
              }
            />
          ) : null}
        </DragOverlay>
      </DndContext>

      <div
        ref={utilGroupRef}
        className={`${styles.utilGroup} ${settingsOpen ? styles.utilGroupPinned : ""}`}
        role="toolbar"
        aria-label="Table actions"
        onKeyDown={(e) => {
          if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
          const target = e.target as HTMLElement;
          if (!target.hasAttribute("data-toolbar-btn")) return;
          const group = utilGroupRef.current;
          if (!group) return;
          const buttons = Array.from(
            group.querySelectorAll<HTMLElement>("button[data-toolbar-btn]"),
          );
          if (buttons.length === 0) return;
          const idx = buttons.indexOf(target);
          const next =
            e.key === "ArrowRight"
              ? (idx + 1) % buttons.length
              : (idx - 1 + buttons.length) % buttons.length;
          e.preventDefault();
          activeToolbarIdx.current = next;
          buttons.forEach((b, i) => {
            b.tabIndex = i === next ? 0 : -1;
          });
          buttons[next]?.focus();
        }}
        onFocus={(e) => {
          const target = e.target as HTMLElement;
          if (!target.hasAttribute("data-toolbar-btn")) return;
          const group = utilGroupRef.current;
          if (!group) return;
          const buttons = Array.from(
            group.querySelectorAll<HTMLElement>("button[data-toolbar-btn]"),
          );
          const idx = buttons.indexOf(target);
          if (idx !== -1) {
            activeToolbarIdx.current = idx;
            buttons.forEach((b, i) => {
              b.tabIndex = i === idx ? 0 : -1;
            });
          }
        }}
      >
        {!locked && initialConfig && !configsEqual(config, initialConfig) && (
          <span className={styles.tooltipWrap} data-tooltip="Reset Config">
            <button
              data-testid="toolbar-reset"
              data-toolbar-btn
              tabIndex={-1}
              className={`${styles.utilButton} ${styles.iconButton}`}
              onClick={() => onConfigChange?.(initialConfig)}
              aria-label="Reset config"
            >
              <ResetIcon />
            </button>
          </span>
        )}
        {!locked && (
          <span
            className={styles.tooltipWrap}
            data-tooltip="Swap Rows / Columns"
          >
            <button
              data-testid="toolbar-swap"
              data-toolbar-btn
              tabIndex={-1}
              className={`${styles.utilButton} ${styles.iconButton}`}
              onClick={handleSwapRowsCols}
              aria-label="Swap rows and columns"
            >
              <SwapIcon />
            </button>
          </span>
        )}
        {!locked && onConfigChange && (
          <ConfigIOControls
            config={config}
            columnTypes={columnTypes}
            onConfigChange={onConfigChange}
          />
        )}
        {pivotData && (
          <ExportDataControls
            pivotData={pivotData}
            config={config}
            exportFilename={exportFilename}
            adaptiveDateGrains={adaptiveDateGrains}
          />
        )}
        {showRowGroups && (
          <>
            <span className={styles.tooltipWrap} data-tooltip="Expand All">
              <button
                data-testid="pivot-group-toggle-expand-all"
                data-toolbar-btn
                tabIndex={-1}
                className={`${styles.utilButton} ${styles.iconButton}`}
                onClick={expandAllGroups}
                aria-label="Expand all row groups"
              >
                <ExpandIcon />
              </button>
            </span>
            <span className={styles.tooltipWrap} data-tooltip="Collapse All">
              <button
                data-testid="pivot-group-toggle-collapse-all"
                data-toolbar-btn
                tabIndex={-1}
                className={`${styles.utilButton} ${styles.iconButton}`}
                onClick={collapseAllGroups}
                aria-label="Collapse all row groups"
              >
                <CollapseIcon />
              </button>
            </span>
          </>
        )}
        {showColGroups && (
          <>
            <span className={styles.tooltipWrap} data-tooltip="Expand Columns">
              <button
                data-testid="pivot-col-group-expand-all"
                data-toolbar-btn
                tabIndex={-1}
                className={`${styles.utilButton} ${styles.iconButton}`}
                onClick={expandAllColGroups}
                aria-label="Expand all column groups"
              >
                <ExpandIcon />
              </button>
            </span>
            <span
              className={styles.tooltipWrap}
              data-tooltip="Collapse Columns"
            >
              <button
                data-testid="pivot-col-group-collapse-all"
                data-toolbar-btn
                tabIndex={-1}
                className={`${styles.utilButton} ${styles.iconButton}`}
                onClick={collapseAllColGroups}
                aria-label="Collapse all column groups"
              >
                <CollapseIcon />
              </button>
            </span>
          </>
        )}
        {onToggleFullscreen && (
          <span
            className={styles.tooltipWrap}
            data-tooltip={isFullscreen ? "Exit Fullscreen" : "Fullscreen"}
          >
            <button
              data-testid="toolbar-fullscreen"
              data-toolbar-btn
              tabIndex={-1}
              className={`${styles.utilButton} ${styles.iconButton}`}
              onClick={onToggleFullscreen}
              aria-label={isFullscreen ? "Exit fullscreen" : "Enter fullscreen"}
            >
              {isFullscreen ? <FullscreenExitIcon /> : <FullscreenEnterIcon />}
            </button>
          </span>
        )}
        <div ref={settingsWrapperRef} className={styles.settingsWrapper}>
          <span className={styles.tooltipWrap} data-tooltip="Table Settings">
            <button
              data-testid="toolbar-settings"
              data-toolbar-btn
              tabIndex={-1}
              className={`${styles.utilButton} ${styles.iconButton} ${settingsOpen ? styles.iconButtonActive : ""}`}
              onClick={() => setSettingsOpen((v) => !v)}
              aria-label="Table settings"
              aria-haspopup="dialog"
              aria-expanded={settingsOpen}
            >
              <SettingsIcon />
            </button>
          </span>
          <SettingsPanel
            config={config}
            allColumns={allColumns}
            numericColumns={numericColumns}
            syntheticSourceColumns={syntheticSourceColumns}
            onConfigChange={onConfigChange!}
            locked={locked}
            frozenColumns={frozenColumns}
            showStickyToggle={showStickyToggle}
            open={settingsOpen}
            onClose={handleSettingsClose}
            excludeRef={settingsWrapperRef}
          />
        </div>
      </div>
    </div>
  );
};

// ---------------------------------------------------------------------------
// ZoneCard — read-only zone display with cross-zone DnD chips
// ---------------------------------------------------------------------------

const SHOW_AS_BADGE_LABELS: Record<string, string> = {
  pct_of_total: "% of Grand Total",
  pct_of_row: "% of Row Total",
  pct_of_col: "% of Column Total",
  diff_from_prev: "vs previous period",
  pct_diff_from_prev: "% vs previous period",
  diff_from_prev_year: "vs prior year",
  pct_diff_from_prev_year: "% vs prior year",
};

interface ZoneCardProps {
  label: string;
  testId: string;
  zoneId: string;
  selected: string[];
  disabled?: boolean;
  frozenColumns?: Set<string>;
  sortConfig?: SortConfig;
  filters?: Record<string, DimensionFilter>;
  showValuesAs?: Record<string, ShowValuesAs>;
  aggregation?: AggregationConfig;
  syntheticMeasures?: SyntheticMeasureConfig[];
  isDropHighlighted?: boolean;
  activeId?: string | null;
  onAggregationChange?: (field: string, agg: AggregationType) => void;
  onRemove?: (field: string) => void;
  aggPortalTarget?: HTMLElement | null;
  aggTestIdPrefix?: string;
  displayLabelForField?: (field: string) => string;
  isValues?: boolean;
  emptyHint?: string;
}

const ZoneCard: FC<ZoneCardProps> = ({
  label,
  testId,
  zoneId,
  selected,
  disabled,
  frozenColumns,
  sortConfig,
  filters,
  showValuesAs,
  aggregation,
  syntheticMeasures = [],
  isDropHighlighted,
  activeId,
  onAggregationChange,
  onRemove,
  aggPortalTarget,
  aggTestIdPrefix,
  displayLabelForField,
  isValues,
  emptyHint,
}): ReactElement => {
  const resolvedEmptyHint =
    emptyHint ?? (disabled ? undefined : "Drag fields here");
  const { setNodeRef: setDroppableRef } = useDroppable({
    id: zoneId,
    data: { type: "container" },
  });
  const sortableIds = useMemo(
    () => selected.map((f) => makeItemId(zoneId, f)),
    [zoneId, selected],
  );

  return (
    <div
      className={`${styles.fieldGroup} ${isDropHighlighted ? styles.dropZoneActive : ""}`}
      ref={setDroppableRef}
    >
      <div className={styles.sectionHeader} data-testid={`${testId}-header`}>
        <span className={styles.sectionTitle}>{label}</span>
        {sortConfig && (
          <span
            className={styles.chipSortIndicator}
            title="Sorted"
            data-testid={`${testId}-sort-indicator`}
          >
            ⇅
          </span>
        )}
        {selected.length > 0 && (
          <span className={styles.countBadge}>{selected.length}</span>
        )}
      </div>

      <SortableContext
        items={sortableIds}
        strategy={horizontalListSortingStrategy}
      >
        {selected.length > 0 || (isValues && syntheticMeasures.length > 0) ? (
          <div className={styles.chipRow} data-testid={`${testId}-chips`}>
            {selected.map((col) => {
              const isFrozen = frozenColumns?.has(col);
              const hasFilter =
                filters?.[col] &&
                ((filters[col].include && filters[col].include.length > 0) ||
                  (filters[col].exclude && filters[col].exclude.length > 0));
              const currentShowAs = showValuesAs?.[col] ?? "raw";
              const currentAggregation = aggregation?.[col] ?? "sum";
              const showAsBadge = currentShowAs !== "raw";
              return (
                <SortableFieldChip
                  key={col}
                  id={makeItemId(zoneId, col)}
                  zone={zoneId}
                  field={col}
                  displayLabel={displayLabelForField?.(col)}
                  testId={testId}
                  isFrozen={isFrozen}
                  disabled={disabled}
                  isValuesField={isValues}
                  aggregationLabel={
                    isValues ? AGG_CHIP_LABELS[currentAggregation] : undefined
                  }
                  showAsBadge={showAsBadge}
                  showAsBadgeTitle={
                    showAsBadge
                      ? (SHOW_AS_BADGE_LABELS[currentShowAs] ?? currentShowAs)
                      : undefined
                  }
                  hasFilter={!!hasFilter}
                  onRemove={disabled ? undefined : onRemove}
                  aggregationControl={
                    isValues && !disabled && onAggregationChange ? (
                      <InlineAggPicker
                        field={col}
                        value={currentAggregation}
                        onChange={onAggregationChange}
                        portalTarget={aggPortalTarget}
                        testIdPrefix={aggTestIdPrefix}
                        triggerVariant="chevron"
                      />
                    ) : undefined
                  }
                  activeId={activeId}
                />
              );
            })}
            {isValues &&
              syntheticMeasures.map((measure) => (
                <span
                  key={measure.id}
                  className={styles.chip}
                  data-testid={`${testId}-synthetic-${measure.id}`}
                >
                  <span className={styles.aggChipIcon}>fx</span>
                  {measure.label}
                </span>
              ))}
          </div>
        ) : resolvedEmptyHint ? (
          <div className={styles.emptyDropZone}>{resolvedEmptyHint}</div>
        ) : null}
      </SortableContext>
    </div>
  );
};

// ---------------------------------------------------------------------------
// SVG icon helpers for config I/O
// ---------------------------------------------------------------------------

const CopyIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
  </svg>
);

const CheckIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="20 6 9 17 4 12" />
  </svg>
);

const ImportIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="16 18 22 12 16 6" />
    <polyline points="8 6 2 12 8 18" />
  </svg>
);

const SwapIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="7 16 3 12 7 8" />
    <line x1="3" y1="12" x2="21" y2="12" />
    <polyline points="17 8 21 12 17 16" />
  </svg>
);

const ResetIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="1 4 1 10 7 10" />
    <path d="M3.51 15a9 9 0 1 0 2.13-9.36L1 10" />
  </svg>
);

const ExpandIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="15 3 21 3 21 9" />
    <polyline points="9 21 3 21 3 15" />
    <line x1="21" y1="3" x2="14" y2="10" />
    <line x1="3" y1="21" x2="10" y2="14" />
  </svg>
);

const CollapseIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="4 14 10 14 10 20" />
    <polyline points="20 10 14 10 14 4" />
    <line x1="14" y1="10" x2="21" y2="3" />
    <line x1="3" y1="21" x2="10" y2="14" />
  </svg>
);

const FullscreenEnterIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="9 3 3 3 3 9" />
    <polyline points="15 3 21 3 21 9" />
    <polyline points="3 15 3 21 9 21" />
    <polyline points="21 15 21 21 15 21" />
  </svg>
);

const FullscreenExitIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="10 4 10 8 6 8" />
    <polyline points="14 4 14 8 18 8" />
    <polyline points="6 16 10 16 10 20" />
    <polyline points="18 16 14 16 14 20" />
  </svg>
);

const SettingsIcon: FC = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
    <path d="M21 5c0-1.1-.9-2-2-2h-9v5h11V5zM3 19c0 1.1.9 2 2 2h3V10H3v9zM3 5v3h5V3H5c-1.1 0-2 .9-2 2zm15 3.99L14 13l1.41 1.41 1.59-1.6V15c0 1.1-.9 2-2 2h-2.17l1.59-1.59L13 14l-4 4 4 4 1.41-1.41L12.83 19H15c2.21 0 4-1.79 4-4v-2.18l1.59 1.6L22 13l-4-4.01z" />
  </svg>
);

// ---------------------------------------------------------------------------
// Config I/O Controls
// ---------------------------------------------------------------------------

interface ConfigIOControlsProps {
  config: PivotConfigV1;
  columnTypes?: ColumnTypeMap;
  onConfigChange: (config: PivotConfigV1) => void;
}

const ConfigIOControls: FC<ConfigIOControlsProps> = ({
  config,
  columnTypes,
  onConfigChange,
}): ReactElement => {
  const [importing, setImporting] = useState(false);
  const [importText, setImportText] = useState("");
  const [importError, setImportError] = useState<string | null>(null);
  const [exportFeedback, setExportFeedback] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const closeImport = useCallback(() => {
    setImporting(false);
    requestAnimationFrame(() => triggerRef.current?.focus());
  }, []);
  useClickOutside(containerRef, closeImport, importing);

  useEffect(() => {
    if (!importing) return;
    requestAnimationFrame(() => {
      const textarea = panelRef.current?.querySelector<HTMLElement>("textarea");
      (textarea ?? panelRef.current)?.focus();
    });
  }, [importing]);

  const handleImportKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        closeImport();
      }
    },
    [closeImport],
  );

  const handleImportFocusOut = useCallback(() => {
    requestAnimationFrame(() => {
      if (!panelRef.current) return;
      const root = panelRef.current.getRootNode() as Document | ShadowRoot;
      if (!panelRef.current.contains(root.activeElement)) {
        closeImport();
      }
    });
  }, [closeImport]);

  const handleExport = useCallback(() => {
    const json = JSON.stringify(config, null, 2);
    navigator.clipboard.writeText(json).then(
      () => {
        setExportFeedback("Copied!");
        setTimeout(() => setExportFeedback(null), 2000);
      },
      () => {
        setExportFeedback("Copy failed");
        setTimeout(() => setExportFeedback(null), 2000);
      },
    );
  }, [config]);

  const handleImportApply = useCallback(() => {
    try {
      const parsed = JSON.parse(importText);
      const validated = validatePivotConfigV1(parsed);
      onConfigChange(validatePivotConfigRuntime(validated, columnTypes));
      setImporting(false);
      setImportText("");
      setImportError(null);
      requestAnimationFrame(() => triggerRef.current?.focus());
    } catch (e) {
      setImportError(e instanceof Error ? e.message : "Invalid JSON");
    }
  }, [columnTypes, importText, onConfigChange]);

  return (
    <>
      <span
        className={styles.tooltipWrap}
        data-tooltip={exportFeedback || "Copy Config"}
      >
        <button
          data-testid="toolbar-export"
          data-toolbar-btn
          tabIndex={-1}
          className={`${styles.utilButton} ${styles.iconButton}`}
          onClick={handleExport}
          aria-label="Copy config to clipboard"
        >
          {exportFeedback ? <CheckIcon /> : <CopyIcon />}
        </button>
      </span>
      <div className={styles.importWrapper} ref={containerRef}>
        <span className={styles.tooltipWrap} data-tooltip="Import Config">
          <button
            ref={triggerRef}
            data-testid="toolbar-import-toggle"
            data-toolbar-btn
            tabIndex={-1}
            className={`${styles.utilButton} ${styles.iconButton} ${importing ? styles.iconButtonActive : ""}`}
            onClick={() => {
              setImporting((v) => !v);
              setImportError(null);
            }}
            aria-label="Import config from JSON"
            aria-haspopup="dialog"
            aria-expanded={importing}
          >
            <ImportIcon />
          </button>
        </span>
        {importing && (
          <div
            ref={panelRef}
            tabIndex={-1}
            className={styles.importPopover}
            data-testid="toolbar-import-panel"
            role="dialog"
            aria-label="Import configuration"
            onKeyDown={handleImportKeyDown}
            onBlur={handleImportFocusOut}
          >
            <textarea
              data-testid="toolbar-import-textarea"
              className={styles.importTextarea}
              value={importText}
              onChange={(e) => {
                setImportText(e.target.value);
                setImportError(null);
              }}
              placeholder="Paste config JSON..."
              rows={5}
              aria-label="Configuration JSON"
            />
            {importError && (
              <div
                className={styles.importError}
                role="alert"
                data-testid="toolbar-import-error"
              >
                {importError}
              </div>
            )}
            <button
              type="button"
              data-testid="toolbar-import-apply"
              className={styles.exportActionButton}
              onClick={handleImportApply}
            >
              Apply
            </button>
          </div>
        )}
      </div>
    </>
  );
};

// ---------------------------------------------------------------------------
// Radio Toggle Group (arrows move focus, Space/Enter selects)
// ---------------------------------------------------------------------------

interface RadioToggleGroupProps<T extends string> {
  labelId: string;
  label: string;
  options: { id: T; label: string }[];
  value: T;
  onChange: (value: T) => void;
  testIdPrefix: string;
}

function RadioToggleGroup<T extends string>({
  labelId,
  label,
  options,
  value,
  onChange,
  testIdPrefix,
}: RadioToggleGroupProps<T>): ReactElement {
  const groupRef = useRef<HTMLDivElement>(null);

  const handleGroupKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
    e.preventDefault();
    e.stopPropagation();
    const buttons = groupRef.current
      ? Array.from(groupRef.current.querySelectorAll<HTMLElement>("button"))
      : [];
    if (buttons.length === 0) return;
    const root = groupRef.current?.getRootNode() as Document | ShadowRoot;
    const active = root?.activeElement as HTMLElement | null;
    const idx = active ? buttons.indexOf(active) : 0;
    const next =
      e.key === "ArrowRight"
        ? (idx + 1) % buttons.length
        : (idx - 1 + buttons.length) % buttons.length;
    buttons[next]?.focus();
  }, []);

  const handleSelect = useCallback(
    (e: React.KeyboardEvent, id: T) => {
      if (e.key === " " || e.key === "Enter") {
        e.preventDefault();
        onChange(id);
      }
    },
    [onChange],
  );

  return (
    <div className={styles.exportRow}>
      <span className={styles.exportLabel} id={labelId}>
        {label}
      </span>
      <div
        ref={groupRef}
        className={styles.exportToggleGroup}
        role="radiogroup"
        aria-labelledby={labelId}
        onKeyDown={handleGroupKeyDown}
      >
        {options.map((opt) => (
          <button
            key={opt.id}
            type="button"
            role="radio"
            aria-checked={value === opt.id}
            tabIndex={value === opt.id ? 0 : -1}
            className={`${styles.exportToggle} ${value === opt.id ? styles.exportToggleActive : ""}`}
            onClick={() => onChange(opt.id)}
            onKeyDown={(e) => handleSelect(e, opt.id)}
            data-testid={`${testIdPrefix}-${opt.id}`}
            data-label={opt.label}
          >
            {opt.label}
          </button>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Data Export Controls
// ---------------------------------------------------------------------------

const DownloadIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
    <polyline points="7 10 12 15 17 10" />
    <line x1="12" y1="15" x2="12" y2="3" />
  </svg>
);

interface ExportDataControlsProps {
  pivotData: PivotData;
  config: PivotConfigV1;
  exportFilename?: string;
  adaptiveDateGrains?: Record<string, DateGrain>;
}

const FORMAT_OPTIONS: { id: ExportFormat; label: string }[] = [
  { id: "xlsx", label: "Excel" },
  { id: "csv", label: "CSV" },
  { id: "tsv", label: "TSV" },
  { id: "clipboard", label: "Clipboard" },
];

const CONTENT_OPTIONS: { id: ExportContent; label: string }[] = [
  { id: "formatted", label: "Formatted" },
  { id: "raw", label: "Raw" },
];

const ExportDataControls: FC<ExportDataControlsProps> = ({
  pivotData,
  config,
  exportFilename,
  adaptiveDateGrains,
}): ReactElement => {
  const [open, setOpen] = useState(false);
  const [format, setFormat] = useState<ExportFormat>("xlsx");
  const [content, setContent] = useState<ExportContent>("formatted");
  const [feedback, setFeedback] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const closePopover = useCallback(() => {
    setOpen(false);
    requestAnimationFrame(() => triggerRef.current?.focus());
  }, []);
  useClickOutside(containerRef, closePopover, open);

  useEffect(() => {
    if (!open) return;
    requestAnimationFrame(() => {
      const firstRadio = panelRef.current?.querySelector<HTMLElement>(
        "[role='radio'][aria-checked='true']",
      );
      (firstRadio ?? panelRef.current)?.focus();
    });
  }, [open]);

  const panelKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        closePopover();
      }
    },
    [closePopover],
  );

  const handleExportFocusOut = useCallback(() => {
    requestAnimationFrame(() => {
      if (!panelRef.current) return;
      const root = panelRef.current.getRootNode() as Document | ShadowRoot;
      if (!panelRef.current.contains(root.activeElement)) {
        closePopover();
      }
    });
  }, [closePopover]);

  const handleExport = useCallback(async () => {
    const ok = await exportPivotData(
      pivotData,
      config,
      { format, content },
      exportFilename,
      adaptiveDateGrains,
    );
    if (format === "clipboard") {
      setFeedback(ok ? "Copied!" : "Copy failed");
    } else {
      setFeedback(ok ? "Downloaded!" : "Download failed");
    }
    setTimeout(() => setFeedback(null), 2000);
    setOpen(false);
    requestAnimationFrame(() => triggerRef.current?.focus());
  }, [pivotData, config, format, content, exportFilename, adaptiveDateGrains]);

  const buttonLabel = feedback ?? "Export Data";

  return (
    <div className={styles.importWrapper} ref={containerRef}>
      <span className={styles.tooltipWrap} data-tooltip={buttonLabel}>
        <button
          ref={triggerRef}
          data-testid="toolbar-export-data"
          data-toolbar-btn
          tabIndex={-1}
          className={`${styles.utilButton} ${styles.iconButton} ${open ? styles.iconButtonActive : ""}`}
          onClick={() => setOpen((v) => !v)}
          aria-label="Export pivot table data"
          aria-haspopup="dialog"
          aria-expanded={open}
        >
          {feedback ? <CheckIcon /> : <DownloadIcon />}
        </button>
      </span>
      {open && (
        <div
          ref={panelRef}
          tabIndex={-1}
          className={styles.importPopover}
          data-testid="toolbar-export-data-panel"
          role="dialog"
          aria-label="Export data options"
          onKeyDown={panelKeyDown}
          onBlur={handleExportFocusOut}
        >
          <RadioToggleGroup
            labelId="export-format-label"
            label="Format"
            options={FORMAT_OPTIONS}
            value={format}
            onChange={setFormat}
            testIdPrefix="export-format"
          />
          <RadioToggleGroup
            labelId="export-content-label"
            label="Content"
            options={CONTENT_OPTIONS}
            value={content}
            onChange={setContent}
            testIdPrefix="export-content"
          />
          <button
            type="button"
            className={styles.exportActionButton}
            onClick={handleExport}
            data-testid="toolbar-export-data-action"
          >
            {format === "clipboard"
              ? "Copy to Clipboard"
              : format === "xlsx"
                ? "Export Excel"
                : `Export ${format.toUpperCase()}`}
          </button>
        </div>
      )}
    </div>
  );
};

// SettingsPopover is replaced by the SettingsPanel component (./SettingsPanel.tsx)

export default Toolbar;
