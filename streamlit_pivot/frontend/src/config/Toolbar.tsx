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
  ReactNode,
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
  getRenderedValueFields,
  getRenderedValueLabel,
  normalizeAggregationConfig,
  reconcileValueOrder,
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
  getPrimarySortConfig,
  pruneSortConfigByField,
  validatePivotConfigRuntime,
  validatePivotConfigV1,
} from "../engine/types";
import { useClickOutside } from "../shared/useClickOutside";
import {
  SortableFieldChip,
  SortableSyntheticChip,
  DragOverlayChip,
  SyntheticDragOverlayChip,
  makeItemId,
  parseItemId,
} from "./DndFieldChip";
import SettingsPanel, {
  canDropFieldToZone,
  InlineAggPicker,
  InlineDisplayPicker,
  cleanupConfigAfterFieldChanges,
} from "./SettingsPanel";
import styles from "./Toolbar.module.css";

export interface ToolbarProps {
  /** Stable per-instance key from the component framework; forwarded to SettingsPanel for localStorage namespacing. */
  instanceKey?: string;
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
  /** PivotData instance for data export and filter picker. */
  pivotData?: PivotData;
  /** Pre-computed unique values sidecar for off-axis filter fields (hybrid mode). */
  filterFieldValues?: Record<string, string[]>;
  /** Top-level container element for picker portals (from PivotRoot). */
  pickerPortalTarget?: Element | null;
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

/**
 * Extract the canonical measure id (real field name or synthetic id) from a
 * dnd-kit drag-data payload. Returns null for non-value-zone drags.
 */
function getMeasureIdFromDragData(
  data:
    | {
        zone?: string;
        field?: string;
        type?: string;
        syntheticId?: string;
      }
    | undefined,
): string | null {
  if (!data) return null;
  if (data.type === "synthetic") return data.syntheticId ?? null;
  if (data.zone === "values") return data.field ?? null;
  return null;
}

/**
 * Apply a new Values display order, clearing `value_order` when it matches the
 * default `[...values, ...synthetic_ids]` so the config stays minimal.
 */
function withValueOrder(
  config: PivotConfigV1,
  nextOrder: readonly string[],
): PivotConfigV1 {
  const syntheticIds = (config.synthetic_measures ?? []).map((m) => m.id);
  const defaults = [...config.values, ...syntheticIds];
  const isDefault =
    nextOrder.length === defaults.length &&
    nextOrder.every((id, i) => id === defaults[i]);
  if (isDefault) {
    if (config.value_order === undefined) return config;
    const { value_order: _omit, ...rest } = config;
    return rest;
  }
  return { ...config, value_order: [...nextOrder] };
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

type ZoneKey = "rows" | "columns" | "values" | "filters";

export interface DragMoveParams {
  sourceZone: ZoneKey;
  targetZone: ZoneKey;
  field: string;
  /** For same-zone reorder: the field being hovered over. */
  overField?: string;
  config: PivotConfigV1;
  numericColumns: string[];
}

/** Returns the array for a given zone key, handling 'filters' → filter_fields. */
function getZoneArray(zone: ZoneKey, cfg: PivotConfigV1): string[] {
  return zone === "filters" ? (cfg.filter_fields ?? []) : cfg[zone];
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
    const arr = getZoneArray(sourceZone, config);
    const oldIndex = arr.indexOf(field);
    const newIndex = overField ? arr.indexOf(overField) : -1;
    if (oldIndex === -1 || newIndex === -1 || oldIndex === newIndex)
      return null;

    const reordered = arrayMove(arr, oldIndex, newIndex);
    let updated: PivotConfigV1;
    if (sourceZone === "filters") {
      updated = {
        ...config,
        filter_fields: reordered,
      };
    } else {
      updated = { ...config, [sourceZone]: reordered };
    }
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

  const srcArr = getZoneArray(sourceZone, config);
  const tgtArr = getZoneArray(targetZone, config);

  // For moves TO filters zone: add to filter_fields but keep in source zone (dual-role).
  // For moves OUT of filters zone: remove from filter_fields, add to target zone.
  let updated: PivotConfigV1;
  if (targetZone === "filters") {
    updated = {
      ...config,
      filter_fields: tgtArr.includes(field) ? tgtArr : [...tgtArr, field],
    };
    // Do NOT remove from source zone — filters allow dual-role
  } else if (sourceZone === "filters") {
    const rowColSet = new Set([...config.rows, ...config.columns]);
    const newFilterFields = srcArr.filter((f) => f !== field);
    updated = {
      ...config,
      filter_fields: newFilterFields.length > 0 ? newFilterFields : undefined,
      [targetZone]: [...tgtArr, field],
    };
    // Dual-role cleanup: clear filter entry only if field not also in rows/columns of result
    if (!rowColSet.has(field) && updated.filters?.[field]) {
      const newFilters = { ...updated.filters };
      delete newFilters[field];
      updated.filters =
        Object.keys(newFilters).length > 0 ? newFilters : undefined;
    }
  } else {
    updated = {
      ...config,
      [sourceZone]: srcArr.filter((f: string) => f !== field),
      [targetZone]: [...tgtArr, field],
    };
  }

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
    {
      const rs = pruneSortConfigByField(updated.row_sort, {
        valueField: field,
      });
      if (rs === undefined) delete updated.row_sort;
      else updated.row_sort = rs;
    }
    {
      const cs = pruneSortConfigByField(updated.col_sort, {
        valueField: field,
      });
      if (cs === undefined) delete updated.col_sort;
      else updated.col_sort = cs;
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
    // Prune analytical filters whose "by" measure was dragged out of values.
    if (updated.top_n_filters) {
      updated.top_n_filters = updated.top_n_filters.filter(
        (f) => f.by !== field,
      );
      if (updated.top_n_filters.length === 0) delete updated.top_n_filters;
    }
    if (updated.value_filters) {
      updated.value_filters = updated.value_filters.filter(
        (f) => f.by !== field,
      );
      if (updated.value_filters.length === 0) delete updated.value_filters;
    }
  }

  if (sourceZone === "rows") {
    {
      const rs = pruneSortConfigByField(updated.row_sort, {
        dimensionField: field,
        isFirstAxisDim: config.rows[0] === field,
      });
      if (rs === undefined) delete updated.row_sort;
      else updated.row_sort = rs;
    }
    delete updated.collapsed_groups;
    if (Array.isArray(updated.show_subtotals)) {
      const pruned = (updated.show_subtotals as string[]).filter(
        (f) => f !== field,
      );
      updated.show_subtotals = pruned.length > 0 ? pruned : false;
    }
    // Prune analytical filters on the row dimension that was dragged out.
    if (updated.top_n_filters) {
      updated.top_n_filters = updated.top_n_filters.filter(
        (f) => !((f.axis ?? "rows") === "rows" && f.field === field),
      );
      if (updated.top_n_filters.length === 0) delete updated.top_n_filters;
    }
    if (updated.value_filters) {
      updated.value_filters = updated.value_filters.filter(
        (f) => !((f.axis ?? "rows") === "rows" && f.field === field),
      );
      if (updated.value_filters.length === 0) delete updated.value_filters;
    }
  }

  if (sourceZone === "columns") {
    {
      const cs = pruneSortConfigByField(updated.col_sort, {
        dimensionField: field,
        isFirstAxisDim: config.columns[0] === field,
      });
      if (cs === undefined) delete updated.col_sort;
      else updated.col_sort = cs;
    }
    delete updated.collapsed_col_groups;
    // Prune analytical filters on the column dimension that was dragged out.
    if (updated.top_n_filters) {
      updated.top_n_filters = updated.top_n_filters.filter(
        (f) => !(f.axis === "columns" && f.field === field),
      );
      if (updated.top_n_filters.length === 0) delete updated.top_n_filters;
    }
    if (updated.value_filters) {
      updated.value_filters = updated.value_filters.filter(
        (f) => !(f.axis === "columns" && f.field === field),
      );
      if (updated.value_filters.length === 0) delete updated.value_filters;
    }
  }

  return updated;
}

const Toolbar: FC<ToolbarProps> = ({
  instanceKey,
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
  filterFieldValues,
  pickerPortalTarget,
  exportFilename,
  onCollapseChange,
  isFullscreen,
  onToggleFullscreen,
}): ReactElement => {
  const emitIfChanged = useCallback(
    (updated: PivotConfigV1) => {
      if (!onConfigChange) return;
      // Keep value_order consistent with the current values/synthetic_measures
      // (drop orphans, append new measures, strip when equal to default).
      const reconciled = reconcileValueOrder(updated.value_order, updated);
      const normalized = withValueOrder(updated, reconciled);
      if (!configsEqual(normalized, config)) {
        onConfigChange(normalized);
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

  const handleValueShowAsChange = useCallback(
    (field: string, mode: ShowValuesAs) => {
      const next = { ...(config.show_values_as ?? {}) };
      if (mode === "raw") {
        delete next[field];
      } else {
        next[field] = mode;
      }
      emitIfChanged({ ...config, show_values_as: next });
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

  const handleRemoveSynthetic = useCallback(
    (syntheticId: string) => {
      const current = config.synthetic_measures ?? [];
      const next = current.filter((m) => m.id !== syntheticId);
      if (next.length === current.length) return;
      const updated: typeof config = {
        ...config,
        synthetic_measures: next.length > 0 ? next : undefined,
      };
      // Prune analytical filters whose "by" references the removed synthetic measure.
      if (updated.top_n_filters) {
        updated.top_n_filters = updated.top_n_filters.filter(
          (f) => f.by !== syntheticId,
        );
        if (updated.top_n_filters.length === 0) delete updated.top_n_filters;
      }
      if (updated.value_filters) {
        updated.value_filters = updated.value_filters.filter(
          (f) => f.by !== syntheticId,
        );
        if (updated.value_filters.length === 0) delete updated.value_filters;
      }
      emitIfChanged(updated);
    },
    [config, emitIfChanged],
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
      const activeData = active.data.current as
        | { zone?: string; field?: string; type?: string }
        | undefined;
      // Synthetic chips only participate in Values-zone reordering.
      if (activeData?.type === "synthetic") {
        setActiveOverZone(null);
        return;
      }
      if (!over) {
        setActiveOverZone(null);
        return;
      }

      const sourceZone = activeData?.zone;
      const field = activeData?.field;
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

      const activeData = active.data.current as
        | {
            zone?: string;
            field?: string;
            type?: string;
            syntheticId?: string;
          }
        | undefined;
      const overData = over.data.current as
        | {
            zone?: string;
            field?: string;
            type?: string;
            syntheticId?: string;
          }
        | undefined;

      const activeMeasureId = getMeasureIdFromDragData(activeData);
      const overMeasureId = getMeasureIdFromDragData(overData);

      // Values-zone reorder: any combination of real/synthetic measures.
      if (
        activeMeasureId &&
        overMeasureId &&
        activeMeasureId !== overMeasureId
      ) {
        const currentOrder = getRenderedValueFields(config);
        const oldIdx = currentOrder.indexOf(activeMeasureId);
        const newIdx = currentOrder.indexOf(overMeasureId);
        if (oldIdx !== -1 && newIdx !== -1) {
          const nextOrder = arrayMove(currentOrder, oldIdx, newIdx);
          emitIfChanged(withValueOrder(config, nextOrder));
        }
        return;
      }

      // Synthetic chips cannot leave the Values zone.
      if (activeData?.type === "synthetic") return;

      const sourceZone = activeData?.zone;
      const field = activeData?.field;
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

  // Sections collapse/expand
  const sectionsExpanded = config.show_sections !== false;
  const handleToggleSections = useCallback(
    (e: React.MouseEvent<HTMLButtonElement>) => {
      // Close the settings panel whenever sections are toggled so the panel
      // doesn't linger over a collapsed/expanded layout.
      setSettingsOpen(false);
      emitIfChanged({ ...config, show_sections: !sectionsExpanded });
      // Blur immediately so :focus-within on .utilGroup clears and the toolbar
      // can fade away once the cursor leaves — matching the pre-click hover behaviour.
      e.currentTarget.blur();
    },
    [config, emitIfChanged, sectionsExpanded],
  );

  // Compact summary — labeled segments: "Rows Region · Cols Year · Values Revenue, Profit"
  const compactSummaryText = useMemo((): ReactNode => {
    type Segment = { label: string; fields: string[] };
    const segments: Segment[] = [];
    if (config.rows.length > 0)
      segments.push({
        label: "Rows",
        fields: config.rows.map((f) => getDimensionLabel(config, f)),
      });
    if (config.columns.length > 0)
      segments.push({
        label: "Cols",
        fields: config.columns.map((f) => getDimensionLabel(config, f)),
      });
    const valueFields = getRenderedValueFields(config);
    if (valueFields.length > 0)
      segments.push({
        label: "Values",
        fields: valueFields.map((f) => getRenderedValueLabel(config, f)),
      });
    if (segments.length === 0) return null;
    return segments.map((seg, i) => (
      <span key={seg.label}>
        {i > 0 && <span className={styles.compactDot}> | </span>}
        <span className={styles.compactSectionLabel}>{seg.label}:</span>{" "}
        <span className={styles.compactSectionFields}>
          {seg.fields.join(", ")}
        </span>
      </span>
    ));
  }, [config]);

  const activeFilterCount = useMemo(() => {
    // Count all active filters regardless of zone — includes header-menu filters
    // applied to row/column dimensions, so they are never invisible in collapsed mode.
    if (!config.filters) return 0;
    return Object.keys(config.filters).length;
  }, [config.filters]);

  // Zone card content helpers
  const syntheticMeasures = config.synthetic_measures ?? [];
  const valueOrder = useMemo(() => getRenderedValueFields(config), [config]);
  const emptyHint = locked ? undefined : "Apply fields in settings menu";

  return (
    <div
      ref={toolbarRef}
      data-testid="pivot-toolbar"
      className={`${styles.toolbar} ${locked ? styles.toolbarLocked : ""}`}
    >
      {!sectionsExpanded ? (
        /* ── Compact summary bar ── */
        <div
          className={styles.compactSummary}
          data-testid="toolbar-compact-summary"
        >
          <button
            type="button"
            className={styles.expandBtn}
            onClick={handleToggleSections}
            title="Expand sections"
            aria-label="Expand toolbar sections"
            data-testid="toolbar-expand-sections"
          >
            ▸
          </button>
          <span className={styles.compactSummaryText}>
            {compactSummaryText}
          </span>
          {activeFilterCount > 0 && (
            <>
              <div className={styles.filterDot} />
              <span className={styles.filterDotLabel}>
                {activeFilterCount === 1
                  ? "1 filter"
                  : `${activeFilterCount} filters`}
              </span>
            </>
          )}
        </div>
      ) : (
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
            sortConfig={getPrimarySortConfig(config.row_sort)}
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
            sortConfig={getPrimarySortConfig(config.col_sort)}
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
            renderedMeasures={valueOrder}
            onAggregationChange={handleValueAggChange}
            onShowValuesAsChange={handleValueShowAsChange}
            onRemove={(field) => handleRemoveField("values", field)}
            onRemoveSynthetic={handleRemoveSynthetic}
            aggPortalTarget={toolbarRef.current}
            aggTestIdPrefix="toolbar-values-agg"
            displayLabelForField={(field) =>
              getRenderedValueLabel(config, field)
            }
          />
          <DragOverlay dropAnimation={{ duration: 200 }}>
            {activeField ? (
              activeField.zone === "synthetic" ? (
                <SyntheticDragOverlayChip
                  syntheticId={activeField.field}
                  label={
                    syntheticMeasures.find((m) => m.id === activeField.field)
                      ?.label ?? activeField.field
                  }
                  testId="toolbar-values"
                />
              ) : (
                <DragOverlayChip
                  field={activeField.field}
                  displayLabel={
                    activeField.zone === "rows" ||
                    activeField.zone === "columns"
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
              )
            ) : null}
          </DragOverlay>
        </DndContext>
      )}{" "}
      {/* end sectionsExpanded */}
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
        {!locked && onConfigChange && (
          <span
            className={styles.tooltipWrap}
            data-tooltip={
              sectionsExpanded ? "Collapse Sections" : "Expand Sections"
            }
          >
            <button
              data-testid="toolbar-toggle-sections"
              data-toolbar-btn
              tabIndex={-1}
              className={`${styles.utilButton} ${styles.iconButton} ${!sectionsExpanded ? styles.iconButtonActive : ""}`}
              onClick={handleToggleSections}
              aria-label={
                sectionsExpanded
                  ? "Collapse toolbar sections"
                  : "Expand toolbar sections"
              }
            >
              {sectionsExpanded ? (
                <SectionsCollapseIcon />
              ) : (
                <SectionsExpandIcon />
              )}
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
            instanceKey={instanceKey}
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
            pivotData={pivotData}
            filterFieldValues={filterFieldValues}
            pickerPortalTarget={pickerPortalTarget}
          />
        </div>
      </div>
    </div>
  );
};

// ---------------------------------------------------------------------------
// ZoneCard — read-only zone display with cross-zone DnD chips
// ---------------------------------------------------------------------------

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
  /**
   * Unified display order for the Values zone (interleaves real field names
   * and synthetic measure ids). Only consulted when `isValues` is true.
   */
  renderedMeasures?: readonly string[];
  isDropHighlighted?: boolean;
  activeId?: string | null;
  onAggregationChange?: (field: string, agg: AggregationType) => void;
  onShowValuesAsChange?: (field: string, mode: ShowValuesAs) => void;
  onRemove?: (field: string) => void;
  onRemoveSynthetic?: (syntheticId: string) => void;
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
  renderedMeasures,
  isDropHighlighted,
  activeId,
  onAggregationChange,
  onShowValuesAsChange,
  onRemove,
  onRemoveSynthetic,
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

  const syntheticById = useMemo(
    () => new Map(syntheticMeasures.map((m) => [m.id, m])),
    [syntheticMeasures],
  );
  const valuesSet = useMemo(() => new Set(selected), [selected]);

  // In Values, a single SortableContext spans real fields and synthetic
  // measures so users can reorder them together. Elsewhere the zone only
  // has real fields.
  const displayOrder = useMemo<readonly string[]>(
    () =>
      isValues
        ? (renderedMeasures ?? [
            ...selected,
            ...syntheticMeasures.map((m) => m.id),
          ])
        : selected,
    [isValues, renderedMeasures, selected, syntheticMeasures],
  );

  const sortableIds = useMemo(
    () =>
      displayOrder.map((id) =>
        isValues && syntheticById.has(id)
          ? `synthetic::${id}`
          : makeItemId(zoneId, id),
      ),
    [displayOrder, isValues, syntheticById, zoneId],
  );

  const hasChips =
    selected.length > 0 || (isValues && syntheticMeasures.length > 0);

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

      {hasChips ? (
        <div className={styles.chipRow} data-testid={`${testId}-chips`}>
          <SortableContext
            items={sortableIds}
            strategy={horizontalListSortingStrategy}
          >
            {displayOrder.map((id) => {
              const synthetic =
                isValues && syntheticById.has(id)
                  ? syntheticById.get(id)
                  : undefined;
              if (synthetic) {
                return (
                  <SortableSyntheticChip
                    key={`syn:${synthetic.id}`}
                    id={`synthetic::${synthetic.id}`}
                    syntheticId={synthetic.id}
                    label={synthetic.label}
                    testId={testId}
                    disabled={disabled}
                    onRemove={disabled ? undefined : onRemoveSynthetic}
                  />
                );
              }
              // Real field chip. In Values, only render if still present
              // in `selected` (guards against transient ordering states).
              if (isValues && !valuesSet.has(id)) return null;
              const col = id;
              const isFrozen = frozenColumns?.has(col);
              const hasFilter =
                filters?.[col] &&
                ((filters[col].include && filters[col].include.length > 0) ||
                  (filters[col].exclude && filters[col].exclude.length > 0));
              const currentShowAs = showValuesAs?.[col] ?? "raw";
              const currentAggregation = aggregation?.[col] ?? "sum";
              return (
                <SortableFieldChip
                  key={`fld:${col}`}
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
                  displayControl={
                    isValues && !disabled && onShowValuesAsChange ? (
                      <InlineDisplayPicker
                        field={col}
                        value={currentShowAs}
                        onChange={onShowValuesAsChange}
                        portalTarget={aggPortalTarget}
                        testIdPrefix="toolbar-values-display"
                      />
                    ) : undefined
                  }
                  activeId={activeId}
                />
              );
            })}
          </SortableContext>
        </div>
      ) : resolvedEmptyHint ? (
        <div className={styles.emptyDropZone}>{resolvedEmptyHint}</div>
      ) : null}
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

/** Panel-top icon with chevron up — "sections are visible, click to collapse" */
const SectionsCollapseIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    {/* Panel outline */}
    <rect x="3" y="3" width="18" height="18" rx="2" />
    {/* Horizontal divider separating the "zones" area from the rest */}
    <path d="M3 11h18" />
    {/* Chevron up inside the zones area = collapse it upward */}
    <polyline points="8 8 12 5 16 8" />
  </svg>
);

/** Panel-top icon with chevron down — "sections are collapsed, click to expand" */
const SectionsExpandIcon: FC = () => (
  <svg
    width="14"
    height="14"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    {/* Panel outline */}
    <rect x="3" y="3" width="18" height="18" rx="2" />
    {/* Horizontal divider */}
    <path d="M3 11h18" />
    {/* Chevron down inside the zones area = expand it back */}
    <polyline points="8 5 12 8 16 5" />
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
