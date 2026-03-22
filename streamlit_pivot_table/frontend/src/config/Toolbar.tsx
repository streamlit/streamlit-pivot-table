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
  useRef,
  useState,
} from "react";
import type { PivotData } from "../engine/PivotData";
import {
  type ExportContent,
  type ExportFormat,
  exportPivotData,
} from "../engine/exportData";
import {
  AGGREGATION_TYPES,
  getAggregationForField,
  normalizeAggregationConfig,
  stringifyPivotConfig,
  type AggregationConfig,
  type AggregationType,
  type DimensionFilter,
  type PivotConfigV1,
  type ShowValuesAs,
  type SortConfig,
  type SyntheticMeasureConfig,
  showRowTotals as resolveShowRowTotals,
  showColumnTotals as resolveShowColumnTotals,
  validatePivotConfigV1,
} from "../engine/types";
import {
  formatWithPattern,
  isSupportedFormatPattern,
} from "../engine/formatters";
import { useClickOutside } from "../shared/useClickOutside";
import { useListboxKeyboard } from "../shared/useListboxKeyboard";
import styles from "./Toolbar.module.css";

export interface ToolbarProps {
  config: PivotConfigV1;
  allColumns: string[];
  numericColumns: string[];
  /** Numeric columns allowed as synthetic source fields (can include hidden-from-aggregators). */
  syntheticSourceColumns?: string[];
  onConfigChange?: (config: PivotConfigV1) => void;
  /** Original config from Python for reset. If omitted, reset is hidden. */
  initialConfig?: PivotConfigV1;
  /** Lock config controls (filtering still allowed). */
  locked?: boolean;
  /** Columns that cannot be reassigned between zones. */
  frozenColumns?: Set<string>;
  /** Show the sticky headers toggle (only when table content is scrollable). */
  showStickyToggle?: boolean;
  /** PivotData instance for data export. */
  pivotData?: PivotData;
  /** Base filename (without extension) for exported files. */
  exportFilename?: string;
}

function configsEqual(a: PivotConfigV1, b: PivotConfigV1): boolean {
  return stringifyPivotConfig(a) === stringifyPivotConfig(b);
}

const AGG_ICONS: Record<AggregationType, string> = {
  sum: "+",
  avg: "Σ",
  count: "#",
  min: "↓",
  max: "↑",
  count_distinct: "∅",
  median: "M̃",
  percentile_90: "P₉₀",
  first: "⇤",
  last: "⇥",
};

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

const ChevronIcon: FC<{ open?: boolean }> = ({ open }) => (
  <svg
    className={`${styles.chevron} ${open ? styles.chevronOpen : ""}`}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.5"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="6 9 12 15 18 9" />
  </svg>
);

const Toolbar: FC<ToolbarProps> = ({
  config,
  allColumns,
  numericColumns,
  syntheticSourceColumns,
  onConfigChange,
  initialConfig,
  locked,
  frozenColumns,
  showStickyToggle,
  pivotData,
  exportFilename,
}): ReactElement => {
  const syncAggregation = useCallback(
    (values: string[], aggregation: AggregationConfig = config.aggregation) =>
      normalizeAggregationConfig(aggregation, values),
    [config.aggregation],
  );

  const emitIfChanged = useCallback(
    (updated: PivotConfigV1) => {
      if (onConfigChange && !configsEqual(updated, config)) {
        onConfigChange(updated);
      }
    },
    [config, onConfigChange],
  );

  const toggleRow = useCallback(
    (col: string) => {
      const rows = config.rows.includes(col)
        ? config.rows.filter((r) => r !== col)
        : [...config.rows, col];
      emitIfChanged({ ...config, rows });
    },
    [config, emitIfChanged],
  );

  const toggleColumn = useCallback(
    (col: string) => {
      const columns = config.columns.includes(col)
        ? config.columns.filter((c) => c !== col)
        : [...config.columns, col];
      emitIfChanged({ ...config, columns });
    },
    [config, emitIfChanged],
  );

  const toggleValue = useCallback(
    (col: string) => {
      const values = config.values.includes(col)
        ? config.values.filter((v) => v !== col)
        : [...config.values, col];
      emitIfChanged({
        ...config,
        values,
        aggregation: syncAggregation(values),
      });
    },
    [config, emitIfChanged, syncAggregation],
  );

  const removeRow = useCallback(
    (col: string) => {
      emitIfChanged({ ...config, rows: config.rows.filter((r) => r !== col) });
    },
    [config, emitIfChanged],
  );

  const removeColumn = useCallback(
    (col: string) => {
      emitIfChanged({
        ...config,
        columns: config.columns.filter((c) => c !== col),
      });
    },
    [config, emitIfChanged],
  );

  const removeValue = useCallback(
    (col: string) => {
      const values = config.values.filter((v) => v !== col);
      emitIfChanged({
        ...config,
        values,
        aggregation: syncAggregation(values),
      });
    },
    [config, emitIfChanged, syncAggregation],
  );

  const upsertSyntheticMeasure = useCallback(
    (measure: SyntheticMeasureConfig) => {
      const current = config.synthetic_measures ?? [];
      const idx = current.findIndex((m) => m.id === measure.id);
      const synthetic_measures =
        idx === -1
          ? [...current, measure]
          : current.map((m, i) => (i === idx ? measure : m));
      emitIfChanged({ ...config, synthetic_measures });
    },
    [config, emitIfChanged],
  );

  const removeSyntheticMeasure = useCallback(
    (measureId: string) => {
      const current = config.synthetic_measures ?? [];
      const synthetic_measures = current.filter((m) => m.id !== measureId);
      emitIfChanged({ ...config, synthetic_measures });
    },
    [config, emitIfChanged],
  );

  const handleValueAggregationChange = useCallback(
    (valueField: string, aggregation: AggregationType) => {
      emitIfChanged({
        ...config,
        aggregation: syncAggregation(config.values, {
          ...config.aggregation,
          [valueField]: aggregation,
        }),
      });
    },
    [config, emitIfChanged, syncAggregation],
  );

  const toggleRowTotals = useCallback(() => {
    const current = config.show_row_totals ?? config.show_totals;
    // string[] (partial) -> true (all-on); true -> false; false -> true
    const next = Array.isArray(current) ? true : !current;
    emitIfChanged({ ...config, show_row_totals: next });
  }, [config, emitIfChanged]);

  const toggleColumnTotals = useCallback(() => {
    const current = config.show_column_totals ?? config.show_totals;
    // string[] (partial) -> true (all-on); true -> false; false -> true
    const next = Array.isArray(current) ? true : !current;
    emitIfChanged({ ...config, show_column_totals: next });
  }, [config, emitIfChanged]);

  const toggleSubtotals = useCallback(() => {
    const current = config.show_subtotals;
    // string[] (partial) -> true (all-on); true -> false; false -> true
    const next = Array.isArray(current) ? true : !current;
    const updated = { ...config, show_subtotals: next };
    if (!next) {
      delete updated.collapsed_groups;
      delete updated.repeat_row_labels;
    }
    emitIfChanged(updated);
  }, [config, emitIfChanged]);

  const toggleRepeatLabels = useCallback(() => {
    emitIfChanged({ ...config, repeat_row_labels: !config.repeat_row_labels });
  }, [config, emitIfChanged]);

  const toggleStickyHeaders = useCallback(() => {
    const current = config.sticky_headers !== false;
    emitIfChanged({ ...config, sticky_headers: !current });
  }, [config, emitIfChanged]);

  const expandAllGroups = useCallback(() => {
    emitIfChanged({ ...config, collapsed_groups: [] });
  }, [config, emitIfChanged]);

  const collapseAllGroups = useCallback(() => {
    if (config.rows.length < 2) return;
    emitIfChanged({ ...config, collapsed_groups: ["__ALL__"] });
  }, [config, emitIfChanged]);

  const expandAllColGroups = useCallback(() => {
    emitIfChanged({ ...config, collapsed_col_groups: [] });
  }, [config, emitIfChanged]);

  const collapseAllColGroups = useCallback(() => {
    if (config.columns.length < 2) return;
    emitIfChanged({ ...config, collapsed_col_groups: ["__ALL__"] });
  }, [config, emitIfChanged]);

  const handleSwapRowsCols = useCallback(() => {
    emitIfChanged({
      ...config,
      rows: config.columns,
      columns: config.rows,
      row_sort: config.col_sort,
      col_sort: config.row_sort,
    });
  }, [config, emitIfChanged]);

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

  const availableForRows = allColumns.filter(
    (c) => !config.columns.includes(c),
  );
  const availableForCols = allColumns.filter((c) => !config.rows.includes(c));

  const showRowTotals = resolveShowRowTotals(config);
  const showColTotals = resolveShowColumnTotals(config);
  const rowTotalsDisabled = locked || config.columns.length === 0;
  const colTotalsDisabled = locked || config.rows.length === 0;

  return (
    <div
      data-testid="pivot-toolbar"
      className={`${styles.toolbar} ${locked ? styles.toolbarLocked : ""}`}
    >
      <DropdownMultiSelect
        label="Rows"
        testId="toolbar-rows"
        options={availableForRows}
        selected={config.rows}
        onToggle={toggleRow}
        onRemove={removeRow}
        disabled={locked}
        frozenColumns={frozenColumns}
        sortConfig={config.row_sort}
        filters={config.filters}
      />
      <DropdownMultiSelect
        label="Columns"
        testId="toolbar-columns"
        options={availableForCols}
        selected={config.columns}
        onToggle={toggleColumn}
        onRemove={removeColumn}
        disabled={locked}
        frozenColumns={frozenColumns}
        sortConfig={config.col_sort}
        filters={config.filters}
      />
      <DropdownMultiSelect
        label="Values"
        testId="toolbar-values"
        options={numericColumns}
        selected={config.values}
        onToggle={toggleValue}
        onRemove={removeValue}
        disabled={locked}
        frozenColumns={frozenColumns}
        showValuesAs={config.show_values_as}
        aggregation={config.aggregation}
        syntheticMeasures={config.synthetic_measures ?? []}
        allNumericColumns={syntheticSourceColumns ?? numericColumns}
        onAggregationChange={handleValueAggregationChange}
        onUpsertSyntheticMeasure={upsertSyntheticMeasure}
        onRemoveSyntheticMeasure={removeSyntheticMeasure}
      />

      <div
        ref={utilGroupRef}
        className={styles.utilGroup}
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
          <ConfigIOControls config={config} onConfigChange={onConfigChange} />
        )}
        {!locked && pivotData && (
          <ExportDataControls
            pivotData={pivotData}
            config={config}
            exportFilename={exportFilename}
          />
        )}
        <SettingsPopover
          config={config}
          locked={locked}
          showStickyToggle={showStickyToggle}
          showRowTotals={showRowTotals}
          showColTotals={showColTotals}
          rowTotalsDisabled={rowTotalsDisabled}
          colTotalsDisabled={colTotalsDisabled}
          toggleRowTotals={toggleRowTotals}
          toggleColumnTotals={toggleColumnTotals}
          toggleSubtotals={toggleSubtotals}
          toggleRepeatLabels={toggleRepeatLabels}
          toggleStickyHeaders={toggleStickyHeaders}
          expandAllGroups={expandAllGroups}
          collapseAllGroups={collapseAllGroups}
          expandAllColGroups={expandAllColGroups}
          collapseAllColGroups={collapseAllColGroups}
        />
      </div>
    </div>
  );
};

// ---------------------------------------------------------------------------
// DropdownMultiSelect
// ---------------------------------------------------------------------------

const SHOW_AS_BADGE_LABELS: Record<string, string> = {
  pct_of_total: "% of Grand Total",
  pct_of_row: "% of Row Total",
  pct_of_col: "% of Column Total",
};

const SYNTHETIC_FORMAT_PRESETS = [
  { label: "Percent", value: ".1%" },
  { label: "Currency", value: "$,.0f" },
  { label: "Number", value: ",.2f" },
] as const;
const SYNTHETIC_FORMAT_PREVIEW_NUMBER = 12345.678;
const SYNTHETIC_FORMAT_PREVIEW_PERCENT = 0.1234;

interface DropdownMultiSelectProps {
  label: string;
  testId: string;
  options: string[];
  selected: string[];
  onToggle: (col: string) => void;
  onRemove: (col: string) => void;
  disabled?: boolean;
  frozenColumns?: Set<string>;
  /** Read-only: current sort config for status indicator on chips. */
  sortConfig?: SortConfig;
  /** Read-only: current filters for status indicator on chips. */
  filters?: Record<string, DimensionFilter>;
  /** Read-only: current show-values-as config for badge indicators on chips. */
  showValuesAs?: Record<string, ShowValuesAs>;
  aggregation?: AggregationConfig;
  syntheticMeasures?: SyntheticMeasureConfig[];
  allNumericColumns?: string[];
  onAggregationChange?: (field: string, aggregation: AggregationType) => void;
  onUpsertSyntheticMeasure?: (measure: SyntheticMeasureConfig) => void;
  onRemoveSyntheticMeasure?: (id: string) => void;
}

interface AggregationPickerProps {
  field: string;
  value: AggregationType;
  testId: string;
  disabled?: boolean;
  onChange: (value: AggregationType) => void;
}

const BASIC_AGGREGATIONS: readonly AggregationType[] = [
  "sum",
  "avg",
  "count",
  "min",
  "max",
];

const ADVANCED_AGGREGATIONS: readonly AggregationType[] = [
  "count_distinct",
  "median",
  "percentile_90",
  "first",
  "last",
];

const AggregationPicker: FC<AggregationPickerProps> = ({
  field,
  value,
  testId,
  disabled,
  onChange,
}): ReactElement => {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const closePicker = useCallback(() => setOpen(false), []);

  useClickOutside(containerRef, closePicker, open);

  const listboxKeyDown = useListboxKeyboard(
    panelRef,
    triggerRef,
    open,
    closePicker,
    '[role="option"]',
  );

  const handleSelect = useCallback(
    (next: AggregationType) => {
      onChange(next);
      setOpen(false);
    },
    [onChange],
  );

  return (
    <div className={styles.aggregationPicker} ref={containerRef}>
      <button
        ref={triggerRef}
        type="button"
        className={styles.aggregationPickerTrigger}
        data-testid={`${testId}-trigger`}
        onClick={() => setOpen((prev) => !prev)}
        aria-expanded={open}
        aria-label={`Choose aggregation for ${field}`}
        disabled={disabled}
      >
        <span className={styles.aggregationPickerValue}>
          <span className={styles.aggIcon}>{AGG_ICONS[value]}</span>
          <span>{AGG_LABELS[value]}</span>
        </span>
        <ChevronIcon open={open} />
      </button>
      {open && (
        <div
          ref={panelRef}
          className={styles.aggregationPickerPanel}
          data-testid={`${testId}-panel`}
          role="listbox"
          aria-label={`${field} aggregation options`}
          onKeyDown={listboxKeyDown}
        >
          <div className={styles.dropdownGroupLabel}>Basic</div>
          {BASIC_AGGREGATIONS.map((opt) => (
            <button
              key={opt}
              type="button"
              className={`${styles.dropdownItem} ${opt === value ? styles.dropdownItemActive : ""}`}
              data-testid={`${testId}-option-${opt}`}
              role="option"
              aria-selected={opt === value}
              tabIndex={-1}
              onClick={() => handleSelect(opt)}
            >
              <span className={styles.aggregationPickerOptionContent}>
                <span className={styles.aggIcon}>{AGG_ICONS[opt]}</span>
                <span>{AGG_LABELS[opt]}</span>
              </span>
            </button>
          ))}
          <div className={styles.dropdownGroupDivider} />
          <div className={styles.dropdownGroupLabel}>Advanced</div>
          {ADVANCED_AGGREGATIONS.map((opt) => (
            <button
              key={opt}
              type="button"
              className={`${styles.dropdownItem} ${opt === value ? styles.dropdownItemActive : ""}`}
              data-testid={`${testId}-option-${opt}`}
              role="option"
              aria-selected={opt === value}
              tabIndex={-1}
              onClick={() => handleSelect(opt)}
            >
              <span className={styles.aggregationPickerOptionContent}>
                <span className={styles.aggIcon}>{AGG_ICONS[opt]}</span>
                <span>{AGG_LABELS[opt]}</span>
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

const DropdownMultiSelect: FC<DropdownMultiSelectProps> = ({
  label,
  testId,
  options,
  selected,
  onToggle,
  onRemove,
  disabled,
  frozenColumns,
  sortConfig,
  filters,
  showValuesAs,
  aggregation,
  syntheticMeasures = [],
  allNumericColumns = [],
  onAggregationChange,
  onUpsertSyntheticMeasure,
  onRemoveSyntheticMeasure,
}): ReactElement => {
  const [open, setOpen] = useState(false);
  const [syntheticOpen, setSyntheticOpen] = useState(false);
  const [editingSyntheticId, setEditingSyntheticId] = useState<string | null>(
    null,
  );
  const [syntheticLabel, setSyntheticLabel] = useState("");
  const [syntheticOperation, setSyntheticOperation] = useState<
    "sum_over_sum" | "difference"
  >("sum_over_sum");
  const [syntheticNumerator, setSyntheticNumerator] = useState("");
  const [syntheticDenominator, setSyntheticDenominator] = useState("");
  const [syntheticFormat, setSyntheticFormat] = useState("");
  const [syntheticError, setSyntheticError] = useState<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const closeDropdown = useCallback(() => {
    setOpen(false);
    setSyntheticOpen(false);
    setSyntheticError(null);
  }, []);
  useClickOutside(containerRef, closeDropdown, open || syntheticOpen);
  const listboxKeyDown = useListboxKeyboard(
    panelRef,
    triggerRef,
    open,
    closeDropdown,
    '[role="option"]',
  );
  const isValuesField = label === "Values";

  const openCreateSynthetic = useCallback(() => {
    setOpen(false);
    setEditingSyntheticId(null);
    setSyntheticLabel("");
    setSyntheticOperation("sum_over_sum");
    setSyntheticNumerator(allNumericColumns[0] ?? "");
    setSyntheticDenominator(allNumericColumns[1] ?? allNumericColumns[0] ?? "");
    setSyntheticFormat("");
    setSyntheticError(null);
    setSyntheticOpen(true);
  }, [allNumericColumns]);

  const openEditSynthetic = useCallback((measure: SyntheticMeasureConfig) => {
    setOpen(false);
    setEditingSyntheticId(measure.id);
    setSyntheticLabel(measure.label);
    setSyntheticOperation(measure.operation);
    setSyntheticNumerator(measure.numerator);
    setSyntheticDenominator(measure.denominator);
    setSyntheticFormat(measure.format ?? "");
    setSyntheticError(null);
    setSyntheticOpen(true);
  }, []);

  const saveSynthetic = useCallback(() => {
    if (!onUpsertSyntheticMeasure) return;
    const trimmedLabel = syntheticLabel.trim();
    if (!trimmedLabel) {
      setSyntheticError("Name is required.");
      return;
    }
    if (!syntheticNumerator || !syntheticDenominator) {
      setSyntheticError("Choose both source fields.");
      return;
    }
    const existing = syntheticMeasures.filter(
      (m) => m.id !== editingSyntheticId,
    );
    if (
      existing.some((m) => m.label.toLowerCase() === trimmedLabel.toLowerCase())
    ) {
      setSyntheticError("Synthetic measure name must be unique.");
      return;
    }
    const id =
      editingSyntheticId ??
      `synthetic_${trimmedLabel.toLowerCase().replace(/[^a-z0-9]+/g, "_")}`;
    if (!editingSyntheticId && existing.some((m) => m.id === id)) {
      setSyntheticError(
        "Generated id collides with an existing synthetic measure.",
      );
      return;
    }
    if (!editingSyntheticId && selected.includes(id)) {
      setSyntheticError("Generated id collides with an existing value field.");
      return;
    }
    const trimmedFormat = syntheticFormat.trim();
    if (trimmedFormat && !isSupportedFormatPattern(trimmedFormat)) {
      setSyntheticError(
        "Format is invalid. Try patterns like .1%, $,.0f, or ,.2f.",
      );
      return;
    }
    onUpsertSyntheticMeasure({
      id,
      label: trimmedLabel,
      operation: syntheticOperation,
      numerator: syntheticNumerator,
      denominator: syntheticDenominator,
      format: trimmedFormat || undefined,
    });
    setSyntheticOpen(false);
  }, [
    editingSyntheticId,
    onUpsertSyntheticMeasure,
    syntheticDenominator,
    syntheticFormat,
    syntheticLabel,
    syntheticMeasures,
    syntheticNumerator,
    syntheticOperation,
  ]);

  const formulaPreview =
    syntheticOperation === "sum_over_sum"
      ? `sum(${syntheticNumerator}) / sum(${syntheticDenominator})`
      : `sum(${syntheticNumerator}) - sum(${syntheticDenominator})`;
  const trimmedFormat = syntheticFormat.trim();
  const hasValidFormatPreview =
    trimmedFormat !== "" && isSupportedFormatPattern(trimmedFormat);
  const formatPreviewSample = trimmedFormat.includes("%")
    ? SYNTHETIC_FORMAT_PREVIEW_PERCENT
    : SYNTHETIC_FORMAT_PREVIEW_NUMBER;

  return (
    <div className={styles.fieldGroup} ref={containerRef}>
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
        {!disabled && (
          <div className={styles.dropdownAnchor}>
            <button
              ref={triggerRef}
              type="button"
              className={styles.dropdownToggle}
              data-testid={`${testId}-select`}
              onClick={() => setOpen((v) => !v)}
              aria-expanded={open}
              aria-label={`Toggle ${label} dropdown`}
            >
              <ChevronIcon open={open} />
            </button>
            {open && (
              <div
                ref={panelRef}
                className={`${styles.dropdownPanel} ${isValuesField ? styles.valuesDropdownPanel : ""}`}
                data-testid={`${testId}-panel`}
                role="listbox"
                aria-label={`${label} options`}
                onKeyDown={listboxKeyDown}
              >
                {options.length === 0 ? (
                  <div className={styles.emptyHint}>No fields available</div>
                ) : (
                  options.map((col) => {
                    const checked = selected.includes(col);
                    const frozen = checked && !!frozenColumns?.has(col);
                    return (
                      <label
                        key={col}
                        className={`${styles.dropdownItem} ${checked ? styles.dropdownItemActive : ""} ${frozen ? styles.dropdownItemDisabled : ""}`}
                        data-testid={`${testId}-option-${col}`}
                        role="option"
                        aria-selected={checked}
                        aria-disabled={frozen}
                        tabIndex={-1}
                        onKeyDown={(e) => {
                          if (frozen) return;
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault();
                            onToggle(col);
                          }
                        }}
                      >
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => !frozen && onToggle(col)}
                          disabled={frozen}
                          tabIndex={-1}
                        />
                        <span>{col}</span>
                      </label>
                    );
                  })
                )}
                {isValuesField && (
                  <>
                    <div className={styles.syntheticBuilderMenuRow}>
                      <button
                        type="button"
                        className={`${styles.subtotalActionBtn} ${styles.syntheticAddButton}`}
                        data-testid={`${testId}-add-synthetic`}
                        onClick={openCreateSynthetic}
                      >
                        + Add measure
                      </button>
                    </div>
                    {selected.length > 0 && onAggregationChange && (
                      <>
                        <div className={styles.dropdownGroupDivider} />
                        <div className={styles.dropdownGroupLabel}>
                          Measure Aggregation
                        </div>
                        <div data-testid={`${testId}-aggregation-controls`}>
                          {selected.map((col) => (
                            <div
                              key={`${col}-aggregation`}
                              className={styles.aggregationControlRow}
                            >
                              <span className={styles.aggregationControlLabel}>
                                {col}
                              </span>
                              <AggregationPicker
                                field={col}
                                value={aggregation?.[col] ?? "sum"}
                                testId={`${testId}-aggregation-${col}`}
                                disabled={disabled}
                                onChange={(next) =>
                                  onAggregationChange(col, next)
                                }
                              />
                            </div>
                          ))}
                        </div>
                      </>
                    )}
                  </>
                )}
              </div>
            )}
          </div>
        )}
      </div>

      {(selected.length > 0 ||
        (isValuesField && syntheticMeasures.length > 0)) && (
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
              <span key={col} className={styles.chip}>
                <span
                  className={styles.chipLabelText}
                  data-testid={`${testId}-chip-label-${col}`}
                >
                  <span className={styles.chipPrimaryText}>{col}</span>
                  {isValuesField && (
                    <span
                      className={styles.chipAggregationLabel}
                      data-testid={`${testId}-aggregation-label-${col}`}
                    >
                      {" "}
                      ({AGG_CHIP_LABELS[currentAggregation]})
                    </span>
                  )}
                </span>
                {showAsBadge && (
                  <span
                    className={styles.chipShowAsBadge}
                    title={SHOW_AS_BADGE_LABELS[currentShowAs] ?? currentShowAs}
                    data-testid={`${testId}-show-as-badge-${col}`}
                  >
                    %
                  </span>
                )}
                {hasFilter && (
                  <span
                    className={styles.chipFilterDot}
                    title={`Filtered: ${col}`}
                    data-testid={`${testId}-filter-indicator-${col}`}
                  />
                )}
                {!disabled && !isFrozen && (
                  <button
                    className={styles.chipRemove}
                    onClick={() => onRemove(col)}
                    aria-label={`Remove ${col}`}
                    data-testid={`${testId}-remove-${col}`}
                  >
                    ×
                  </button>
                )}
              </span>
            );
          })}
          {isValuesField &&
            syntheticMeasures.map((measure) => (
              <span
                key={measure.id}
                className={styles.chip}
                data-testid={`${testId}-synthetic-${measure.id}`}
              >
                <span className={styles.aggChipIcon}>fx</span>
                {measure.label}
                {!disabled && (
                  <>
                    <button
                      className={styles.chipRemove}
                      onClick={() => openEditSynthetic(measure)}
                      aria-label={`Edit ${measure.label}`}
                      data-testid={`${testId}-edit-synthetic-${measure.id}`}
                    >
                      ✎
                    </button>
                    <button
                      className={styles.chipRemove}
                      onClick={() => onRemoveSyntheticMeasure?.(measure.id)}
                      aria-label={`Remove ${measure.label}`}
                      data-testid={`${testId}-remove-synthetic-${measure.id}`}
                    >
                      ×
                    </button>
                  </>
                )}
              </span>
            ))}
        </div>
      )}
      {isValuesField && syntheticOpen && (
        <div
          className={styles.syntheticBuilderPopover}
          data-testid={`${testId}-synthetic-builder`}
        >
          <label className={styles.syntheticBuilderField}>
            <span className={styles.syntheticBuilderFieldLabel}>Name</span>
            <input
              className={styles.syntheticBuilderInput}
              value={syntheticLabel}
              onChange={(e) => {
                setSyntheticLabel(e.target.value);
                setSyntheticError(null);
              }}
            />
          </label>
          <label className={styles.syntheticBuilderField}>
            <span className={styles.syntheticBuilderFieldLabel}>Operation</span>
            <select
              className={styles.syntheticBuilderSelect}
              value={syntheticOperation}
              onChange={(e) =>
                setSyntheticOperation(
                  e.target.value as "sum_over_sum" | "difference",
                )
              }
            >
              <option value="sum_over_sum">Ratio of sums</option>
              <option value="difference">Difference of sums</option>
            </select>
            {syntheticNumerator && syntheticDenominator && (
              <div
                className={styles.syntheticFormulaHint}
                data-testid={`${testId}-formula-preview`}
              >
                {formulaPreview}
              </div>
            )}
          </label>
          <label className={styles.syntheticBuilderField}>
            <span className={styles.syntheticBuilderFieldLabel}>Measure A</span>
            <select
              className={styles.syntheticBuilderSelect}
              value={syntheticNumerator}
              onChange={(e) => setSyntheticNumerator(e.target.value)}
            >
              {allNumericColumns.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.syntheticBuilderField}>
            <span className={styles.syntheticBuilderFieldLabel}>Measure B</span>
            <select
              className={styles.syntheticBuilderSelect}
              value={syntheticDenominator}
              onChange={(e) => setSyntheticDenominator(e.target.value)}
            >
              {allNumericColumns.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </label>
          <label className={styles.syntheticBuilderField}>
            <span className={styles.syntheticBuilderFieldLabel}>
              Format (optional)
            </span>
            <div className={styles.syntheticFormatPresets}>
              {SYNTHETIC_FORMAT_PRESETS.map((preset) => (
                <button
                  key={preset.value}
                  type="button"
                  className={`${styles.syntheticFormatPresetBtn} ${syntheticFormat.trim() === preset.value ? styles.syntheticFormatPresetBtnActive : ""}`}
                  data-testid={`${testId}-format-preset-${preset.label.toLowerCase()}`}
                  onClick={() => {
                    setSyntheticFormat(preset.value);
                    setSyntheticError(null);
                  }}
                >
                  {preset.label}
                </button>
              ))}
            </div>
            <input
              className={styles.syntheticBuilderInput}
              value={syntheticFormat}
              placeholder="e.g. .1%, $,.0f, ,.2f"
              onChange={(e) => {
                setSyntheticFormat(e.target.value);
                setSyntheticError(null);
              }}
            />
            {trimmedFormat && (
              <div
                className={styles.syntheticFormatPreview}
                data-testid={`${testId}-format-preview`}
              >
                {hasValidFormatPreview
                  ? `Example: ${formatWithPattern(formatPreviewSample, trimmedFormat)}`
                  : "Example unavailable (invalid format pattern)"}
              </div>
            )}
          </label>
          {syntheticError && (
            <div className={styles.importError}>{syntheticError}</div>
          )}
          <div className={styles.syntheticBuilderActions}>
            <button
              type="button"
              className={styles.syntheticBuilderButton}
              onClick={() => setSyntheticOpen(false)}
            >
              Cancel
            </button>
            <button
              type="button"
              className={`${styles.syntheticBuilderButton} ${styles.syntheticBuilderButtonPrimary}`}
              onClick={saveSynthetic}
            >
              Save
            </button>
          </div>
        </div>
      )}
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

const GearIcon: FC = () => (
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
    <circle cx="12" cy="12" r="3" />
    <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
  </svg>
);

// ---------------------------------------------------------------------------
// Config I/O Controls
// ---------------------------------------------------------------------------

interface ConfigIOControlsProps {
  config: PivotConfigV1;
  onConfigChange: (config: PivotConfigV1) => void;
}

const ConfigIOControls: FC<ConfigIOControlsProps> = ({
  config,
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
      onConfigChange(validated);
      setImporting(false);
      setImportText("");
      setImportError(null);
      requestAnimationFrame(() => triggerRef.current?.focus());
    } catch (e) {
      setImportError(e instanceof Error ? e.message : "Invalid JSON");
    }
  }, [importText, onConfigChange]);

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
}

const FORMAT_OPTIONS: { id: ExportFormat; label: string }[] = [
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
}): ReactElement => {
  const [open, setOpen] = useState(false);
  const [format, setFormat] = useState<ExportFormat>("csv");
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
    );
    if (format === "clipboard") {
      setFeedback(ok ? "Copied!" : "Copy failed");
    } else {
      setFeedback(ok ? "Downloaded!" : "Download failed");
    }
    setTimeout(() => setFeedback(null), 2000);
    setOpen(false);
    requestAnimationFrame(() => triggerRef.current?.focus());
  }, [pivotData, config, format, content, exportFilename]);

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
              : `Export ${format.toUpperCase()}`}
          </button>
        </div>
      )}
    </div>
  );
};

// ---------------------------------------------------------------------------
// Settings Popover (gear icon → display toggles + group controls)
// ---------------------------------------------------------------------------

interface SettingsPopoverProps {
  config: PivotConfigV1;
  locked?: boolean;
  showStickyToggle?: boolean;
  showRowTotals: boolean;
  showColTotals: boolean;
  rowTotalsDisabled: boolean;
  colTotalsDisabled: boolean;
  toggleRowTotals: () => void;
  toggleColumnTotals: () => void;
  toggleSubtotals: () => void;
  toggleRepeatLabels: () => void;
  toggleStickyHeaders: () => void;
  expandAllGroups: () => void;
  collapseAllGroups: () => void;
  expandAllColGroups: () => void;
  collapseAllColGroups: () => void;
}

const SettingsPopover: FC<SettingsPopoverProps> = ({
  config,
  locked,
  showStickyToggle,
  showRowTotals,
  showColTotals,
  rowTotalsDisabled,
  colTotalsDisabled,
  toggleRowTotals,
  toggleColumnTotals,
  toggleSubtotals,
  toggleRepeatLabels,
  toggleStickyHeaders,
  expandAllGroups,
  collapseAllGroups,
  expandAllColGroups,
  collapseAllColGroups,
}): ReactElement => {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const rawRowTotals = config.show_row_totals ?? config.show_totals;
  const rowTotalsIndeterminate =
    Array.isArray(rawRowTotals) && rawRowTotals.length > 0;
  const rawColTotals = config.show_column_totals ?? config.show_totals;
  const colTotalsIndeterminate =
    Array.isArray(rawColTotals) && rawColTotals.length > 0;
  const subtotalsIndeterminate =
    Array.isArray(config.show_subtotals) && config.show_subtotals.length > 0;

  const closePopover = useCallback(() => {
    setOpen(false);
    requestAnimationFrame(() => triggerRef.current?.focus());
  }, []);

  useClickOutside(containerRef, closePopover, open);

  useEffect(() => {
    if (!open) return;
    requestAnimationFrame(() => {
      const first =
        panelRef.current?.querySelector<HTMLElement>("input, button");
      (first ?? panelRef.current)?.focus();
    });
  }, [open]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        closePopover();
      }
    },
    [closePopover],
  );

  const showRowGroups =
    config.show_subtotals && config.rows.length >= 2 && !locked;
  const showColGroups = config.columns.length >= 2 && !locked;

  return (
    <div className={styles.settingsWrapper} ref={containerRef}>
      <span className={styles.tooltipWrap} data-tooltip="Table Settings">
        <button
          ref={triggerRef}
          data-testid="toolbar-settings"
          data-toolbar-btn
          tabIndex={-1}
          className={`${styles.utilButton} ${styles.iconButton} ${open ? styles.iconButtonActive : ""}`}
          onClick={() => setOpen((v) => !v)}
          aria-label="Table settings"
          aria-haspopup="dialog"
          aria-expanded={open}
        >
          <GearIcon />
        </button>
      </span>
      {open && (
        <div
          ref={panelRef}
          tabIndex={-1}
          className={styles.settingsPopover}
          data-testid="toolbar-settings-panel"
          role="dialog"
          aria-label="Table settings"
          onKeyDown={handleKeyDown}
        >
          <span className={styles.settingsSectionTitle}>Display</span>
          <label
            className={styles.checkboxLabel}
            data-testid="toolbar-row-totals"
            title={
              rowTotalsDisabled && !locked
                ? "Requires at least one column dimension"
                : undefined
            }
          >
            <input
              ref={(el) => {
                if (el) el.indeterminate = rowTotalsIndeterminate;
              }}
              type="checkbox"
              checked={showRowTotals}
              onChange={toggleRowTotals}
              disabled={rowTotalsDisabled}
            />
            <span>Row Totals</span>
          </label>
          <label
            className={styles.checkboxLabel}
            data-testid="toolbar-col-totals"
            title={
              colTotalsDisabled && !locked
                ? "Requires at least one row dimension"
                : undefined
            }
          >
            <input
              ref={(el) => {
                if (el) el.indeterminate = colTotalsIndeterminate;
              }}
              type="checkbox"
              checked={showColTotals}
              onChange={toggleColumnTotals}
              disabled={colTotalsDisabled}
            />
            <span>Column Totals</span>
          </label>
          {config.rows.length >= 2 && (
            <label
              className={styles.checkboxLabel}
              data-testid="toolbar-subtotals"
            >
              <input
                ref={(el) => {
                  if (el) el.indeterminate = subtotalsIndeterminate;
                }}
                type="checkbox"
                checked={
                  config.show_subtotals === true || subtotalsIndeterminate
                }
                onChange={toggleSubtotals}
                disabled={locked}
              />
              <span>Subtotals</span>
            </label>
          )}
          {config.rows.length >= 2 && (
            <label
              className={styles.checkboxLabel}
              data-testid="toolbar-repeat-labels"
            >
              <input
                type="checkbox"
                checked={!!config.repeat_row_labels}
                onChange={toggleRepeatLabels}
                disabled={locked}
              />
              <span>Repeat Labels</span>
            </label>
          )}
          {showStickyToggle && (
            <label
              className={styles.checkboxLabel}
              data-testid="toolbar-sticky-headers"
            >
              <input
                type="checkbox"
                checked={config.sticky_headers !== false}
                onChange={toggleStickyHeaders}
                disabled={locked}
              />
              <span>Sticky Headers</span>
            </label>
          )}
          {(showRowGroups || showColGroups) && (
            <>
              <div className={styles.settingsDivider} />
              <span className={styles.settingsSectionTitle}>Groups</span>
              {showRowGroups && (
                <div className={styles.subtotalActions}>
                  <button
                    type="button"
                    className={styles.subtotalActionBtn}
                    onClick={expandAllGroups}
                    data-testid="pivot-group-toggle-expand-all"
                    aria-label="Expand all row groups"
                  >
                    Expand All
                  </button>
                  <button
                    type="button"
                    className={styles.subtotalActionBtn}
                    onClick={collapseAllGroups}
                    data-testid="pivot-group-toggle-collapse-all"
                    aria-label="Collapse all row groups"
                  >
                    Collapse All
                  </button>
                </div>
              )}
              {showColGroups && (
                <div className={styles.subtotalActions}>
                  <button
                    type="button"
                    className={styles.subtotalActionBtn}
                    onClick={expandAllColGroups}
                    data-testid="pivot-col-group-expand-all"
                    aria-label="Expand all column groups"
                  >
                    Expand Columns
                  </button>
                  <button
                    type="button"
                    className={styles.subtotalActionBtn}
                    onClick={collapseAllColGroups}
                    data-testid="pivot-col-group-collapse-all"
                    aria-label="Collapse all column groups"
                  >
                    Collapse Columns
                  </button>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
};

export default Toolbar;
