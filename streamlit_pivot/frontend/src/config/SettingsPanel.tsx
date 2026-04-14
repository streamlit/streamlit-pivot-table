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
  type FC,
  type ReactElement,
  type ReactNode,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import {
  DndContext,
  DragOverlay,
  PointerSensor,
  useSensor,
  useSensors,
  pointerWithin,
  useDraggable,
  useDroppable,
  type DragStartEvent,
  type DragOverEvent,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  SortableContext,
  horizontalListSortingStrategy,
  arrayMove,
  useSortable,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import {
  normalizeAggregationConfig,
  stringifyPivotConfig,
  type AggregationConfig,
  type AggregationType,
  type PivotConfigV1,
  type SyntheticMeasureConfig,
  showRowTotals as resolveShowRowTotals,
  showColumnTotals as resolveShowColumnTotals,
} from "../engine/types";
import {
  formatWithPattern,
  isSupportedFormatPattern,
} from "../engine/formatters";
import { useClickOutside } from "../shared/useClickOutside";
import { useListboxKeyboard } from "../shared/useListboxKeyboard";
import styles from "./SettingsPanel.module.css";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const FIELD_SEARCH_THRESHOLD = 8;

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

const BASIC_AGGS: readonly AggregationType[] = [
  "sum",
  "avg",
  "count",
  "min",
  "max",
];
const ADVANCED_AGGS: readonly AggregationType[] = [
  "count_distinct",
  "median",
  "percentile_90",
  "first",
  "last",
];

const SYNTHETIC_FORMAT_PRESETS = [
  { label: "Percent", value: ".1%" },
  { label: "Currency", value: "$,.0f" },
  { label: "Number", value: ",.2f" },
] as const;
const SYNTHETIC_OPERATION_OPTIONS = [
  { value: "sum_over_sum", label: "Ratio of sums" },
  { value: "difference", label: "Difference of sums" },
] as const;
const SYNTHETIC_FORMAT_PREVIEW_NUMBER = 12345.678;
const SYNTHETIC_FORMAT_PREVIEW_PERCENT = 0.1234;

type ZoneKey = "rows" | "columns" | "values";

// ---------------------------------------------------------------------------
// DnD routing logic (extracted for testability)
// ---------------------------------------------------------------------------

export interface DragEndParams {
  activeData: { zone?: string; field?: string } | undefined;
  overId: string | number;
  overData: { zone?: string; type?: string; field?: string } | undefined;
}

export type DragEndAction =
  | { type: "reorder"; zone: ZoneKey; field: string; overField: string }
  | { type: "cross-zone"; field: string; fromZone: ZoneKey; toZone: ZoneKey }
  | { type: "add-from-available"; field: string; toZone: ZoneKey }
  | null;

export function resolveDragEnd(params: DragEndParams): DragEndAction {
  const { activeData, overId, overData } = params;
  const sourceZone = activeData?.zone as ZoneKey | undefined;
  const field = activeData?.field;
  if (!field) return null;

  let targetZone: ZoneKey | undefined;
  if (overData?.type === "container") {
    targetZone = String(overId).replace("sp-zone-", "") as ZoneKey;
  } else {
    targetZone = overData?.zone as ZoneKey | undefined;
  }
  if (!targetZone) return null;

  if (sourceZone && sourceZone === targetZone) {
    const overField = overData?.field;
    if (!overField || overField === field) return null;
    return { type: "reorder", zone: targetZone, field, overField };
  }
  if (sourceZone && sourceZone !== targetZone) {
    return {
      type: "cross-zone",
      field,
      fromZone: sourceZone,
      toZone: targetZone,
    };
  }
  return { type: "add-from-available", field, toZone: targetZone };
}

// ---------------------------------------------------------------------------
// Config cleanup utility
// ---------------------------------------------------------------------------

/**
 * Computes a cleaned config by diffing old and new zone assignments and
 * pruning stale derived config (aggregation, show_values_as, sorts, CF, etc).
 * This mirrors the cleanup logic in applyDragMove but works on bulk changes.
 */
export function cleanupConfigAfterFieldChanges(
  oldConfig: PivotConfigV1,
  newConfig: PivotConfigV1,
): PivotConfigV1 {
  let updated = { ...newConfig };

  // Normalize aggregation for current values
  updated.aggregation = normalizeAggregationConfig(
    updated.aggregation,
    updated.values,
  );

  // Fields removed from values
  const oldValues = new Set(oldConfig.values);
  const newValues = new Set(updated.values);
  const removedFromValues = [...oldValues].filter((f) => !newValues.has(f));

  for (const field of removedFromValues) {
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
          return {
            ...rule,
            apply_to: rule.apply_to.filter((f) => f !== field),
          };
        })
        .filter((rule) => rule.apply_to.length > 0);
    }
  }

  // Fields removed from rows
  const oldRows = new Set(oldConfig.rows);
  const newRows = new Set(updated.rows);
  const rowsChanged = oldConfig.rows.join(",") !== updated.rows.join(",");
  if (rowsChanged) {
    const removedFromRows = [...oldRows].filter((f) => !newRows.has(f));
    for (const field of removedFromRows) {
      if (updated.row_sort?.dimension === field) {
        delete updated.row_sort;
      }
    }

    // Validate show_subtotals against the eligible dimensions (all rows
    // except the last, matching runtime normalization in types.ts).
    if (Array.isArray(updated.show_subtotals)) {
      const eligible = new Set(updated.rows.slice(0, -1));
      const pruned = (updated.show_subtotals as string[]).filter((f) =>
        eligible.has(f),
      );
      updated.show_subtotals = pruned.length > 0 ? pruned : false;
    }

    delete updated.collapsed_groups;
  }

  // Fields removed from columns
  const oldCols = new Set(oldConfig.columns);
  const newCols = new Set(updated.columns);
  const colsChanged = oldConfig.columns.join(",") !== updated.columns.join(",");
  if (colsChanged) {
    const removedFromCols = [...oldCols].filter((f) => !newCols.has(f));
    for (const field of removedFromCols) {
      if (updated.col_sort?.dimension === field) {
        delete updated.col_sort;
      }
    }
    delete updated.collapsed_col_groups;
  }

  return updated;
}

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface SettingsPanelProps {
  config: PivotConfigV1;
  allColumns: string[];
  numericColumns: string[];
  syntheticSourceColumns?: string[];
  onConfigChange: (config: PivotConfigV1) => void;
  locked?: boolean;
  frozenColumns?: Set<string>;
  showStickyToggle?: boolean;
  /** Externally controlled open state (managed by Toolbar). */
  open: boolean;
  onClose: () => void;
  /** Ref to an ancestor element that should NOT trigger click-outside close (e.g. the settings button wrapper). */
  excludeRef?: React.RefObject<HTMLElement | null>;
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

const ZONE_SEPARATOR = "::sp::";

function makeZoneItemId(zone: string, field: string): string {
  return `${zone}${ZONE_SEPARATOR}${field}`;
}

function parseZoneItemId(id: string): { zone: string; field: string } {
  const idx = id.indexOf(ZONE_SEPARATOR);
  if (idx === -1) return { zone: "", field: id };
  return {
    zone: id.slice(0, idx),
    field: id.slice(idx + ZONE_SEPARATOR.length),
  };
}

const GripDotsIcon: FC = () => (
  <svg
    className={styles.dragHandle}
    width="8"
    height="14"
    viewBox="0 0 8 14"
    fill="currentColor"
    aria-hidden="true"
  >
    <circle cx="2" cy="2" r="1.2" />
    <circle cx="6" cy="2" r="1.2" />
    <circle cx="2" cy="7" r="1.2" />
    <circle cx="6" cy="7" r="1.2" />
    <circle cx="2" cy="12" r="1.2" />
    <circle cx="6" cy="12" r="1.2" />
  </svg>
);

const DropdownIcon: FC = () => (
  <svg
    aria-hidden="true"
    width="14"
    height="14"
    viewBox="0 0 14 14"
    fill="none"
  >
    <path
      d="M3.5 5.25L7 8.75L10.5 5.25"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

// -- Sortable chip inside a zone --

interface ZoneChipProps {
  id: string;
  zone: ZoneKey;
  field: string;
  isFrozen?: boolean;
  isValues?: boolean;
  aggLabel?: string;
  onRemove: (field: string) => void;
  menuItems: { label: string; action: () => void }[];
  aggregationControl?: ReactNode;
}

const ZoneChip: FC<ZoneChipProps> = ({
  id,
  zone,
  field,
  isFrozen,
  isValues,
  aggLabel,
  onRemove,
  menuItems,
  aggregationControl,
}): ReactElement => {
  const [menuOpen, setMenuOpen] = useState(false);
  const chipRef = useRef<HTMLSpanElement>(null);
  const isDragDisabled = !!isFrozen;
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({
    id,
    disabled: isDragDisabled,
    data: { zone, field },
  });

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.3 : undefined,
  };

  useClickOutside(chipRef, () => setMenuOpen(false), menuOpen);

  return (
    <span
      ref={(node) => {
        setNodeRef(node);
        (chipRef as React.MutableRefObject<HTMLSpanElement | null>).current =
          node;
      }}
      className={`${styles.zoneChip} ${isDragging ? styles.zoneChipDragging : ""} ${isFrozen ? styles.zoneChipFrozen : ""}`}
      style={style}
      {...attributes}
      {...(isDragDisabled ? {} : listeners)}
      data-testid={`settings-${zone}-chip-${field}`}
    >
      {!isFrozen && <GripDotsIcon />}
      {field}
      {isValues && aggLabel && (
        <span className={styles.chipAggLabel}>({aggLabel})</span>
      )}
      {aggregationControl}
      {!isFrozen && menuItems.length > 0 && (
        <button
          type="button"
          className={styles.chipMenuBtn}
          onClick={(e) => {
            e.stopPropagation();
            setMenuOpen((v) => !v);
          }}
          aria-label={`Actions for ${field}`}
          data-testid={`settings-${zone}-menu-${field}`}
        >
          ▾
        </button>
      )}
      {!isFrozen && (
        <button
          type="button"
          className={styles.chipRemoveBtn}
          onClick={(e) => {
            e.stopPropagation();
            onRemove(field);
          }}
          aria-label={`Remove ${field}`}
          data-testid={`settings-${zone}-remove-${field}`}
        >
          ×
        </button>
      )}
      {menuOpen && menuItems.length > 0 && (
        <div className={styles.addMenu}>
          {menuItems.map((item) => (
            <button
              key={item.label}
              type="button"
              className={styles.addMenuItem}
              onClick={(e) => {
                e.stopPropagation();
                item.action();
                setMenuOpen(false);
              }}
            >
              {item.label}
            </button>
          ))}
        </div>
      )}
    </span>
  );
};

// -- Droppable zone area --

interface DropZoneProps {
  zoneId: ZoneKey;
  children: React.ReactNode;
  isActive?: boolean;
}

const DropZone: FC<DropZoneProps> = ({
  zoneId,
  children,
  isActive,
}): ReactElement => {
  const { setNodeRef } = useDroppable({
    id: `sp-zone-${zoneId}`,
    data: { type: "container", zone: zoneId },
  });
  return (
    <div
      ref={setNodeRef}
      className={`${styles.zoneArea} ${isActive ? styles.zoneAreaActive : ""}`}
      data-testid={`settings-zone-${zoneId}`}
    >
      {children}
    </div>
  );
};

// -- Draggable chip for available fields pool --

interface DraggableAvailableChipProps {
  field: string;
  onToggleMenu: () => void;
  chipRef?: (el: HTMLSpanElement | null) => void;
  isAssigned?: boolean;
}

const DraggableAvailableChip: FC<DraggableAvailableChipProps> = ({
  field,
  onToggleMenu,
  chipRef,
  isAssigned,
}): ReactElement => {
  const { attributes, listeners, setNodeRef, isDragging } = useDraggable({
    id: `available-${field}`,
    data: { field },
  });
  return (
    <span
      ref={(node) => {
        setNodeRef(node);
        chipRef?.(node);
      }}
      className={`${styles.availableChip} ${isAssigned ? styles.availableChipAssigned : ""} ${isDragging ? styles.availableChipDragging : ""}`}
      style={isDragging ? { opacity: 0.3 } : undefined}
      onClick={onToggleMenu}
      data-testid={`settings-available-${field}`}
      {...attributes}
      {...listeners}
    >
      <GripDotsIcon />
      {field}
    </span>
  );
};

// -- Inline aggregation picker for value chips --

export interface InlineAggPickerProps {
  field: string;
  value: AggregationType;
  onChange: (field: string, agg: AggregationType) => void;
  portalTarget?: HTMLElement | null;
  testIdPrefix?: string;
  triggerVariant?: "full" | "chevron";
}

export const InlineAggPicker: FC<InlineAggPickerProps> = ({
  field,
  value,
  onChange,
  portalTarget,
  testIdPrefix = "settings-agg",
  triggerVariant = "full",
}): ReactElement => {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState({ top: 0, left: 0 });

  useEffect(() => {
    if (!open) return;
    const handleClickOutside = (e: MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node) &&
        triggerRef.current &&
        !triggerRef.current.contains(e.target as Node)
      ) {
        setOpen(false);
      }
    };
    const updatePosition = () => {
      if (triggerRef.current) {
        const rect = triggerRef.current.getBoundingClientRect();
        setPos({ top: rect.bottom + 2, left: rect.left });
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    window.addEventListener("scroll", updatePosition, true);
    window.addEventListener("resize", updatePosition);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
      window.removeEventListener("scroll", updatePosition, true);
      window.removeEventListener("resize", updatePosition);
    };
  }, [open]);

  const handleOpen = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      if (!open && triggerRef.current) {
        const rect = triggerRef.current.getBoundingClientRect();
        setPos({ top: rect.bottom + 2, left: rect.left });
      }
      setOpen((v) => !v);
    },
    [open],
  );

  const handleTriggerMouseDown = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
  }, []);

  const selectAgg = useCallback(
    (opt: AggregationType) => {
      onChange(field, opt);
      setOpen(false);
    },
    [field, onChange],
  );

  const dropdown = open ? (
    <div
      ref={dropdownRef}
      className={styles.aggPanel}
      style={{ position: "fixed", top: pos.top, left: pos.left }}
      onMouseDown={(e) => e.stopPropagation()}
      data-testid={`${testIdPrefix}-panel-${field}`}
    >
      <div className={styles.aggGroupLabel}>Basic</div>
      {BASIC_AGGS.map((opt) => (
        <button
          key={opt}
          type="button"
          className={`${styles.aggOption} ${opt === value ? styles.aggOptionActive : ""}`}
          onMouseDown={(e) => {
            e.preventDefault();
            e.stopPropagation();
            selectAgg(opt);
          }}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") {
              e.preventDefault();
              selectAgg(opt);
            }
          }}
        >
          <span className={styles.aggIcon}>{AGG_ICONS[opt]}</span>
          {AGG_LABELS[opt]}
        </button>
      ))}
      <div className={styles.aggGroupDivider} />
      <div className={styles.aggGroupLabel}>Advanced</div>
      {ADVANCED_AGGS.map((opt) => (
        <button
          key={opt}
          type="button"
          className={`${styles.aggOption} ${opt === value ? styles.aggOptionActive : ""}`}
          onMouseDown={(e) => {
            e.preventDefault();
            e.stopPropagation();
            selectAgg(opt);
          }}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") {
              e.preventDefault();
              selectAgg(opt);
            }
          }}
        >
          <span className={styles.aggIcon}>{AGG_ICONS[opt]}</span>
          {AGG_LABELS[opt]}
        </button>
      ))}
    </div>
  ) : null;

  return (
    <div className={styles.aggPicker}>
      <button
        ref={triggerRef}
        type="button"
        className={
          triggerVariant === "chevron" ? styles.chipMenuBtn : styles.aggTrigger
        }
        onMouseDown={handleTriggerMouseDown}
        onClick={handleOpen}
        onKeyDown={(e) => {
          if (e.key === "Escape" && open) {
            e.preventDefault();
            e.stopPropagation();
            setOpen(false);
          } else if (
            e.key === "ArrowDown" ||
            e.key === "Enter" ||
            e.key === " "
          ) {
            if (!open) {
              e.preventDefault();
              if (triggerRef.current) {
                const rect = triggerRef.current.getBoundingClientRect();
                setPos({ top: rect.bottom + 2, left: rect.left });
              }
              setOpen(true);
            }
          }
        }}
        aria-label={`Aggregation for ${field}: ${AGG_LABELS[value]}`}
        aria-expanded={open}
        aria-haspopup="listbox"
        data-testid={`${testIdPrefix}-${field}`}
      >
        {triggerVariant === "chevron" ? (
          "▾"
        ) : (
          <>
            <span className={`${styles.aggIcon} ${styles.aggTriggerIcon}`}>
              {AGG_ICONS[value]}
            </span>
            {AGG_LABELS[value]}
          </>
        )}
      </button>
      {portalTarget ? createPortal(dropdown, portalTarget) : dropdown}
    </div>
  );
};

interface BuilderSelectOption<T extends string> {
  value: T;
  label: string;
}

interface BuilderSelectProps<T extends string> {
  value: T;
  options: readonly BuilderSelectOption<T>[];
  onChange: (value: T) => void;
  testId: string;
  ariaLabel: string;
}

function BuilderSelect<T extends string>({
  value,
  options,
  onChange,
  testId,
  ariaLabel,
}: BuilderSelectProps<T>): ReactElement {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const selectedOptionRef = useRef<HTMLButtonElement>(null);
  const [pos, setPos] = useState({ top: 0, left: 0, width: 0 });

  const selected =
    options.find((option) => option.value === value) ?? options[0];
  const portalTarget = useMemo(() => {
    const root = triggerRef.current?.getRootNode();
    return root instanceof ShadowRoot ? root : document.body;
  }, [open]);

  const close = useCallback(() => setOpen(false), []);

  const updatePosition = useCallback(() => {
    if (!triggerRef.current) return;
    const rect = triggerRef.current.getBoundingClientRect();
    setPos({
      top: rect.bottom + 4,
      left: rect.left,
      width: rect.width,
    });
  }, []);

  useEffect(() => {
    if (!open) return;

    updatePosition();

    const handlePointerDown = (e: MouseEvent) => {
      if (
        panelRef.current?.contains(e.target as Node) ||
        triggerRef.current?.contains(e.target as Node)
      ) {
        return;
      }
      close();
    };

    window.addEventListener("mousedown", handlePointerDown);
    window.addEventListener("scroll", updatePosition, true);
    window.addEventListener("resize", updatePosition);
    return () => {
      window.removeEventListener("mousedown", handlePointerDown);
      window.removeEventListener("scroll", updatePosition, true);
      window.removeEventListener("resize", updatePosition);
    };
  }, [close, open, updatePosition]);

  const handleToggle = useCallback(() => {
    if (!open) updatePosition();
    setOpen((prev) => !prev);
  }, [open, updatePosition]);

  const handleSelect = useCallback(
    (nextValue: T) => {
      onChange(nextValue);
      close();
      requestAnimationFrame(() => triggerRef.current?.focus());
    },
    [close, onChange],
  );

  const handleTriggerKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLButtonElement>) => {
      if (
        e.key === "ArrowDown" ||
        e.key === "ArrowUp" ||
        e.key === "Enter" ||
        e.key === " "
      ) {
        e.preventDefault();
        if (!open) {
          updatePosition();
          setOpen(true);
        }
      } else if (e.key === "Escape" && open) {
        e.preventDefault();
        close();
      }
    },
    [close, open, updatePosition],
  );

  const handleListboxKeyDown = useListboxKeyboard(
    panelRef,
    triggerRef,
    open,
    close,
    '[role="option"]',
    selectedOptionRef,
  );

  const panel = open ? (
    <div
      ref={panelRef}
      className={styles.builderSelectPanel}
      style={{
        position: "fixed",
        top: pos.top,
        left: pos.left,
        minWidth: pos.width,
      }}
      role="listbox"
      aria-label={ariaLabel}
      data-testid={`${testId}-panel`}
      onMouseDown={(e) => e.stopPropagation()}
      onKeyDown={handleListboxKeyDown}
    >
      {options.map((option) => {
        const isSelected = option.value === selected.value;
        return (
          <button
            key={option.value}
            ref={isSelected ? selectedOptionRef : undefined}
            type="button"
            role="option"
            aria-selected={isSelected}
            tabIndex={isSelected ? 0 : -1}
            className={`${styles.builderSelectOption} ${isSelected ? styles.builderSelectOptionActive : ""}`}
            data-testid={`${testId}-${option.value}`}
            onMouseDown={(e) => {
              e.preventDefault();
              e.stopPropagation();
              handleSelect(option.value);
            }}
            onClick={() => handleSelect(option.value)}
          >
            {option.label}
          </button>
        );
      })}
    </div>
  ) : null;

  return (
    <div className={styles.builderSelect}>
      <button
        ref={triggerRef}
        type="button"
        className={styles.builderSelectTrigger}
        data-testid={testId}
        aria-label={ariaLabel}
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={handleToggle}
        onKeyDown={handleTriggerKeyDown}
      >
        <span className={styles.builderSelectLabel}>{selected.label}</span>
        <span className={styles.builderSelectIcon}>
          <DropdownIcon />
        </span>
      </button>
      {typeof document !== "undefined"
        ? createPortal(panel, portalTarget)
        : null}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Locked mode view
// ---------------------------------------------------------------------------

function formatBooleanStatus(value: boolean): string {
  return value ? "On" : "Off";
}

function formatTotalsStatus(
  isEnabled: boolean,
  isUnavailable: boolean,
  isPartial: boolean,
): string {
  if (isUnavailable) return "N/A";
  if (isPartial) return "Some";
  return formatBooleanStatus(isEnabled);
}

function formatSubtotalsStatus(value: PivotConfigV1["show_subtotals"]): string {
  if (Array.isArray(value)) {
    return value.length > 0 ? value.join(", ") : "Off";
  }
  return formatBooleanStatus(!!value);
}

const LockedView: FC<{
  config: PivotConfigV1;
  showStickyToggle?: boolean;
}> = ({ config, showStickyToggle }) => {
  const showRowTotals = resolveShowRowTotals(config);
  const showColTotals = resolveShowColumnTotals(config);
  const rawRowTotals = config.show_row_totals ?? config.show_totals;
  const rawColTotals = config.show_column_totals ?? config.show_totals;
  const rowTotalsPartial =
    Array.isArray(rawRowTotals) && rawRowTotals.length > 0;
  const colTotalsPartial =
    Array.isArray(rawColTotals) && rawColTotals.length > 0;

  return (
    <>
      <span className={styles.sectionTitle}>Current View</span>
      <div className={styles.statusList}>
        <div
          className={styles.statusRow}
          data-testid="settings-row-totals-status"
        >
          <span className={styles.statusLabel}>Row Totals</span>
          <span className={styles.statusValue}>
            {formatTotalsStatus(
              showRowTotals,
              config.columns.length === 0,
              rowTotalsPartial,
            )}
          </span>
        </div>
        <div
          className={styles.statusRow}
          data-testid="settings-col-totals-status"
        >
          <span className={styles.statusLabel}>Column Totals</span>
          <span className={styles.statusValue}>
            {formatTotalsStatus(
              showColTotals,
              config.rows.length === 0,
              colTotalsPartial,
            )}
          </span>
        </div>
        {config.rows.length >= 2 && (
          <div
            className={styles.statusRow}
            data-testid="settings-subtotals-status"
          >
            <span className={styles.statusLabel}>Subtotals</span>
            <span className={styles.statusValue}>
              {formatSubtotalsStatus(config.show_subtotals)}
            </span>
          </div>
        )}
        {config.rows.length >= 2 && (
          <div
            className={styles.statusRow}
            data-testid="settings-repeat-labels-status"
          >
            <span className={styles.statusLabel}>Repeat Labels</span>
            <span className={styles.statusValue}>
              {formatBooleanStatus(!!config.repeat_row_labels)}
            </span>
          </div>
        )}
        {showStickyToggle && (
          <div
            className={styles.statusRow}
            data-testid="settings-sticky-status"
          >
            <span className={styles.statusLabel}>Sticky Headers</span>
            <span className={styles.statusValue}>
              {formatBooleanStatus(config.sticky_headers !== false)}
            </span>
          </div>
        )}
      </div>
    </>
  );
};

// ---------------------------------------------------------------------------
// Synthetic Measure Builder
// ---------------------------------------------------------------------------

interface SyntheticBuilderState {
  editing: boolean;
  editId: string | null;
  label: string;
  operation: "sum_over_sum" | "difference";
  numerator: string;
  denominator: string;
  format: string;
  error: string | null;
}

const initialSyntheticState: SyntheticBuilderState = {
  editing: false,
  editId: null,
  label: "",
  operation: "sum_over_sum",
  numerator: "",
  denominator: "",
  format: "",
  error: null,
};

// ---------------------------------------------------------------------------
// Main Component
// ---------------------------------------------------------------------------

const SettingsPanel: FC<SettingsPanelProps> = ({
  config,
  allColumns,
  numericColumns,
  syntheticSourceColumns,
  onConfigChange,
  locked,
  frozenColumns,
  showStickyToggle,
  open,
  onClose,
  excludeRef,
}): ReactElement | null => {
  const panelRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // -- Staged local state --
  const [localRows, setLocalRows] = useState<string[]>(config.rows);
  const [localCols, setLocalCols] = useState<string[]>(config.columns);
  const [localVals, setLocalVals] = useState<string[]>(config.values);
  const [localAgg, setLocalAgg] = useState<AggregationConfig>(
    config.aggregation,
  );
  const [localSynthetics, setLocalSynthetics] = useState<
    SyntheticMeasureConfig[]
  >(config.synthetic_measures ?? []);
  const [localShowRowTotals, setLocalShowRowTotals] = useState(
    config.show_row_totals ?? config.show_totals,
  );
  const [localShowColTotals, setLocalShowColTotals] = useState(
    config.show_column_totals ?? config.show_totals,
  );
  const [localShowSubtotals, setLocalShowSubtotals] = useState(
    config.show_subtotals,
  );
  const [localRepeatLabels, setLocalRepeatLabels] = useState(
    !!config.repeat_row_labels,
  );
  const [localStickyHeaders, setLocalStickyHeaders] = useState(
    config.sticky_headers !== false,
  );

  // Search for available fields
  const [searchQuery, setSearchQuery] = useState("");

  // Synthetic builder state
  const [synState, setSynState] = useState<SyntheticBuilderState>(
    initialSyntheticState,
  );

  // DnD state
  const [dragActiveZone, setDragActiveZone] = useState<string | null>(null);

  // External config change detection
  const configFingerprintRef = useRef(stringifyPivotConfig(config));

  // Reset local state when panel opens
  useEffect(() => {
    if (open) {
      setLocalRows(config.rows);
      setLocalCols(config.columns);
      setLocalVals(config.values);
      setLocalAgg(config.aggregation);
      setLocalSynthetics(config.synthetic_measures ?? []);
      setLocalShowRowTotals(config.show_row_totals ?? config.show_totals);
      setLocalShowColTotals(config.show_column_totals ?? config.show_totals);
      setLocalShowSubtotals(config.show_subtotals);
      setLocalRepeatLabels(!!config.repeat_row_labels);
      setLocalStickyHeaders(config.sticky_headers !== false);
      setSearchQuery("");
      setSynState(initialSyntheticState);
      configFingerprintRef.current = stringifyPivotConfig(config);
    }
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  // Close panel on external config mutation (semantic change only)
  const isApplyingRef = useRef(false);
  useEffect(() => {
    if (!open) return;
    if (isApplyingRef.current) return;
    const newFingerprint = stringifyPivotConfig(config);
    if (newFingerprint !== configFingerprintRef.current) {
      onClose();
    }
  }, [config, open, onClose]);

  // Click outside to close (excludeRef prevents the settings button from racing)
  useClickOutside(containerRef, onClose, open, excludeRef);

  // Focus panel on open
  useEffect(() => {
    if (open) {
      requestAnimationFrame(() => {
        const first =
          panelRef.current?.querySelector<HTMLElement>("input, button");
        (first ?? panelRef.current)?.focus();
      });
    }
  }, [open]);

  // Keyboard: Escape to close
  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        onClose();
      }
    },
    [onClose],
  );

  // -- Available fields computation --
  const numericSet = useMemo(() => new Set(numericColumns), [numericColumns]);

  const availableFields = useMemo(() => {
    return allColumns;
  }, [allColumns]);

  const showFieldSearch = availableFields.length > FIELD_SEARCH_THRESHOLD;
  const normalizedSearch = searchQuery.trim().toLowerCase();
  const filteredAvailable = useMemo(
    () =>
      !normalizedSearch
        ? availableFields
        : availableFields.filter((c) =>
            c.toLowerCase().includes(normalizedSearch),
          ),
    [availableFields, normalizedSearch],
  );

  // -- Add field to zone --
  const addToZone = useCallback(
    (field: string, zone: ZoneKey) => {
      if (zone === "rows") {
        if (localCols.includes(field)) return;
        if (!localRows.includes(field)) setLocalRows((r) => [...r, field]);
      } else if (zone === "columns") {
        if (localRows.includes(field)) return;
        if (!localCols.includes(field)) setLocalCols((c) => [...c, field]);
      } else if (zone === "values") {
        if (!numericSet.has(field)) return;
        if (!localVals.includes(field)) {
          const nextVals = [...localVals, field];
          setLocalVals(nextVals);
          setLocalAgg((agg) => normalizeAggregationConfig(agg, nextVals));
        }
      }
    },
    [localRows, localCols, localVals, numericSet],
  );

  // -- Remove from zone --
  const removeFromZone = useCallback(
    (field: string, zone: ZoneKey) => {
      if (frozenColumns?.has(field)) return;
      if (zone === "rows") {
        setLocalRows((r) => r.filter((f) => f !== field));
      } else if (zone === "columns") {
        setLocalCols((c) => c.filter((f) => f !== field));
      } else if (zone === "values") {
        const nextVals = localVals.filter((f) => f !== field);
        setLocalVals(nextVals);
        setLocalAgg((agg) => normalizeAggregationConfig(agg, nextVals));
      }
    },
    [frozenColumns, localVals],
  );

  // -- Move between zones (validates constraints before mutating) --
  const moveToZone = useCallback(
    (field: string, fromZone: ZoneKey, toZone: ZoneKey) => {
      if (frozenColumns?.has(field) || fromZone === toZone) return;

      if (toZone === "values" && !numericSet.has(field)) return;

      if (
        toZone === "rows" &&
        fromZone !== "columns" &&
        localCols.includes(field)
      )
        return;
      if (
        toZone === "columns" &&
        fromZone !== "rows" &&
        localRows.includes(field)
      )
        return;

      const setterFor = (z: ZoneKey) =>
        z === "rows"
          ? setLocalRows
          : z === "columns"
            ? setLocalCols
            : setLocalVals;

      setterFor(fromZone)((arr) => arr.filter((f) => f !== field));
      setterFor(toZone)((arr) => (arr.includes(field) ? arr : [...arr, field]));

      if (fromZone === "values") {
        const nextVals = localVals.filter((f) => f !== field);
        setLocalAgg((agg) => {
          const next = { ...agg };
          delete next[field];
          return normalizeAggregationConfig(next, nextVals);
        });
      }
      if (toZone === "values") {
        const nextVals = [...localVals, field];
        setLocalAgg((agg) => normalizeAggregationConfig(agg, nextVals));
      }
    },
    [frozenColumns, numericSet, localVals, localRows, localCols],
  );

  // -- Aggregation change --
  const handleAggChange = useCallback(
    (field: string, agg: AggregationType) => {
      setLocalAgg((prev) =>
        normalizeAggregationConfig({ ...prev, [field]: agg }, localVals),
      );
    },
    [localVals],
  );

  // -- Zone chip menu items (source-aware) --
  const getRowChipMenu = useCallback(
    (field: string) => {
      const items: { label: string; action: () => void }[] = [];
      items.push({
        label: "Move to Columns",
        action: () => moveToZone(field, "rows", "columns"),
      });
      if (numericSet.has(field) && !localVals.includes(field)) {
        items.push({
          label: "Also add to Values",
          action: () => addToZone(field, "values"),
        });
      }
      return items;
    },
    [moveToZone, addToZone, numericSet, localVals],
  );

  const getColChipMenu = useCallback(
    (field: string) => {
      const items: { label: string; action: () => void }[] = [];
      items.push({
        label: "Move to Rows",
        action: () => moveToZone(field, "columns", "rows"),
      });
      if (numericSet.has(field) && !localVals.includes(field)) {
        items.push({
          label: "Also add to Values",
          action: () => addToZone(field, "values"),
        });
      }
      return items;
    },
    [moveToZone, addToZone, numericSet, localVals],
  );

  const getValChipMenu = useCallback(
    (field: string) => {
      const items: { label: string; action: () => void }[] = [];
      const inRows = localRows.includes(field);
      const inCols = localCols.includes(field);
      if (!inRows && !inCols) {
        items.push({
          label: "Also add to Rows",
          action: () => addToZone(field, "rows"),
        });
        items.push({
          label: "Also add to Columns",
          action: () => addToZone(field, "columns"),
        });
      }
      if (!inCols) {
        items.push({
          label: "Move to Rows",
          action: () => moveToZone(field, "values", "rows"),
        });
      }
      if (!inRows) {
        items.push({
          label: "Move to Columns",
          action: () => moveToZone(field, "values", "columns"),
        });
      }
      return items;
    },
    [localRows, localCols, moveToZone, addToZone],
  );

  const getAvailableChipMenu = useCallback(
    (field: string) => {
      if (frozenColumns?.has(field)) return [];

      const inRows = localRows.includes(field);
      const inCols = localCols.includes(field);
      const inVals = localVals.includes(field);

      if (!inRows && !inCols && !inVals) {
        const items: { label: string; action: () => void }[] = [
          { label: "Add to Rows", action: () => addToZone(field, "rows") },
          {
            label: "Add to Columns",
            action: () => addToZone(field, "columns"),
          },
        ];
        if (numericSet.has(field)) {
          items.push({
            label: "Add to Values",
            action: () => addToZone(field, "values"),
          });
        }
        return items;
      }

      if (inRows) {
        return getRowChipMenu(field);
      }
      if (inCols) {
        return getColChipMenu(field);
      }
      if (inVals) {
        return getValChipMenu(field);
      }
      return [];
    },
    [
      frozenColumns,
      localRows,
      localCols,
      localVals,
      numericSet,
      addToZone,
      getRowChipMenu,
      getColChipMenu,
      getValChipMenu,
    ],
  );

  // -- Display toggles --
  const toggleRowTotals = useCallback(() => {
    setLocalShowRowTotals((current) =>
      Array.isArray(current) ? true : !current,
    );
  }, []);

  const toggleColTotals = useCallback(() => {
    setLocalShowColTotals((current) =>
      Array.isArray(current) ? true : !current,
    );
  }, []);

  const toggleSubtotals = useCallback(() => {
    setLocalShowSubtotals((current) =>
      Array.isArray(current) ? true : !current,
    );
  }, []);

  const toggleRepeatLabels = useCallback(() => {
    setLocalRepeatLabels((v) => !v);
  }, []);

  const toggleStickyHeaders = useCallback(() => {
    setLocalStickyHeaders((v) => !v);
  }, []);

  // Derived display state
  const showRowTotals =
    localShowRowTotals === true ||
    (Array.isArray(localShowRowTotals) && localShowRowTotals.length > 0);
  const showColTotals =
    localShowColTotals === true ||
    (Array.isArray(localShowColTotals) && localShowColTotals.length > 0);
  const rowTotalsIndeterminate =
    Array.isArray(localShowRowTotals) && localShowRowTotals.length > 0;
  const colTotalsIndeterminate =
    Array.isArray(localShowColTotals) && localShowColTotals.length > 0;
  const subtotalsChecked =
    localShowSubtotals === true ||
    (Array.isArray(localShowSubtotals) && localShowSubtotals.length > 0);
  const subtotalsIndeterminate =
    Array.isArray(localShowSubtotals) && localShowSubtotals.length > 0;
  const rowTotalsDisabled = localCols.length === 0;
  const colTotalsDisabled = localRows.length === 0;

  // -- Synthetic measure builder --
  const allNumeric = syntheticSourceColumns ?? numericColumns;

  const openCreateSynthetic = useCallback(() => {
    setSynState({
      editing: true,
      editId: null,
      label: "",
      operation: "sum_over_sum",
      numerator: allNumeric[0] ?? "",
      denominator: allNumeric[1] ?? allNumeric[0] ?? "",
      format: "",
      error: null,
    });
  }, [allNumeric]);

  const openEditSynthetic = useCallback((measure: SyntheticMeasureConfig) => {
    setSynState({
      editing: true,
      editId: measure.id,
      label: measure.label,
      operation: measure.operation,
      numerator: measure.numerator,
      denominator: measure.denominator,
      format: measure.format ?? "",
      error: null,
    });
  }, []);

  const saveSynthetic = useCallback(() => {
    const trimmedLabel = synState.label.trim();
    if (!trimmedLabel) {
      setSynState((s) => ({ ...s, error: "Name is required." }));
      return;
    }
    if (!synState.numerator || !synState.denominator) {
      setSynState((s) => ({ ...s, error: "Choose both source fields." }));
      return;
    }
    const existing = localSynthetics.filter((m) => m.id !== synState.editId);
    if (
      existing.some((m) => m.label.toLowerCase() === trimmedLabel.toLowerCase())
    ) {
      setSynState((s) => ({
        ...s,
        error: "Synthetic measure name must be unique.",
      }));
      return;
    }
    const id =
      synState.editId ??
      `synthetic_${trimmedLabel.toLowerCase().replace(/[^a-z0-9]+/g, "_")}`;
    if (!synState.editId && existing.some((m) => m.id === id)) {
      setSynState((s) => ({
        ...s,
        error: "Generated id collides with an existing synthetic measure.",
      }));
      return;
    }
    if (!synState.editId && localVals.includes(id)) {
      setSynState((s) => ({
        ...s,
        error: "Generated id collides with an existing value field.",
      }));
      return;
    }
    const trimmedFormat = synState.format.trim();
    if (trimmedFormat && !isSupportedFormatPattern(trimmedFormat)) {
      setSynState((s) => ({
        ...s,
        error: "Format is invalid. Try patterns like .1%, $,.0f, or ,.2f.",
      }));
      return;
    }
    const measure: SyntheticMeasureConfig = {
      id,
      label: trimmedLabel,
      operation: synState.operation,
      numerator: synState.numerator,
      denominator: synState.denominator,
      format: trimmedFormat || undefined,
    };
    const idx = localSynthetics.findIndex((m) => m.id === synState.editId);
    if (idx >= 0) {
      setLocalSynthetics((arr) => arr.map((m, i) => (i === idx ? measure : m)));
    } else {
      setLocalSynthetics((arr) => [...arr, measure]);
    }
    setSynState(initialSyntheticState);
  }, [synState, localSynthetics, localVals]);

  const removeSynthetic = useCallback((id: string) => {
    setLocalSynthetics((arr) => arr.filter((m) => m.id !== id));
  }, []);

  const formulaPreview =
    synState.operation === "sum_over_sum"
      ? `sum(${synState.numerator}) / sum(${synState.denominator})`
      : `sum(${synState.numerator}) - sum(${synState.denominator})`;
  const trimmedFormat = synState.format.trim();
  const hasValidFormatPreview =
    trimmedFormat !== "" && isSupportedFormatPattern(trimmedFormat);
  const formatPreviewSample = trimmedFormat.includes("%")
    ? SYNTHETIC_FORMAT_PREVIEW_PERCENT
    : SYNTHETIC_FORMAT_PREVIEW_NUMBER;

  // -- DnD within panel --
  const pointerSensor = useSensor(PointerSensor, {
    activationConstraint: { distance: 5 },
  });
  const sensors = useSensors(pointerSensor);

  const [dragOverlay, setDragOverlay] = useState<{
    field: string;
    zone?: string;
  } | null>(null);

  const handleDragStart = useCallback((event: DragStartEvent) => {
    const data = event.active.data.current as
      | { field?: string; zone?: string }
      | undefined;
    if (data?.field) {
      setDragOverlay({ field: data.field, zone: data.zone });
    }
    setAddMenuField(null);
  }, []);

  const handleDragOver = useCallback((event: DragOverEvent) => {
    const { over } = event;
    if (!over) {
      setDragActiveZone(null);
      return;
    }
    const targetZone =
      over.data.current?.zone ??
      (over.data.current?.type === "container"
        ? String(over.id).replace("sp-zone-", "")
        : null);
    setDragActiveZone(targetZone);
  }, []);

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const { active, over } = event;
      setDragActiveZone(null);
      setDragOverlay(null);
      if (!over) return;

      const action = resolveDragEnd({
        activeData: active.data.current as
          | { zone?: string; field?: string }
          | undefined,
        overId: over.id,
        overData: over.data.current as
          | { zone?: string; type?: string; field?: string }
          | undefined,
      });
      if (!action) return;

      if (action.type === "reorder") {
        const { zone, field, overField } = action;
        const setter =
          zone === "rows"
            ? setLocalRows
            : zone === "columns"
              ? setLocalCols
              : setLocalVals;
        setter((arr) => {
          const oldIdx = arr.indexOf(field);
          const newIdx = arr.indexOf(overField);
          if (oldIdx === -1 || newIdx === -1) return arr;
          return arrayMove(arr, oldIdx, newIdx);
        });
      } else if (action.type === "cross-zone") {
        moveToZone(action.field, action.fromZone, action.toZone);
      } else if (action.type === "add-from-available") {
        addToZone(action.field, action.toZone);
      }
    },
    [moveToZone, addToZone],
  );

  const handleDragCancel = useCallback(() => {
    setDragActiveZone(null);
    setDragOverlay(null);
  }, []);

  // Sortable IDs for each zone
  const rowSortableIds = useMemo(
    () => localRows.map((f) => makeZoneItemId("rows", f)),
    [localRows],
  );
  const colSortableIds = useMemo(
    () => localCols.map((f) => makeZoneItemId("columns", f)),
    [localCols],
  );
  const valSortableIds = useMemo(
    () => localVals.map((f) => makeZoneItemId("values", f)),
    [localVals],
  );

  // -- Apply handler --
  const handleApply = useCallback(() => {
    isApplyingRef.current = true;

    const subtotalsValue = (() => {
      if (!localShowSubtotals) return false;
      if (Array.isArray(localShowSubtotals) && localShowSubtotals.length === 0)
        return false;
      return localShowSubtotals;
    })();

    let newConfig: PivotConfigV1 = {
      ...config,
      rows: localRows,
      columns: localCols,
      values: localVals,
      aggregation: localAgg,
      synthetic_measures:
        localSynthetics.length > 0 ? localSynthetics : undefined,
      show_row_totals: localShowRowTotals,
      show_column_totals: localShowColTotals,
      show_subtotals: subtotalsValue,
      repeat_row_labels: localRepeatLabels || undefined,
      sticky_headers: localStickyHeaders,
    };

    // Cleanup after subtotals off
    if (!subtotalsValue) {
      delete newConfig.collapsed_groups;
      delete newConfig.repeat_row_labels;
    }

    newConfig = cleanupConfigAfterFieldChanges(config, newConfig);
    onConfigChange(newConfig);
    configFingerprintRef.current = stringifyPivotConfig(newConfig);

    requestAnimationFrame(() => {
      isApplyingRef.current = false;
    });
    onClose();
  }, [
    config,
    localRows,
    localCols,
    localVals,
    localAgg,
    localSynthetics,
    localShowRowTotals,
    localShowColTotals,
    localShowSubtotals,
    localRepeatLabels,
    localStickyHeaders,
    onConfigChange,
    onClose,
  ]);

  // -- Available fields container: lock height after first render --
  const availChipsRef = useRef<HTMLDivElement>(null);
  const [availChipsHeight, setAvailChipsHeight] = useState<number | null>(null);
  useEffect(() => {
    if (availChipsRef.current && availChipsHeight === null) {
      const el = availChipsRef.current;
      setAvailChipsHeight(el.offsetHeight);
    }
  }, [availChipsHeight]);

  // -- Dirty check: disable Apply when nothing changed --
  const hasChanges = useMemo(() => {
    const arrEq = (a: string[], b: string[]) =>
      a.length === b.length && a.every((v, i) => v === b[i]);
    const initRowTotals = config.show_row_totals ?? config.show_totals;
    const initColTotals = config.show_column_totals ?? config.show_totals;
    const initSynthetics = config.synthetic_measures ?? [];

    if (!arrEq(localRows, config.rows)) return true;
    if (!arrEq(localCols, config.columns)) return true;
    if (!arrEq(localVals, config.values)) return true;
    if (JSON.stringify(localAgg) !== JSON.stringify(config.aggregation))
      return true;
    if (JSON.stringify(localSynthetics) !== JSON.stringify(initSynthetics))
      return true;
    if (JSON.stringify(localShowRowTotals) !== JSON.stringify(initRowTotals))
      return true;
    if (JSON.stringify(localShowColTotals) !== JSON.stringify(initColTotals))
      return true;
    if (
      JSON.stringify(localShowSubtotals) !==
      JSON.stringify(config.show_subtotals)
    )
      return true;
    if (localRepeatLabels !== !!config.repeat_row_labels) return true;
    if (localStickyHeaders !== (config.sticky_headers !== false)) return true;
    return false;
  }, [
    config,
    localRows,
    localCols,
    localVals,
    localAgg,
    localSynthetics,
    localShowRowTotals,
    localShowColTotals,
    localShowSubtotals,
    localRepeatLabels,
    localStickyHeaders,
  ]);

  // -- Panel open/close animation --
  const [visible, setVisible] = useState(false);
  const [animatingOut, setAnimatingOut] = useState(false);

  useEffect(() => {
    if (open) {
      setVisible(true);
      setAnimatingOut(false);
    } else if (visible) {
      setAnimatingOut(true);
      const timer = setTimeout(() => {
        setVisible(false);
        setAnimatingOut(false);
      }, 150);
      return () => clearTimeout(timer);
    }
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  // -- Available field chip with add menu --
  const [addMenuField, setAddMenuField] = useState<string | null>(null);
  const addMenuRef = useRef<HTMLDivElement>(null);
  const availChipRefs = useRef<Map<string, HTMLSpanElement>>(new Map());
  const [addMenuPos, setAddMenuPos] = useState<{
    top: number;
    left: number;
  } | null>(null);
  useClickOutside(addMenuRef, () => setAddMenuField(null), !!addMenuField);

  useEffect(() => {
    if (!addMenuField) return;
    const panel = panelRef.current;
    if (!panel) return;
    const closeMenu = () => setAddMenuField(null);
    panel.addEventListener("scroll", closeMenu, { passive: true });
    window.addEventListener("resize", closeMenu);
    return () => {
      panel.removeEventListener("scroll", closeMenu);
      window.removeEventListener("resize", closeMenu);
    };
  }, [addMenuField]);

  const openAddMenu = useCallback((field: string) => {
    setAddMenuField((prev) => {
      if (prev === field) return null;
      const el = availChipRefs.current.get(field);
      const wrapperEl = containerRef.current;
      if (el && wrapperEl) {
        const chipRect = el.getBoundingClientRect();
        const wrapperRect = wrapperEl.getBoundingClientRect();
        setAddMenuPos({
          top: chipRect.bottom - wrapperRect.top + 2,
          left: chipRect.left - wrapperRect.left,
        });
      }
      return field;
    });
  }, []);

  if (!visible) return null;

  const panelClassName = `${styles.panel} ${animatingOut ? styles.panelExit : styles.panelEnter}`;

  // -- Locked mode --
  if (locked) {
    return (
      <div ref={containerRef} className={styles.wrapper}>
        <div
          ref={panelRef}
          tabIndex={-1}
          className={panelClassName}
          role="dialog"
          aria-label="Table settings"
          onKeyDown={handleKeyDown}
          data-testid="settings-panel"
        >
          <LockedView config={config} showStickyToggle={showStickyToggle} />
        </div>
      </div>
    );
  }

  // -- Interactive mode --
  return (
    <div ref={containerRef} className={styles.wrapper}>
      <div
        ref={panelRef}
        tabIndex={-1}
        className={panelClassName}
        role="dialog"
        aria-label="Table settings"
        onKeyDown={handleKeyDown}
        data-testid="settings-panel"
      >
        <DndContext
          sensors={sensors}
          collisionDetection={pointerWithin}
          onDragStart={handleDragStart}
          onDragOver={handleDragOver}
          onDragEnd={handleDragEnd}
          onDragCancel={handleDragCancel}
        >
          {/* Available Fields */}
          <span className={styles.sectionTitle}>Available Fields</span>
          <span className={styles.sectionHint}>
            Drag fields to a zone below, or click to add
          </span>
          {showFieldSearch && (
            <div className={styles.fieldSearchWrapper}>
              <input
                type="text"
                className={styles.fieldSearchInput}
                placeholder="Search fields..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                data-testid="settings-field-search"
              />
            </div>
          )}
          <div
            ref={availChipsRef}
            className={styles.availableFieldsChips}
            style={
              availChipsHeight ? { minHeight: availChipsHeight } : undefined
            }
          >
            {filteredAvailable.length === 0 ? (
              <span className={styles.noFields}>
                {searchQuery ? "No matching fields" : "No fields available"}
              </span>
            ) : (
              filteredAvailable.map((f) => (
                <DraggableAvailableChip
                  key={f}
                  field={f}
                  onToggleMenu={() => {
                    if (getAvailableChipMenu(f).length === 0) return;
                    openAddMenu(f);
                  }}
                  isAssigned={
                    localRows.includes(f) ||
                    localCols.includes(f) ||
                    localVals.includes(f)
                  }
                  chipRef={(el) => {
                    if (el) availChipRefs.current.set(f, el);
                    else availChipRefs.current.delete(f);
                  }}
                />
              ))
            )}
          </div>

          {/* Portaled add-menu for available field chips */}
          {addMenuField &&
            addMenuPos &&
            containerRef.current &&
            createPortal(
              <div
                ref={addMenuRef}
                className={styles.addMenu}
                style={{
                  position: "absolute",
                  top: addMenuPos.top,
                  left: addMenuPos.left,
                  zIndex: 50,
                }}
                onMouseDown={(e) => e.stopPropagation()}
              >
                {getAvailableChipMenu(addMenuField).map((item) => (
                  <button
                    key={item.label}
                    type="button"
                    className={styles.addMenuItem}
                    onMouseDown={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                    }}
                    onClick={(e) => {
                      e.stopPropagation();
                      item.action();
                      setAddMenuField(null);
                    }}
                  >
                    {item.label}
                  </button>
                ))}
              </div>,
              containerRef.current,
            )}

          {/* Rows zone */}
          <span className={styles.sectionTitle}>Rows</span>
          <DropZone zoneId="rows" isActive={dragActiveZone === "rows"}>
            <SortableContext
              items={rowSortableIds}
              strategy={horizontalListSortingStrategy}
            >
              {localRows.length === 0 ? (
                <span className={styles.zoneEmpty}>Drop fields here</span>
              ) : (
                localRows.map((field) => (
                  <ZoneChip
                    key={field}
                    id={makeZoneItemId("rows", field)}
                    zone="rows"
                    field={field}
                    isFrozen={frozenColumns?.has(field)}
                    onRemove={(f) => removeFromZone(f, "rows")}
                    menuItems={[]}
                  />
                ))
              )}
            </SortableContext>
          </DropZone>

          {/* Columns zone */}
          <span className={styles.sectionTitle}>Columns</span>
          <DropZone zoneId="columns" isActive={dragActiveZone === "columns"}>
            <SortableContext
              items={colSortableIds}
              strategy={horizontalListSortingStrategy}
            >
              {localCols.length === 0 ? (
                <span className={styles.zoneEmpty}>Drop fields here</span>
              ) : (
                localCols.map((field) => (
                  <ZoneChip
                    key={field}
                    id={makeZoneItemId("columns", field)}
                    zone="columns"
                    field={field}
                    isFrozen={frozenColumns?.has(field)}
                    onRemove={(f) => removeFromZone(f, "columns")}
                    menuItems={[]}
                  />
                ))
              )}
            </SortableContext>
          </DropZone>

          {/* Values zone */}
          <span className={styles.sectionTitle}>Values</span>
          <DropZone zoneId="values" isActive={dragActiveZone === "values"}>
            <SortableContext
              items={valSortableIds}
              strategy={horizontalListSortingStrategy}
            >
              {localVals.length === 0 && localSynthetics.length === 0 ? (
                <span className={styles.zoneEmpty}>Drop fields here</span>
              ) : (
                <>
                  {localVals.map((field) => (
                    <ZoneChip
                      key={field}
                      id={makeZoneItemId("values", field)}
                      zone="values"
                      field={field}
                      isFrozen={frozenColumns?.has(field)}
                      isValues
                      aggLabel={
                        AGG_LABELS[localAgg?.[field] ?? "sum"]
                          ? AGG_LABELS[localAgg?.[field] ?? "sum"].slice(0, 3)
                          : "Sum"
                      }
                      aggregationControl={
                        !frozenColumns?.has(field) ? (
                          <InlineAggPicker
                            field={field}
                            value={localAgg?.[field] ?? "sum"}
                            onChange={handleAggChange}
                            portalTarget={containerRef.current}
                            triggerVariant="chevron"
                          />
                        ) : undefined
                      }
                      onRemove={(f) => removeFromZone(f, "values")}
                      menuItems={[]}
                    />
                  ))}
                  {localSynthetics.map((measure) => (
                    <span
                      key={measure.id}
                      className={styles.syntheticChip}
                      data-testid={`settings-synthetic-${measure.id}`}
                    >
                      <span className={styles.syntheticIcon}>fx</span>
                      {measure.label}
                      <button
                        type="button"
                        className={styles.chipMenuBtn}
                        onClick={() => openEditSynthetic(measure)}
                        aria-label={`Edit ${measure.label}`}
                      >
                        ✎
                      </button>
                      <button
                        type="button"
                        className={styles.chipRemoveBtn}
                        onClick={() => removeSynthetic(measure.id)}
                        aria-label={`Remove ${measure.label}`}
                      >
                        ×
                      </button>
                    </span>
                  ))}
                </>
              )}
            </SortableContext>
          </DropZone>
          <button
            type="button"
            className={styles.addMeasureBtn}
            onClick={openCreateSynthetic}
            data-testid="settings-add-synthetic"
          >
            + Add measure
          </button>

          {/* Synthetic builder */}
          {synState.editing && (
            <div
              className={styles.syntheticBuilder}
              data-testid="settings-synthetic-builder"
            >
              <div className={styles.builderField}>
                <span className={styles.builderFieldLabel}>Name</span>
                <input
                  className={styles.builderInput}
                  value={synState.label}
                  onChange={(e) =>
                    setSynState((s) => ({
                      ...s,
                      label: e.target.value,
                      error: null,
                    }))
                  }
                />
              </div>
              <div className={styles.builderField}>
                <span className={styles.builderFieldLabel}>Operation</span>
                <BuilderSelect
                  testId="settings-synthetic-operation"
                  ariaLabel="Synthetic measure operation"
                  value={synState.operation}
                  options={SYNTHETIC_OPERATION_OPTIONS}
                  onChange={(operation) =>
                    setSynState((s) => ({
                      ...s,
                      operation,
                    }))
                  }
                />
                {synState.numerator && synState.denominator && (
                  <div className={styles.formulaHint}>{formulaPreview}</div>
                )}
              </div>
              <div className={styles.builderField}>
                <span className={styles.builderFieldLabel}>Measure A</span>
                <BuilderSelect
                  testId="settings-synthetic-numerator"
                  ariaLabel="Synthetic measure numerator"
                  value={synState.numerator}
                  options={allNumeric.map((c) => ({ value: c, label: c }))}
                  onChange={(numerator) =>
                    setSynState((s) => ({ ...s, numerator }))
                  }
                />
              </div>
              <div className={styles.builderField}>
                <span className={styles.builderFieldLabel}>Measure B</span>
                <BuilderSelect
                  testId="settings-synthetic-denominator"
                  ariaLabel="Synthetic measure denominator"
                  value={synState.denominator}
                  options={allNumeric.map((c) => ({ value: c, label: c }))}
                  onChange={(denominator) =>
                    setSynState((s) => ({ ...s, denominator }))
                  }
                />
              </div>
              <div className={styles.builderField}>
                <span className={styles.builderFieldLabel}>
                  Format (optional)
                </span>
                <div className={styles.formatPresets}>
                  {SYNTHETIC_FORMAT_PRESETS.map((preset) => (
                    <button
                      key={preset.value}
                      type="button"
                      className={`${styles.formatPresetBtn} ${synState.format.trim() === preset.value ? styles.formatPresetBtnActive : ""}`}
                      onClick={() =>
                        setSynState((s) => ({
                          ...s,
                          format: preset.value,
                          error: null,
                        }))
                      }
                    >
                      {preset.label}
                    </button>
                  ))}
                </div>
                <input
                  className={styles.builderInput}
                  value={synState.format}
                  placeholder="e.g. .1%, $,.0f, ,.2f"
                  onChange={(e) =>
                    setSynState((s) => ({
                      ...s,
                      format: e.target.value,
                      error: null,
                    }))
                  }
                />
                {trimmedFormat && (
                  <div className={styles.formatPreview}>
                    {hasValidFormatPreview
                      ? `Example: ${formatWithPattern(formatPreviewSample, trimmedFormat)}`
                      : "Example unavailable (invalid format pattern)"}
                  </div>
                )}
              </div>
              {synState.error && (
                <div className={styles.builderError}>{synState.error}</div>
              )}
              <div className={styles.builderActions}>
                <button
                  type="button"
                  className={styles.builderBtn}
                  onClick={() => setSynState(initialSyntheticState)}
                  data-testid="settings-synthetic-cancel"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  className={`${styles.builderBtn} ${styles.builderBtnPrimary}`}
                  onClick={saveSynthetic}
                  data-testid="settings-synthetic-save"
                >
                  Save
                </button>
              </div>
            </div>
          )}

          <DragOverlay dropAnimation={{ duration: 200 }}>
            {dragOverlay ? (
              <span
                className={`${dragOverlay.zone ? styles.zoneChip : styles.availableChip} ${styles.dragOverlayChip}`}
              >
                <GripDotsIcon />
                {dragOverlay.field}
              </span>
            ) : null}
          </DragOverlay>
        </DndContext>

        {/* Display toggles */}
        <span className={styles.sectionTitle} style={{ marginTop: 0 }}>
          Display
        </span>
        <div className={styles.displayGrid}>
          <label
            className={styles.checkboxLabel}
            data-testid="settings-row-totals"
            title={
              rowTotalsDisabled
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
            data-testid="settings-col-totals"
            title={
              colTotalsDisabled
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
              onChange={toggleColTotals}
              disabled={colTotalsDisabled}
            />
            <span>Column Totals</span>
          </label>
          {localRows.length >= 2 && (
            <label
              className={styles.checkboxLabel}
              data-testid="settings-subtotals"
            >
              <input
                ref={(el) => {
                  if (el) el.indeterminate = subtotalsIndeterminate;
                }}
                type="checkbox"
                checked={subtotalsChecked}
                onChange={toggleSubtotals}
              />
              <span>Subtotals</span>
            </label>
          )}
          {localRows.length >= 2 && (
            <label
              className={styles.checkboxLabel}
              data-testid="settings-repeat-labels"
            >
              <input
                type="checkbox"
                checked={localRepeatLabels}
                onChange={toggleRepeatLabels}
              />
              <span>Repeat Labels</span>
            </label>
          )}
          {showStickyToggle && (
            <label
              className={styles.checkboxLabel}
              data-testid="settings-sticky-headers"
            >
              <input
                type="checkbox"
                checked={localStickyHeaders}
                onChange={toggleStickyHeaders}
              />
              <span>Sticky Headers</span>
            </label>
          )}
        </div>

        {!synState.editing && (
          <div className={styles.footer}>
            <button
              type="button"
              className={styles.cancelBtn}
              onClick={onClose}
              data-testid="settings-cancel"
            >
              Cancel
            </button>
            <button
              type="button"
              className={styles.applyBtn}
              onClick={handleApply}
              disabled={!hasChanges}
              data-testid="settings-apply"
            >
              Apply
            </button>
          </div>
        )}
      </div>
    </div>
  );
};

export default SettingsPanel;
