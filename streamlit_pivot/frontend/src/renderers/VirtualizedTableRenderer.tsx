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
  useMemo,
  useRef,
  useState,
} from "react";
import type { PivotData, GroupedRow } from "../engine/PivotData";
import {
  getRenderedValueFields,
  isSyntheticMeasure,
  showColumnTotals,
  type CellClickPayload,
  type DateGrain,
  type DimensionFilter,
  type PivotConfigV1,
  type ShowValuesAs,
  type SortConfig,
} from "../engine/types";
import VirtualScroll from "../shared/VirtualScroll";
import HeaderMenu from "./HeaderMenu";
import { makeKeyString } from "../engine/PivotData";
import {
  computeColSlots,
  renderColumnHeaders,
  renderDataRow,
  renderSubtotalRow,
  renderTemporalParentRow,
  renderTotalsRow as renderTotalsRowFn,
  HEADER_ROW_HEIGHT,
  type ColSlot,
} from "./TableRenderer";
import {
  applyTemporalRowCollapse,
  computeHeaderLevels,
  computeNumRowHeaderLevels,
  computeProjectedRowHeaderSpans,
  computeRowHeaderLevels,
  computeTemporalColInfos,
  computeTemporalColSlots,
  computeTemporalRowInfos,
  projectVisibleRowEntries,
  toggleTemporalCollapse,
  toggleTemporalRowCollapse,
} from "./temporalHierarchy";
import { useHeaderMenu } from "./useHeaderMenu";
import tableStyles from "./TableRenderer.module.css";
import { resolveEffectiveWidth, resolveFieldWidth } from "./fieldWidthResolver";
import {
  styleToCSS,
  densityClass,
  bordersClass,
  stripesOffClass,
  hoverOffClass,
  DENSITY_ROW_HEIGHT,
} from "./styleHelpers";

export interface VirtualizedTableRendererProps {
  pivotData: PivotData;
  config: PivotConfigV1;
  onCellClick?: (payload: CellClickPayload) => void;
  maxColumns?: number;
  containerHeight: number;
  rowHeight?: number;
  columnWidth?: number;
  headerHeight?: number;
  onSortChange?: (
    axis: "row" | "col",
    sort: SortConfig | undefined,
    dimension: string,
  ) => void;
  onFilterChange?: (field: string, filter: DimensionFilter | undefined) => void;
  onConfigChange?: (config: PivotConfigV1) => void;
  onShowValuesAsChange?: (field: string, mode: ShowValuesAs) => void;
  onCollapseChange?: (axis: "row" | "col", collapsed: string[]) => void;
  adaptiveDateGrains?: Record<string, DateGrain>;
  menuLimit?: number;
  /** When true, the wrapper becomes a flex item that fills remaining space. */
  scrollable?: boolean;
}

const DEFAULT_ROW_HEIGHT = 36;
const DEFAULT_COL_WIDTH = 120;
const DEFAULT_HEADER_HEIGHT = 72;
const MIN_COL_WIDTH = 40;

const VirtualizedTableRenderer: FC<VirtualizedTableRendererProps> = ({
  pivotData,
  config,
  onCellClick,
  maxColumns,
  containerHeight,
  rowHeight, // no default — keep undefined so density can take effect
  columnWidth = DEFAULT_COL_WIDTH,
  headerHeight = DEFAULT_HEADER_HEIGHT,
  onSortChange,
  onFilterChange,
  onConfigChange,
  onShowValuesAsChange,
  onCollapseChange,
  adaptiveDateGrains,
  menuLimit,
  scrollable,
}): ReactElement => {
  const effectiveRowHeight =
    rowHeight ??
    DENSITY_ROW_HEIGHT[config.style?.density ?? "default"] ??
    DEFAULT_ROW_HEIGHT;

  const wrapperRef = useRef<HTMLDivElement>(null);
  const [measuredHeight, setMeasuredHeight] = useState(containerHeight);

  const [columnWidthMap, setColumnWidthMap] = useState<Map<number, number>>(
    () => new Map(),
  );
  const [valFieldWidthMap, setValFieldWidthMap] = useState<Map<string, number>>(
    () => new Map(),
  );
  const [isResizing, setIsResizing] = useState(false);
  const resizeDragRef = useRef<{
    key: number | string;
    startX: number;
    startWidth: number;
  } | null>(null);

  const handleResizeDoubleClick = useCallback(
    (slotIndex: number | string, e: React.MouseEvent<HTMLDivElement>) => {
      e.preventDefault();
      e.stopPropagation();

      const th = (e.target as HTMLElement).closest("th");
      const table = wrapperRef.current?.querySelector("table");
      if (!th || !table) return;

      const headerRow = th.parentElement as HTMLTableRowElement;
      let physicalCol = 0;
      for (const cell of headerRow.cells) {
        if (cell === th) break;
        physicalCol += cell.colSpan || 1;
      }

      let maxWidth = MIN_COL_WIDTH;
      for (const row of table.querySelectorAll("tbody tr")) {
        const cells = (row as HTMLTableRowElement).cells;
        let col = 0;
        for (const cell of cells) {
          const span = cell.colSpan || 1;
          if (col <= physicalCol && physicalCol < col + span) {
            const ruler = document.createElement("span");
            ruler.style.visibility = "hidden";
            ruler.style.position = "absolute";
            ruler.style.whiteSpace = "nowrap";
            ruler.style.font = getComputedStyle(cell).font;
            ruler.textContent = cell.textContent;
            document.body.appendChild(ruler);
            const w = Math.ceil(ruler.getBoundingClientRect().width);
            document.body.removeChild(ruler);

            const cs = getComputedStyle(cell);
            const padH =
              parseFloat(cs.paddingLeft) + parseFloat(cs.paddingRight);
            const natural = Math.ceil(w + padH) + 2;
            const perCol = Math.ceil(natural / span);
            if (perCol > maxWidth) maxWidth = perCol;
            break;
          }
          col += span;
        }
      }

      const headerCells = th.closest("thead")?.querySelectorAll("th");
      if (headerCells) {
        for (const hc of headerCells) {
          const hRow = hc.parentElement as HTMLTableRowElement;
          let hCol = 0;
          for (const cell of hRow.cells) {
            if (cell === hc) break;
            hCol += cell.colSpan || 1;
          }
          const hSpan = hc.colSpan || 1;
          if (hCol <= physicalCol && physicalCol < hCol + hSpan) {
            const inner =
              hc.querySelector<HTMLElement>("[class*='headerCellInner']") ||
              hc.querySelector<HTMLElement>("[class*='valueLabel']") ||
              hc;
            const ruler = document.createElement("span");
            ruler.style.visibility = "hidden";
            ruler.style.position = "absolute";
            ruler.style.whiteSpace = "nowrap";
            ruler.style.font = getComputedStyle(inner).font;
            ruler.textContent = inner.textContent;
            document.body.appendChild(ruler);
            const w = Math.ceil(ruler.getBoundingClientRect().width);
            document.body.removeChild(ruler);

            const cs = getComputedStyle(hc);
            const padH =
              parseFloat(cs.paddingLeft) + parseFloat(cs.paddingRight);
            const natural = Math.ceil(w + padH) + 18;
            const perCol = Math.ceil(natural / hSpan);
            if (perCol > maxWidth) maxWidth = perCol;
          }
        }
      }

      if (typeof slotIndex === "string") {
        setValFieldWidthMap((prev) => {
          const next = new Map(prev);
          next.set(slotIndex, maxWidth);
          return next;
        });
      } else {
        setColumnWidthMap((prev) => {
          const next = new Map(prev);
          next.set(slotIndex, maxWidth);
          return next;
        });
      }
    },
    [],
  );

  const handleResizeMouseDown = useCallback(
    (slotIndex: number | string, e: React.MouseEvent<HTMLDivElement>) => {
      if (e.detail >= 2) return;
      e.preventDefault();
      e.stopPropagation();
      const el = (e.target as HTMLElement).closest("th");
      let startWidth = el ? el.offsetWidth : columnWidth;
      const existingWidth =
        typeof slotIndex === "string"
          ? valFieldWidthMap.get(slotIndex)
          : columnWidthMap.get(slotIndex);
      if (existingWidth != null) {
        startWidth = existingWidth;
      }
      resizeDragRef.current = { key: slotIndex, startX: e.clientX, startWidth };
      setIsResizing(true);

      const onMouseMove = (ev: globalThis.MouseEvent) => {
        const drag = resizeDragRef.current;
        if (!drag) return;
        ev.preventDefault();
        const delta = ev.clientX - drag.startX;
        const newWidth = Math.max(MIN_COL_WIDTH, drag.startWidth + delta);
        if (typeof drag.key === "string") {
          setValFieldWidthMap((prev) => {
            const next = new Map(prev);
            next.set(drag.key as string, newWidth);
            return next;
          });
        } else {
          setColumnWidthMap((prev) => {
            const next = new Map(prev);
            next.set(drag.key as number, newWidth);
            return next;
          });
        }
      };

      const cleanup = () => {
        resizeDragRef.current = null;
        setIsResizing(false);
        window.removeEventListener("mousemove", onMouseMove);
        window.removeEventListener("mouseup", cleanup);
        window.removeEventListener("mouseleave", cleanup);
      };

      window.addEventListener("mousemove", onMouseMove);
      window.addEventListener("mouseup", cleanup);
      window.addEventListener("mouseleave", cleanup);
    },
    [columnWidth, columnWidthMap, valFieldWidthMap],
  );

  useEffect(() => {
    if (!scrollable || !wrapperRef.current) {
      setMeasuredHeight(containerHeight);
      return;
    }
    const el = wrapperRef.current;
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const h = Math.round(entry.contentRect.height);
        if (h > 0) setMeasuredHeight(h);
      }
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, [scrollable, containerHeight]);
  const allRowKeys = pivotData.getRowKeys();
  const allColKeys = pivotData.getColKeys();
  const colKeys =
    maxColumns != null ? allColKeys.slice(0, maxColumns) : allColKeys;
  const renderedValueFields = getRenderedValueFields(config);
  const hasMultipleValues = renderedValueFields.length > 1;
  const numRowDims = Math.max(config.rows.length, 1);
  const numColDims = config.columns.length;
  const columnTypes = pivotData.getColumnTypes();

  const colSlots = useMemo(
    () => computeColSlots(colKeys, config.collapsed_col_groups, numColDims),
    [colKeys, config.collapsed_col_groups, numColDims],
  );
  const temporalInfos = useMemo(
    () => computeTemporalColInfos(config, columnTypes, adaptiveDateGrains),
    [config, columnTypes, adaptiveDateGrains],
  );
  const headerLevels = useMemo(
    () => computeHeaderLevels(config, temporalInfos),
    [config, temporalInfos],
  );
  const effectiveColSlots: ColSlot[] = useMemo(
    () => computeTemporalColSlots(colSlots, temporalInfos, config),
    [colSlots, temporalInfos, config],
  );
  const rowTemporalInfos = useMemo(
    () => computeTemporalRowInfos(config, columnTypes, adaptiveDateGrains),
    [config, columnTypes, adaptiveDateGrains],
  );
  const rowHeaderLevels = useMemo(
    () => computeRowHeaderLevels(config, rowTemporalInfos),
    [config, rowTemporalInfos],
  );
  const effectiveNumRowDims =
    config.row_layout === "hierarchy"
      ? 1
      : config.rows.length === 0
        ? 1
        : rowTemporalInfos.length > 0
          ? computeNumRowHeaderLevels(config, rowTemporalInfos)
          : numRowDims;
  const totalDataColumns = effectiveColSlots.length;

  const useSubtotals =
    config.rows.length >= 2 &&
    (config.row_layout === "hierarchy" || !!config.show_subtotals);
  const collapsedSet = useMemo(() => {
    const raw = config.collapsed_groups ?? [];
    if (raw.includes("__ALL__")) {
      const level0 = [
        ...new Set(allRowKeys.map((k) => makeKeyString(k.slice(0, 1)))),
      ];
      const result = new Set(raw);
      result.delete("__ALL__");
      for (const p of level0) result.add(p);
      return result;
    }
    return new Set(raw);
  }, [config.collapsed_groups, allRowKeys]);

  const [measuredHeaderHeight, setMeasuredHeaderHeight] = useState<
    number | null
  >(null);
  const [headerRowOffsets, setHeaderRowOffsets] = useState<number[]>([]);

  const theadObserverRef = useRef<ResizeObserver | null>(null);
  const theadCallbackRef = useCallback(
    (node: HTMLTableSectionElement | null) => {
      theadObserverRef.current?.disconnect();
      if (!node || typeof ResizeObserver === "undefined") return;
      const measure = () => {
        const rows = node.querySelectorAll("tr");
        const offsets: number[] = [0];
        let cumulative = 0;
        rows.forEach((row) => {
          cumulative += row.getBoundingClientRect().height;
          offsets.push(Math.round(cumulative));
        });
        setHeaderRowOffsets(offsets);
        setMeasuredHeaderHeight(Math.round(cumulative));
      };
      const obs = new ResizeObserver(measure);
      obs.observe(node);
      theadObserverRef.current = obs;
      measure();
    },
    [],
  );

  const computedHeaderHeight = useMemo(() => {
    const colLevels = Math.max(config.columns.length, 1);
    const hasValueRow = renderedValueFields.length > 1;
    return (
      colLevels * HEADER_ROW_HEIGHT + (hasValueRow ? HEADER_ROW_HEIGHT : 0)
    );
  }, [config.columns.length, renderedValueFields.length]);
  const effectiveHeaderHeight =
    measuredHeaderHeight ??
    (headerHeight !== DEFAULT_HEADER_HEIGHT
      ? headerHeight
      : computedHeaderHeight);

  const groupedRows: GroupedRow[] | null = useMemo(
    () =>
      useSubtotals
        ? config.row_layout === "hierarchy"
          ? pivotData.getHierarchyRowKeys()
          : pivotData.getGroupedRowKeys()
        : null,
    [useSubtotals, pivotData, config.row_layout],
  );

  const visibleRowEntries = useMemo(() => {
    if (rowTemporalInfos.length === 0) return null;
    const baseEntries: GroupedRow[] = groupedRows
      ? groupedRows
      : allRowKeys.map((key) => ({
          type: "data" as const,
          key,
          level: config.rows.length - 1,
        }));
    return applyTemporalRowCollapse(baseEntries, rowTemporalInfos, config);
  }, [rowTemporalInfos, groupedRows, allRowKeys, config]);

  const projectedRowEntries = useMemo(() => {
    if (!visibleRowEntries) return null;
    return projectVisibleRowEntries(
      visibleRowEntries,
      config,
      rowHeaderLevels,
      rowTemporalInfos,
    );
  }, [visibleRowEntries, config, rowHeaderLevels, rowTemporalInfos]);

  const handleToggleGroup = useCallback(
    (groupKeyStr: string) => {
      if (!onCollapseChange) return;
      const collapsed = new Set(config.collapsed_groups ?? []);
      if (collapsed.has(groupKeyStr)) {
        collapsed.delete(groupKeyStr);
      } else {
        collapsed.add(groupKeyStr);
      }
      onCollapseChange("row", [...collapsed].sort());
    },
    [config, onCollapseChange],
  );

  const handleToggleColGroup = useCallback(
    (groupKeyStr: string) => {
      if (!onCollapseChange) return;
      const collapsed = new Set(config.collapsed_col_groups ?? []);
      if (collapsed.has(groupKeyStr)) {
        collapsed.delete(groupKeyStr);
      } else {
        collapsed.add(groupKeyStr);
      }
      onCollapseChange("col", [...collapsed].sort());
    },
    [config, onCollapseChange],
  );
  const handleTemporalToggle = useCallback(
    (field: string, collapseKey: string) => {
      if (!onConfigChange) return;
      const updated = toggleTemporalCollapse(
        config.collapsed_temporal_groups,
        field,
        collapseKey,
      );
      onConfigChange({
        ...config,
        collapsed_temporal_groups:
          Object.keys(updated).length > 0 ? updated : undefined,
      });
    },
    [config, onConfigChange],
  );
  const handleTemporalRowToggle = useCallback(
    (field: string, collapseKey: string) => {
      if (!onConfigChange) return;
      const updated = toggleTemporalRowCollapse(
        config.collapsed_temporal_row_groups,
        field,
        collapseKey,
      );
      onConfigChange({
        ...config,
        collapsed_temporal_row_groups:
          Object.keys(updated).length > 0 ? updated : undefined,
      });
    },
    [config, onConfigChange],
  );

  const {
    menuTarget,
    handleOpenMenu,
    handleCloseMenu,
    handleMenuSortChange,
    handleMenuFilterChange,
    hasHeaderMenu,
    menuPosition,
    menuUniqueValues,
    menuSortConfig,
    menuShowFilter,
    menuShowValuesAs,
    menuOnShowValuesAsChange,
    menuOnSubtotalToggle,
    menuOnTotalToggle,
    menuConfig,
    menuTitle,
    menuDateGrain,
    menuOnDateGrainChange,
    menuOnDateDrill,
    menuSupportsPeriodComparison,
    menuFormatLabel,
    handleCellKeyDown,
  } = useHeaderMenu({
    config,
    pivotData,
    onSortChange,
    onFilterChange,
    onCellClick,
    onShowValuesAsChange,
    onConfigChange,
    adaptiveDateGrains,
  });

  // Close the header menu on any scroll so the popover does not float
  // detached from its anchor cell. The scroll event does not bubble, so we
  // listen in the capture phase at the document root to catch scroll on the
  // wrapper, any descendant overflow container, or the window itself.
  //
  // A 150 ms guard delay prevents stale scroll events that were queued
  // synchronously during the click-to-open sequence (e.g. scrollIntoView
  // followed by click) from immediately re-closing the menu before the user
  // has had a chance to interact with it.
  useEffect(() => {
    if (!menuTarget) return;
    let listener: (() => void) | null = null;
    const timerId = setTimeout(() => {
      const onScroll = () => handleCloseMenu();
      listener = onScroll;
      document.addEventListener("scroll", onScroll, {
        passive: true,
        capture: true,
      });
    }, 150);
    return () => {
      clearTimeout(timerId);
      if (listener)
        document.removeEventListener("scroll", listener, { capture: true });
    };
  }, [menuTarget, handleCloseMenu]);

  const totalVirtualRows = projectedRowEntries
    ? projectedRowEntries.length
    : groupedRows
      ? groupedRows.length
      : allRowKeys.length;
  const numGroupingDims = useSubtotals ? config.rows.length - 1 : 0;

  const grpContext = useMemo(
    () =>
      useSubtotals
        ? {
            onToggleGroup: onCollapseChange ? handleToggleGroup : undefined,
            collapsedSet,
            subtotalsEnabled: true,
            numGroupingDims,
          }
        : undefined,
    [
      useSubtotals,
      onCollapseChange,
      handleToggleGroup,
      collapsedSet,
      numGroupingDims,
    ],
  );

  const groupBoundaryMap = useMemo(() => {
    const sourceRows = projectedRowEntries ?? groupedRows;
    if (!sourceRows || numGroupingDims === 0) return null;
    const map = new Map<number, number>();
    let prevDataKey: string[] | null = null;
    for (let i = 0; i < sourceRows.length; i++) {
      const entry = sourceRows[i]!;
      if (entry.type === "subtotal") {
        continue;
      }
      if (prevDataKey) {
        for (let d = 0; d < numGroupingDims; d++) {
          if (entry.key[d] !== prevDataKey[d]) {
            map.set(i, d);
            break;
          }
        }
      }
      prevDataKey = entry.key;
    }
    return map;
  }, [projectedRowEntries, groupedRows, numGroupingDims]);

  const groupEvenMap = useMemo(() => {
    const sourceRows = projectedRowEntries ?? groupedRows;
    if (!sourceRows) return null;
    const map = new Map<number, boolean>();
    let groupDataIdx = 0;
    for (let i = 0; i < sourceRows.length; i++) {
      const entry = sourceRows[i]!;
      if (entry.type === "subtotal") {
        groupDataIdx = 0;
        continue;
      }
      const boundary = groupBoundaryMap?.get(i);
      if (boundary === 0) groupDataIdx = 0;
      map.set(i, groupDataIdx % 2 === 1);
      groupDataIdx++;
    }
    return map;
  }, [projectedRowEntries, groupedRows, groupBoundaryMap]);

  const renderRow = useCallback(
    (rowIndex: number, visibleColRange: [number, number]): ReactElement => {
      if (projectedRowEntries) {
        const entry = projectedRowEntries[rowIndex]!;
        if (entry.type === "subtotal") {
          return renderSubtotalRow(
            entry.key,
            entry.level,
            effectiveColSlots,
            pivotData,
            config,
            hasMultipleValues,
            collapsedSet.has(makeKeyString(entry.key)),
            onCollapseChange ? handleToggleGroup : undefined,
            visibleColRange,
            onCellClick,
            onCellClick ? handleCellKeyDown : undefined,
            undefined,
            {
              projectedEntry: entry,
              rowHeaderLevels,
              onTemporalToggle: handleTemporalRowToggle,
            },
          );
        }
        if (entry.type === "temporal_parent") {
          return renderTemporalParentRow(
            entry,
            effectiveColSlots,
            pivotData,
            config,
            hasMultipleValues,
            rowHeaderLevels,
            handleTemporalRowToggle,
            visibleColRange,
          );
        }
        return renderDataRow(
          entry.key,
          effectiveColSlots,
          pivotData,
          config,
          hasMultipleValues,
          onCellClick,
          onCellClick ? handleCellKeyDown : undefined,
          visibleColRange,
          undefined,
          groupBoundaryMap?.get(rowIndex),
          grpContext,
          groupEvenMap?.get(rowIndex) ?? false,
          {
            projectedEntry: entry,
            rowHeaderLevels,
            onTemporalToggle: handleTemporalRowToggle,
          },
        );
      }
      if (groupedRows) {
        const entry = groupedRows[rowIndex];
        if (entry.type === "subtotal") {
          return renderSubtotalRow(
            entry.key,
            entry.level,
            effectiveColSlots,
            pivotData,
            config,
            hasMultipleValues,
            collapsedSet.has(makeKeyString(entry.key)),
            onCollapseChange ? handleToggleGroup : undefined,
            visibleColRange,
            onCellClick,
            onCellClick ? handleCellKeyDown : undefined,
          );
        }
        return renderDataRow(
          entry.key,
          effectiveColSlots,
          pivotData,
          config,
          hasMultipleValues,
          onCellClick,
          onCellClick ? handleCellKeyDown : undefined,
          visibleColRange,
          undefined,
          groupBoundaryMap?.get(rowIndex),
          grpContext,
          groupEvenMap?.get(rowIndex) ?? false,
        );
      }
      const rowKey = allRowKeys[rowIndex];
      return renderDataRow(
        rowKey,
        effectiveColSlots,
        pivotData,
        config,
        hasMultipleValues,
        onCellClick,
        onCellClick ? handleCellKeyDown : undefined,
        visibleColRange,
        undefined,
        undefined,
        undefined,
        rowIndex % 2 === 1,
      );
    },
    [
      allRowKeys,
      projectedRowEntries,
      groupedRows,
      effectiveColSlots,
      pivotData,
      config,
      hasMultipleValues,
      onCellClick,
      handleCellKeyDown,
      collapsedSet,
      onConfigChange,
      handleToggleGroup,
      groupBoundaryMap,
      groupEvenMap,
      grpContext,
      rowHeaderLevels,
      handleTemporalRowToggle,
    ],
  );

  const renderHeader = useCallback(
    (visibleColRange: [number, number]): ReactElement[] => {
      return renderColumnHeaders(
        effectiveColSlots,
        config,
        effectiveNumRowDims,
        hasMultipleValues,
        visibleColRange,
        hasHeaderMenu ? handleOpenMenu : undefined,
        menuTarget?.dimension,
        numColDims >= 2 && onCollapseChange ? handleToggleColGroup : undefined,
        pivotData,
        onCollapseChange,
        onConfigChange,
        handleResizeMouseDown,
        columnWidthMap,
        headerRowOffsets.length > 1 ? headerRowOffsets : undefined,
        handleResizeDoubleClick,
        adaptiveDateGrains,
        temporalInfos.length > 0 ? temporalInfos : undefined,
        temporalInfos.length > 0 ? headerLevels : undefined,
        handleTemporalToggle,
        columnTypes,
        rowTemporalInfos.length > 0 ? rowHeaderLevels : undefined,
        rowTemporalInfos.length > 0 ? rowTemporalInfos : undefined,
        valFieldWidthMap,
      );
    },
    [
      effectiveColSlots,
      config,
      effectiveNumRowDims,
      numColDims,
      hasMultipleValues,
      hasHeaderMenu,
      handleOpenMenu,
      menuTarget?.dimension,
      onCollapseChange,
      handleToggleColGroup,
      pivotData,
      handleResizeMouseDown,
      handleResizeDoubleClick,
      headerRowOffsets,
      adaptiveDateGrains,
      temporalInfos,
      headerLevels,
      handleTemporalToggle,
      columnTypes,
      rowTemporalInfos,
      rowHeaderLevels,
      valFieldWidthMap,
    ],
  );

  const renderTotals = useCallback(
    (visibleColRange: [number, number]): ReactElement | null => {
      if (!showColumnTotals(config)) return null;
      return renderTotalsRowFn(
        effectiveColSlots,
        pivotData,
        config,
        effectiveNumRowDims,
        hasMultipleValues,
        visibleColRange,
        onCellClick,
        onCellClick ? handleCellKeyDown : undefined,
      );
    },
    [
      effectiveColSlots,
      pivotData,
      config,
      effectiveNumRowDims,
      hasMultipleValues,
      onCellClick,
      handleCellKeyDown,
    ],
  );

  const dataColWidth = hasMultipleValues
    ? columnWidth * renderedValueFields.length
    : columnWidth;

  // Per-slot configured width from `column_config.field_widths`. Each data
  // slot represents one column-key position in the column-header grid. In
  // single-value mode the slot width is the single measure's configured
  // width; in multi-value mode a slot contains every measure, so the width
  // is the sum of each measure's configured width (falling back to the
  // default per-measure `columnWidth` when no entry is configured). This
  // keeps the body column widths in sync with the per-measure header cells
  // rendered by the shared `renderColumnHeaders` helper.
  const configuredSlotWidth = useMemo(() => {
    if (!config.field_widths) return undefined;
    if (hasMultipleValues) {
      let total = 0;
      let anyConfigured = false;
      for (const f of renderedValueFields) {
        const w = resolveFieldWidth(config, f);
        if (w != null) anyConfigured = true;
        total += w ?? columnWidth;
      }
      return anyConfigured ? total : undefined;
    }
    const singleField = renderedValueFields[0];
    return resolveFieldWidth(config, singleField);
  }, [config, hasMultipleValues, renderedValueFields, columnWidth]);

  // Merge runtime resize with configured widths.
  // Precedence per slot:
  //   1. Per-field `valFieldWidthMap` sum (dragging a value-label handle in
  //      multi-value mode — keeps body columns aligned with headers).
  //   2. Slot-level `columnWidthMap` (dragging a column-slot header handle).
  //   3. Static `configuredSlotWidth` from `column_config.field_widths`.
  //   4. Uniform `dataColWidth` fallback.
  // The array is only materialized when at least one non-default width exists;
  // otherwise `undefined` lets `VirtualScroll` use the uniform fast path.
  const variableColumnWidths = useMemo(() => {
    const hasValFieldWidths = valFieldWidthMap.size > 0;
    if (
      columnWidthMap.size === 0 &&
      !hasValFieldWidths &&
      configuredSlotWidth == null
    ) {
      return undefined;
    }
    return Array.from({ length: totalDataColumns }, (_, i) => {
      // In multi-value mode, if any per-field drag has occurred for this slot,
      // sum the individual field widths so the body stays in sync with the
      // value-label header cells.
      if (hasMultipleValues && hasValFieldWidths) {
        let total = 0;
        let anyDragged = false;
        for (let vfi = 0; vfi < renderedValueFields.length; vfi++) {
          const perFieldWidth = valFieldWidthMap.get(`${i}-${vfi}`);
          if (perFieldWidth != null) {
            anyDragged = true;
            total += perFieldWidth;
          } else {
            total +=
              resolveFieldWidth(config, renderedValueFields[vfi]) ??
              columnWidth;
          }
        }
        if (anyDragged) return total;
      }
      return (
        resolveEffectiveWidth(columnWidthMap.get(i), configuredSlotWidth) ??
        dataColWidth
      );
    });
  }, [
    columnWidthMap,
    valFieldWidthMap,
    totalDataColumns,
    dataColWidth,
    configuredSlotWidth,
    hasMultipleValues,
    renderedValueFields,
    config,
    columnWidth,
  ]);

  return (
    <>
      {isResizing && (
        <div
          data-testid="resize-overlay"
          style={{
            position: "fixed",
            inset: 0,
            zIndex: 9999,
            cursor: "col-resize",
            opacity: 0,
          }}
        />
      )}
      <div
        ref={wrapperRef}
        className={[
          tableStyles.tableWrapper,
          densityClass(config.style, tableStyles),
          bordersClass(config.style, tableStyles),
          stripesOffClass(config.style, tableStyles),
          hoverOffClass(config.style, tableStyles),
        ]
          .filter(Boolean)
          .join(" ")}
        style={{
          ...styleToCSS(config.style),
          ...(scrollable ? { flex: "1 1 0", minHeight: 0 } : undefined),
          ...(isResizing ? { userSelect: "none" } : undefined),
        }}
      >
        <VirtualScroll
          totalRows={totalVirtualRows}
          totalColumns={totalDataColumns}
          rowHeight={effectiveRowHeight}
          columnWidth={dataColWidth}
          tableClassName={tableStyles.pivotTable}
          columnWidths={variableColumnWidths}
          containerHeight={measuredHeight}
          renderRow={renderRow}
          renderHeader={renderHeader}
          renderTotalsRow={renderTotals}
          headerHeight={effectiveHeaderHeight}
          theadRef={theadCallbackRef}
        />

        {menuTarget && menuPosition && (
          <div
            className={tableStyles.headerMenuOverlay}
            style={{ top: menuPosition.top, left: menuPosition.left }}
          >
            <HeaderMenu
              dimension={menuTarget.dimension}
              title={menuTitle}
              axis={menuTarget.axis === "value" ? "col" : menuTarget.axis}
              sortConfig={menuSortConfig}
              onSortChange={handleMenuSortChange}
              filter={config.filters?.[menuTarget.dimension]}
              uniqueValues={menuUniqueValues}
              onFilterChange={handleMenuFilterChange}
              valueFields={renderedValueFields}
              colKeys={
                menuTarget.axis === "row" ? pivotData.getColKeys() : undefined
              }
              menuLimit={menuLimit}
              showFilter={menuShowFilter}
              showValuesAs={menuShowValuesAs}
              onShowValuesAsChange={
                menuTarget?.axis === "value" &&
                isSyntheticMeasure(config, menuTarget.dimension)
                  ? undefined
                  : menuOnShowValuesAsChange
              }
              config={menuConfig}
              onSubtotalToggle={menuOnSubtotalToggle}
              onTotalToggle={menuOnTotalToggle}
              formatLabel={menuFormatLabel}
              dateGrain={menuDateGrain}
              onDateGrainChange={menuOnDateGrainChange}
              onDateDrill={menuOnDateDrill}
              supportsPeriodComparison={menuSupportsPeriodComparison}
              onClose={handleCloseMenu}
            />
          </div>
        )}
      </div>
    </>
  );
};

export default VirtualizedTableRenderer;
