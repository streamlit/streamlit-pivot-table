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
  renderTotalsRow as renderTotalsRowFn,
  HEADER_ROW_HEIGHT,
  type ColSlot,
} from "./TableRenderer";
import { useHeaderMenu } from "./useHeaderMenu";
import tableStyles from "./TableRenderer.module.css";

export interface VirtualizedTableRendererProps {
  pivotData: PivotData;
  config: PivotConfigV1;
  onCellClick?: (payload: CellClickPayload) => void;
  maxColumns?: number;
  containerHeight: number;
  rowHeight?: number;
  columnWidth?: number;
  headerHeight?: number;
  onSortChange?: (axis: "row" | "col", sort: SortConfig | undefined) => void;
  onFilterChange?: (field: string, filter: DimensionFilter | undefined) => void;
  onConfigChange?: (config: PivotConfigV1) => void;
  onShowValuesAsChange?: (field: string, mode: ShowValuesAs) => void;
  menuLimit?: number;
  /** When true, the wrapper becomes a flex item that fills remaining space. */
  scrollable?: boolean;
}

const DEFAULT_ROW_HEIGHT = 36;
const DEFAULT_COL_WIDTH = 120;
const DEFAULT_HEADER_HEIGHT = 72;

const VirtualizedTableRenderer: FC<VirtualizedTableRendererProps> = ({
  pivotData,
  config,
  onCellClick,
  maxColumns,
  containerHeight,
  rowHeight = DEFAULT_ROW_HEIGHT,
  columnWidth = DEFAULT_COL_WIDTH,
  headerHeight = DEFAULT_HEADER_HEIGHT,
  onSortChange,
  onFilterChange,
  onConfigChange,
  onShowValuesAsChange,
  menuLimit,
  scrollable,
}): ReactElement => {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const [measuredHeight, setMeasuredHeight] = useState(containerHeight);

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

  const colSlots = useMemo(
    () => computeColSlots(colKeys, config.collapsed_col_groups, numColDims),
    [colKeys, config.collapsed_col_groups, numColDims],
  );
  const totalDataColumns = colSlots.length;

  const useSubtotals = !!config.show_subtotals && config.rows.length >= 2;
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

  const computedHeaderHeight = useMemo(() => {
    const colLevels = Math.max(config.columns.length, 1);
    const hasValueRow = renderedValueFields.length > 1;
    return (
      colLevels * HEADER_ROW_HEIGHT + (hasValueRow ? HEADER_ROW_HEIGHT : 0)
    );
  }, [config.columns.length, renderedValueFields.length]);
  const effectiveHeaderHeight =
    headerHeight !== DEFAULT_HEADER_HEIGHT
      ? headerHeight
      : computedHeaderHeight;

  const groupedRows: GroupedRow[] | null = useMemo(
    () => (useSubtotals ? pivotData.getGroupedRowKeys() : null),
    [useSubtotals, pivotData],
  );

  const handleToggleGroup = useCallback(
    (groupKeyStr: string) => {
      if (!onConfigChange) return;
      const collapsed = new Set(config.collapsed_groups ?? []);
      if (collapsed.has(groupKeyStr)) {
        collapsed.delete(groupKeyStr);
      } else {
        collapsed.add(groupKeyStr);
      }
      onConfigChange({ ...config, collapsed_groups: [...collapsed] });
    },
    [config, onConfigChange],
  );

  const handleToggleColGroup = useCallback(
    (groupKeyStr: string) => {
      if (!onConfigChange) return;
      const collapsed = new Set(config.collapsed_col_groups ?? []);
      if (collapsed.has(groupKeyStr)) {
        collapsed.delete(groupKeyStr);
      } else {
        collapsed.add(groupKeyStr);
      }
      onConfigChange({ ...config, collapsed_col_groups: [...collapsed] });
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
    handleCellKeyDown,
  } = useHeaderMenu({
    config,
    pivotData,
    onSortChange,
    onFilterChange,
    onCellClick,
    onShowValuesAsChange,
    onConfigChange,
  });

  const totalVirtualRows = groupedRows ? groupedRows.length : allRowKeys.length;
  const numGroupingDims = useSubtotals ? config.rows.length - 1 : 0;

  const grpContext = useMemo(
    () =>
      useSubtotals
        ? {
            onToggleGroup: onConfigChange ? handleToggleGroup : undefined,
            collapsedSet,
            subtotalsEnabled: true,
            numGroupingDims,
          }
        : undefined,
    [
      useSubtotals,
      onConfigChange,
      handleToggleGroup,
      collapsedSet,
      numGroupingDims,
    ],
  );

  const groupBoundaryMap = useMemo(() => {
    if (!groupedRows || numGroupingDims === 0) return null;
    const map = new Map<number, number>();
    let prevDataKey: string[] | null = null;
    for (let i = 0; i < groupedRows.length; i++) {
      const entry = groupedRows[i];
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
  }, [groupedRows, numGroupingDims]);

  const renderRow = useCallback(
    (rowIndex: number, visibleColRange: [number, number]): ReactElement => {
      if (groupedRows) {
        const entry = groupedRows[rowIndex];
        if (entry.type === "subtotal") {
          return renderSubtotalRow(
            entry.key,
            entry.level,
            colSlots,
            pivotData,
            config,
            hasMultipleValues,
            collapsedSet.has(makeKeyString(entry.key)),
            onConfigChange ? handleToggleGroup : undefined,
            visibleColRange,
            onCellClick,
            onCellClick ? handleCellKeyDown : undefined,
          );
        }
        return renderDataRow(
          entry.key,
          colSlots,
          pivotData,
          config,
          hasMultipleValues,
          onCellClick,
          onCellClick ? handleCellKeyDown : undefined,
          visibleColRange,
          undefined,
          groupBoundaryMap?.get(rowIndex),
          grpContext,
        );
      }
      const rowKey = allRowKeys[rowIndex];
      return renderDataRow(
        rowKey,
        colSlots,
        pivotData,
        config,
        hasMultipleValues,
        onCellClick,
        onCellClick ? handleCellKeyDown : undefined,
        visibleColRange,
      );
    },
    [
      allRowKeys,
      groupedRows,
      colSlots,
      pivotData,
      config,
      hasMultipleValues,
      onCellClick,
      handleCellKeyDown,
      collapsedSet,
      onConfigChange,
      handleToggleGroup,
      groupBoundaryMap,
      grpContext,
    ],
  );

  const renderHeader = useCallback(
    (visibleColRange: [number, number]): ReactElement[] => {
      return renderColumnHeaders(
        colSlots,
        config,
        numRowDims,
        hasMultipleValues,
        visibleColRange,
        hasHeaderMenu ? handleOpenMenu : undefined,
        menuTarget?.dimension,
        numColDims >= 2 && onConfigChange ? handleToggleColGroup : undefined,
        pivotData,
        onConfigChange,
      );
    },
    [
      colSlots,
      config,
      numRowDims,
      numColDims,
      hasMultipleValues,
      hasHeaderMenu,
      handleOpenMenu,
      menuTarget?.dimension,
      onConfigChange,
      handleToggleColGroup,
      pivotData,
    ],
  );

  const renderTotals = useCallback(
    (visibleColRange: [number, number]): ReactElement | null => {
      if (!showColumnTotals(config)) return null;
      return renderTotalsRowFn(
        colSlots,
        pivotData,
        config,
        numRowDims,
        hasMultipleValues,
        visibleColRange,
        onCellClick,
        onCellClick ? handleCellKeyDown : undefined,
      );
    },
    [
      colSlots,
      pivotData,
      config,
      numRowDims,
      hasMultipleValues,
      onCellClick,
      handleCellKeyDown,
    ],
  );

  const dataColWidth = hasMultipleValues
    ? columnWidth * renderedValueFields.length
    : columnWidth;

  return (
    <div
      ref={wrapperRef}
      style={scrollable ? { flex: "1 1 0", minHeight: 0 } : undefined}
    >
      <VirtualScroll
        totalRows={totalVirtualRows}
        totalColumns={totalDataColumns}
        rowHeight={rowHeight}
        columnWidth={dataColWidth}
        containerHeight={measuredHeight}
        renderRow={renderRow}
        renderHeader={renderHeader}
        renderTotalsRow={renderTotals}
        headerHeight={effectiveHeaderHeight}
      />

      {menuTarget && menuPosition && (
        <div
          className={tableStyles.headerMenuOverlay}
          style={{ top: menuPosition.top, left: menuPosition.left }}
        >
          <HeaderMenu
            dimension={menuTarget.dimension}
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
            onClose={handleCloseMenu}
          />
        </div>
      )}
    </div>
  );
};

export default VirtualizedTableRenderer;
