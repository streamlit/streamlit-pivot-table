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

import { KeyboardEvent, useCallback, useRef, useState } from "react";
import type { PivotData } from "../engine/PivotData";
import type {
  CellClickPayload,
  DimensionFilter,
  PivotConfigV1,
  ShowValuesAs,
  SortConfig,
} from "../engine/types";
import { normalizeToggleList } from "../engine/types";
import {
  buildCellClickPayload,
  type HeaderMenuTarget,
  type MenuAxis,
} from "./TableRenderer";

const NOOP_FILTER = (
  _field: string,
  _filter: DimensionFilter | undefined,
): void => {};

export interface UseHeaderMenuOptions {
  config: PivotConfigV1;
  pivotData: PivotData;
  onSortChange?: (axis: "row" | "col", sort: SortConfig | undefined) => void;
  onFilterChange?: (field: string, filter: DimensionFilter | undefined) => void;
  onCellClick?: (payload: CellClickPayload) => void;
  onShowValuesAsChange?: (field: string, mode: ShowValuesAs) => void;
  onConfigChange?: (config: PivotConfigV1) => void;
}

export interface UseHeaderMenuResult {
  menuTarget: HeaderMenuTarget | null;
  handleOpenMenu: (
    dimension: string,
    axis: MenuAxis,
    rect: DOMRect,
    triggerEl: HTMLElement,
  ) => void;
  handleCloseMenu: () => void;
  /** Sort change handler; undefined when sort is disabled (locked mode). */
  handleMenuSortChange: ((sort: SortConfig | undefined) => void) | undefined;
  handleMenuFilterChange: (
    field: string,
    filter: DimensionFilter | undefined,
  ) => void;
  hasHeaderMenu: boolean;
  menuPosition: { top: number; left: number } | undefined;
  menuUniqueValues: string[];
  menuSortConfig: SortConfig | undefined;
  /** Whether to show the filter section (false for value-field menus). */
  menuShowFilter: boolean;
  /** Current ShowValuesAs mode for the open menu's field (value menus only). */
  menuShowValuesAs: ShowValuesAs | undefined;
  /** Callback to change ShowValuesAs for the open menu's field (value menus only). */
  menuOnShowValuesAsChange: ((mode: ShowValuesAs) => void) | undefined;
  menuOnSubtotalToggle: ((dimension: string) => void) | undefined;
  menuOnTotalToggle:
    | ((measure: string, axis: "row" | "col") => void)
    | undefined;
  menuConfig: PivotConfigV1 | undefined;
  handleCellKeyDown: (
    e: KeyboardEvent,
    rowKey: readonly string[],
    colKey: readonly string[],
    value: number | null,
    valueField: string,
  ) => void;
}

export function useHeaderMenu({
  config,
  pivotData,
  onSortChange,
  onFilterChange,
  onCellClick,
  onShowValuesAsChange,
  onConfigChange,
}: UseHeaderMenuOptions): UseHeaderMenuResult {
  const [menuTarget, setMenuTarget] = useState<HeaderMenuTarget | null>(null);
  const menuTargetRef = useRef<HeaderMenuTarget | null>(null);
  const triggerRef = useRef<HTMLElement | null>(null);

  const handleOpenMenu = useCallback(
    (
      dimension: string,
      axis: MenuAxis,
      rect: DOMRect,
      triggerEl: HTMLElement,
    ) => {
      triggerRef.current = triggerEl;
      setMenuTarget((prev) => {
        if (prev && prev.dimension === dimension && prev.axis === axis) {
          menuTargetRef.current = null;
          requestAnimationFrame(() => triggerEl.focus());
          return null;
        }
        const next = { dimension, axis, rect };
        menuTargetRef.current = next;
        return next;
      });
    },
    [],
  );

  const handleCloseMenu = useCallback(() => {
    menuTargetRef.current = null;
    setMenuTarget(null);
    requestAnimationFrame(() => {
      triggerRef.current?.focus();
    });
  }, []);

  const applySortChange = useCallback(
    (sort: SortConfig | undefined) => {
      const target = menuTargetRef.current;
      if (target && onSortChange && target.axis !== "value") {
        onSortChange(
          target.axis,
          sort ? { ...sort, dimension: target.dimension } : undefined,
        );
      }
    },
    [onSortChange],
  );

  const isValueMenu = menuTarget?.axis === "value";

  const handleMenuSortChange =
    onSortChange && !isValueMenu ? applySortChange : undefined;
  const handleMenuFilterChange = onFilterChange ?? NOOP_FILTER;

  const hasHeaderMenu = !!(
    onSortChange ||
    onFilterChange ||
    onShowValuesAsChange ||
    onConfigChange
  );

  const menuPosition = menuTarget
    ? { top: menuTarget.rect.bottom, left: menuTarget.rect.left }
    : undefined;

  const menuUniqueValues =
    menuTarget && !isValueMenu
      ? pivotData.getUniqueValues(menuTarget.dimension)
      : [];

  const axisSort =
    menuTarget && !isValueMenu
      ? menuTarget.axis === "row"
        ? config.row_sort
        : config.col_sort
      : undefined;
  const menuSortConfig =
    axisSort?.dimension === menuTarget?.dimension ? axisSort : undefined;

  const menuShowFilter = !isValueMenu;

  const menuShowValuesAs: ShowValuesAs | undefined =
    isValueMenu && menuTarget
      ? (config.show_values_as?.[menuTarget.dimension] ?? "raw")
      : undefined;

  const menuOnShowValuesAsChange = useCallback(
    (mode: ShowValuesAs) => {
      const target = menuTargetRef.current;
      if (target && onShowValuesAsChange) {
        onShowValuesAsChange(target.dimension, mode);
      }
    },
    [onShowValuesAsChange],
  );

  const handleSubtotalToggle = useCallback(
    (dimension: string) => {
      if (!onConfigChange) return;
      const cfg = config;
      const eligibleDims = cfg.rows.slice(0, -1);
      const result = normalizeToggleList(
        cfg.show_subtotals ?? false,
        eligibleDims,
        dimension,
      );
      onConfigChange({ ...cfg, show_subtotals: result });
    },
    [config, onConfigChange],
  );

  const handleTotalToggle = useCallback(
    (measure: string, axis: "row" | "col") => {
      if (!onConfigChange) return;
      const cfg = config;
      const values = cfg.values;
      const field = axis === "row" ? "show_row_totals" : "show_column_totals";
      const current =
        axis === "row"
          ? (cfg.show_row_totals ?? cfg.show_totals)
          : (cfg.show_column_totals ?? cfg.show_totals);
      const result = normalizeToggleList(current, values, measure);
      onConfigChange({ ...cfg, [field]: result });
    },
    [config, onConfigChange],
  );

  const menuOnSubtotalToggle =
    onConfigChange &&
    menuTarget?.axis === "row" &&
    menuTarget.dimension !== config.rows[config.rows.length - 1] &&
    config.rows.length >= 2
      ? handleSubtotalToggle
      : undefined;

  const menuOnTotalToggle =
    onConfigChange && menuTarget?.axis === "value"
      ? handleTotalToggle
      : undefined;

  const handleCellKeyDown = useCallback(
    (
      e: KeyboardEvent,
      rowKey: readonly string[],
      colKey: readonly string[],
      value: number | null,
      valueField: string,
    ) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onCellClick?.(
          buildCellClickPayload(rowKey, colKey, value, config, valueField),
        );
      }
    },
    [onCellClick, config],
  );

  return {
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
    menuOnShowValuesAsChange:
      isValueMenu && onShowValuesAsChange
        ? menuOnShowValuesAsChange
        : undefined,
    menuOnSubtotalToggle,
    menuOnTotalToggle,
    menuConfig: menuTarget ? config : undefined,
    handleCellKeyDown,
  };
}
