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
  Ref,
  useRef,
  useState,
  useCallback,
  useEffect,
  useMemo,
} from "react";
import { COLUMN_VIRTUALIZATION_THRESHOLD } from "../engine/perf";

export interface VirtualScrollProps {
  totalRows: number;
  totalColumns: number;
  rowHeight: number;
  columnWidth: number;
  /** Per-column widths for variable-width mode. When provided, overrides
   *  the uniform `columnWidth` for positioning calculations. */
  columnWidths?: number[];
  containerHeight: number;
  overscanRows?: number;
  overscanColumns?: number;
  renderRow: (
    rowIndex: number,
    visibleColRange: [number, number],
  ) => ReactElement;
  renderHeader: (visibleColRange: [number, number]) => ReactElement[];
  renderTotalsRow?: (visibleColRange: [number, number]) => ReactElement | null;
  headerHeight: number;
  theadRef?: Ref<HTMLTableSectionElement>;
  /**
   * Optional CSS class applied to all three internal <table> elements.
   * Pass `tableStyles.pivotTable` so .pivotTable-scoped CSS rules (stripe,
   * hover, hierarchy hover, last-row border) fire correctly in virtualized mode.
   */
  tableClassName?: string;
}

/**
 * Lightweight virtualizer for both rows and columns.
 * Renders only the visible subset of the grid plus an overscan buffer.
 * Measures its own container width to determine the column viewport.
 */
/** Binary search: find the first offset index where offsets[i] > target. */
function upperBound(offsets: number[], target: number): number {
  let lo = 0;
  let hi = offsets.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (offsets[mid] <= target) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

const VirtualScroll: FC<VirtualScrollProps> = ({
  totalRows,
  totalColumns,
  rowHeight,
  columnWidth,
  columnWidths,
  containerHeight,
  overscanRows = 5,
  overscanColumns = 3,
  renderRow,
  renderHeader,
  renderTotalsRow,
  headerHeight,
  theadRef,
  tableClassName,
}): ReactElement => {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [scrollLeft, setScrollLeft] = useState(0);
  const [measuredWidth, setMeasuredWidth] = useState(0);

  const handleScroll = useCallback(() => {
    const el = scrollRef.current;
    if (el) {
      setScrollTop(el.scrollTop);
      setScrollLeft(el.scrollLeft);
    }
  }, []);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    el.addEventListener("scroll", handleScroll, { passive: true });
    return () => el.removeEventListener("scroll", handleScroll);
  }, [handleScroll]);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    setMeasuredWidth(el.clientWidth);
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setMeasuredWidth(entry.contentRect.width);
      }
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  const bodyHeight = containerHeight - headerHeight;
  const totalContentHeight = totalRows * rowHeight;

  const colOffsets = useMemo(() => {
    if (!columnWidths || columnWidths.length === 0) return null;
    const offsets = new Array(columnWidths.length + 1);
    offsets[0] = 0;
    for (let i = 0; i < columnWidths.length; i++) {
      offsets[i + 1] = offsets[i] + columnWidths[i];
    }
    return offsets as number[];
  }, [columnWidths]);

  const totalContentWidth = colOffsets
    ? (colOffsets[totalColumns] ?? 0)
    : totalColumns * columnWidth;
  const viewportWidth = measuredWidth || totalContentWidth;

  const effectiveOverscanColumns = useMemo(
    () =>
      totalColumns > COLUMN_VIRTUALIZATION_THRESHOLD
        ? Math.max(overscanColumns, 6)
        : overscanColumns,
    [totalColumns, overscanColumns],
  );

  const { startRow, endRow } = useMemo(() => {
    const start = Math.max(0, Math.floor(scrollTop / rowHeight) - overscanRows);
    const visibleCount = Math.ceil(bodyHeight / rowHeight);
    const end = Math.min(totalRows, start + visibleCount + overscanRows * 2);
    return { startRow: start, endRow: end };
  }, [scrollTop, rowHeight, bodyHeight, totalRows, overscanRows]);

  const { startCol, endCol } = useMemo(() => {
    if (colOffsets) {
      const rawStart = Math.max(0, upperBound(colOffsets, scrollLeft) - 1);
      const start = Math.max(0, rawStart - effectiveOverscanColumns);
      const rawEnd = upperBound(colOffsets, scrollLeft + viewportWidth);
      const end = Math.min(totalColumns, rawEnd + effectiveOverscanColumns);
      return { startCol: start, endCol: end };
    }
    const start = Math.max(
      0,
      Math.floor(scrollLeft / columnWidth) - effectiveOverscanColumns,
    );
    const visibleCount = Math.ceil(viewportWidth / columnWidth);
    const end = Math.min(
      totalColumns,
      start + visibleCount + effectiveOverscanColumns * 2,
    );
    return { startCol: start, endCol: end };
  }, [
    scrollLeft,
    columnWidth,
    colOffsets,
    viewportWidth,
    totalColumns,
    effectiveOverscanColumns,
  ]);

  const colRange: [number, number] = [startCol, endCol];

  return (
    <div
      ref={scrollRef}
      data-testid="virtual-scroll-container"
      style={{
        height: containerHeight,
        width: "100%",
        overflow: "auto",
        position: "relative",
      }}
    >
      <div
        style={{
          height: totalContentHeight + headerHeight,
          width: totalContentWidth,
          minWidth: "100%",
          position: "relative",
        }}
      >
        <table
          data-testid="pivot-table"
          role="grid"
          className={tableClassName}
          style={{
            position: "sticky",
            top: 0,
            zIndex: 2,
            width: "100%",
            borderCollapse: "separate",
            borderSpacing: 0,
          }}
        >
          <thead ref={theadRef}>{renderHeader(colRange)}</thead>
        </table>

        <div
          style={{
            position: "absolute",
            top: headerHeight + startRow * rowHeight,
            left: 0,
            right: 0,
          }}
        >
          <table
            role="grid"
            className={tableClassName}
            style={{
              width: "100%",
              borderCollapse: "separate",
              borderSpacing: 0,
            }}
          >
            <tbody>
              {Array.from({ length: endRow - startRow }, (_, i) =>
                renderRow(startRow + i, colRange),
              )}
            </tbody>
          </table>
        </div>

        {renderTotalsRow && (
          <div
            style={{
              position: "sticky",
              bottom: 0,
              zIndex: 2,
              backgroundColor: "var(--st-background-color)",
            }}
          >
            <table
              role="grid"
              className={tableClassName}
              style={{
                width: "100%",
                borderCollapse: "separate",
                borderSpacing: 0,
              }}
            >
              <tbody>{renderTotalsRow(colRange)}</tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default VirtualScroll;
