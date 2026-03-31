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
  useRef,
  useState,
  useCallback,
  useEffect,
  useMemo,
} from "react";

export interface VirtualScrollProps {
  totalRows: number;
  totalColumns: number;
  rowHeight: number;
  columnWidth: number;
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
}

/**
 * Lightweight virtualizer for both rows and columns.
 * Renders only the visible subset of the grid plus an overscan buffer.
 * Measures its own container width to determine the column viewport.
 */
const VirtualScroll: FC<VirtualScrollProps> = ({
  totalRows,
  totalColumns,
  rowHeight,
  columnWidth,
  containerHeight,
  overscanRows = 5,
  overscanColumns = 3,
  renderRow,
  renderHeader,
  renderTotalsRow,
  headerHeight,
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
  const totalContentWidth = totalColumns * columnWidth;
  const viewportWidth = measuredWidth || totalContentWidth;

  const { startRow, endRow } = useMemo(() => {
    const start = Math.max(0, Math.floor(scrollTop / rowHeight) - overscanRows);
    const visibleCount = Math.ceil(bodyHeight / rowHeight);
    const end = Math.min(totalRows, start + visibleCount + overscanRows * 2);
    return { startRow: start, endRow: end };
  }, [scrollTop, rowHeight, bodyHeight, totalRows, overscanRows]);

  const { startCol, endCol } = useMemo(() => {
    const start = Math.max(
      0,
      Math.floor(scrollLeft / columnWidth) - overscanColumns,
    );
    const visibleCount = Math.ceil(viewportWidth / columnWidth);
    const end = Math.min(
      totalColumns,
      start + visibleCount + overscanColumns * 2,
    );
    return { startCol: start, endCol: end };
  }, [scrollLeft, columnWidth, viewportWidth, totalColumns, overscanColumns]);

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
          style={{
            position: "sticky",
            top: 0,
            zIndex: 2,
            width: "100%",
            borderCollapse: "separate",
            borderSpacing: 0,
          }}
        >
          <thead>{renderHeader(colRange)}</thead>
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
