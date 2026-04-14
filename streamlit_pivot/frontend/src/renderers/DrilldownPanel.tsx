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
import type { PivotData, DataRecord } from "../engine/PivotData";
import type {
  CellClickPayload,
  ColumnType,
  ColumnTypeMap,
} from "../engine/types";
import { measureSync, type PerfActionMeasurement } from "../engine/perf";
import {
  formatNumber,
  formatWithPattern,
  formatDateValue,
  formatDateTimeValue,
} from "../engine/formatters";
import styles from "./DrilldownPanel.module.css";

const RECORD_LIMIT = 500;
type DrilldownSortDirection = "asc" | "desc";

export interface DrilldownPanelProps {
  pivotData: PivotData;
  payload: CellClickPayload;
  onClose: () => void;
  onMeasured?: (action: PerfActionMeasurement) => void;
  sortColumn?: string;
  sortDirection?: DrilldownSortDirection;
  onSortChange?: (
    sortColumn: string | undefined,
    sortDirection: DrilldownSortDirection | undefined,
  ) => void;
  /** Pre-fetched rows from a hybrid-mode server round-trip. */
  serverRecords?: Record<string, unknown>[];
  /** Column names for server-provided rows, in display order. */
  serverColumns?: string[];
  /** Total matching row count before capping (server mode). */
  serverTotalCount?: number;
  /** True while waiting for the server round-trip to complete. */
  isLoading?: boolean;
  /** Current zero-based page index (server pagination). */
  serverPage?: number;
  /** Rows per page (server pagination). */
  serverPageSize?: number;
  /** Called when the user navigates to a different page. */
  onPageChange?: (page: number) => void;
  /** Per-field number format patterns from config (e.g. {"Revenue": "$,.0f"}). */
  numberFormat?: Record<string, string>;
  /** Per-field alignment overrides from config. */
  columnAlignment?: Record<string, string>;
  /** Merged column type map for date/datetime formatting in cells. */
  columnTypes?: ColumnTypeMap;
}

const CloseIcon: FC = () => (
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
    <line x1="18" y1="6" x2="6" y2="18" />
    <line x1="6" y1="6" x2="18" y2="18" />
  </svg>
);

const SortIndicator: FC<{ direction: DrilldownSortDirection }> = ({
  direction,
}) => (
  <svg
    width="8"
    height="8"
    viewBox="0 0 8 8"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.5"
    strokeLinecap="round"
    strokeLinejoin="round"
    className={styles.sortIndicator}
    aria-hidden="true"
    data-testid={`sort-indicator-${direction}`}
  >
    {direction === "asc" ? (
      <polyline points="1,5.5 4,2 7,5.5" />
    ) : (
      <polyline points="1,2.5 4,6 7,2.5" />
    )}
  </svg>
);

function formatDrilldownHeader(
  payload: CellClickPayload,
  pivotData?: PivotData,
): string {
  const parts: string[] = [];
  for (const [dim, val] of Object.entries(payload.filters)) {
    const label = pivotData ? pivotData.formatDimLabel(dim, val) : val;
    parts.push(`${dim}: ${label}`);
  }
  if (parts.length === 0) {
    const rowLabel = payload.rowKey.join(" > ");
    const colLabel = payload.colKey.join(" > ");
    if (rowLabel && colLabel) return `${rowLabel} × ${colLabel}`;
    return rowLabel || colLabel || "All records";
  }
  return parts.join(", ");
}

function formatCellValue(
  value: unknown,
  column: string,
  numberFormat?: Record<string, string>,
  columnTypes?: ColumnTypeMap,
): string {
  if (value === null || value === undefined) return "";
  const colType = columnTypes?.get(column);
  if (colType === "datetime") return formatDateTimeValue(value);
  if (colType === "date") return formatDateValue(value);
  if (typeof value === "number") {
    if (Number.isNaN(value)) return "";
    const pattern = numberFormat?.[column] ?? numberFormat?.["__all__"];
    if (pattern) return formatWithPattern(value, pattern);
    return formatNumber(value);
  }
  return String(value);
}

function getCellAlign(
  value: unknown,
  column: string,
  columnAlignment?: Record<string, string>,
): React.CSSProperties["textAlign"] | undefined {
  const explicit = columnAlignment?.[column];
  if (explicit) return explicit as React.CSSProperties["textAlign"];
  if (typeof value === "number" && !Number.isNaN(value)) return "right";
  return undefined;
}

export function isBlankSortValue(value: unknown): boolean {
  return (
    value === null ||
    value === undefined ||
    value === "" ||
    (typeof value === "number" && Number.isNaN(value))
  );
}

function parseTemporalSortValue(value: unknown): number | null {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const parsed = Date.parse(String(value));
  return Number.isNaN(parsed) ? null : parsed;
}

export function compareDrilldownValues(
  a: unknown,
  b: unknown,
  columnType?: ColumnType,
): number {
  if (isBlankSortValue(a) && isBlankSortValue(b)) return 0;
  if (isBlankSortValue(a)) return 1;
  if (isBlankSortValue(b)) return -1;

  if (columnType === "date" || columnType === "datetime") {
    const aTs = parseTemporalSortValue(a);
    const bTs = parseTemporalSortValue(b);
    if (aTs != null && bTs != null) return aTs - bTs;
  }

  if (columnType === "integer" || columnType === "float") {
    const aNum = Number(a);
    const bNum = Number(b);
    if (!Number.isNaN(aNum) && !Number.isNaN(bNum)) return aNum - bNum;
  }

  if (typeof a === "number" && typeof b === "number") return a - b;
  if (typeof a === "boolean" || typeof b === "boolean") {
    return Number(Boolean(a)) - Number(Boolean(b));
  }

  return String(a).localeCompare(String(b), undefined, {
    numeric: true,
    sensitivity: "base",
  });
}

const DrilldownPanel: FC<DrilldownPanelProps> = ({
  pivotData,
  payload,
  onClose,
  onMeasured,
  sortColumn,
  sortDirection,
  onSortChange,
  serverRecords,
  serverColumns,
  serverTotalCount,
  isLoading,
  serverPage,
  serverPageSize,
  onPageChange,
  numberFormat,
  columnAlignment,
  columnTypes,
}): ReactElement => {
  const panelRef = useRef<HTMLDivElement>(null);
  const closeRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    closeRef.current?.focus();
  }, []);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    },
    [onClose],
  );

  const useServerData = serverRecords != null;
  const [clientPage, setClientPage] = useState(0);

  const { allRecords, totalCount, elapsedMs } = useMemo(() => {
    if (useServerData) {
      return {
        allRecords: serverRecords as DataRecord[],
        totalCount: serverTotalCount ?? serverRecords!.length,
        elapsedMs: 0,
      };
    }
    const measured = measureSync(() =>
      pivotData.getMatchingRecords(payload.filters, Infinity),
    );
    return {
      allRecords: measured.result.records,
      totalCount: measured.result.totalCount,
      elapsedMs: measured.elapsedMs,
    };
  }, [
    pivotData,
    payload.filters,
    useServerData,
    serverRecords,
    serverTotalCount,
  ]);

  useEffect(() => {
    setClientPage(0);
  }, [payload.filters, sortColumn, sortDirection]);

  useEffect(() => {
    if (!onMeasured) return;
    onMeasured({
      kind: "drilldown",
      elapsedMs: Math.round(elapsedMs * 100) / 100,
      totalCount,
    });
  }, [elapsedMs, onMeasured, totalCount]);

  const columns = useMemo(() => {
    if (serverColumns) return serverColumns;
    return pivotData.getColumnNames();
  }, [pivotData, serverColumns]);

  const orderedRecords = useMemo(() => {
    if (!sortColumn || !sortDirection || useServerData) return allRecords;
    const columnType = columnTypes?.get(sortColumn);
    return allRecords
      .map((record, index) => ({ record, index }))
      .sort((a, b) => {
        const cmp = compareDrilldownValues(
          a.record[sortColumn],
          b.record[sortColumn],
          columnType,
        );
        if (cmp !== 0) return sortDirection === "asc" ? cmp : -cmp;
        return a.index - b.index;
      })
      .map(({ record }) => record);
  }, [allRecords, columnTypes, sortColumn, sortDirection, useServerData]);

  const headerText = formatDrilldownHeader(payload, pivotData);
  const pageSize = serverPageSize ?? RECORD_LIMIT;
  const currentPage = useServerData ? (serverPage ?? 0) : clientPage;
  const totalPages = totalCount > 0 ? Math.ceil(totalCount / pageSize) : 1;
  const hasPagination =
    totalPages > 1 && (useServerData ? onPageChange != null : true);

  const records = useMemo(() => {
    if (useServerData) return orderedRecords;
    const start = currentPage * pageSize;
    return orderedRecords.slice(start, start + pageSize);
  }, [useServerData, orderedRecords, currentPage, pageSize]);

  const rangeStart = currentPage * pageSize + 1;
  const rangeEnd = Math.min(rangeStart + records.length - 1, totalCount);

  const handlePageChange = useCallback(
    (page: number) => {
      if (onPageChange) {
        onPageChange(page);
      } else {
        setClientPage(page);
      }
    },
    [onPageChange],
  );

  const cappedMessage = isLoading
    ? "Loading…"
    : hasPagination
      ? `${rangeStart}–${rangeEnd} of ${totalCount} records`
      : totalCount > records.length
        ? `Showing ${records.length} of ${totalCount} records`
        : `${totalCount} record${totalCount !== 1 ? "s" : ""}`;

  return (
    <div
      ref={panelRef}
      className={styles.panel}
      data-testid="drilldown-panel"
      onKeyDown={handleKeyDown}
    >
      <div className={styles.header}>
        <div className={styles.headerInfo}>
          <span className={styles.headerTitle}>Drill-down</span>
          <span className={styles.headerContext}>{headerText}</span>
          <span className={styles.headerCount}>{cappedMessage}</span>
        </div>
        <button
          ref={closeRef}
          className={styles.closeButton}
          onClick={onClose}
          aria-label="Close drill-down panel"
          data-testid="drilldown-close"
        >
          <CloseIcon />
        </button>
      </div>
      {isLoading ? (
        <div className={styles.loadingContainer}>
          <div className={styles.spinner} />
          <span className={styles.loadingText}>Loading…</span>
        </div>
      ) : records.length === 0 ? (
        <div className={styles.emptyMessage}>No matching records found.</div>
      ) : (
        <>
          <div className={styles.tableWrapper}>
            <table className={styles.table} data-testid="drilldown-table">
              <thead>
                <tr>
                  {columns.map((col) => {
                    const isSorted = sortColumn === col && !!sortDirection;
                    const ariaSort = isSorted
                      ? sortDirection === "asc"
                        ? "ascending"
                        : "descending"
                      : "none";
                    return (
                      <th
                        key={col}
                        className={`${styles.th} ${isSorted ? styles.thSorted : ""}`}
                        aria-sort={ariaSort}
                      >
                        <button
                          type="button"
                          className={styles.sortButton}
                          onClick={() => {
                            if (!onSortChange) return;
                            if (sortColumn !== col) {
                              onSortChange(col, "asc");
                              return;
                            }
                            if (sortDirection === "asc") {
                              onSortChange(col, "desc");
                              return;
                            }
                            if (sortDirection === "desc") {
                              onSortChange(undefined, undefined);
                              return;
                            }
                            onSortChange(col, "asc");
                          }}
                          disabled={isLoading}
                          data-testid={`drilldown-sort-${col}`}
                        >
                          <span className={styles.sortButtonInner}>
                            <span>{col}</span>
                            <span
                              className={styles.sortIndicatorSlot}
                              aria-hidden="true"
                            >
                              {isSorted && sortDirection && (
                                <SortIndicator direction={sortDirection} />
                              )}
                            </span>
                          </span>
                        </button>
                      </th>
                    );
                  })}
                </tr>
              </thead>
              <tbody>
                {records.map((record: DataRecord, idx: number) => (
                  <tr
                    key={idx}
                    className={idx % 2 === 1 ? styles.altRow : undefined}
                  >
                    {columns.map((col) => {
                      const align = getCellAlign(
                        record[col],
                        col,
                        columnAlignment,
                      );
                      return (
                        <td
                          key={col}
                          className={styles.td}
                          style={align ? { textAlign: align } : undefined}
                        >
                          {formatCellValue(
                            record[col],
                            col,
                            numberFormat,
                            columnTypes,
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {hasPagination && (
            <div
              className={styles.pagination}
              data-testid="drilldown-pagination"
            >
              <button
                className={styles.pageButton}
                disabled={currentPage === 0 || isLoading}
                onClick={() => handlePageChange(currentPage - 1)}
                data-testid="drilldown-prev"
                aria-label="Previous page"
              >
                <svg
                  width="12"
                  height="12"
                  viewBox="0 0 16 16"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <polyline points="10 3 5 8 10 13" />
                </svg>
                Prev
              </button>
              <span className={styles.pageInfo}>
                Page {currentPage + 1} of {totalPages}
              </span>
              <button
                className={styles.pageButton}
                disabled={currentPage >= totalPages - 1 || isLoading}
                onClick={() => handlePageChange(currentPage + 1)}
                data-testid="drilldown-next"
                aria-label="Next page"
              >
                Next
                <svg
                  width="12"
                  height="12"
                  viewBox="0 0 16 16"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <polyline points="6 3 11 8 6 13" />
                </svg>
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default DrilldownPanel;
