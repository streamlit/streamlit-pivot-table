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
import type { CellClickPayload } from "../engine/types";
import { measureSync, type PerfActionMeasurement } from "../engine/perf";
import styles from "./DrilldownPanel.module.css";

const RECORD_LIMIT = 500;

export interface DrilldownPanelProps {
  pivotData: PivotData;
  payload: CellClickPayload;
  onClose: () => void;
  onMeasured?: (action: PerfActionMeasurement) => void;
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

function formatDrilldownHeader(payload: CellClickPayload): string {
  const parts: string[] = [];
  for (const [dim, val] of Object.entries(payload.filters)) {
    parts.push(`${dim}: ${val}`);
  }
  if (parts.length === 0) {
    const rowLabel = payload.rowKey.join(" > ");
    const colLabel = payload.colKey.join(" > ");
    if (rowLabel && colLabel) return `${rowLabel} × ${colLabel}`;
    return rowLabel || colLabel || "All records";
  }
  return parts.join(", ");
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") {
    if (Number.isNaN(value)) return "";
    return String(value);
  }
  return String(value);
}

const DrilldownPanel: FC<DrilldownPanelProps> = ({
  pivotData,
  payload,
  onClose,
  onMeasured,
  serverRecords,
  serverColumns,
  serverTotalCount,
  isLoading,
  serverPage,
  serverPageSize,
  onPageChange,
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
  }, [payload.filters]);

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

  const headerText = formatDrilldownHeader(payload);
  const pageSize = serverPageSize ?? RECORD_LIMIT;
  const currentPage = useServerData ? (serverPage ?? 0) : clientPage;
  const totalPages = totalCount > 0 ? Math.ceil(totalCount / pageSize) : 1;
  const hasPagination =
    totalPages > 1 && (useServerData ? onPageChange != null : true);

  const records = useMemo(() => {
    if (useServerData) return allRecords;
    const start = currentPage * pageSize;
    return allRecords.slice(start, start + pageSize);
  }, [useServerData, allRecords, currentPage, pageSize]);

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
        <div className={styles.emptyMessage}>Loading drill-down data…</div>
      ) : records.length === 0 ? (
        <div className={styles.emptyMessage}>No matching records found.</div>
      ) : (
        <>
          <div className={styles.tableWrapper}>
            <table className={styles.table} data-testid="drilldown-table">
              <thead>
                <tr>
                  {columns.map((col) => (
                    <th key={col} className={styles.th}>
                      {col}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {records.map((record: DataRecord, idx: number) => (
                  <tr
                    key={idx}
                    className={idx % 2 === 1 ? styles.altRow : undefined}
                  >
                    {columns.map((col) => (
                      <td key={col} className={styles.td}>
                        {formatCellValue(record[col])}
                      </td>
                    ))}
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
              >
                ← Prev
              </button>
              <span className={styles.pageInfo}>
                Page {currentPage + 1} of {totalPages}
              </span>
              <button
                className={styles.pageButton}
                disabled={currentPage >= totalPages - 1 || isLoading}
                onClick={() => handlePageChange(currentPage + 1)}
                data-testid="drilldown-next"
              >
                Next →
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default DrilldownPanel;
