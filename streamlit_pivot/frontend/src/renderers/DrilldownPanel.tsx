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
} from "react";
import type { PivotData, DataRecord } from "../engine/PivotData";
import type { CellClickPayload } from "../engine/types";
import styles from "./DrilldownPanel.module.css";

const RECORD_LIMIT = 500;

export interface DrilldownPanelProps {
  pivotData: PivotData;
  payload: CellClickPayload;
  onClose: () => void;
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

  const { records, totalCount } = useMemo(
    () => pivotData.getMatchingRecords(payload.filters, RECORD_LIMIT),
    [pivotData, payload.filters],
  );

  const columns = useMemo(() => pivotData.getColumnNames(), [pivotData]);

  const headerText = formatDrilldownHeader(payload);
  const cappedMessage =
    totalCount > RECORD_LIMIT
      ? `Showing ${RECORD_LIMIT} of ${totalCount} records`
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
      {records.length === 0 ? (
        <div className={styles.emptyMessage}>No matching records found.</div>
      ) : (
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
      )}
    </div>
  );
};

export default DrilldownPanel;
