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

import { tableFromIPC, Table } from "apache-arrow";
import type { DataRecord } from "./PivotData";

/**
 * CCv2 delivers DataFrames as Apache Arrow Table objects (not raw Uint8Array).
 * The framework's `reconstructMixedData` already calls `tableFromIPC` and
 * replaces the placeholder with a `Table` instance before the data reaches
 * the component's renderer function.
 *
 * We accept `unknown` because the `PivotTableData.dataframe` is typed as
 * `unknown`, and handle both Table instances and raw Uint8Array for safety.
 */

function toTable(arrowData: unknown): Table | null {
  if (!arrowData) return null;

  if (arrowData instanceof Table) {
    return arrowData;
  }

  if (arrowData instanceof Uint8Array) {
    if (arrowData.byteLength === 0) return null;
    try {
      return tableFromIPC(arrowData);
    } catch {
      console.warn("Failed to parse Arrow IPC data");
      return null;
    }
  }

  // In some environments the Table prototype check fails due to different
  // module instances. Fall back to duck-typing: if it has schema.fields
  // and numRows, treat it as a Table.
  if (
    typeof arrowData === "object" &&
    arrowData !== null &&
    "schema" in arrowData &&
    "numRows" in arrowData &&
    "getChild" in arrowData
  ) {
    return arrowData as Table;
  }

  return null;
}

/**
 * Parse Arrow data (Table or IPC buffer) into plain JS records suitable
 * for the PivotData engine.
 */
export function parseArrowToRecords(arrowData: unknown): DataRecord[] {
  const table = toTable(arrowData);
  if (!table) return [];

  const columnNames = table.schema.fields.map((f) => f.name);
  const columns = columnNames.map((name) => ({
    name,
    vector: table.getChild(name),
  }));
  const numRows = table.numRows;
  const records: DataRecord[] = new Array(numRows);

  for (let i = 0; i < numRows; i++) {
    const record: DataRecord = {};
    for (const column of columns) {
      record[column.name] = column.vector ? column.vector.get(i) : null;
    }
    records[i] = record;
  }

  return records;
}

/**
 * Extract column names from Arrow data without fully materializing records.
 */
export function getArrowColumnNames(arrowData: unknown): string[] {
  const table = toTable(arrowData);
  if (!table) return [];
  return table.schema.fields.map((f) => f.name);
}

/**
 * Detect which columns are numeric (for auto-detecting values/measures).
 */
export function getNumericColumns(arrowData: unknown): string[] {
  const table = toTable(arrowData);
  if (!table) return [];
  return table.schema.fields
    .filter((f) => {
      const typeId = f.type.typeId;
      // Arrow numeric type IDs: Int=2, Float=3, Decimal=7
      return typeId === 2 || typeId === 3 || typeId === 7;
    })
    .map((f) => f.name);
}

export { toTable };
