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

import { describe, expect, it } from "vitest";
import { tableToIPC, tableFromArrays, type Table } from "apache-arrow";
import {
  parseArrowToRecords,
  getArrowColumnNames,
  getNumericColumns,
} from "./parseArrow";

function makeArrowBuffer(data: Record<string, unknown[]>): Uint8Array {
  const table = tableFromArrays(data);
  return tableToIPC(table);
}

function makeArrowTable(data: Record<string, unknown[]>): Table {
  return tableFromArrays(data);
}

describe("parseArrowToRecords", () => {
  it("returns empty array for null input", () => {
    expect(parseArrowToRecords(null)).toEqual([]);
  });

  it("returns empty array for empty Uint8Array", () => {
    expect(parseArrowToRecords(new Uint8Array(0))).toEqual([]);
  });

  it("returns empty array for invalid IPC data", () => {
    expect(parseArrowToRecords(new Uint8Array([1, 2, 3]))).toEqual([]);
  });

  it("parses a simple table", () => {
    const buf = makeArrowBuffer({
      region: ["US", "EU", "US"],
      year: ["2023", "2023", "2024"],
      revenue: [100, 200, 150],
    });
    const records = parseArrowToRecords(buf);
    expect(records).toHaveLength(3);
    expect(records[0]).toEqual({ region: "US", year: "2023", revenue: 100 });
    expect(records[1]).toEqual({ region: "EU", year: "2023", revenue: 200 });
    expect(records[2]).toEqual({ region: "US", year: "2024", revenue: 150 });
  });

  it("handles null values in columns", () => {
    const buf = makeArrowBuffer({
      name: ["Alice", null, "Charlie"],
      score: [90, null, 70],
    });
    const records = parseArrowToRecords(buf);
    expect(records).toHaveLength(3);
    expect(records[1].name).toBeNull();
    expect(records[1].score).toBeNull();
  });
});

describe("getArrowColumnNames", () => {
  it("returns empty array for null input", () => {
    expect(getArrowColumnNames(null)).toEqual([]);
  });

  it("returns column names", () => {
    const buf = makeArrowBuffer({
      region: ["US"],
      year: ["2023"],
      revenue: [100],
    });
    expect(getArrowColumnNames(buf)).toEqual(["region", "year", "revenue"]);
  });
});

describe("getNumericColumns", () => {
  it("returns empty array for null input", () => {
    expect(getNumericColumns(null)).toEqual([]);
  });

  it("detects numeric columns", () => {
    const buf = makeArrowBuffer({
      name: ["Alice"],
      age: [30],
      score: [95.5],
    });
    const numericCols = getNumericColumns(buf);
    expect(numericCols).toContain("age");
    expect(numericCols).toContain("score");
    expect(numericCols).not.toContain("name");
  });
});

describe("Arrow Table input (CCv2 production path)", () => {
  it("parseArrowToRecords accepts an Arrow Table directly", () => {
    const table = makeArrowTable({
      region: ["US", "EU"],
      revenue: [100, 200],
    });
    const records = parseArrowToRecords(table);
    expect(records).toHaveLength(2);
    expect(records[0]).toEqual({ region: "US", revenue: 100 });
    expect(records[1]).toEqual({ region: "EU", revenue: 200 });
  });

  it("getArrowColumnNames accepts an Arrow Table directly", () => {
    const table = makeArrowTable({
      region: ["US"],
      year: ["2023"],
      revenue: [100],
    });
    expect(getArrowColumnNames(table)).toEqual(["region", "year", "revenue"]);
  });

  it("getNumericColumns accepts an Arrow Table directly", () => {
    const table = makeArrowTable({
      name: ["Alice"],
      age: [30],
      score: [95.5],
    });
    const numericCols = getNumericColumns(table);
    expect(numericCols).toContain("age");
    expect(numericCols).toContain("score");
    expect(numericCols).not.toContain("name");
  });

  it("returns empty for undefined input", () => {
    expect(parseArrowToRecords(undefined)).toEqual([]);
    expect(getArrowColumnNames(undefined)).toEqual([]);
    expect(getNumericColumns(undefined)).toEqual([]);
  });
});
