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
import {
  PivotData,
  type DataRecord,
  makeKeyString,
  buildSidecarFingerprint,
} from "./PivotData";
import type { ColumnType, PivotConfigV1 } from "./types";
import { buildModifiedColKey } from "./dateGrouping";

function makeConfig(overrides: Partial<PivotConfigV1> = {}): PivotConfigV1 {
  const values = overrides.values ?? ["revenue"];
  return {
    version: 1,
    rows: ["region"],
    columns: ["order_date"],
    values,
    aggregation: Object.fromEntries(
      values.map((v) => [v, overrides.aggregation?.[v] ?? "sum"]),
    ),
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    auto_date_hierarchy: false,
    date_grains: { order_date: "month" },
    ...overrides,
  };
}

const MONTH_DATA: DataRecord[] = [
  { region: "East", order_date: "2024-01-15", revenue: 100 },
  { region: "East", order_date: "2024-02-10", revenue: 200 },
  { region: "East", order_date: "2024-04-05", revenue: 150 },
  { region: "West", order_date: "2024-01-20", revenue: 300 },
  { region: "West", order_date: "2024-02-25", revenue: 400 },
];

const columnTypes = new Map([
  ["region", "string" as ColumnType],
  ["order_date", "date" as ColumnType],
  ["revenue", "number" as ColumnType],
]);

describe("PivotData temporal hierarchy subtotals", () => {
  it("getTemporalColSubtotal aggregates by year for month grain", () => {
    const config = makeConfig();
    const pd = new PivotData(MONTH_DATA, config, { columnTypes });

    const modifiedKey = buildModifiedColKey(
      ["2024-01"],
      0,
      "order_date",
      "2024",
    );
    const agg = pd.getTemporalColSubtotal(["East"], modifiedKey, "revenue");
    // East: 2024-01 (100) + 2024-02 (200) + 2024-04 (150) = 450
    expect(agg.value()).toBe(450);
  });

  it("getTemporalColSubtotal returns different values per row", () => {
    const config = makeConfig();
    const pd = new PivotData(MONTH_DATA, config, { columnTypes });

    const modifiedKey = buildModifiedColKey(
      ["2024-01"],
      0,
      "order_date",
      "2024",
    );
    const eastAgg = pd.getTemporalColSubtotal(["East"], modifiedKey, "revenue");
    const westAgg = pd.getTemporalColSubtotal(["West"], modifiedKey, "revenue");
    expect(eastAgg.value()).toBe(450);
    expect(westAgg.value()).toBe(700);
  });

  it("getTemporalColSubtotalGrand aggregates all rows for a parent", () => {
    const config = makeConfig();
    const pd = new PivotData(MONTH_DATA, config, { columnTypes });

    const modifiedKey = buildModifiedColKey(
      ["2024-01"],
      0,
      "order_date",
      "2024",
    );
    const agg = pd.getTemporalColSubtotalGrand(modifiedKey, "revenue");
    // Total for 2024: 100 + 200 + 150 + 300 + 400 = 1150
    expect(agg.value()).toBe(1150);
  });

  it("returns zero aggregator for non-existent parent", () => {
    const config = makeConfig();
    const pd = new PivotData(MONTH_DATA, config, { columnTypes });

    const modifiedKey = buildModifiedColKey(
      ["2025-01"],
      0,
      "order_date",
      "2025",
    );
    const agg = pd.getTemporalColSubtotal(["East"], modifiedKey, "revenue");
    expect(agg.value()).toBeNull();
  });

  describe("non-additive aggregation (avg)", () => {
    it("computes avg correctly for collapsed parent", () => {
      const config = makeConfig({
        aggregation: { revenue: "avg" },
      });
      const pd = new PivotData(MONTH_DATA, config, { columnTypes });

      const modifiedKey = buildModifiedColKey(
        ["2024-01"],
        0,
        "order_date",
        "2024",
      );
      const agg = pd.getTemporalColSubtotal(["East"], modifiedKey, "revenue");
      // East has 3 records: 100, 200, 150 → avg = 150
      expect(agg.value()).toBe(150);
    });
  });

  describe("multi-dimension columns", () => {
    it("preserves sibling context in modified column key", () => {
      const config = makeConfig({
        columns: ["region", "order_date"],
        rows: ["category"],
      });
      const data: DataRecord[] = [
        {
          category: "A",
          region: "East",
          order_date: "2024-01-15",
          revenue: 100,
        },
        {
          category: "A",
          region: "East",
          order_date: "2024-02-10",
          revenue: 200,
        },
        {
          category: "A",
          region: "West",
          order_date: "2024-01-20",
          revenue: 300,
        },
        {
          category: "A",
          region: "West",
          order_date: "2024-02-25",
          revenue: 400,
        },
      ];
      const types = new Map([
        ["category", "string" as ColumnType],
        ["region", "string" as ColumnType],
        ["order_date", "date" as ColumnType],
        ["revenue", "number" as ColumnType],
      ]);

      const pd = new PivotData(data, config, { columnTypes: types });

      const eastKey = buildModifiedColKey(
        ["East", "2024-01"],
        1,
        "order_date",
        "2024",
      );
      const westKey = buildModifiedColKey(
        ["West", "2024-01"],
        1,
        "order_date",
        "2024",
      );

      const eastAgg = pd.getTemporalColSubtotal(["A"], eastKey, "revenue");
      const westAgg = pd.getTemporalColSubtotal(["A"], westKey, "revenue");

      // East: 100 + 200 = 300
      expect(eastAgg.value()).toBe(300);
      // West: 300 + 400 = 700
      expect(westAgg.value()).toBe(700);
    });
  });

  describe("quarter parent level for month grain", () => {
    it("aggregates by quarter", () => {
      const config = makeConfig();
      const pd = new PivotData(MONTH_DATA, config, { columnTypes });

      // Q1 2024: Jan (100) + Feb (200) = 300 for East
      const q1Key = buildModifiedColKey(
        ["2024-01"],
        0,
        "order_date",
        "2024-Q1",
      );
      const agg = pd.getTemporalColSubtotal(["East"], q1Key, "revenue");
      expect(agg.value()).toBe(300);

      // Q2 2024: Apr (150) for East
      const q2Key = buildModifiedColKey(
        ["2024-04"],
        0,
        "order_date",
        "2024-Q2",
      );
      const aggQ2 = pd.getTemporalColSubtotal(["East"], q2Key, "revenue");
      expect(aggQ2.value()).toBe(150);
    });
  });

  describe("hybrid sidecar temporal parent", () => {
    it("uses hybrid values when available", () => {
      const config = makeConfig();
      const fp = buildSidecarFingerprint(config, undefined);
      const modifiedColKey = buildModifiedColKey(
        ["2024-01"],
        0,
        "order_date",
        "2024",
      );
      const pd = new PivotData(MONTH_DATA, config, {
        columnTypes,
        hybridTotals: {
          sidecar_fingerprint: fp,
          grand: {},
          row: [],
          col: [],
          temporal_parent: [
            {
              row: ["East"],
              col: modifiedColKey,
              field: "order_date",
              grain: "year",
              values: { revenue: 999 },
            },
          ],
        },
      });

      const agg = pd.getTemporalColSubtotal(
        ["East"],
        modifiedColKey,
        "revenue",
      );
      expect(agg.value()).toBe(999);
    });

    it("uses hybrid grand values when available", () => {
      const config = makeConfig();
      const fp = buildSidecarFingerprint(config, undefined);
      const modifiedColKey = buildModifiedColKey(
        ["2024-01"],
        0,
        "order_date",
        "2024",
      );
      const pd = new PivotData(MONTH_DATA, config, {
        columnTypes,
        hybridTotals: {
          sidecar_fingerprint: fp,
          grand: {},
          row: [],
          col: [],
          temporal_parent_grand: [
            {
              col: modifiedColKey,
              field: "order_date",
              grain: "year",
              values: { revenue: 8888 },
            },
          ],
        },
      });

      const agg = pd.getTemporalColSubtotalGrand(modifiedColKey, "revenue");
      expect(agg.value()).toBe(8888);
    });
  });
});
