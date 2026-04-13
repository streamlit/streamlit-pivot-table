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
  applyTemporalRowCollapse,
  computeTemporalColInfos,
  computeTemporalRowInfos,
  computeHeaderLevels,
  computeNumHeaderLevels,
  computeProjectedRowHeaderSpans,
  computeRowHeaderLevels,
  computeParentGroups,
  computeTemporalColSlots,
  projectVisibleRowEntries,
  toggleTemporalCollapse,
  type TemporalColInfo,
} from "./temporalHierarchy";
import type { ColSlot } from "./TableRenderer";
import type { ColumnType, PivotConfigV1 } from "../engine/types";
import { makeKeyString } from "../engine/PivotData";
import {
  buildModifiedColKey,
  buildModifiedRowKey,
} from "../engine/dateGrouping";

function makeConfig(overrides: Partial<PivotConfigV1> = {}): PivotConfigV1 {
  const values = overrides.values ?? ["revenue"];
  return {
    version: 1,
    rows: [],
    columns: [],
    values,
    aggregation: Object.fromEntries(values.map((v) => [v, "sum"])),
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    auto_date_hierarchy: true,
    ...overrides,
  };
}

function makeColumnTypes(
  mapping: Record<string, ColumnType>,
): Map<string, ColumnType> {
  return new Map(Object.entries(mapping));
}

describe("computeTemporalColInfos", () => {
  it("returns empty for non-temporal columns", () => {
    const config = makeConfig({ columns: ["region"] });
    const types = makeColumnTypes({ region: "string" });
    expect(computeTemporalColInfos(config, types)).toEqual([]);
  });

  it("returns empty for year grain (no hierarchy)", () => {
    const config = makeConfig({
      columns: ["order_date"],
      date_grains: { order_date: "year" },
    });
    const types = makeColumnTypes({ order_date: "date" });
    expect(computeTemporalColInfos(config, types)).toEqual([]);
  });

  it("returns info for month grain", () => {
    const config = makeConfig({
      columns: ["order_date"],
      date_grains: { order_date: "month" },
    });
    const types = makeColumnTypes({ order_date: "date" });
    const result = computeTemporalColInfos(config, types);
    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject({
      dimIndex: 0,
      field: "order_date",
      grain: "month",
      hierarchyLevels: ["year", "quarter", "month"],
    });
  });

  it("returns info for quarter grain", () => {
    const config = makeConfig({
      columns: ["order_date"],
      date_grains: { order_date: "quarter" },
    });
    const types = makeColumnTypes({ order_date: "date" });
    const result = computeTemporalColInfos(config, types);
    expect(result).toHaveLength(1);
    expect(result[0]!.hierarchyLevels).toEqual(["year", "quarter"]);
  });

  it("returns info for day grain (skips quarter)", () => {
    const config = makeConfig({
      columns: ["order_date"],
      date_grains: { order_date: "day" },
    });
    const types = makeColumnTypes({ order_date: "date" });
    const result = computeTemporalColInfos(config, types);
    expect(result).toHaveLength(1);
    expect(result[0]!.hierarchyLevels).toEqual(["year", "month", "day"]);
  });

  it("returns info for multi-column with temporal", () => {
    const config = makeConfig({
      columns: ["region", "order_date"],
      date_grains: { order_date: "month" },
    });
    const types = makeColumnTypes({ region: "string", order_date: "date" });
    const result = computeTemporalColInfos(config, types);
    expect(result).toHaveLength(1);
    expect(result[0]!.dimIndex).toBe(1);
  });
});

describe("computeHeaderLevels", () => {
  it("maps flat columns to single levels", () => {
    const config = makeConfig({ columns: ["region", "product"] });
    const levels = computeHeaderLevels(config, []);
    expect(levels).toHaveLength(2);
    expect(levels[0]!.field).toBe("region");
    expect(levels[0]!.isLeaf).toBe(true);
    expect(levels[1]!.field).toBe("product");
    expect(levels[1]!.isLeaf).toBe(true);
  });

  it("expands temporal field into multiple levels", () => {
    const config = makeConfig({ columns: ["order_date"] });
    const tInfos: TemporalColInfo[] = [
      {
        dimIndex: 0,
        field: "order_date",
        grain: "month",
        hierarchyLevels: ["year", "quarter", "month"],
      },
    ];
    const levels = computeHeaderLevels(config, tInfos);
    expect(levels).toHaveLength(3);
    expect(levels[0]).toMatchObject({
      field: "order_date",
      grain: "year",
      hierarchyOffset: 0,
      isLeaf: false,
      isTemporal: true,
    });
    expect(levels[1]).toMatchObject({
      grain: "quarter",
      hierarchyOffset: 1,
      isLeaf: false,
      isTemporal: true,
    });
    expect(levels[2]).toMatchObject({
      grain: "month",
      hierarchyOffset: 2,
      isLeaf: true,
      isTemporal: true,
    });
  });

  it("mixes temporal and non-temporal columns", () => {
    const config = makeConfig({ columns: ["region", "order_date"] });
    const tInfos: TemporalColInfo[] = [
      {
        dimIndex: 1,
        field: "order_date",
        grain: "quarter",
        hierarchyLevels: ["year", "quarter"],
      },
    ];
    const levels = computeHeaderLevels(config, tInfos);
    expect(levels).toHaveLength(3);
    expect(levels[0]!.field).toBe("region");
    expect(levels[0]!.isTemporal).toBe(false);
    expect(levels[1]!.field).toBe("order_date");
    expect(levels[1]!.grain).toBe("year");
    expect(levels[2]!.field).toBe("order_date");
    expect(levels[2]!.grain).toBe("quarter");
  });
});

describe("computeNumHeaderLevels", () => {
  it("returns column count for flat columns", () => {
    const config = makeConfig({ columns: ["a", "b"] });
    expect(computeNumHeaderLevels(config, [])).toBe(2);
  });

  it("sums hierarchy levels for temporal columns", () => {
    const config = makeConfig({ columns: ["region", "order_date"] });
    const tInfos: TemporalColInfo[] = [
      {
        dimIndex: 1,
        field: "order_date",
        grain: "month",
        hierarchyLevels: ["year", "quarter", "month"],
      },
    ];
    expect(computeNumHeaderLevels(config, tInfos)).toBe(4);
  });
});

describe("computeParentGroups", () => {
  it("groups month slots by year", () => {
    const slots: ColSlot[] = [
      { key: ["2024-01"] },
      { key: ["2024-02"] },
      { key: ["2024-03"] },
      { key: ["2025-01"] },
    ];
    const tInfo: TemporalColInfo = {
      dimIndex: 0,
      field: "order_date",
      grain: "month",
      hierarchyLevels: ["year", "quarter", "month"],
    };
    const config = makeConfig({ columns: ["order_date"] });
    const groups = computeParentGroups(
      slots,
      0,
      slots.length,
      tInfo,
      "year",
      0,
      config,
    );
    expect(groups).toHaveLength(2);
    expect(groups[0]!.parentBucket).toBe("2024");
    expect(groups[0]!.startIdx).toBe(0);
    expect(groups[0]!.endIdx).toBe(3);
    expect(groups[0]!.isCollapsed).toBe(false);
    expect(groups[1]!.parentBucket).toBe("2025");
    expect(groups[1]!.startIdx).toBe(3);
    expect(groups[1]!.endIdx).toBe(4);
  });

  it("respects collapsed state", () => {
    const slots: ColSlot[] = [{ key: ["2024-01"] }, { key: ["2024-02"] }];
    const tInfo: TemporalColInfo = {
      dimIndex: 0,
      field: "order_date",
      grain: "month",
      hierarchyLevels: ["year", "quarter", "month"],
    };
    const collapseKey = makeKeyString(
      buildModifiedColKey(["2024-01"], 0, "order_date", "2024"),
    );
    const config = makeConfig({
      columns: ["order_date"],
      collapsed_temporal_groups: { order_date: [collapseKey] },
    });
    const groups = computeParentGroups(
      slots,
      0,
      slots.length,
      tInfo,
      "year",
      0,
      config,
    );
    expect(groups[0]!.isCollapsed).toBe(true);
  });

  it("preserves sibling context in multi-dim columns", () => {
    const slots: ColSlot[] = [
      { key: ["East", "2024-01"] },
      { key: ["East", "2024-02"] },
      { key: ["West", "2024-01"] },
      { key: ["West", "2024-02"] },
    ];
    const tInfo: TemporalColInfo = {
      dimIndex: 1,
      field: "order_date",
      grain: "month",
      hierarchyLevels: ["year", "quarter", "month"],
    };
    const config = makeConfig({ columns: ["region", "order_date"] });
    const groups = computeParentGroups(
      slots,
      0,
      slots.length,
      tInfo,
      "year",
      0,
      config,
    );
    expect(groups).toHaveLength(2);
    expect(groups[0]!.parentBucket).toBe("2024");
    expect(groups[0]!.startIdx).toBe(0);
    expect(groups[0]!.endIdx).toBe(2);
    expect(groups[1]!.parentBucket).toBe("2024");
    expect(groups[1]!.startIdx).toBe(2);
    expect(groups[1]!.endIdx).toBe(4);
    // Different collapse keys because outer sibling context differs
    expect(groups[0]!.collapseKey).not.toBe(groups[1]!.collapseKey);
  });

  it("handles null temporal buckets gracefully", () => {
    const slots: ColSlot[] = [
      { key: ["(null)"] },
      { key: ["2024-01"] },
      { key: ["2024-02"] },
    ];
    const tInfo: TemporalColInfo = {
      dimIndex: 0,
      field: "order_date",
      grain: "month",
      hierarchyLevels: ["year", "quarter", "month"],
    };
    const config = makeConfig({ columns: ["order_date"] });
    const groups = computeParentGroups(
      slots,
      0,
      slots.length,
      tInfo,
      "year",
      0,
      config,
    );
    // (null) gets its own group, 2024 months get one group
    expect(groups).toHaveLength(2);
    expect(groups[0]!.parentBucket).toBe("(null)");
    expect(groups[0]!.startIdx).toBe(0);
    expect(groups[0]!.endIdx).toBe(1);
    expect(groups[1]!.parentBucket).toBe("2024");
  });

  it("spans across child dimensions after the temporal field", () => {
    // columns=["order_date", "product"] — product is a child dimension
    const slots: ColSlot[] = [
      { key: ["2024-01", "Widget"] },
      { key: ["2024-01", "Gadget"] },
      { key: ["2024-02", "Widget"] },
      { key: ["2024-02", "Gadget"] },
      { key: ["2025-01", "Widget"] },
    ];
    const tInfo: TemporalColInfo = {
      dimIndex: 0,
      field: "order_date",
      grain: "month",
      hierarchyLevels: ["year", "quarter", "month"],
    };
    const config = makeConfig({ columns: ["order_date", "product"] });
    const groups = computeParentGroups(
      slots,
      0,
      slots.length,
      tInfo,
      "year",
      0,
      config,
    );
    // All 2024 slots (4) should be in one group, not split by product
    expect(groups).toHaveLength(2);
    expect(groups[0]!.parentBucket).toBe("2024");
    expect(groups[0]!.startIdx).toBe(0);
    expect(groups[0]!.endIdx).toBe(4);
    expect(groups[1]!.parentBucket).toBe("2025");
    expect(groups[1]!.startIdx).toBe(4);
    expect(groups[1]!.endIdx).toBe(5);
  });
});

describe("computeTemporalColSlots", () => {
  it("passes through when no temporal infos", () => {
    const slots: ColSlot[] = [{ key: ["a"] }, { key: ["b"] }];
    const result = computeTemporalColSlots(slots, [], makeConfig());
    expect(result).toEqual(slots);
  });

  it("collapses leaf slots under collapsed parent", () => {
    const slots: ColSlot[] = [
      { key: ["2024-01"] },
      { key: ["2024-02"] },
      { key: ["2024-03"] },
      { key: ["2025-01"] },
    ];
    const tInfo: TemporalColInfo = {
      dimIndex: 0,
      field: "order_date",
      grain: "month",
      hierarchyLevels: ["year", "quarter", "month"],
    };
    const collapseKey = makeKeyString(
      buildModifiedColKey(["2024-01"], 0, "order_date", "2024"),
    );
    const config = makeConfig({
      columns: ["order_date"],
      collapsed_temporal_groups: { order_date: [collapseKey] },
    });
    const result = computeTemporalColSlots(slots, [tInfo], config);
    // 3 leaf slots for 2024 collapsed into 1 + 1 for 2025
    expect(result).toHaveLength(2);
    expect(result[0]!.temporalCollapse).toBeDefined();
    expect(result[0]!.temporalCollapse!.parentBucket).toBe("2024");
    expect(result[1]!.temporalCollapse).toBeUndefined();
  });

  it("collapses null temporal bucket when toggled", () => {
    const slots: ColSlot[] = [
      { key: ["(null)"] },
      { key: ["2024-01"] },
      { key: ["2024-02"] },
    ];
    const tInfo: TemporalColInfo = {
      dimIndex: 0,
      field: "order_date",
      grain: "month",
      hierarchyLevels: ["year", "quarter", "month"],
    };
    // Build the collapse key for (null) — same fallback as computeParentGroups
    const collapseKey = makeKeyString(
      buildModifiedColKey(["(null)"], 0, "order_date", "(null)").slice(0, 1),
    );
    const config = makeConfig({
      columns: ["order_date"],
      collapsed_temporal_groups: { order_date: [collapseKey] },
    });
    const result = computeTemporalColSlots(slots, [tInfo], config);
    // (null) slot should be collapsed, the two 2024 slots remain
    expect(result).toHaveLength(3);
    expect(result[0]!.temporalCollapse).toBeDefined();
    expect(result[0]!.temporalCollapse!.parentBucket).toBe("(null)");
    expect(result[1]!.temporalCollapse).toBeUndefined();
    expect(result[2]!.temporalCollapse).toBeUndefined();
  });
});

describe("toggleTemporalCollapse", () => {
  it("adds new collapse key", () => {
    const result = toggleTemporalCollapse(undefined, "order_date", "key1");
    expect(result).toEqual({ order_date: ["key1"] });
  });

  it("removes existing collapse key", () => {
    const result = toggleTemporalCollapse(
      { order_date: ["key1", "key2"] },
      "order_date",
      "key1",
    );
    expect(result).toEqual({ order_date: ["key2"] });
  });

  it("removes field when last key is toggled off", () => {
    const result = toggleTemporalCollapse(
      { order_date: ["key1"] },
      "order_date",
      "key1",
    );
    expect(result).toEqual({});
  });

  it("preserves other fields", () => {
    const result = toggleTemporalCollapse(
      { order_date: ["key1"], ship_date: ["key2"] },
      "order_date",
      "key1",
    );
    expect(result).toEqual({ ship_date: ["key2"] });
  });
});

describe("row-side temporal hierarchy helpers", () => {
  it("computes row temporal metadata and expanded row header levels", () => {
    const config = makeConfig({
      rows: ["region", "order_date"],
      date_grains: { order_date: "month" },
    });
    const types = makeColumnTypes({ region: "string", order_date: "date" });
    const infos = computeTemporalRowInfos(config, types);
    const levels = computeRowHeaderLevels(config, infos);

    expect(infos).toHaveLength(1);
    expect(infos[0]).toMatchObject({
      dimIndex: 1,
      field: "order_date",
      hierarchyLevels: ["year", "quarter", "month"],
    });
    expect(levels.map((level) => `${level.field}:${level.grain}`)).toEqual([
      "region:region",
      "order_date:year",
      "order_date:quarter",
      "order_date:month",
    ]);
  });

  it("replaces collapsed temporal descendants with one synthetic parent row", () => {
    const collapseKey = makeKeyString(
      buildModifiedRowKey(["US", "2024-01"], 1, "order_date", "2024").slice(
        0,
        2,
      ),
    );
    const config = makeConfig({
      rows: ["region", "order_date"],
      date_grains: { order_date: "month" },
      collapsed_temporal_row_groups: { order_date: [collapseKey] },
    });
    const types = makeColumnTypes({ region: "string", order_date: "date" });
    const infos = computeTemporalRowInfos(config, types);
    const entries = applyTemporalRowCollapse(
      [
        { type: "data", key: ["US", "2024-01"], level: 1 },
        { type: "data", key: ["US", "2024-02"], level: 1 },
        { type: "data", key: ["US", "2025-01"], level: 1 },
      ],
      infos,
      config,
    );

    expect(entries).toHaveLength(2);
    expect(entries[0]).toMatchObject({
      type: "temporal_parent",
      key: ["US", "2024"],
    });
    expect(entries[1]).toMatchObject({
      type: "data",
      key: ["US", "2025-01"],
    });
  });

  it("projects row entries into effective header columns and spans", () => {
    const collapseKey = makeKeyString(
      buildModifiedRowKey(["US", "2024-01"], 1, "order_date", "2024").slice(
        0,
        2,
      ),
    );
    const config = makeConfig({
      rows: ["region", "order_date"],
      date_grains: { order_date: "month" },
      collapsed_temporal_row_groups: { order_date: [collapseKey] },
    });
    const types = makeColumnTypes({ region: "string", order_date: "date" });
    const infos = computeTemporalRowInfos(config, types);
    const rowLevels = computeRowHeaderLevels(config, infos);
    const projected = projectVisibleRowEntries(
      applyTemporalRowCollapse(
        [
          { type: "data", key: ["US", "2024-01"], level: 1 },
          { type: "data", key: ["US", "2024-02"], level: 1 },
          { type: "data", key: ["US", "2025-01"], level: 1 },
        ],
        infos,
        config,
      ),
      config,
      rowLevels,
      infos,
    );
    const spans = computeProjectedRowHeaderSpans(projected);

    expect(projected[0]?.headerValues).toEqual(["US", "2024", "", ""]);
    expect(projected[0]?.headerVisible).toEqual([true, true, true, true]);
    expect(projected[0]?.headerSpacer).toEqual([false, false, true, true]);
    expect(projected[1]?.headerValues).toEqual([
      "US",
      "2025",
      "2025-Q1",
      "2025-01",
    ]);
    expect(spans[0]?.[0]).toBe(2);
    expect(spans[0]?.[1]).toBe(1);
  });

  it("keeps outer-dimension span across temporal parent and later data row", () => {
    const collapseKey = makeKeyString(
      buildModifiedRowKey(["US", "2024-01"], 1, "order_date", "2024").slice(
        0,
        2,
      ),
    );
    const config = makeConfig({
      rows: ["region", "order_date"],
      date_grains: { order_date: "month" },
      collapsed_temporal_row_groups: { order_date: [collapseKey] },
    });
    const types = makeColumnTypes({ region: "string", order_date: "date" });
    const infos = computeTemporalRowInfos(config, types);
    const rowLevels = computeRowHeaderLevels(config, infos);
    const projected = projectVisibleRowEntries(
      applyTemporalRowCollapse(
        [
          { type: "data", key: ["US", "2024-01"], level: 1 },
          { type: "data", key: ["US", "2024-02"], level: 1 },
          { type: "data", key: ["US", "2025-01"], level: 1 },
        ],
        infos,
        config,
      ),
      config,
      rowLevels,
      infos,
    );
    const spans = computeProjectedRowHeaderSpans(projected);

    expect(projected[0]?.type).toBe("temporal_parent");
    expect(projected[1]?.type).toBe("data");
    expect(projected[0]?.headerValues[0]).toBe("US");
    expect(projected[1]?.headerValues[0]).toBe("US");
    expect(projected[0]?.headerSpacer.slice(1)).toEqual([false, true, true]);
    expect(projected[1]?.headerSpacer).toEqual([false, false, false, false]);
    expect(spans[0]?.[0]).toBe(2);
    expect(spans[1]?.[0]).toBe(0);
  });
});
