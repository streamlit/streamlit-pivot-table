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

/**
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Cross-feature tests for values_axis="rows" — Commit 5.
 *
 * Verifies:
 *  - Row key structure at all depths (leaf, subtotal, grand total)
 *  - getValueFieldForRowKey returns the correct field at each depth
 *  - Column keys are dimension-only (no __vf__ encoding)
 *  - buildCellClickPayload strips the encoded segment
 *  - getGroupedRowKeys returns encoded keys with correct expansion
 *  - getRowKeys returns expanded keys
 *  - Aggregate lookups (strip __vf__ and use slice) produce correct values
 */

import { describe, expect, it } from "vitest";
import {
  PivotData,
  encodeValueFieldSegment,
  decodeValueFieldSegment,
  getValueFieldForRowKey,
  type DataRecord,
} from "./PivotData";
import { buildCellClickPayload } from "../renderers/TableRenderer";
import type { PivotConfigV1 } from "./types";

// ---------------------------------------------------------------------------
// Test data: a small 2-row-dim, 1-col-dim, 2-value-field dataset
// ---------------------------------------------------------------------------

const DATA: DataRecord[] = [
  {
    Region: "East",
    Category: "Furniture",
    Year: "2023",
    Revenue: 100,
    Units: 10,
  },
  {
    Region: "East",
    Category: "Electronics",
    Year: "2023",
    Revenue: 200,
    Units: 20,
  },
  {
    Region: "West",
    Category: "Furniture",
    Year: "2023",
    Revenue: 150,
    Units: 15,
  },
  {
    Region: "West",
    Category: "Electronics",
    Year: "2023",
    Revenue: 250,
    Units: 25,
  },
  {
    Region: "East",
    Category: "Furniture",
    Year: "2024",
    Revenue: 110,
    Units: 11,
  },
  {
    Region: "East",
    Category: "Electronics",
    Year: "2024",
    Revenue: 210,
    Units: 21,
  },
  {
    Region: "West",
    Category: "Furniture",
    Year: "2024",
    Revenue: 160,
    Units: 16,
  },
  {
    Region: "West",
    Category: "Electronics",
    Year: "2024",
    Revenue: 260,
    Units: 26,
  },
];

const BASE_CONFIG: PivotConfigV1 = {
  version: 1,
  rows: ["Region", "Category"],
  columns: ["Year"],
  values: ["Revenue", "Units"],
  aggregation: { Revenue: "sum", Units: "sum" },
  show_totals: true,
  show_row_totals: true,
  show_column_totals: true,
  empty_cell_value: "-",
  interactive: false,
};

const CONFIG_ROWS: PivotConfigV1 = {
  ...BASE_CONFIG,
  values_axis: "rows",
};

function makePivot(config: PivotConfigV1): PivotData {
  return new PivotData(DATA, config);
}

// ---------------------------------------------------------------------------
// encode/decode helpers
// ---------------------------------------------------------------------------

describe("encodeValueFieldSegment / decodeValueFieldSegment / getValueFieldForRowKey", () => {
  it("encodes a field id with the __vf__: prefix", () => {
    expect(encodeValueFieldSegment("revenue")).toBe("__vf__:revenue");
    expect(encodeValueFieldSegment("Units")).toBe("__vf__:Units");
  });

  it("decodes an encoded segment", () => {
    expect(decodeValueFieldSegment("__vf__:revenue")).toBe("revenue");
    expect(decodeValueFieldSegment("__vf__:Units")).toBe("Units");
  });

  it("returns null for non-encoded segments", () => {
    expect(decodeValueFieldSegment("Revenue")).toBeNull();
    expect(decodeValueFieldSegment("East")).toBeNull();
    expect(decodeValueFieldSegment("")).toBeNull();
  });

  it("getValueFieldForRowKey returns field from last segment", () => {
    expect(
      getValueFieldForRowKey(["East", "Furniture", "__vf__:Revenue"]),
    ).toBe("Revenue");
    expect(getValueFieldForRowKey(["East", "__vf__:Revenue"])).toBe("Revenue");
    expect(getValueFieldForRowKey(["__vf__:Revenue"])).toBe("Revenue");
  });

  it("getValueFieldForRowKey returns null for non-encoded keys", () => {
    expect(getValueFieldForRowKey(["East", "Furniture"])).toBeNull();
    expect(getValueFieldForRowKey(["East"])).toBeNull();
    expect(getValueFieldForRowKey([])).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Row key structure with values_axis="rows"
// ---------------------------------------------------------------------------

describe("getRowKeys() with values_axis='rows'", () => {
  it("returns encoded leaf keys — one per dimension combo × value field", () => {
    const pv = makePivot(CONFIG_ROWS);
    const rowKeys = pv.getRowKeys();
    // 4 dimension combos × 2 value fields = 8 row keys
    expect(rowKeys).toHaveLength(8);

    // Every key should end with an encoded value field
    for (const rk of rowKeys) {
      expect(getValueFieldForRowKey(rk)).not.toBeNull();
    }

    // Value field labels should be Revenue and Units (in order)
    const lastSegments = rowKeys.map((rk) => rk[rk.length - 1]);
    const uniqueEncoded = [...new Set(lastSegments)];
    expect(uniqueEncoded).toContain(encodeValueFieldSegment("Revenue"));
    expect(uniqueEncoded).toContain(encodeValueFieldSegment("Units"));
  });

  it("adjacent keys for same dimension combo have different value fields", () => {
    const pv = makePivot(CONFIG_ROWS);
    const rowKeys = pv.getRowKeys();
    // Revenue key followed immediately by Units key for each dim combo
    for (let i = 0; i < rowKeys.length - 1; i += 2) {
      const dimKey1 = rowKeys[i].slice(0, -1).join("|");
      const dimKey2 = rowKeys[i + 1].slice(0, -1).join("|");
      expect(dimKey1).toBe(dimKey2);
      expect(rowKeys[i][rowKeys[i].length - 1]).toBe(
        encodeValueFieldSegment("Revenue"),
      );
      expect(rowKeys[i + 1][rowKeys[i + 1].length - 1]).toBe(
        encodeValueFieldSegment("Units"),
      );
    }
  });

  it("getRowKeys without values_axis='rows' returns dimension-only keys", () => {
    const pv = makePivot(BASE_CONFIG);
    const rowKeys = pv.getRowKeys();
    expect(rowKeys).toHaveLength(4);
    for (const rk of rowKeys) {
      expect(getValueFieldForRowKey(rk)).toBeNull();
    }
  });
});

// ---------------------------------------------------------------------------
// Column keys are unchanged
// ---------------------------------------------------------------------------

describe("getColKeys() with values_axis='rows'", () => {
  it("column keys contain only dimension members — no __vf__ encoding", () => {
    const pv = makePivot(CONFIG_ROWS);
    const colKeys = pv.getColKeys();
    for (const ck of colKeys) {
      for (const seg of ck) {
        expect(decodeValueFieldSegment(seg)).toBeNull();
      }
    }
    // Should be ["2023"] and ["2024"]
    expect(colKeys).toHaveLength(2);
    expect(colKeys.map((ck) => ck[0]).sort()).toEqual(["2023", "2024"]);
  });
});

// ---------------------------------------------------------------------------
// getGroupedRowKeys expansion
// ---------------------------------------------------------------------------

describe("getGroupedRowKeys() with values_axis='rows'", () => {
  it("each data entry has an encoded value field segment", () => {
    const pvRows = makePivot(CONFIG_ROWS);
    const grouped = pvRows.getGroupedRowKeys();
    const dataEntries = grouped.filter((e) => e.type === "data");
    for (const entry of dataEntries) {
      expect(getValueFieldForRowKey(entry.key)).not.toBeNull();
    }
  });

  it("subtotal entries also have encoded value field segments", () => {
    const configWithSubtotals: PivotConfigV1 = {
      ...CONFIG_ROWS,
      show_subtotals: true,
    };
    const pv = makePivot(configWithSubtotals);
    const grouped = pv.getGroupedRowKeys();
    const subtotalEntries = grouped.filter((e) => e.type === "subtotal");
    // Should have 2 subtotals per region (one per value field) × 2 regions = 4
    expect(subtotalEntries).toHaveLength(4);
    for (const entry of subtotalEntries) {
      expect(getValueFieldForRowKey(entry.key)).not.toBeNull();
      // Strip gives the dimension prefix
      const dimKey = entry.key.slice(0, -1);
      expect(dimKey.length).toBe(1); // one region
    }
  });
});

// ---------------------------------------------------------------------------
// Aggregate lookups: strip __vf__ segment, use correct dimension key
// ---------------------------------------------------------------------------

describe("aggregate lookups with values_axis='rows' encoded keys", () => {
  it("getAggregator with stripped dimRowKey matches direct lookup", () => {
    const pv = makePivot(CONFIG_ROWS);
    const pvBase = makePivot(BASE_CONFIG);

    const rowKeys = pv.getRowKeys();
    const colKeys = pv.getColKeys();

    for (const rk of rowKeys) {
      const valField = getValueFieldForRowKey(rk)!;
      const dimRowKey = rk.slice(0, -1);
      for (const ck of colKeys) {
        const aggRows = pv.getAggregator(dimRowKey, ck, valField);
        const aggBase = pvBase.getAggregator(dimRowKey, ck, valField);
        expect(aggRows.value()).toBe(aggBase.value());
      }
    }
  });

  it("getRowTotal with stripped dimRowKey matches direct lookup", () => {
    const pv = makePivot(CONFIG_ROWS);
    const pvBase = makePivot(BASE_CONFIG);

    const rowKeys = pv.getRowKeys();
    for (const rk of rowKeys) {
      const valField = getValueFieldForRowKey(rk)!;
      const dimRowKey = rk.slice(0, -1);
      const totalRows = pv.getRowTotal(dimRowKey, valField);
      const totalBase = pvBase.getRowTotal(dimRowKey, valField);
      expect(totalRows.value()).toBe(totalBase.value());
    }
  });

  it("getGrandTotal is unaffected by values_axis setting", () => {
    const pv = makePivot(CONFIG_ROWS);
    const pvBase = makePivot(BASE_CONFIG);
    for (const vf of ["Revenue", "Units"]) {
      expect(pv.getGrandTotal(vf).value()).toBe(
        pvBase.getGrandTotal(vf).value(),
      );
    }
    // Total Revenue = 100+200+150+250+110+210+160+260 = 1440
    expect(pv.getGrandTotal("Revenue").value()).toBe(1440);
    // Total Units = 10+20+15+25+11+21+16+26 = 144
    expect(pv.getGrandTotal("Units").value()).toBe(144);
  });
});

// ---------------------------------------------------------------------------
// buildCellClickPayload strips the encoded segment
// ---------------------------------------------------------------------------

describe("buildCellClickPayload with values_axis='rows'", () => {
  it("strips the __vf__ segment and does not include it in filters", () => {
    const rowKey = ["East", "Furniture", "__vf__:Revenue"];
    const colKey = ["2023"];
    const payload = buildCellClickPayload(
      rowKey,
      colKey,
      100,
      CONFIG_ROWS,
      "Revenue",
    );

    // rowKey in payload should be the stripped dimension key
    expect(payload.rowKey).toEqual(["East", "Furniture"]);
    // filters should have dimension values, not the encoded field
    expect(payload.filters["Region"]).toBe("East");
    expect(payload.filters["Category"]).toBe("Furniture");
    expect(payload.filters["Year"]).toBe("2023");
    expect(Object.keys(payload.filters)).not.toContain("__vf__:Revenue");
    expect(payload.valueField).toBe("Revenue");
  });

  it("does NOT strip for values_axis='columns' (default)", () => {
    const rowKey = ["East", "Furniture"];
    const colKey = ["2023"];
    const payload = buildCellClickPayload(
      rowKey,
      colKey,
      100,
      BASE_CONFIG,
      "Revenue",
    );
    expect(payload.rowKey).toEqual(["East", "Furniture"]);
    expect(payload.filters["Region"]).toBe("East");
    expect(payload.filters["Category"]).toBe("Furniture");
  });

  it("handles grand total encoded key (length-1 key)", () => {
    const rowKey = ["__vf__:Revenue"];
    const colKey = ["2023"];
    const payload = buildCellClickPayload(
      rowKey,
      colKey,
      1000,
      CONFIG_ROWS,
      "Revenue",
    );
    expect(payload.rowKey).toEqual([]);
    expect(Object.keys(payload.filters)).not.toContain("Region");
  });

  it("handles subtotal encoded key (level-1 key)", () => {
    const rowKey = ["East", "__vf__:Revenue"];
    const colKey = ["2023"];
    const payload = buildCellClickPayload(
      rowKey,
      colKey,
      300,
      CONFIG_ROWS,
      "Revenue",
    );
    expect(payload.rowKey).toEqual(["East"]);
    expect(payload.filters["Region"]).toBe("East");
    expect(Object.keys(payload.filters)).not.toContain("Category");
  });
});

// ---------------------------------------------------------------------------
// Single value field — degenerate case
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// getHierarchyRowKeys() with values_axis="rows"
// ---------------------------------------------------------------------------

describe("getHierarchyRowKeys() with values_axis='rows'", () => {
  const CONFIG_HIER: PivotConfigV1 = {
    ...BASE_CONFIG,
    row_layout: "hierarchy",
    values_axis: "rows",
  };

  it("subtotal entries have __vf__ as last segment, dim key as second-to-last", () => {
    const pv = makePivot(CONFIG_HIER);
    const grouped = pv.getHierarchyRowKeys();
    const subtotals = grouped.filter((e) => e.type === "subtotal");
    expect(subtotals.length).toBeGreaterThan(0);
    for (const entry of subtotals) {
      // Last segment must be encoded value field
      expect(getValueFieldForRowKey(entry.key)).not.toBeNull();
      // Key without last segment should equal the dim-only subtotal prefix
      const dimKey = entry.key.slice(0, -1);
      expect(dimKey.length).toBe(entry.level + 1);
    }
  });

  it("data entries have __vf__ as last segment and full dim path before it", () => {
    const pv = makePivot(CONFIG_HIER);
    const grouped = pv.getHierarchyRowKeys();
    const dataEntries = grouped.filter((e) => e.type === "data");
    // 4 leaf combos × 2 value fields = 8 data entries
    expect(dataEntries).toHaveLength(8);
    for (const entry of dataEntries) {
      expect(getValueFieldForRowKey(entry.key)).not.toBeNull();
      const dimKey = entry.key.slice(0, -1);
      // Full 2-dim path
      expect(dimKey).toHaveLength(2);
    }
  });

  it("produces 2 entries per original hierarchy row (one per value field)", () => {
    // Without values_axis: 2 subtotals (East, West) + 4 leaf data rows = 6 entries.
    // With values_axis="rows": each of those × 2 value fields = 12 entries.
    const pvBase = makePivot({ ...BASE_CONFIG, row_layout: "hierarchy" });
    const pvVA = makePivot(CONFIG_HIER);
    expect(pvVA.getHierarchyRowKeys()).toHaveLength(
      pvBase.getHierarchyRowKeys().length * 2,
    );
  });

  it("collapsed group check uses dim-only key (slice(0,-1) matches collapsedSet)", () => {
    // Simulates what renderBody does: encode a subtotal entry key and verify
    // that slicing off the __vf__ segment produces the correct dim-only key
    // that would be stored in config.collapsed_groups.
    const pv = makePivot(CONFIG_HIER);
    const grouped = pv.getHierarchyRowKeys();
    const firstSubtotal = grouped.find((e) => e.type === "subtotal");
    expect(firstSubtotal).toBeDefined();
    // The dim-only key (what collapsedSet stores) should NOT contain __vf__
    const dimKey = firstSubtotal!.key.slice(0, -1);
    for (const seg of dimKey) {
      expect(seg).not.toMatch(/^__vf__:/);
    }
    // The full key (what entry.key contains) has __vf__ at the end
    expect(firstSubtotal!.key[firstSubtotal!.key.length - 1]).toMatch(
      /^__vf__:/,
    );
  });
});

describe("values_axis='rows' with single value field", () => {
  const CONFIG_SINGLE: PivotConfigV1 = {
    ...BASE_CONFIG,
    values: ["Revenue"],
    aggregation: { Revenue: "sum" },
    values_axis: "rows",
  };

  it("row keys have the encoded single value field", () => {
    const pv = makePivot(CONFIG_SINGLE);
    const rowKeys = pv.getRowKeys();
    // 4 dimension combos × 1 value = 4 keys
    expect(rowKeys).toHaveLength(4);
    for (const rk of rowKeys) {
      expect(getValueFieldForRowKey(rk)).toBe("Revenue");
    }
  });
});
