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

import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import VirtualizedTableRenderer from "./VirtualizedTableRenderer";
import { PivotData, type DataRecord } from "../engine/PivotData";
import { makeConfig } from "../test-utils";

function makeRecords(numRegions: number, numYears: number): DataRecord[] {
  const records: DataRecord[] = [];
  for (let r = 0; r < numRegions; r++) {
    for (let y = 0; y < numYears; y++) {
      records.push({
        region: `R${r}`,
        year: `Y${y}`,
        revenue: (r + 1) * 100 + y * 10,
      });
    }
  }
  return records;
}

class MockResizeObserver {
  callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
  }
  observe(target: Element) {
    this.callback(
      [{ contentRect: { width: 300 } } as unknown as ResizeObserverEntry],
      this as unknown as ResizeObserver,
    );
  }
  unobserve() {}
  disconnect() {}
}

beforeEach(() => {
  vi.stubGlobal("ResizeObserver", MockResizeObserver);
});

describe("VirtualizedTableRenderer", () => {
  it("renders a virtual scroll container", () => {
    const records = makeRecords(5, 3);
    const config = makeConfig();
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    expect(screen.getByTestId("virtual-scroll-container")).toBeInTheDocument();
  });

  it("renders only a windowed subset of rows for large datasets", () => {
    const records = makeRecords(100, 3);
    const config = makeConfig();
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={200}
        rowHeight={36}
      />,
    );

    const dataRows = screen.getAllByTestId("pivot-data-row");
    expect(dataRows.length).toBeLessThan(100);
  });

  it("renders only a windowed subset of columns for wide datasets", () => {
    const records = makeRecords(3, 50);
    const config = makeConfig({ show_totals: false });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
        columnWidth={120}
      />,
    );

    const dataRows = screen.getAllByTestId("pivot-data-row");
    const dataCells = screen.getAllByTestId("pivot-data-cell");
    const cellsPerRow = dataCells.length / dataRows.length;
    expect(cellsPerRow).toBeLessThan(50);
  });

  it("respects maxColumns truncation", () => {
    const records = makeRecords(3, 20);
    const config = makeConfig();
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
        maxColumns={5}
      />,
    );

    const headers = screen.getAllByTestId("pivot-header-cell");
    const colHeaders = headers.filter((h) => h.textContent !== "Total");
    expect(colHeaders.length).toBeLessThanOrEqual(5);
  });

  it("column windowing with row dims always renders row headers and correct data columns", () => {
    const records = makeRecords(2, 30);
    const config = makeConfig({ show_totals: false });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
        columnWidth={120}
      />,
    );

    const dataRows = screen.getAllByTestId("pivot-data-row");
    const rowHeaders = screen.getAllByTestId("pivot-row-header");
    const dataCells = screen.getAllByTestId("pivot-data-cell");

    // Row headers are always rendered (1 per row, since 1 row dim)
    expect(rowHeaders.length).toBe(dataRows.length);

    // Data cells should be windowed: fewer than 30 cols * 2 rows = 60
    expect(dataCells.length).toBeLessThan(60);

    // Each row should have the same number of data cells
    const cellsPerRow = dataCells.length / dataRows.length;
    expect(cellsPerRow).toBeLessThan(30);

    // First visible data cells should contain actual values (not row header text)
    const firstCell = dataCells[0];
    const cellText = firstCell.textContent ?? "";
    // Should be a number, not a region name like "R0"
    expect(cellText).not.toMatch(/^R\d+$/);
  });

  it("renders hierarchy layout with a single row header column", () => {
    const records = [
      { region: "US", category: "A", year: "2024", revenue: 100 },
      { region: "US", category: "B", year: "2024", revenue: 150 },
      { region: "EU", category: "A", year: "2024", revenue: 200 },
    ];
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
      row_layout: "hierarchy",
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={300}
      />,
    );

    expect(
      screen.getByTestId("pivot-row-dim-label-hierarchy"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("pivot-row-dim-breadcrumb-region-0"),
    ).toBeInTheDocument();
    const firstDataRow = screen.getAllByTestId("pivot-data-row")[0];
    expect(
      firstDataRow.querySelectorAll('th[data-testid="pivot-row-header"]'),
    ).toHaveLength(1);
  });

  it("wide column counts stay DOM-bounded (500+ columns)", () => {
    const records = makeRecords(4, 600);
    const config = makeConfig({ show_totals: false });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
        columnWidth={120}
      />,
    );

    const dataRows = screen.getAllByTestId("pivot-data-row");
    const dataCells = screen.getAllByTestId("pivot-data-cell");
    const cellsPerRow = dataCells.length / dataRows.length;
    expect(cellsPerRow).toBeLessThan(600);
    expect(dataCells.length).toBeLessThan(600 * dataRows.length);
  });

  it("column windowing with multiple row dims still renders all row header columns", () => {
    // Create records with 2 row dimensions
    const records: DataRecord[] = [];
    for (let r = 0; r < 2; r++) {
      for (let s = 0; s < 2; s++) {
        for (let y = 0; y < 20; y++) {
          records.push({
            country: `C${r}`,
            state: `S${s}`,
            year: `Y${y}`,
            revenue: r * 1000 + s * 100 + y,
          });
        }
      }
    }
    const config = makeConfig({
      rows: ["country", "state"],
      columns: ["year"],
      values: ["revenue"],
      show_totals: false,
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
        columnWidth={120}
      />,
    );

    const dataRows = screen.getAllByTestId("pivot-data-row");
    const rowHeaders = screen.getAllByTestId("pivot-row-header");

    // 2 row dims * number of data rows = 2 headers per row
    expect(rowHeaders.length).toBe(dataRows.length * 2);

    // Data cells should be windowed
    const dataCells = screen.getAllByTestId("pivot-data-cell");
    const cellsPerRow = dataCells.length / dataRows.length;
    expect(cellsPerRow).toBeLessThan(20);
  });
});

describe("VirtualizedTableRenderer - column_config.help tooltips", () => {
  it("sets title on row dim header from field_help", () => {
    const records = makeRecords(3, 2);
    const config = makeConfig({
      field_help: { region: "Geographic region" },
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    const rowDimTh = screen.getByTestId("pivot-row-dim-label-region");
    expect(rowDimTh.getAttribute("title")).toBe("Geographic region");
  });

  it("sets title on measure headers from field_help", () => {
    const records = makeRecords(3, 2);
    const config = makeConfig({
      values: ["revenue"],
      columns: [],
      field_help: { revenue: "Total revenue" },
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    const headerCells = screen.getAllByTestId("pivot-header-cell");
    const measureHeader = headerCells.find(
      (cell) => cell.getAttribute("title") === "Total revenue",
    );
    expect(measureHeader).toBeDefined();
  });
});

describe("VirtualizedTableRenderer - column_config.field_widths", () => {
  // Helper: the virtual scroll content width is applied to the first direct
  // child of `virtual-scroll-container`. That value equals the sum of the
  // per-slot widths (one per column key, in single-value mode) — so asserting
  // on it verifies `field_widths` actually reached the body layout, not just
  // the header cells.
  function getContentWidth(): number {
    const container = screen.getByTestId("virtual-scroll-container");
    const inner = container.firstElementChild as HTMLElement;
    return parseInt(inner.style.width, 10);
  }

  it("seeds body column widths from field_widths in single-value mode", () => {
    // 3 year slots with a 200px width for the single measure.
    // Expected content width: 3 * 200 = 600.
    const records = makeRecords(3, 3);
    const config = makeConfig({
      values: ["revenue"],
      show_totals: false,
      field_widths: { revenue: 200 },
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    expect(getContentWidth()).toBe(600);
  });

  it("falls back to default column width when field_widths is absent", () => {
    // No field_widths configured → uniform default of 120 * 3 slots = 360.
    const records = makeRecords(3, 3);
    const config = makeConfig({
      values: ["revenue"],
      show_totals: false,
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    expect(getContentWidth()).toBe(360);
  });

  it("sums per-measure widths per slot in multi-value mode", () => {
    // Multi-value mode: each slot holds both measures; the slot width is
    // the sum of each measure's configured width. 3 slots × (180 + 100) = 840.
    const records: DataRecord[] = [];
    for (let r = 0; r < 3; r++) {
      for (let y = 0; y < 3; y++) {
        records.push({
          region: `R${r}`,
          year: `Y${y}`,
          revenue: r * 10 + y,
          profit: r + y,
        });
      }
    }
    const config = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue", "profit"],
      show_totals: false,
      field_widths: { revenue: 180, profit: 100 },
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    expect(getContentWidth()).toBe(3 * (180 + 100));
  });

  it("fills in default measure width for unconfigured measures in multi-value mode", () => {
    // Only `revenue` has a configured width; `profit` should use the
    // default columnWidth (120). 3 slots × (180 + 120) = 900.
    const records: DataRecord[] = [];
    for (let r = 0; r < 3; r++) {
      for (let y = 0; y < 3; y++) {
        records.push({
          region: `R${r}`,
          year: `Y${y}`,
          revenue: r * 10 + y,
          profit: r + y,
        });
      }
    }
    const config = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue", "profit"],
      show_totals: false,
      field_widths: { revenue: 180 },
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    expect(getContentWidth()).toBe(3 * (180 + 120));
  });

  it("applies width preset strings", () => {
    // "large" preset = 200px. 3 slots × 200 = 600.
    const records = makeRecords(3, 3);
    const config = makeConfig({
      values: ["revenue"],
      show_totals: false,
      field_widths: { revenue: "large" },
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    expect(getContentWidth()).toBe(600);
  });

  it("does not seed variable widths when field_widths has no matching measures", () => {
    // If field_widths only contains an unrelated/row-dim field, the measure
    // slots must fall back to uniform columnWidth (no variable widths array).
    // 3 slots × 120 default = 360.
    const records = makeRecords(3, 3);
    const config = makeConfig({
      values: ["revenue"],
      show_totals: false,
      field_widths: { region: 180 },
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    expect(getContentWidth()).toBe(360);
  });

  it("multi-measure with subset show_row_totals keeps slot width across all measures", () => {
    // Trickier case: `show_row_totals=["revenue"]` is a *subset* list — only
    // revenue contributes to the row-totals column. But the normal (non-total)
    // data slots still carry every measure in `values`, so the configured
    // slot width must still sum across ALL measures (revenue + profit), not
    // just the subset. The subset selection only affects *cell rendering*
    // (placeholder "–" cells for excluded measures), not the column widths.
    const records: DataRecord[] = [];
    for (let r = 0; r < 3; r++) {
      for (let y = 0; y < 3; y++) {
        records.push({
          region: `R${r}`,
          year: `Y${y}`,
          revenue: r * 10 + y,
          profit: r + y,
        });
      }
    }
    const config = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue", "profit"],
      show_row_totals: ["revenue"],
      show_totals: true,
      field_widths: { revenue: 180, profit: 100 },
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    // Body data columns: 3 year slots × (180 + 100) = 840 (subset doesn't
    // shrink the data slot width — all measures still occupy each slot).
    expect(getContentWidth()).toBe(3 * (180 + 100));

    // Per-measure value-label widths: both measures still carry their own
    // widths in the totals column header (even though profit's total cells
    // render as placeholders). This is what keeps the subset-total header
    // aligned with the non-total slots above.
    const valueLabels = screen.getAllByTestId("pivot-value-label");
    const revWidth180 = valueLabels.filter(
      (v) => (v as HTMLElement).style.width === "180px",
    );
    const profWidth100 = valueLabels.filter(
      (v) => (v as HTMLElement).style.width === "100px",
    );
    expect(revWidth180.length).toBeGreaterThan(0);
    expect(profWidth100.length).toBeGreaterThan(0);

    // Body: each data row has one real row-total (revenue) + one placeholder
    // (profit). The column layout/widths come from the header above — this
    // assertion just locks in the subset cell-rendering contract so a
    // future refactor of `getRowTotalValueFields` can't silently drop the
    // placeholder and change column count.
    const dataRows = screen.getAllByTestId("pivot-data-row");
    expect(dataRows.length).toBeGreaterThan(0);
    const firstRow = dataRows[0];
    expect(
      firstRow.querySelectorAll('td[data-testid="pivot-row-total"]'),
    ).toHaveLength(1);
    expect(
      firstRow.querySelectorAll('td[data-testid="pivot-excluded-total"]'),
    ).toHaveLength(1);
  });

  it("honors per-measure header widths on multi-value value-label row", () => {
    // Sanity check that the shared `renderColumnHeaders` helper is already
    // emitting per-measure widths in virtualized mode (same path as the
    // non-virtualized TableRenderer). If this passes while a body-width
    // test fails, we know the regression is body-only.
    const records: DataRecord[] = [];
    for (let r = 0; r < 2; r++) {
      for (let y = 0; y < 2; y++) {
        records.push({
          region: `R${r}`,
          year: `Y${y}`,
          revenue: r + y,
          profit: r * 2 + y,
        });
      }
    }
    const config = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue", "profit"],
      show_totals: false,
      field_widths: { revenue: 180 },
    });
    const pivotData = new PivotData(records, config);

    render(
      <VirtualizedTableRenderer
        pivotData={pivotData}
        config={config}
        containerHeight={400}
      />,
    );

    const valueLabels = screen.getAllByTestId("pivot-value-label");
    const revCells = valueLabels.filter(
      (v) => (v as HTMLElement).style.width === "180px",
    );
    expect(revCells.length).toBeGreaterThan(0);
  });
});
