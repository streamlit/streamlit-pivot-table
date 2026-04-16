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
