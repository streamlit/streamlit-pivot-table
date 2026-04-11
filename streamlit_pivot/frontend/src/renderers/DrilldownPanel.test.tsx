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

import { describe, expect, it, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import DrilldownPanel from "./DrilldownPanel";
import { PivotData, type DataRecord } from "../engine/PivotData";
import type { CellClickPayload } from "../engine/types";
import { makeConfig } from "../test-utils";

const SAMPLE_DATA: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100, profit: 40 },
  { region: "US", year: "2024", revenue: 150, profit: 60 },
  { region: "EU", year: "2023", revenue: 200, profit: 80 },
  { region: "EU", year: "2024", revenue: 250, profit: 100 },
  { region: "US", year: "2023", revenue: 50, profit: 20 },
];

function makePivotData(data: DataRecord[] = SAMPLE_DATA) {
  return new PivotData(data, makeConfig());
}

function makePayload(
  overrides: Partial<CellClickPayload> = {},
): CellClickPayload {
  return {
    rowKey: ["US"],
    colKey: ["2023"],
    value: 150,
    filters: { region: "US", year: "2023" },
    ...overrides,
  };
}

describe("DrilldownPanel", () => {
  it("renders the panel with header and table", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByTestId("drilldown-panel")).toBeInTheDocument();
    expect(screen.getByTestId("drilldown-table")).toBeInTheDocument();
    expect(screen.getByText(/region: US/)).toBeInTheDocument();
  });

  it("shows correct record count", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
      />,
    );
    // US + 2023 = 2 records
    expect(screen.getByText("2 records")).toBeInTheDocument();
  });

  it("displays all columns from the source data", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
      />,
    );
    const table = screen.getByTestId("drilldown-table");
    const headers = table.querySelectorAll("th");
    const headerTexts = Array.from(headers).map((h) => h.textContent);
    expect(headerTexts).toContain("region");
    expect(headerTexts).toContain("year");
    expect(headerTexts).toContain("revenue");
    expect(headerTexts).toContain("profit");
  });

  it("calls onClose when close button is clicked", () => {
    const onClose = vi.fn();
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={onClose}
      />,
    );
    fireEvent.click(screen.getByTestId("drilldown-close"));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("calls onClose on Escape key", () => {
    const onClose = vi.fn();
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={onClose}
      />,
    );
    fireEvent.keyDown(screen.getByTestId("drilldown-panel"), { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("shows empty message when no records match", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload({ filters: { region: "APAC" } })}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByText(/No matching records found/)).toBeInTheDocument();
  });

  it("shows total key labels for totals", () => {
    const pd = makePivotData();
    const payload = makePayload({
      rowKey: ["Total"],
      colKey: ["2023"],
      filters: { year: "2023" },
    });
    render(
      <DrilldownPanel pivotData={pd} payload={payload} onClose={vi.fn()} />,
    );
    // Should show all 2023 records (3 total: 2 US + 1 EU)
    expect(screen.getByText("3 records")).toBeInTheDocument();
  });
});

describe("DrilldownPanel — server mode (hybrid drill-down)", () => {
  const SERVER_RECORDS = [
    { region: "US", year: "2023", revenue: 100, profit: 40 },
    { region: "US", year: "2023", revenue: 50, profit: 20 },
  ];
  const SERVER_COLUMNS = ["region", "year", "revenue", "profit"];

  it("renders server-provided records instead of calling pivotData", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={SERVER_RECORDS}
        serverColumns={SERVER_COLUMNS}
        serverTotalCount={2}
      />,
    );
    expect(screen.getByTestId("drilldown-table")).toBeInTheDocument();
    expect(screen.getByText("2 records")).toBeInTheDocument();
    const rows = screen
      .getByTestId("drilldown-table")
      .querySelectorAll("tbody tr");
    expect(rows).toHaveLength(2);
  });

  it("uses serverColumns for table headers", () => {
    const pd = makePivotData();
    const customCols = ["revenue", "profit"];
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={SERVER_RECORDS}
        serverColumns={customCols}
        serverTotalCount={2}
      />,
    );
    const headers = screen
      .getByTestId("drilldown-table")
      .querySelectorAll("th");
    const headerTexts = Array.from(headers).map((h) => h.textContent);
    expect(headerTexts).toEqual(["revenue", "profit"]);
  });

  it("shows capped message when serverTotalCount exceeds returned rows", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={SERVER_RECORDS}
        serverColumns={SERVER_COLUMNS}
        serverTotalCount={1200}
      />,
    );
    expect(screen.getByText("Showing 2 of 1200 records")).toBeInTheDocument();
  });

  it("shows loading state before server data arrives", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        isLoading
      />,
    );
    expect(screen.getByText(/Loading drill-down data/)).toBeInTheDocument();
    expect(screen.queryByTestId("drilldown-table")).not.toBeInTheDocument();
  });

  it("header count shows Loading while waiting for server", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        isLoading
      />,
    );
    expect(screen.getByText("Loading…")).toBeInTheDocument();
  });

  it("shows empty message when server returns zero records", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={[]}
        serverColumns={SERVER_COLUMNS}
        serverTotalCount={0}
      />,
    );
    expect(screen.getByText(/No matching records found/)).toBeInTheDocument();
  });
});

describe("DrilldownPanel — server pagination", () => {
  const PAGE_RECORDS = [
    { region: "US", year: "2023", revenue: 100, profit: 40 },
    { region: "US", year: "2023", revenue: 50, profit: 20 },
  ];
  const COLUMNS = ["region", "year", "revenue", "profit"];

  it("shows pagination controls when totalCount exceeds pageSize", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={PAGE_RECORDS}
        serverColumns={COLUMNS}
        serverTotalCount={1200}
        serverPage={0}
        serverPageSize={500}
        onPageChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("drilldown-pagination")).toBeInTheDocument();
    expect(screen.getByText("Page 1 of 3")).toBeInTheDocument();
  });

  it("shows range-based header on paginated view", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={PAGE_RECORDS}
        serverColumns={COLUMNS}
        serverTotalCount={1200}
        serverPage={0}
        serverPageSize={500}
        onPageChange={vi.fn()}
      />,
    );
    expect(screen.getByText("1–2 of 1200 records")).toBeInTheDocument();
  });

  it("disables Prev on first page", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={PAGE_RECORDS}
        serverColumns={COLUMNS}
        serverTotalCount={1200}
        serverPage={0}
        serverPageSize={500}
        onPageChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("drilldown-prev")).toBeDisabled();
    expect(screen.getByTestId("drilldown-next")).not.toBeDisabled();
  });

  it("disables Next on last page", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={PAGE_RECORDS}
        serverColumns={COLUMNS}
        serverTotalCount={1200}
        serverPage={2}
        serverPageSize={500}
        onPageChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("drilldown-next")).toBeDisabled();
    expect(screen.getByTestId("drilldown-prev")).not.toBeDisabled();
  });

  it("calls onPageChange with next page number", () => {
    const onPageChange = vi.fn();
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={PAGE_RECORDS}
        serverColumns={COLUMNS}
        serverTotalCount={1200}
        serverPage={0}
        serverPageSize={500}
        onPageChange={onPageChange}
      />,
    );
    fireEvent.click(screen.getByTestId("drilldown-next"));
    expect(onPageChange).toHaveBeenCalledWith(1);
  });

  it("calls onPageChange with previous page number", () => {
    const onPageChange = vi.fn();
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={PAGE_RECORDS}
        serverColumns={COLUMNS}
        serverTotalCount={1200}
        serverPage={1}
        serverPageSize={500}
        onPageChange={onPageChange}
      />,
    );
    fireEvent.click(screen.getByTestId("drilldown-prev"));
    expect(onPageChange).toHaveBeenCalledWith(0);
  });

  it("hides pagination when only one page exists", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={PAGE_RECORDS}
        serverColumns={COLUMNS}
        serverTotalCount={2}
        serverPage={0}
        serverPageSize={500}
        onPageChange={vi.fn()}
      />,
    );
    expect(
      screen.queryByTestId("drilldown-pagination"),
    ).not.toBeInTheDocument();
  });
});

describe("DrilldownPanel — client-side pagination", () => {
  function generateRecords(n: number): DataRecord[] {
    return Array.from({ length: n }, (_, i) => ({
      region: i % 2 === 0 ? "US" : "EU",
      year: "2023",
      revenue: 10 * (i + 1),
      profit: 5 * (i + 1),
    }));
  }

  it("shows pagination when client records exceed page size", () => {
    const records = generateRecords(600);
    const pd = makePivotData(records);
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload({ filters: {} })}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByTestId("drilldown-pagination")).toBeInTheDocument();
    expect(screen.getByText("Page 1 of 2")).toBeInTheDocument();
    expect(screen.getByText("1–500 of 600 records")).toBeInTheDocument();
  });

  it("navigates to next page via client-side pagination", () => {
    const records = generateRecords(600);
    const pd = makePivotData(records);
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload({ filters: {} })}
        onClose={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("drilldown-next"));
    expect(screen.getByText("Page 2 of 2")).toBeInTheDocument();
    expect(screen.getByText("501–600 of 600 records")).toBeInTheDocument();
    expect(screen.getByTestId("drilldown-next")).toBeDisabled();
  });

  it("navigates back to previous page", () => {
    const records = generateRecords(600);
    const pd = makePivotData(records);
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload({ filters: {} })}
        onClose={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("drilldown-next"));
    fireEvent.click(screen.getByTestId("drilldown-prev"));
    expect(screen.getByText("Page 1 of 2")).toBeInTheDocument();
    expect(screen.getByTestId("drilldown-prev")).toBeDisabled();
  });

  it("hides pagination when all records fit on one page", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload({ filters: {} })}
        onClose={vi.fn()}
      />,
    );
    expect(
      screen.queryByTestId("drilldown-pagination"),
    ).not.toBeInTheDocument();
    expect(screen.getByText("5 records")).toBeInTheDocument();
  });

  it("only shows page-sized slice of rows in the table", () => {
    const records = generateRecords(600);
    const pd = makePivotData(records);
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload({ filters: {} })}
        onClose={vi.fn()}
      />,
    );
    const rows = screen
      .getByTestId("drilldown-table")
      .querySelectorAll("tbody tr");
    expect(rows).toHaveLength(500);
  });
});
