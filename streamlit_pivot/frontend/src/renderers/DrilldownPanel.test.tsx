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

import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import DrilldownPanel, {
  type DrilldownPanelProps,
  compareDrilldownValues,
  isBlankSortValue,
} from "./DrilldownPanel";
import { PivotData, type DataRecord } from "../engine/PivotData";
import type { CellClickPayload, ColumnType } from "../engine/types";
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

function renderControlledSortPanel(
  props: Omit<
    DrilldownPanelProps,
    "sortColumn" | "sortDirection" | "onSortChange"
  >,
) {
  function Wrapper() {
    const [sortColumn, setSortColumn] = useState<string | undefined>(undefined);
    const [sortDirection, setSortDirection] = useState<
      "asc" | "desc" | undefined
    >(undefined);
    return (
      <DrilldownPanel
        {...props}
        sortColumn={sortColumn}
        sortDirection={sortDirection}
        onSortChange={(column, direction) => {
          setSortColumn(column);
          setSortDirection(direction);
        }}
      />
    );
  }

  return render(<Wrapper />);
}

function getTableRows() {
  return Array.from(
    screen.getByTestId("drilldown-table").querySelectorAll("tbody tr"),
  );
}

function getCellText(row: Element, columnIndex: number): string {
  const cell = row.querySelectorAll("td")[columnIndex];
  return cell?.textContent ?? "";
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
    expect(document.querySelector(".spinner")).toBeInTheDocument();
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
    expect(screen.getAllByText("Loading…")).toHaveLength(2);
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

  it("renders year-like integer columns without locale grouping", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={[{ region: "", year: 2024, revenue: 5, order: "N1" }]}
        serverColumns={["region", "year", "revenue", "order"]}
        serverTotalCount={1}
        columnTypes={new Map([["year", "integer"]])}
      />,
    );
    expect(screen.getByText("2024")).toBeInTheDocument();
    expect(screen.queryByText("2,024")).not.toBeInTheDocument();
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

describe("DrilldownPanel — sorting", () => {
  it("cycles header sort none -> asc -> desc -> none", () => {
    const pd = makePivotData();
    renderControlledSortPanel({
      pivotData: pd,
      payload: makePayload({ filters: {} }),
      onClose: vi.fn(),
    });

    const revenueHeader = screen.getByTestId("drilldown-sort-revenue");
    const getFirstRevenue = () => getCellText(getTableRows()[0]!, 2);

    expect(getFirstRevenue()).toBe("100");

    fireEvent.click(revenueHeader);
    expect(screen.getByTestId("sort-indicator-asc")).toBeInTheDocument();
    expect(getFirstRevenue()).toBe("50");

    fireEvent.click(revenueHeader);
    expect(screen.getByTestId("sort-indicator-desc")).toBeInTheDocument();
    expect(getFirstRevenue()).toBe("250");

    fireEvent.click(revenueHeader);
    expect(screen.queryByTestId("sort-indicator-asc")).not.toBeInTheDocument();
    expect(screen.queryByTestId("sort-indicator-desc")).not.toBeInTheDocument();
    expect(getFirstRevenue()).toBe("100");
  });

  it("clicking a different header starts a new ascending sort", () => {
    const pd = makePivotData();
    renderControlledSortPanel({
      pivotData: pd,
      payload: makePayload({ filters: {} }),
      onClose: vi.fn(),
    });

    fireEvent.click(screen.getByTestId("drilldown-sort-revenue"));
    expect(screen.getByTestId("sort-indicator-asc")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("drilldown-sort-profit"));
    expect(screen.getByTestId("sort-indicator-asc")).toBeInTheDocument();
    expect(getCellText(getTableRows()[0]!, 3)).toBe("20");
  });

  it("sorts client-side results before pagination", () => {
    const records = Array.from({ length: 600 }, (_, i) => ({
      region: i % 2 === 0 ? "US" : "EU",
      year: "2023",
      revenue: 10 * (i + 1),
      profit: 5 * (i + 1),
    }));
    const pd = makePivotData(records);
    renderControlledSortPanel({
      pivotData: pd,
      payload: makePayload({ filters: {} }),
      onClose: vi.fn(),
    });

    const revenueHeader = screen.getByTestId("drilldown-sort-revenue");
    fireEvent.click(revenueHeader);
    fireEvent.click(revenueHeader);

    expect(getCellText(getTableRows()[0]!, 2)).toBe("6,000");

    fireEvent.click(screen.getByTestId("drilldown-next"));
    expect(screen.getByText("Page 2 of 2")).toBeInTheDocument();
    expect(getCellText(getTableRows()[0]!, 2)).toBe("1,000");
  });

  it("calls onSortChange with hybrid sort metadata", () => {
    const onSortChange = vi.fn();
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        sortColumn={undefined}
        sortDirection={undefined}
        onSortChange={onSortChange}
        serverRecords={[
          { region: "US", year: "2023", revenue: 100, profit: 40 },
          { region: "US", year: "2023", revenue: 50, profit: 20 },
        ]}
        serverColumns={["region", "year", "revenue", "profit"]}
        serverTotalCount={2}
      />,
    );

    fireEvent.click(screen.getByTestId("drilldown-sort-revenue"));
    expect(onSortChange).toHaveBeenCalledWith("revenue", "asc");
  });
});

describe("isBlankSortValue", () => {
  it.each([
    [null, true],
    [undefined, true],
    ["", true],
    [NaN, true],
    [0, false],
    [false, false],
    ["hello", false],
    [" ", false],
  ])("isBlankSortValue(%j) === %s", (input, expected) => {
    expect(isBlankSortValue(input)).toBe(expected);
  });
});

describe("compareDrilldownValues", () => {
  it("sorts numbers numerically", () => {
    expect(compareDrilldownValues(2, 10)).toBeLessThan(0);
    expect(compareDrilldownValues(10, 2)).toBeGreaterThan(0);
    expect(compareDrilldownValues(5, 5)).toBe(0);
  });

  it("sorts strings lexicographically with numeric awareness", () => {
    expect(compareDrilldownValues("apple", "banana")).toBeLessThan(0);
    expect(compareDrilldownValues("banana", "apple")).toBeGreaterThan(0);
    expect(compareDrilldownValues("item2", "item10")).toBeLessThan(0);
  });

  it("sorts booleans (false < true)", () => {
    expect(compareDrilldownValues(false, true)).toBeLessThan(0);
    expect(compareDrilldownValues(true, false)).toBeGreaterThan(0);
    expect(compareDrilldownValues(true, true)).toBe(0);
  });

  it("pushes null/undefined/NaN/empty last regardless of order", () => {
    expect(compareDrilldownValues(null, 5)).toBeGreaterThan(0);
    expect(compareDrilldownValues(5, null)).toBeLessThan(0);
    expect(compareDrilldownValues(undefined, "a")).toBeGreaterThan(0);
    expect(compareDrilldownValues("a", undefined)).toBeLessThan(0);
    expect(compareDrilldownValues(NaN, 1)).toBeGreaterThan(0);
    expect(compareDrilldownValues("", "a")).toBeGreaterThan(0);
  });

  it("treats two blanks as equal", () => {
    expect(compareDrilldownValues(null, undefined)).toBe(0);
    expect(compareDrilldownValues("", NaN)).toBe(0);
  });

  it("sorts ISO date strings chronologically with datetime columnType", () => {
    const ct: ColumnType = "datetime";
    expect(
      compareDrilldownValues(
        "2024-01-15T00:00:00Z",
        "2024-03-01T00:00:00Z",
        ct,
      ),
    ).toBeLessThan(0);
    expect(
      compareDrilldownValues(
        "2024-12-31T00:00:00Z",
        "2024-01-01T00:00:00Z",
        ct,
      ),
    ).toBeGreaterThan(0);
  });

  it("sorts ISO date strings chronologically with date columnType", () => {
    const ct: ColumnType = "date";
    expect(compareDrilldownValues("2024-01-01", "2024-06-15", ct)).toBeLessThan(
      0,
    );
  });

  it("uses numeric coercion when columnType is integer or float", () => {
    expect(compareDrilldownValues("9", "10", "integer")).toBeLessThan(0);
    expect(compareDrilldownValues("2.5", "1.1", "float")).toBeGreaterThan(0);
  });

  it("falls back to string comparison without columnType", () => {
    expect(compareDrilldownValues("9", "10")).toBeLessThan(0);
  });
});

describe("DrilldownPanel — sort edge cases", () => {
  it("disables sort buttons while isLoading is true", () => {
    const pd = makePivotData();
    const onSortChange = vi.fn();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        sortColumn={undefined}
        sortDirection={undefined}
        onSortChange={onSortChange}
        serverRecords={[
          { region: "US", year: "2023", revenue: 100, profit: 40 },
        ]}
        serverColumns={["region", "year", "revenue", "profit"]}
        serverTotalCount={100}
        serverPage={0}
        serverPageSize={500}
        onPageChange={vi.fn()}
        isLoading
      />,
    );
    // Loading state hides the table, so sort buttons are not rendered
    expect(
      screen.queryByTestId("drilldown-sort-revenue"),
    ).not.toBeInTheDocument();
  });

  it("does not sort when onSortChange is not provided", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload({ filters: {} })}
        onClose={vi.fn()}
      />,
    );
    const rows = getTableRows();
    const firstRevenue = getCellText(rows[0]!, 2);
    fireEvent.click(screen.getByTestId("drilldown-sort-revenue"));
    expect(getCellText(getTableRows()[0]!, 2)).toBe(firstRevenue);
  });

  it("resets to page 1 when sort changes on a paginated panel", () => {
    const records = Array.from({ length: 600 }, (_, i) => ({
      region: "US",
      year: "2023",
      revenue: i + 1,
      profit: i + 1,
    }));
    const pd = makePivotData(records);
    renderControlledSortPanel({
      pivotData: pd,
      payload: makePayload({ filters: {} }),
      onClose: vi.fn(),
    });

    fireEvent.click(screen.getByTestId("drilldown-next"));
    expect(screen.getByText("Page 2 of 2")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("drilldown-sort-revenue"));
    expect(screen.getByText("Page 1 of 2")).toBeInTheDocument();
  });
});

describe("DrilldownPanel — number formatting", () => {
  it("applies numberFormat patterns to numeric cells", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        numberFormat={{ revenue: "$,.0f", profit: "$,.0f" }}
      />,
    );
    const table = screen.getByTestId("drilldown-table");
    const cells = table.querySelectorAll("tbody td");
    const texts = Array.from(cells).map((c) => c.textContent);
    expect(texts).toContain("$100");
    expect(texts).toContain("$40");
  });

  it("applies __all__ fallback format to all numeric cells", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        numberFormat={{ __all__: ",.2f" }}
      />,
    );
    const table = screen.getByTestId("drilldown-table");
    const cells = table.querySelectorAll("tbody td");
    const texts = Array.from(cells).map((c) => c.textContent);
    expect(texts).toContain("100.00");
    expect(texts).toContain("40.00");
  });

  it("formats numbers with locale grouping when no pattern is set", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
      />,
    );
    const table = screen.getByTestId("drilldown-table");
    const cells = table.querySelectorAll("tbody td");
    const texts = Array.from(cells).map((c) => c.textContent);
    // formatNumber(100) -> "100", formatNumber(40) -> "40"
    expect(texts).toContain("100");
    expect(texts).toContain("40");
  });
});

describe("DrilldownPanel — cell alignment", () => {
  it("right-aligns numeric cells by default", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
      />,
    );
    const table = screen.getByTestId("drilldown-table");
    const row = table.querySelector("tbody tr")!;
    const cells = row.querySelectorAll("td");
    // columns: region, year, revenue, profit
    // region (string) -> no explicit alignment
    expect(cells[0].style.textAlign).toBe("");
    // year (string) -> no explicit alignment
    expect(cells[1].style.textAlign).toBe("");
    // revenue (number) -> right
    expect(cells[2].style.textAlign).toBe("right");
    // profit (number) -> right
    expect(cells[3].style.textAlign).toBe("right");
  });

  it("respects explicit columnAlignment overrides", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        columnAlignment={{ revenue: "center", region: "right" }}
      />,
    );
    const table = screen.getByTestId("drilldown-table");
    const row = table.querySelector("tbody tr")!;
    const cells = row.querySelectorAll("td");
    expect(cells[0].style.textAlign).toBe("right"); // region override
    expect(cells[2].style.textAlign).toBe("center"); // revenue override
  });

  it("does not right-align year-like integer columns", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
        serverRecords={[{ region: "", year: 2024, revenue: 5, order: "N1" }]}
        serverColumns={["region", "year", "revenue", "order"]}
        serverTotalCount={1}
        columnTypes={new Map([["year", "integer"]])}
      />,
    );
    const row = screen
      .getByTestId("drilldown-table")
      .querySelector("tbody tr")!;
    const cells = row.querySelectorAll("td");
    expect(cells[1].style.textAlign).toBe("");
  });
});

describe("DrilldownPanel — column dividers", () => {
  it("renders border-right on th and td elements (except last)", () => {
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
    const cells = table.querySelectorAll("tbody td");
    // Headers and cells should have the CSS class that includes border-right.
    // We verify the elements exist and are rendered with the expected count.
    expect(headers.length).toBe(4);
    expect(cells.length).toBeGreaterThan(0);
  });
});
