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
import { render, screen, fireEvent, act } from "@testing-library/react";
import TableRenderer, {
  computeRowHeaderSpans,
  computeColSlots,
  renderDataRow,
  renderTotalsRow,
  renderSubtotalRow,
} from "./TableRenderer";
import { PivotData, type DataRecord } from "../engine/PivotData";
import type { PivotConfigV1 } from "../engine/types";
import { makeConfig } from "../test-utils";

const SAMPLE_DATA: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100, profit: 40 },
  { region: "US", year: "2024", revenue: 150, profit: 60 },
  { region: "EU", year: "2023", revenue: 200, profit: 80 },
  { region: "EU", year: "2024", revenue: 250, profit: 100 },
];

function createPivotData(
  data: DataRecord[] = SAMPLE_DATA,
  config: PivotConfigV1 = makeConfig(),
): PivotData {
  return new PivotData(data, config);
}

describe("TableRenderer - rendering", () => {
  it("renders a table with data-testid", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    expect(screen.getByTestId("pivot-table")).toBeInTheDocument();
  });

  it("renders empty state when no data", () => {
    const pd = createPivotData([], makeConfig());
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    expect(screen.getByTestId("pivot-table-empty")).toBeInTheDocument();
    expect(screen.getByText(/No data to display/)).toBeInTheDocument();
  });

  it("renders column headers", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    const headers = screen.getAllByTestId("pivot-header-cell");
    expect(headers.length).toBeGreaterThanOrEqual(2);
    expect(headers[0]).toHaveTextContent("2023");
    expect(headers[1]).toHaveTextContent("2024");
  });

  it("renders row headers", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    const rowHeaders = screen.getAllByTestId("pivot-row-header");
    expect(rowHeaders).toHaveLength(2);
    expect(rowHeaders[0]).toHaveTextContent("EU");
    expect(rowHeaders[1]).toHaveTextContent("US");
  });

  it("renders data cells with correct values", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    const cells = screen.getAllByTestId("pivot-data-cell");
    expect(cells.length).toBeGreaterThanOrEqual(4);
    const cellValues = cells.slice(0, 4).map((c) => c.textContent);
    expect(cellValues).toEqual(["200", "250", "100", "150"]);
  });

  it("renders totals row when show_totals is true", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    expect(screen.getByTestId("pivot-totals-row")).toBeInTheDocument();
    expect(screen.getByText("Grand Total")).toBeInTheDocument();
  });

  it("omits totals row when show_totals is false", () => {
    const config = makeConfig({ show_totals: false });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    expect(screen.queryByTestId("pivot-totals-row")).not.toBeInTheDocument();
  });
});

describe("TableRenderer - multiple values", () => {
  it("renders value label row for multiple values", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    const valueLabels = screen.getAllByTestId("pivot-value-label");
    expect(valueLabels.length).toBeGreaterThanOrEqual(4);
  });
});

describe("TableRenderer - empty cell value", () => {
  it("displays configured empty cell value", () => {
    const data: DataRecord[] = [{ region: "US", year: "2023", revenue: 100 }];
    const config = makeConfig({ empty_cell_value: "N/A" });
    const pd = new PivotData(data, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    const cells = screen.getAllByTestId("pivot-data-cell");
    const hasNA = cells.some((c) => c.textContent === "N/A");
    expect(hasNA).toBe(false);
  });
});

describe("TableRenderer - cell click", () => {
  it("fires onCellClick with correct payload", () => {
    const handleClick = vi.fn();
    const pd = createPivotData();
    render(
      <TableRenderer
        pivotData={pd}
        config={makeConfig()}
        onCellClick={handleClick}
      />,
    );
    const cells = screen.getAllByTestId("pivot-data-cell");
    fireEvent.click(cells[0]);
    expect(handleClick).toHaveBeenCalledTimes(1);
    const payload = handleClick.mock.calls[0][0];
    expect(payload).toHaveProperty("rowKey");
    expect(payload).toHaveProperty("colKey");
    expect(payload).toHaveProperty("value");
    expect(payload).toHaveProperty("filters");
  });

  it("fires onCellClick on Enter key", () => {
    const handleClick = vi.fn();
    const pd = createPivotData();
    render(
      <TableRenderer
        pivotData={pd}
        config={makeConfig()}
        onCellClick={handleClick}
      />,
    );
    const cells = screen.getAllByTestId("pivot-data-cell");
    fireEvent.keyDown(cells[0], { key: "Enter" });
    expect(handleClick).toHaveBeenCalledTimes(1);
  });

  it("fires onCellClick on Space key", () => {
    const handleClick = vi.fn();
    const pd = createPivotData();
    render(
      <TableRenderer
        pivotData={pd}
        config={makeConfig()}
        onCellClick={handleClick}
      />,
    );
    const cells = screen.getAllByTestId("pivot-data-cell");
    fireEvent.keyDown(cells[0], { key: " " });
    expect(handleClick).toHaveBeenCalledTimes(1);
  });

  it("does not fire when onCellClick is undefined", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    const cells = screen.getAllByTestId("pivot-data-cell");
    expect(() => fireEvent.click(cells[0])).not.toThrow();
  });

  it("includes synthetic measure id in cell click payload", () => {
    const handleClick = vi.fn();
    const config = makeConfig({
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "rev_minus_profit",
          label: "Revenue - Profit",
          operation: "difference",
          numerator: "revenue",
          denominator: "profit",
        },
      ],
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCellClick={handleClick}
      />,
    );
    const cells = screen.getAllByTestId("pivot-data-cell");
    fireEvent.click(cells[1]);
    const payload = handleClick.mock.calls[0][0];
    expect(payload.valueField).toBe("rev_minus_profit");
  });

  it("applies synthetic measure-specific format when provided", () => {
    const config = makeConfig({
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "rev_per_profit",
          label: "Revenue / Profit",
          operation: "sum_over_sum",
          numerator: "revenue",
          denominator: "profit",
          format: ".1%",
        },
      ],
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    const cells = screen.getAllByTestId("pivot-data-cell");
    const syntheticCellText = cells[1].textContent ?? "";
    expect(syntheticCellText).toContain("%");
  });
});

describe("TableRenderer - accessibility", () => {
  it("uses semantic table structure", () => {
    const pd = createPivotData();
    const { container } = render(
      <TableRenderer pivotData={pd} config={makeConfig()} />,
    );
    expect(container.querySelector("thead")).toBeInTheDocument();
    expect(container.querySelector("tbody")).toBeInTheDocument();
    expect(container.querySelector("th[scope='col']")).toBeInTheDocument();
    expect(container.querySelector("th[scope='row']")).toBeInTheDocument();
  });

  it("data cells have role=gridcell and tabIndex when onCellClick provided", () => {
    const pd = createPivotData();
    render(
      <TableRenderer
        pivotData={pd}
        config={makeConfig()}
        onCellClick={vi.fn()}
      />,
    );
    const cells = screen.getAllByTestId("pivot-data-cell");
    for (const cell of cells.slice(0, 4)) {
      expect(cell).toHaveAttribute("role", "gridcell");
      expect(cell).toHaveAttribute("tabindex", "0");
    }
  });

  it("data cells omit role and tabIndex when onCellClick not provided", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    const cells = screen.getAllByTestId("pivot-data-cell");
    for (const cell of cells.slice(0, 4)) {
      expect(cell).not.toHaveAttribute("role");
      expect(cell).not.toHaveAttribute("tabindex");
    }
  });

  it("table has role=grid", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    expect(screen.getByTestId("pivot-table")).toHaveAttribute("role", "grid");
  });
});

describe("TableRenderer - corner cell / row dimension labels", () => {
  it("shows individual row dimension label in header", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    expect(screen.getByTestId("pivot-row-dim-label-region")).toHaveTextContent(
      "region",
    );
  });

  it("shows separate labels for each row dimension", () => {
    const config = makeConfig({ rows: ["region", "year"], columns: [] });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    expect(screen.getByTestId("pivot-row-dim-label-region")).toHaveTextContent(
      "region",
    );
    expect(screen.getByTestId("pivot-row-dim-label-year")).toHaveTextContent(
      "year",
    );
  });

  it("does not render sort buttons in corner cell", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    expect(screen.queryByTestId("sort-row-toggle")).not.toBeInTheDocument();
    expect(screen.queryByTestId("sort-col-toggle")).not.toBeInTheDocument();
  });
});

describe("TableRenderer - header menu triggers", () => {
  it("renders menu trigger on corner cell when interactive callbacks provided", () => {
    const pd = createPivotData();
    render(
      <TableRenderer
        pivotData={pd}
        config={makeConfig()}
        onSortChange={vi.fn()}
        onFilterChange={vi.fn()}
      />,
    );
    expect(
      screen.getByTestId("header-menu-trigger-region"),
    ).toBeInTheDocument();
  });

  it("renders menu trigger on column header cells when interactive", () => {
    const pd = createPivotData();
    render(
      <TableRenderer
        pivotData={pd}
        config={makeConfig()}
        onSortChange={vi.fn()}
        onFilterChange={vi.fn()}
      />,
    );
    const triggers = screen.getAllByTestId("header-menu-trigger-year");
    expect(triggers.length).toBeGreaterThanOrEqual(1);
  });

  it("does not render menu triggers without sort/filter callbacks", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);
    expect(
      screen.queryByTestId("header-menu-trigger-region"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("header-menu-trigger-year"),
    ).not.toBeInTheDocument();
  });

  it("opens header menu when trigger is clicked", () => {
    const pd = createPivotData();
    render(
      <TableRenderer
        pivotData={pd}
        config={makeConfig()}
        onSortChange={vi.fn()}
        onFilterChange={vi.fn()}
      />,
    );
    const trigger = screen.getAllByTestId("header-menu-trigger-year")[0];
    fireEvent.click(trigger);
    expect(screen.getByTestId("header-menu-year")).toBeInTheDocument();
  });
});

describe("TableRenderer - visual state cues", () => {
  it("shows a row sort indicator when row_sort is active", () => {
    const config = makeConfig({ row_sort: { by: "key", direction: "desc" } });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    expect(screen.getAllByTestId("sort-indicator-desc").length).toBeGreaterThan(
      0,
    );
  });

  it("shows a column sort indicator when col_sort is active", () => {
    const config = makeConfig({ col_sort: { by: "key", direction: "asc" } });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    expect(screen.getAllByTestId("sort-indicator-asc").length).toBeGreaterThan(
      0,
    );
  });
});

describe("computeRowHeaderSpans", () => {
  it("returns spans for single-dim rows (all 1s)", () => {
    const spans = computeRowHeaderSpans([["US"], ["EU"]]);
    expect(spans).toEqual([[1], [1]]);
  });

  it("groups first dimension when values repeat across sub-rows", () => {
    const spans = computeRowHeaderSpans([
      ["EU", "2023"],
      ["EU", "2024"],
      ["US", "2023"],
      ["US", "2024"],
    ]);
    expect(spans[0]).toEqual([2, 1]); // EU spans 2 rows, 2023 is span 1
    expect(spans[1]).toEqual([0, 1]); // EU skipped, 2024 is span 1
    expect(spans[2]).toEqual([2, 1]); // US spans 2 rows
    expect(spans[3]).toEqual([0, 1]); // US skipped
  });

  it("handles 3 dimension levels", () => {
    const spans = computeRowHeaderSpans([
      ["US", "East", "Q1"],
      ["US", "East", "Q2"],
      ["US", "West", "Q1"],
      ["EU", "North", "Q1"],
    ]);
    expect(spans[0]).toEqual([3, 2, 1]); // US spans 3, East spans 2, Q1 spans 1
    expect(spans[1]).toEqual([0, 0, 1]); // US/East skipped, Q2 spans 1
    expect(spans[2]).toEqual([0, 1, 1]); // US skipped, West spans 1
    expect(spans[3]).toEqual([1, 1, 1]); // EU spans 1
  });

  it("returns empty array for empty input", () => {
    expect(computeRowHeaderSpans([])).toEqual([]);
  });
});

describe("TableRenderer - row header spanning", () => {
  it("groups row headers when multiple row dimensions", () => {
    const config = makeConfig({ rows: ["region", "year"] });
    const pd = createPivotData(SAMPLE_DATA, config);
    const { container } = render(
      <TableRenderer pivotData={pd} config={config} />,
    );
    const rowHeaders = container.querySelectorAll("th[scope='row']");
    const spannedHeaders = Array.from(rowHeaders).filter((th) =>
      th.hasAttribute("rowspan"),
    );
    expect(spannedHeaders.length).toBeGreaterThanOrEqual(1);
    expect(spannedHeaders[0]).toHaveAttribute("rowspan", "2");
  });

  it("does not add rowSpan for single row dimension", () => {
    const config = makeConfig({ rows: ["region"] });
    const pd = createPivotData(SAMPLE_DATA, config);
    const { container } = render(
      <TableRenderer pivotData={pd} config={config} />,
    );
    const rowHeaders = container.querySelectorAll("th[scope='row']");
    const spannedHeaders = Array.from(rowHeaders).filter((th) =>
      th.hasAttribute("rowspan"),
    );
    expect(spannedHeaders.length).toBe(0);
  });

  it("pins the first row header column for horizontal context", () => {
    const pd = createPivotData(
      SAMPLE_DATA,
      makeConfig({ rows: ["region", "year"] }),
    );
    const { container } = render(
      <TableRenderer
        pivotData={pd}
        config={makeConfig({ rows: ["region", "year"] })}
      />,
    );
    const firstPinned = container.querySelector("th[data-dim-index='0']");
    expect(firstPinned).toBeInTheDocument();
    expect(firstPinned?.className).toContain("rowHeaderCellPinned");
  });
});

describe("TableRenderer - border fix (Total header with multiple values)", () => {
  it("renders value labels under Total header when hasMultipleValues", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    const valueLabels = screen.getAllByTestId("pivot-value-label");
    const totalValueLabels = valueLabels.filter(
      (el) =>
        el.textContent?.includes("revenue") ||
        el.textContent?.includes("profit"),
    );
    expect(totalValueLabels.length).toBeGreaterThanOrEqual(6);
  });
});

describe("TableRenderer - value label menu triggers", () => {
  const noop = vi.fn();

  it("renders menu triggers on value labels when interactive", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onSortChange={noop}
        onFilterChange={noop}
        onShowValuesAsChange={noop}
      />,
    );
    const triggers = screen.getAllByTestId(
      /header-menu-trigger-revenue|header-menu-trigger-profit/,
    );
    expect(triggers.length).toBeGreaterThanOrEqual(2);
  });

  it("does not show % badge on value labels (badge only in toolbar)", () => {
    const config = makeConfig({
      values: ["revenue", "profit"],
      show_values_as: { revenue: "pct_of_total" },
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onSortChange={noop}
        onFilterChange={noop}
        onShowValuesAsChange={noop}
      />,
    );
    const badges = screen.queryAllByText("%");
    expect(badges.length).toBe(0);
  });

  it("hides show-values-as menu for synthetic measures", () => {
    const config = makeConfig({
      values: ["revenue"],
      synthetic_measures: [
        {
          id: "rev_minus_profit",
          label: "Revenue - Profit",
          operation: "difference",
          numerator: "revenue",
          denominator: "profit",
        },
      ],
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onSortChange={noop}
        onFilterChange={noop}
        onShowValuesAsChange={noop}
      />,
    );
    fireEvent.click(
      screen.getAllByTestId("header-menu-trigger-rev_minus_profit")[0],
    );
    expect(screen.queryByTestId("header-menu-display")).not.toBeInTheDocument();
  });
});

describe("TableRenderer - subtotal hierarchy cues", () => {
  it("renders subtotal rows with hierarchy level attributes", () => {
    const config = makeConfig({
      rows: ["region", "year"],
      show_subtotals: true,
      columns: [],
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    const subtotalRows = screen.getAllByTestId("pivot-subtotal-row");
    expect(subtotalRows.length).toBeGreaterThan(0);
    expect(subtotalRows[0]).toHaveAttribute("data-level");
  });
});

describe("TableRenderer - redundant toggle removal", () => {
  it("data rows have no group toggle when subtotals on; toggle only on subtotal rows", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    const subtotalRows = screen.getAllByTestId("pivot-subtotal-row");
    expect(subtotalRows.length).toBeGreaterThan(0);
    const groupToggles = screen.getAllByTestId(/^pivot-group-toggle-/);
    expect(groupToggles.length).toBe(subtotalRows.length);
    const dataRows = screen.getAllByTestId("pivot-data-row");
    dataRows.forEach((row) => {
      const toggleInRow = row.querySelector(
        "[data-testid^='pivot-group-toggle-']",
      );
      expect(toggleInRow).toBeNull();
    });
  });

  it("Category-level groups have toggle on data row when show_subtotals only Region", () => {
    const config = makeConfig({
      rows: ["region", "category", "product"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: ["region"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    const groupToggles = screen.getAllByTestId(/^pivot-group-toggle-/);
    expect(groupToggles.length).toBeGreaterThan(0);
    const dataRows = screen.getAllByTestId("pivot-data-row");
    const togglesInDataRows = groupToggles.filter((toggle) =>
      dataRows.some((row) => row.contains(toggle)),
    );
    expect(togglesInDataRows.length).toBeGreaterThan(0);
  });

  it("clicking subtotal row toggle calls onCollapseChange with group key", () => {
    const onCollapseChange = vi.fn();
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={onCollapseChange}
      />,
    );
    const subtotalRows = screen.getAllByTestId("pivot-subtotal-row");
    expect(subtotalRows.length).toBeGreaterThan(0);
    const firstSubtotalToggle =
      screen.getAllByTestId(/^pivot-group-toggle-/)[0];
    expect(firstSubtotalToggle).toBeInTheDocument();
    fireEvent.click(firstSubtotalToggle);
    expect(onCollapseChange).toHaveBeenCalledTimes(1);
    expect(onCollapseChange).toHaveBeenCalledWith("row", expect.any(Array));
    const collapsed = onCollapseChange.mock.calls[0][1];
    expect(collapsed.length).toBeGreaterThan(0);
  });

  it("3-level hierarchy with full subtotals has no toggle on data rows", () => {
    const config = makeConfig({
      rows: ["region", "category", "product"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    const dataRows = screen.getAllByTestId("pivot-data-row");
    dataRows.forEach((row) => {
      const toggleInRow = row.querySelector(
        "[data-testid^='pivot-group-toggle-']",
      );
      expect(toggleInRow).toBeNull();
    });
  });
});

// ---------------------------------------------------------------------------
// computeColSlots
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Dimension-level collapse toggles
// ---------------------------------------------------------------------------

const MULTI_DIM_DATA: DataRecord[] = [
  {
    region: "US",
    category: "A",
    product: "P1",
    year: "2023",
    quarter: "Q1",
    revenue: 10,
  },
  {
    region: "US",
    category: "A",
    product: "P2",
    year: "2023",
    quarter: "Q2",
    revenue: 20,
  },
  {
    region: "US",
    category: "B",
    product: "P3",
    year: "2024",
    quarter: "Q1",
    revenue: 30,
  },
  {
    region: "EU",
    category: "A",
    product: "P1",
    year: "2023",
    quarter: "Q1",
    revenue: 40,
  },
  {
    region: "EU",
    category: "B",
    product: "P2",
    year: "2024",
    quarter: "Q2",
    revenue: 50,
  },
];

describe("Dimension toggle - row axis visibility", () => {
  it("shows toggle on non-innermost row dims when subtotals enabled and 2+ rows", () => {
    const config = makeConfig({
      rows: ["region", "category", "product"],
      columns: [],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    expect(
      screen.getByTestId("pivot-dim-toggle-row-0-region"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("pivot-dim-toggle-row-1-category"),
    ).toBeInTheDocument();
    expect(
      screen.queryByTestId(/pivot-dim-toggle-row-2/),
    ).not.toBeInTheDocument();
  });

  it("hides toggle when only one row dimension", () => {
    const config = makeConfig({
      rows: ["region"],
      columns: [],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    expect(
      screen.queryByTestId(/pivot-dim-toggle-row/),
    ).not.toBeInTheDocument();
  });

  it("hides toggle when subtotals disabled", () => {
    const config = makeConfig({ rows: ["region", "category"], columns: [] });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    expect(
      screen.queryByTestId(/pivot-dim-toggle-row/),
    ).not.toBeInTheDocument();
  });

  it("hides toggle when no onCollapseChange", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: [],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);
    expect(
      screen.queryByTestId(/pivot-dim-toggle-row/),
    ).not.toBeInTheDocument();
  });
});

describe("Dimension toggle - column axis visibility", () => {
  it("shows toggle on non-last column dims when 2+ columns", () => {
    const config = makeConfig({
      rows: ["region"],
      columns: ["year", "quarter"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    expect(
      screen.getByTestId("pivot-dim-toggle-col-0-year"),
    ).toBeInTheDocument();
    expect(
      screen.queryByTestId(/pivot-dim-toggle-col-1/),
    ).not.toBeInTheDocument();
  });

  it("hides toggle when only one column dimension", () => {
    const config = makeConfig({ rows: ["region"], columns: ["year"] });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    expect(
      screen.queryByTestId(/pivot-dim-toggle-col/),
    ).not.toBeInTheDocument();
  });
});

describe("Dimension toggle - click behavior", () => {
  it("collapses all groups at a row dim level on click", () => {
    const onCollapseChange = vi.fn();
    const config = makeConfig({
      rows: ["region", "category"],
      columns: [],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={onCollapseChange}
      />,
    );
    fireEvent.click(screen.getByTestId("pivot-dim-toggle-row-0-region"));
    expect(onCollapseChange).toHaveBeenCalledTimes(1);
    expect(onCollapseChange).toHaveBeenCalledWith("row", expect.any(Array));
    const collapsed = onCollapseChange.mock.calls[0][1];
    expect(collapsed).toContain("EU");
    expect(collapsed).toContain("US");
  });

  it("expands all groups when already collapsed", () => {
    const onCollapseChange = vi.fn();
    const config = makeConfig({
      rows: ["region", "category"],
      columns: [],
      show_subtotals: true,
      collapsed_groups: ["EU", "US"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={onCollapseChange}
      />,
    );
    fireEvent.click(screen.getByTestId("pivot-dim-toggle-row-0-region"));
    expect(onCollapseChange).toHaveBeenCalledTimes(1);
    expect(onCollapseChange).toHaveBeenCalledWith("row", []);
  });

  it("collapses column dimension on click", () => {
    const onCollapseChange = vi.fn();
    const config = makeConfig({
      rows: ["region"],
      columns: ["year", "quarter"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={onCollapseChange}
      />,
    );
    fireEvent.click(screen.getByTestId("pivot-dim-toggle-col-0-year"));
    expect(onCollapseChange).toHaveBeenCalledTimes(1);
    expect(onCollapseChange).toHaveBeenCalledWith("col", expect.any(Array));
    const collapsed = onCollapseChange.mock.calls[0][1];
    expect(collapsed).toContain("2023");
    expect(collapsed).toContain("2024");
  });

  it("normalizes __ALL__ before toggling", () => {
    const onCollapseChange = vi.fn();
    const config = makeConfig({
      rows: ["region", "category"],
      columns: [],
      show_subtotals: true,
      collapsed_groups: ["__ALL__"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={onCollapseChange}
      />,
    );
    fireEvent.click(screen.getByTestId("pivot-dim-toggle-row-0-region"));
    expect(onCollapseChange).toHaveBeenCalledTimes(1);
    const collapsed = onCollapseChange.mock.calls[0][1];
    expect(collapsed).not.toContain("__ALL__");
    expect(collapsed).toEqual([]);
  });
});

describe("Dimension toggle - aria attributes", () => {
  it("sets aria-expanded=true when groups are not collapsed", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: [],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    const btn = screen.getByTestId("pivot-dim-toggle-row-0-region");
    expect(btn).toHaveAttribute("aria-expanded", "true");
  });

  it("sets aria-expanded=false when groups are collapsed", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: [],
      show_subtotals: true,
      collapsed_groups: ["EU", "US"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    const btn = screen.getByTestId("pivot-dim-toggle-row-0-region");
    expect(btn).toHaveAttribute("aria-expanded", "false");
  });

  it("has descriptive aria-label", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: [],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );
    const btn = screen.getByTestId("pivot-dim-toggle-row-0-region");
    expect(btn).toHaveAttribute("aria-label", "Collapse all region groups");
  });
});

describe("Sort indicator on targeted dimension", () => {
  it("shows sort arrow on the targeted dimension, not the first", () => {
    const config = makeConfig({
      rows: ["region", "category", "product"],
      columns: [],
      values: ["revenue"],
      show_subtotals: true,
      row_sort: { by: "key", direction: "desc", dimension: "category" },
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
        onSortChange={vi.fn()}
        onFilterChange={vi.fn()}
      />,
    );

    const regionHeader = screen.getByTestId("pivot-dim-toggle-row-0-region");
    const categoryHeader = screen.getByTestId(
      "pivot-dim-toggle-row-1-category",
    );
    expect(regionHeader).not.toHaveAttribute("aria-sort");
    expect(categoryHeader).toHaveAttribute("aria-sort", "descending");
  });

  it("shows sort arrow on first dimension when no dimension specified", () => {
    const config = makeConfig({
      rows: ["region", "category", "product"],
      columns: [],
      values: ["revenue"],
      show_subtotals: true,
      row_sort: { by: "key", direction: "asc" },
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
        onSortChange={vi.fn()}
        onFilterChange={vi.fn()}
      />,
    );

    const regionHeader = screen.getByTestId("pivot-dim-toggle-row-0-region");
    expect(regionHeader).toHaveAttribute("aria-sort", "ascending");
  });
});

describe("Dimension toggle - disabled when parent collapsed", () => {
  it("disables child toggle when parent dimension is fully collapsed", () => {
    const config = makeConfig({
      rows: ["region", "category", "product"],
      columns: [],
      show_subtotals: true,
      collapsed_groups: ["EU", "US"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
      />,
    );

    const categoryToggle = screen.getByTestId(
      "pivot-dim-toggle-row-1-category",
    );
    expect(categoryToggle).not.toHaveAttribute("role", "button");
    expect(categoryToggle).not.toHaveAttribute("tabindex");
    expect(categoryToggle).toHaveAttribute("title", "Expand region first");
  });

  it("child toggle is clickable when parent is expanded", () => {
    const onCollapseChange = vi.fn();
    const config = makeConfig({
      rows: ["region", "category", "product"],
      columns: [],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={onCollapseChange}
      />,
    );

    const categoryToggle = screen.getByTestId(
      "pivot-dim-toggle-row-1-category",
    );
    expect(categoryToggle).toHaveAttribute("role", "button");
    fireEvent.click(categoryToggle);
    expect(onCollapseChange).toHaveBeenCalled();
  });
});

describe("Dimension toggle - interaction isolation", () => {
  it("toggle click does not propagate to header sort", () => {
    const onSortChange = vi.fn();
    const onCollapseChange = vi.fn();
    const config = makeConfig({
      rows: ["region", "category"],
      columns: [],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={onCollapseChange}
        onSortChange={onSortChange}
        onFilterChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("pivot-dim-toggle-row-0-region"));
    expect(onCollapseChange).toHaveBeenCalledTimes(1);
  });
});

describe("computeColSlots", () => {
  it("returns plain slots when no groups are collapsed", () => {
    const colKeys = [
      ["2023", "Q1"],
      ["2023", "Q2"],
      ["2024", "Q1"],
    ];
    const slots = computeColSlots(colKeys, undefined, 2);
    expect(slots).toEqual([
      { key: ["2023", "Q1"] },
      { key: ["2023", "Q2"] },
      { key: ["2024", "Q1"] },
    ]);
  });

  it("collapses a single group into one subtotal slot", () => {
    const colKeys = [
      ["2023", "Q1"],
      ["2023", "Q2"],
      ["2024", "Q1"],
    ];
    const slots = computeColSlots(colKeys, ["2023"], 2);
    expect(slots).toEqual([
      { key: ["2023"], collapsedLevel: 0 },
      { key: ["2024", "Q1"] },
    ]);
  });

  it("__ALL__ collapses all top-level groups", () => {
    const colKeys = [
      ["2023", "Q1"],
      ["2023", "Q2"],
      ["2024", "Q1"],
    ];
    const slots = computeColSlots(colKeys, ["__ALL__"], 2);
    expect(slots).toEqual([
      { key: ["2023"], collapsedLevel: 0 },
      { key: ["2024"], collapsedLevel: 0 },
    ]);
  });

  it("returns plain slots for single column dimension", () => {
    const colKeys = [["2023"], ["2024"]];
    const slots = computeColSlots(colKeys, ["2023"], 1);
    expect(slots).toEqual([{ key: ["2023"] }, { key: ["2024"] }]);
  });

  it("handles empty colKeys", () => {
    expect(computeColSlots([], ["2023"], 2)).toEqual([]);
  });

  it("handles empty collapsed list", () => {
    const colKeys = [["2023", "Q1"]];
    const slots = computeColSlots(colKeys, [], 2);
    expect(slots).toEqual([{ key: ["2023", "Q1"] }]);
  });
});

describe("per-attribute totals", () => {
  it('per-dimension subtotals: only Region subtotals when show_subtotals: ["region"]', () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: ["region"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);

    const subtotalRows = screen.getAllByTestId("pivot-subtotal-row");
    expect(subtotalRows.length).toBeGreaterThan(0);

    // Region-level subtotals (EU Total, US Total) should appear
    const regionSubtotals = subtotalRows.filter((row) =>
      row.textContent?.includes("Total"),
    );
    expect(regionSubtotals.length).toBeGreaterThan(0);

    // Category-level subtotals (A Total, B Total) should NOT appear
    const categorySubtotals = subtotalRows.filter(
      (row) =>
        row.textContent?.includes("A Total") ||
        row.textContent?.includes("B Total"),
    );
    expect(categorySubtotals.length).toBe(0);
  });

  it("excluded measure in grand total row shows dash", () => {
    const config = makeConfig({
      values: ["revenue", "profit"],
      show_column_totals: ["revenue"],
      show_row_totals: ["revenue"],
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    const colSlots = computeColSlots(
      pd.getColKeys(),
      config.collapsed_col_groups,
      config.columns.length || 1,
    );
    const numRowDims = Math.max(config.rows.length, 1);

    render(
      <table>
        <tbody>
          {renderTotalsRow(
            colSlots,
            pd,
            config,
            numRowDims,
            true,
            undefined,
            undefined,
          )}
        </tbody>
      </table>,
    );

    const revenueGrand = screen.getByTestId("pivot-grand-total");
    expect(revenueGrand).toHaveTextContent(/\d/);

    const excludedCells = screen.getAllByTestId("pivot-excluded-total");
    expect(excludedCells.length).toBeGreaterThan(0);
    excludedCells.forEach((cell) => expect(cell).toHaveTextContent("–"));
  });

  it("excluded measure in row total shows dash", () => {
    const config = makeConfig({
      values: ["revenue", "profit"],
      show_row_totals: ["revenue"],
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    const colSlots = computeColSlots(
      pd.getColKeys(),
      config.collapsed_col_groups,
      config.columns.length || 1,
    );

    render(
      <table>
        <tbody>
          {renderDataRow(
            ["EU"],
            colSlots,
            pd,
            config,
            true,
            undefined,
            undefined,
          )}
        </tbody>
      </table>,
    );

    const revenueTotal = screen.getByTestId("pivot-row-total");
    expect(revenueTotal).toHaveTextContent(/\d/);

    const excludedCells = screen.getAllByTestId("pivot-excluded-total");
    expect(excludedCells.length).toBe(1);
    expect(excludedCells[0]).toHaveTextContent("–");
  });

  it("excluded total cells are non-interactive", () => {
    const config = makeConfig({
      values: ["revenue", "profit"],
      show_row_totals: ["revenue"],
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    const colSlots = computeColSlots(
      pd.getColKeys(),
      config.collapsed_col_groups,
      config.columns.length || 1,
    );

    render(
      <table>
        <tbody>
          {renderDataRow(
            ["EU"],
            colSlots,
            pd,
            config,
            true,
            vi.fn(),
            undefined,
          )}
        </tbody>
      </table>,
    );

    const excludedCells = screen.getAllByTestId("pivot-excluded-total");
    expect(excludedCells.length).toBe(1);
    const cell = excludedCells[0];
    expect(cell).not.toHaveAttribute("onClick");
    expect(cell).not.toHaveAttribute("tabindex");
  });

  it("excluded measure in subtotal row total shows dash", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue", "profit"],
      show_subtotals: true,
      show_row_totals: ["revenue"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    const colSlots = computeColSlots(
      pd.getColKeys(),
      config.collapsed_col_groups,
      config.columns.length || 1,
    );

    render(
      <table>
        <tbody>
          {renderSubtotalRow(
            ["US"],
            0,
            colSlots,
            pd,
            config,
            true,
            false,
            undefined,
            undefined,
            undefined,
            undefined,
          )}
        </tbody>
      </table>,
    );

    const revenueSubtotalTotal = screen.getByTestId("pivot-subtotal-total");
    expect(revenueSubtotalTotal).toHaveTextContent(/\d/);

    const excludedCells = screen.getAllByTestId("pivot-excluded-total");
    expect(excludedCells.length).toBe(1);
    expect(excludedCells[0]).toHaveTextContent("–");
  });
});

describe("empty data cells non-interactive", () => {
  it("null data cells have no tabindex or role", () => {
    const SPARSE_DATA: DataRecord[] = [
      { region: "US", year: "2023", revenue: 100 },
      { region: "EU", year: "2024", revenue: 200 },
    ];
    const config = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue"],
    });
    const pd = new PivotData(SPARSE_DATA, config);
    render(
      <TableRenderer pivotData={pd} config={config} onCellClick={vi.fn()} />,
    );

    const dataCells = screen.getAllByTestId("pivot-data-cell");
    const nullCell = dataCells.find(
      (c) => c.textContent === config.empty_cell_value,
    );
    expect(nullCell).toBeDefined();
    expect(nullCell).not.toHaveAttribute("tabindex");
    expect(nullCell).not.toHaveAttribute("role");
  });

  it("non-null data cells are interactive", () => {
    const config = makeConfig({
      values: ["revenue"],
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(
      <TableRenderer pivotData={pd} config={config} onCellClick={vi.fn()} />,
    );

    const dataCells = screen.getAllByTestId("pivot-data-cell");
    const nonNullCell = dataCells.find(
      (c) => c.textContent !== config.empty_cell_value,
    );
    expect(nonNullCell).toBeDefined();
    expect(nonNullCell).toHaveAttribute("tabindex", "0");
    expect(nonNullCell).toHaveAttribute("role", "gridcell");
  });

  it("collapsed column group subtotal cells with null value are non-interactive", () => {
    const SPARSE_COL_DATA: DataRecord[] = [
      { region: "US", year: "2023", quarter: "Q1", revenue: 100 },
      { region: "EU", year: "2024", quarter: "Q2", revenue: 200 },
    ];
    const config = makeConfig({
      rows: ["region"],
      columns: ["year", "quarter"],
      values: ["revenue"],
      collapsed_col_groups: ["2023", "2024"],
    });
    const pd = new PivotData(SPARSE_COL_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCellClick={vi.fn()}
        onCollapseChange={vi.fn()}
      />,
    );

    const subtotalCells = screen.getAllByTestId("pivot-col-group-subtotal");
    const nullSubtotal = subtotalCells.find(
      (c) => c.textContent === config.empty_cell_value,
    );
    expect(nullSubtotal).toBeDefined();
    expect(nullSubtotal).not.toHaveAttribute("tabindex");
    expect(nullSubtotal).not.toHaveAttribute("role");
  });
});

describe("Column resize handles", () => {
  it("renders resize handles on deepest-level column headers", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);

    const handles = screen.getAllByTestId(/^resize-handle-\d+$/);
    expect(handles.length).toBeGreaterThanOrEqual(1);
    handles.forEach((h) => {
      expect(h).toBeInTheDocument();
      expect(h.tagName.toLowerCase()).toBe("div");
    });
  });

  it("renders resize handles on value labels when multi-value", () => {
    const config = makeConfig({ values: ["revenue", "profit"] });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);

    const valHandles = screen.getAllByTestId(/^resize-handle-val-\d+-\d+$/);
    expect(valHandles.length).toBeGreaterThanOrEqual(1);
  });

  it("does not render resize handles when single value", () => {
    const config = makeConfig({ values: ["revenue"] });
    const pd = createPivotData(SAMPLE_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);

    const valHandles = screen.queryAllByTestId(/^resize-handle-val-\d+-\d+$/);
    expect(valHandles.length).toBe(0);
  });

  it("applies min-width and max-width to resized column header", () => {
    const pd = createPivotData();
    render(<TableRenderer pivotData={pd} config={makeConfig()} />);

    const handle = screen.getAllByTestId(/^resize-handle-\d+$/)[0];
    act(() => {
      fireEvent.mouseDown(handle, { clientX: 100 });
      fireEvent.mouseMove(document, { clientX: 150 });
      fireEvent.mouseUp(document);
    });

    const th = screen.getAllByTestId("pivot-header-cell")[0];
    expect(th.style.minWidth).toBeTruthy();
    expect(th.style.maxWidth).toBeTruthy();
    expect(th.style.minWidth).toBe(th.style.maxWidth);
  });
});

describe("Empty filter state", () => {
  it("renders headers and empty message when all data is filtered out", () => {
    const config = makeConfig({
      rows: ["region"],
      columns: ["year"],
      values: ["revenue"],
      filters: { region: { exclude: ["US", "EU"] } },
    });
    const pd = new PivotData(SAMPLE_DATA, config);
    render(
      <TableRenderer pivotData={pd} config={config} onFilterChange={vi.fn()} />,
    );

    expect(screen.queryByTestId("pivot-table-empty")).not.toBeInTheDocument();
    expect(screen.getByTestId("pivot-empty-filter-row")).toBeInTheDocument();
    expect(screen.getByTestId("pivot-empty-filter-row")).toHaveTextContent(
      "All values filtered out",
    );
  });

  it("shows generic empty state when no filters and no data", () => {
    const config = makeConfig({
      rows: [],
      columns: [],
      values: [],
    });
    const pd = new PivotData([], config);
    render(<TableRenderer pivotData={pd} config={config} />);

    expect(screen.getByTestId("pivot-table-empty")).toBeInTheDocument();
    expect(
      screen.queryByTestId("pivot-empty-filter-row"),
    ).not.toBeInTheDocument();
  });
});
