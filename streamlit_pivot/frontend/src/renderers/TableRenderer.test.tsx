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
import { render, screen, fireEvent, act, within } from "@testing-library/react";
import { useState } from "react";
import TableRenderer, {
  computeRowHeaderSpans,
  computeColSlots,
  renderDataRow,
  renderTotalsRow,
  renderSubtotalRow,
} from "./TableRenderer";
import { PivotData, type DataRecord, makeKeyString } from "../engine/PivotData";
import type { PivotConfigV1 } from "../engine/types";
import { makeConfig } from "../test-utils";
import { buildModifiedRowKey } from "../engine/dateGrouping";

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

  it("renders a single hierarchy row column with parent subtotal rows", () => {
    const data: DataRecord[] = [
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
    const pd = new PivotData(data, config);

    const { container } = render(
      <TableRenderer pivotData={pd} config={config} />,
    );

    expect(
      screen.getByTestId("pivot-row-dim-label-hierarchy"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("pivot-row-dim-breadcrumb-region-0"),
    ).toBeInTheDocument();
    expect(screen.getAllByTestId("pivot-subtotal-row").length).toBeGreaterThan(
      0,
    );
    expect(screen.getAllByTestId("pivot-subtotal-row")[0]).toHaveTextContent(
      "EU",
    );
    expect(container.textContent).not.toContain("US Total");
    const firstDataRow = screen.getAllByTestId("pivot-data-row")[0];
    expect(
      firstDataRow.querySelectorAll('th[data-testid="pivot-row-header"]'),
    ).toHaveLength(1);
  });

  it("shows auto-grouped date labels for temporal dimensions", () => {
    const data: DataRecord[] = [
      { order_date: "2024-01-03", region: "US", revenue: 100 },
      { order_date: "2024-02-10", region: "US", revenue: 150 },
    ];
    const config = makeConfig({
      rows: ["order_date"],
      columns: ["region"],
      values: ["revenue"],
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });
    render(<TableRenderer pivotData={pd} config={config} />);
    expect(screen.getByText("Month")).toBeInTheDocument();
    expect(screen.getByText("Jan 2024")).toBeInTheDocument();
  });

  it("expands row-side temporal hierarchy into multiple row header columns", () => {
    const data: DataRecord[] = [
      { order_date: "2024-01-03", region: "US", revenue: 100 },
      { order_date: "2024-02-10", region: "US", revenue: 150 },
    ];
    const config = makeConfig({
      rows: ["order_date"],
      columns: ["region"],
      values: ["revenue"],
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });
    render(<TableRenderer pivotData={pd} config={config} />);

    expect(screen.getByText("Year")).toBeInTheDocument();
    expect(screen.getByText("Quarter")).toBeInTheDocument();
    expect(screen.getByText("Month")).toBeInTheDocument();
    expect(
      screen.getByTestId("pivot-temporal-row-toggle-order_date-2024"),
    ).toBeInTheDocument();
    expect(screen.getByText("Q1 2024")).toBeInTheDocument();
    expect(screen.getByText("Jan 2024")).toBeInTheDocument();
  });

  it("materializes temporal parent rows in hierarchy layout", () => {
    const data: DataRecord[] = [
      { order_date: "2024-01-03", region: "US", revenue: 100 },
      { order_date: "2024-02-10", region: "US", revenue: 150 },
    ];
    const config = makeConfig({
      rows: ["order_date"],
      columns: ["region"],
      values: ["revenue"],
      row_layout: "hierarchy",
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });

    render(<TableRenderer pivotData={pd} config={config} />);

    expect(
      screen.getAllByTestId("pivot-temporal-parent-row").length,
    ).toBeGreaterThan(0);
    expect(screen.getByText("2024")).toBeInTheDocument();
  });

  it("renders temporal breadcrumb toggles for hierarchy levels", () => {
    const data: DataRecord[] = [
      {
        order_date: "2024-01-03",
        region: "US",
        customer: "Acme",
        revenue: 100,
      },
      {
        order_date: "2024-01-10",
        region: "US",
        customer: "Globex",
        revenue: 120,
      },
      {
        order_date: "2024-02-10",
        region: "EU",
        customer: "Initech",
        revenue: 150,
      },
    ];
    const config = makeConfig({
      rows: ["order_date", "region", "customer"],
      columns: [],
      values: ["revenue"],
      row_layout: "hierarchy",
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });

    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
        onConfigChange={vi.fn()}
      />,
    );

    expect(
      screen.getByTestId("pivot-dim-toggle-row-0-order-date"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("pivot-dim-toggle-row-1-order-date"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("pivot-dim-toggle-row-2-order-date"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("pivot-dim-toggle-row-3-region"),
    ).toBeInTheDocument();
    expect(
      screen.queryByTestId("pivot-dim-toggle-row-4-customer"),
    ).not.toBeInTheDocument();
  });

  it("renders a toggle for temporal leaf rows that have descendants", () => {
    const data: DataRecord[] = [
      {
        order_date: "2024-01-03",
        region: "Europe",
        customer: "Initech",
        revenue: 100,
      },
      {
        order_date: "2024-01-09",
        region: "Europe",
        customer: "Umbrella",
        revenue: 120,
      },
    ];
    const config = makeConfig({
      rows: ["order_date", "region", "customer"],
      columns: [],
      values: ["revenue"],
      row_layout: "hierarchy",
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });

    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
        onConfigChange={vi.fn()}
      />,
    );

    const janCell = screen.getByText("Jan 2024").closest("th");
    expect(janCell).not.toBeNull();
    expect(
      janCell?.querySelector("button[data-testid^='pivot-group-toggle-']"),
    ).not.toBeNull();
  });

  it("renders a synthetic parent row when a row temporal group is collapsed", () => {
    const data: DataRecord[] = [
      { order_date: "2024-01-03", region: "US", revenue: 100 },
      { order_date: "2024-02-10", region: "US", revenue: 150 },
      { order_date: "2025-01-12", region: "US", revenue: 200 },
    ];
    const collapseKey = buildModifiedRowKey(
      ["2024-01"],
      0,
      "order_date",
      "2024",
    ).join("\x00");
    const config = makeConfig({
      rows: ["order_date"],
      columns: ["region"],
      values: ["revenue"],
      collapsed_temporal_row_groups: { order_date: [collapseKey] },
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });
    render(<TableRenderer pivotData={pd} config={config} />);

    expect(screen.getByTestId("pivot-temporal-parent-row")).toBeInTheDocument();
    expect(
      screen.getAllByTestId("pivot-temporal-row-collapse-cell")[0],
    ).toHaveTextContent("250");
    expect(screen.queryByText("Jan 2024")).not.toBeInTheDocument();
  });

  it("renders row temporal parent cells against collapsed column groups", () => {
    const data: DataRecord[] = [
      {
        order_date: "2024-01-03",
        region: "US",
        category: "A",
        revenue: 100,
      },
      {
        order_date: "2024-02-10",
        region: "US",
        category: "A",
        revenue: 150,
      },
      {
        order_date: "2024-01-12",
        region: "US",
        category: "B",
        revenue: 40,
      },
      {
        order_date: "2024-02-14",
        region: "US",
        category: "B",
        revenue: 60,
      },
      {
        order_date: "2024-01-05",
        region: "EU",
        category: "A",
        revenue: 80,
      },
    ];
    const collapseKey = buildModifiedRowKey(
      ["2024-01"],
      0,
      "order_date",
      "2024",
    ).join("\x00");
    const config = makeConfig({
      rows: ["order_date"],
      columns: ["region", "category"],
      values: ["revenue"],
      collapsed_temporal_row_groups: { order_date: [collapseKey] },
      collapsed_col_groups: [makeKeyString(["US"])],
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });
    render(<TableRenderer pivotData={pd} config={config} />);

    expect(screen.getByTestId("pivot-temporal-parent-row")).toBeInTheDocument();
    const texts = screen
      .getAllByTestId("pivot-temporal-row-collapse-cell")
      .map((cell) => cell.textContent ?? "");
    expect(texts).toContain("350");
  });

  it("keeps row temporal parent cells raw under show_values_as", () => {
    const data: DataRecord[] = [
      { order_date: "2024-01-03", region: "US", revenue: 100 },
      { order_date: "2024-02-10", region: "US", revenue: 150 },
      { order_date: "2025-01-12", region: "US", revenue: 200 },
    ];
    const collapseKey = buildModifiedRowKey(
      ["2024-01"],
      0,
      "order_date",
      "2024",
    ).join("\x00");
    const config = makeConfig({
      rows: ["order_date"],
      columns: ["region"],
      values: ["revenue"],
      show_values_as: { revenue: "pct_of_total" },
      collapsed_temporal_row_groups: { order_date: [collapseKey] },
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });
    render(<TableRenderer pivotData={pd} config={config} />);

    const text =
      screen.getAllByTestId("pivot-temporal-row-collapse-cell")[0]
        ?.textContent ?? "";
    expect(text).toContain("250");
    expect(text).not.toContain("%");
  });

  it("keeps row temporal parent cells raw under period comparison modes", () => {
    const data: DataRecord[] = [
      { order_date: "2024-01-03", region: "US", revenue: 100 },
      { order_date: "2024-02-10", region: "US", revenue: 150 },
      { order_date: "2025-01-12", region: "US", revenue: 200 },
    ];
    const collapseKey = buildModifiedRowKey(
      ["2024-01"],
      0,
      "order_date",
      "2024",
    ).join("\x00");
    const config = makeConfig({
      rows: ["order_date"],
      columns: ["region"],
      values: ["revenue"],
      show_values_as: { revenue: "diff_from_prev" },
      collapsed_temporal_row_groups: { order_date: [collapseKey] },
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });
    render(<TableRenderer pivotData={pd} config={config} />);

    expect(
      screen.getAllByTestId("pivot-temporal-row-collapse-cell")[0],
    ).toHaveTextContent("250");
  });

  it("renders spacer cells for collapsed-away inner row hierarchy columns", () => {
    const data: DataRecord[] = [
      { order_date: "2024-01-03", region: "US", revenue: 100 },
      { order_date: "2024-02-10", region: "US", revenue: 150 },
      { order_date: "2025-01-12", region: "US", revenue: 200 },
    ];
    const collapseKey = buildModifiedRowKey(
      ["2024-01"],
      0,
      "order_date",
      "2024",
    ).join("\x00");
    const config = makeConfig({
      rows: ["order_date"],
      columns: ["region"],
      values: ["revenue"],
      collapsed_temporal_row_groups: { order_date: [collapseKey] },
    });
    const pd = new PivotData(data, config, {
      columnTypes: new Map([["order_date", "date"]]),
    });
    render(<TableRenderer pivotData={pd} config={config} />);

    expect(
      screen.getAllByTestId("pivot-row-header-spacer").length,
    ).toBeGreaterThan(0);
  });

  it("preserves collapsed temporal row groups across outer subtotal collapse cycles", () => {
    const data: DataRecord[] = [
      { region: "US", order_date: "2024-01-03", revenue: 100 },
      { region: "US", order_date: "2024-02-10", revenue: 150 },
      { region: "US", order_date: "2025-01-12", revenue: 200 },
      { region: "EU", order_date: "2024-01-08", revenue: 80 },
    ];
    const collapseKey = makeKeyString(
      buildModifiedRowKey(["US", "2024-01"], 1, "order_date", "2024").slice(
        0,
        2,
      ),
    );
    const initialConfig = makeConfig({
      rows: ["region", "order_date"],
      values: ["revenue"],
      show_subtotals: ["region"],
      collapsed_temporal_row_groups: { order_date: [collapseKey] },
    });

    function Harness() {
      const [config, setConfig] = useState(initialConfig);
      const pd = new PivotData(data, config, {
        columnTypes: new Map([["order_date", "date"]]),
      });

      return (
        <TableRenderer
          pivotData={pd}
          config={config}
          onConfigChange={(next) => setConfig(next)}
          onCollapseChange={(axis, collapsed) =>
            setConfig((prev) => ({
              ...prev,
              [axis === "row" ? "collapsed_groups" : "collapsed_col_groups"]:
                collapsed,
            }))
          }
        />
      );
    }

    render(<Harness />);

    expect(screen.getByTestId("pivot-temporal-parent-row")).toBeInTheDocument();

    const usSubtotalToggle = screen.getByTestId("pivot-group-toggle-US");
    expect(usSubtotalToggle).toHaveAttribute("aria-expanded", "true");
    fireEvent.click(usSubtotalToggle);
    expect(usSubtotalToggle).toHaveAttribute("aria-expanded", "false");
    expect(
      screen.queryByTestId("pivot-temporal-parent-row"),
    ).not.toBeInTheDocument();

    fireEvent.click(usSubtotalToggle);
    expect(usSubtotalToggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByTestId("pivot-temporal-parent-row")).toBeInTheDocument();
  });

  it("preserves temporal collapse after collapsing and re-expanding outer row group", () => {
    const data: DataRecord[] = [
      { region: "US", order_date: "2024-01-03", revenue: 100 },
      { region: "US", order_date: "2024-02-10", revenue: 150 },
      { region: "US", order_date: "2025-01-12", revenue: 200 },
      { region: "EU", order_date: "2024-01-08", revenue: 80 },
    ];
    const initialConfig = makeConfig({
      rows: ["region", "order_date"],
      values: ["revenue"],
      show_subtotals: ["region"],
    });

    function Harness() {
      const [config, setConfig] = useState(initialConfig);
      const pd = new PivotData(data, config, {
        columnTypes: new Map([["order_date", "date"]]),
      });

      return (
        <TableRenderer
          pivotData={pd}
          config={config}
          onConfigChange={(next) => setConfig(next)}
          onCollapseChange={(axis, collapsed) =>
            setConfig((prev) => ({
              ...prev,
              [axis === "row" ? "collapsed_groups" : "collapsed_col_groups"]:
                collapsed,
            }))
          }
        />
      );
    }

    render(<Harness />);

    const usTemporalToggle = screen.getByTestId(
      "pivot-temporal-row-toggle-order_date-2024",
    );
    fireEvent.click(usTemporalToggle);
    expect(screen.getByTestId("pivot-temporal-parent-row")).toBeInTheDocument();

    const usSubtotalToggle = screen.getByTestId("pivot-group-toggle-US");
    fireEvent.click(usSubtotalToggle);
    expect(
      screen.queryByTestId("pivot-temporal-parent-row"),
    ).not.toBeInTheDocument();

    fireEvent.click(usSubtotalToggle);
    expect(screen.getByTestId("pivot-temporal-parent-row")).toBeInTheDocument();
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

  it("preserves show_values_as formatting for non-temporal grand total row cells", () => {
    const config = makeConfig({
      show_values_as: { revenue: "pct_of_total" },
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
          {renderTotalsRow(
            colSlots,
            pd,
            config,
            Math.max(config.rows.length, 1),
            false,
          )}
        </tbody>
      </table>,
    );

    expect(screen.getAllByTestId("pivot-col-total")[0]).toHaveTextContent("%");
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

  it("omits unchecked measures from the right-side total group", () => {
    const config = makeConfig({
      values: ["revenue", "profit"],
      show_row_totals: ["revenue"],
    });
    const pd = createPivotData(SAMPLE_DATA, config);
    const { container } = render(
      <TableRenderer pivotData={pd} config={config} />,
    );

    const headerRows = container.querySelectorAll("thead tr");
    const totalGroupHeader = headerRows[0]
      ?.lastElementChild as HTMLTableCellElement | null;
    const totalValueHeader = headerRows[1]
      ?.lastElementChild as HTMLTableCellElement | null;

    expect(totalGroupHeader?.textContent).toContain("Total");
    expect(totalGroupHeader?.colSpan).toBe(1);
    expect(totalValueHeader?.textContent).toContain("revenue");
    expect(totalValueHeader?.textContent).not.toContain("profit");
    expect(
      screen.queryByTestId("pivot-excluded-total"),
    ).not.toBeInTheDocument();
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

  it("single temporal column: row dim label spans all header levels, no colDimLabel", () => {
    const data = [
      { region: "US", order_date: "2024-Q1", revenue: 100 },
      { region: "US", order_date: "2024-Q2", revenue: 150 },
      { region: "EU", order_date: "2024-Q1", revenue: 200 },
    ];
    const config = makeConfig({
      rows: ["region"],
      columns: ["order_date"],
      values: ["revenue"],
      date_grains: { order_date: "quarter" },
      auto_date_hierarchy: true,
    });
    const columnTypes = new Map<string, import("../engine/types").ColumnType>([
      ["order_date", "date"],
      ["region", "string"],
      ["revenue", "float"],
    ]);
    const pd = new PivotData(data, config, { columnTypes });
    const { container } = render(
      <TableRenderer pivotData={pd} config={config} />,
    );
    const cornerLabel = screen.getByTestId("pivot-row-dim-label-region");
    expect(cornerLabel).toHaveTextContent("region");
    // rowSpan should cover year + quarter levels = 2
    expect(cornerLabel).toHaveAttribute("rowspan", "2");
    // No colDimLabel corner cell with "order_date"
    const colDimLabels = container.querySelectorAll("[class*='colDimLabel']");
    expect(colDimLabels).toHaveLength(0);
  });

  it("multi-column with temporal: colDimLabel still present for non-temporal column", () => {
    const data = [
      { region: "US", category: "A", order_date: "2024-Q1", revenue: 100 },
      { region: "US", category: "B", order_date: "2024-Q2", revenue: 150 },
    ];
    const config = makeConfig({
      rows: ["region"],
      columns: ["category", "order_date"],
      values: ["revenue"],
      date_grains: { order_date: "quarter" },
      auto_date_hierarchy: true,
    });
    const columnTypes = new Map<string, import("../engine/types").ColumnType>([
      ["order_date", "date"],
      ["category", "string"],
      ["region", "string"],
      ["revenue", "float"],
    ]);
    const pd = new PivotData(data, config, { columnTypes });
    const { container } = render(
      <TableRenderer pivotData={pd} config={config} />,
    );
    // "category" colDimLabel should still be present
    const colDimLabels = container.querySelectorAll("[class*='colDimLabel']");
    expect(colDimLabels.length).toBeGreaterThanOrEqual(1);
    expect(colDimLabels[0]).toHaveTextContent("category");
    // Row dim label on last row
    expect(screen.getByTestId("pivot-row-dim-label-region")).toHaveTextContent(
      "region",
    );
  });

  it("multi-column temporal: outer header spans across collapsed and expanded siblings", () => {
    // columns=["region", "order_date"] with quarter grain → hierarchy [year, quarter].
    // Collapse key must include the outer sibling context:
    // makeKeyString(["US", "tp:order_date:2024"]) = "US\x00tp:order_date:2024"
    const data = [
      { region: "US", order_date: "2024-Q1", revenue: 100 },
      { region: "US", order_date: "2024-Q2", revenue: 150 },
      { region: "US", order_date: "2025-Q1", revenue: 200 },
    ];
    const config = makeConfig({
      rows: ["region"],
      columns: ["region", "order_date"],
      values: ["revenue"],
      date_grains: { order_date: "quarter" },
      auto_date_hierarchy: true,
      collapsed_temporal_groups: {
        order_date: ["US\x00tp:order_date:2024"],
      },
    });
    const columnTypes = new Map<string, import("../engine/types").ColumnType>([
      ["order_date", "date"],
      ["region", "string"],
      ["revenue", "float"],
    ]);
    const pd = new PivotData(data, config, { columnTypes });
    const { container } = render(
      <TableRenderer pivotData={pd} config={config} />,
    );
    // Verify collapse actually happened: the collapsed parent header should
    // have aria-expanded="false".
    const collapsedHeader = container.querySelector("[aria-expanded='false']");
    expect(collapsedHeader).not.toBeNull();

    // The "region" header row (non-temporal, level 0) should have a single
    // "US" cell spanning both the collapsed 2024 and expanded 2025 slots,
    // not two separate "US" cells.
    const thead = container.querySelector("thead")!;
    const firstHeaderRow = thead.querySelectorAll("tr")[0]!;
    const usCells = Array.from(firstHeaderRow.querySelectorAll("th")).filter(
      (th) => th.textContent?.includes("US"),
    );
    expect(usCells).toHaveLength(1);
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

  it("renders breadcrumb menu triggers in hierarchy mode", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
      row_layout: "hierarchy",
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onSortChange={vi.fn()}
        onFilterChange={vi.fn()}
      />,
    );
    expect(
      screen.getByTestId("header-menu-trigger-region-0"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("header-menu-trigger-category-1"),
    ).toBeInTheDocument();
  });

  it("opens row header menu from a hierarchy breadcrumb trigger", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
      row_layout: "hierarchy",
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onSortChange={vi.fn()}
        onFilterChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("header-menu-trigger-region-0"));
    expect(screen.getByTestId("header-menu-region")).toBeInTheDocument();
  });

  it("omits subtotals toggle from hierarchy breadcrumb menus", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
      row_layout: "hierarchy",
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onSortChange={vi.fn()}
        onFilterChange={vi.fn()}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("header-menu-trigger-region-0"));
    expect(screen.getByTestId("header-menu-region")).toBeInTheDocument();
    expect(
      screen.queryByTestId("header-menu-subtotals"),
    ).not.toBeInTheDocument();
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

describe("TableRenderer - header hierarchy styling hooks", () => {
  it("marks top-level and nested column headers with distinct hierarchy classes", () => {
    const config = makeConfig({
      rows: ["category"],
      columns: ["region", "year"],
      values: ["revenue"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);

    const headerCells = screen.getAllByTestId("pivot-header-cell");
    expect(
      headerCells.some((cell) =>
        cell.className.includes("columnHeaderPrimary"),
      ),
    ).toBe(true);
    expect(
      headerCells.some((cell) =>
        cell.className.includes("columnHeaderSecondary"),
      ),
    ).toBe(true);
  });

  it("marks top-level and nested row headers with distinct hierarchy classes", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(<TableRenderer pivotData={pd} config={config} />);

    const firstDataRow = screen.getAllByTestId("pivot-data-row")[0];
    const rowHeaders = firstDataRow.querySelectorAll("th[scope='row']");
    expect(rowHeaders[0]?.className).toContain("rowHeaderPrimary");
    expect(rowHeaders[1]?.className).toContain("rowHeaderSecondary");
  });

  it("preserves subtotal header hooks while applying hierarchy classes", () => {
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    const { container } = render(
      <TableRenderer pivotData={pd} config={config} />,
    );

    const subtotalHeader = container.querySelector("th.subtotalHeaderCell");
    expect(subtotalHeader).toBeInTheDocument();
    expect(subtotalHeader?.className).toMatch(/rowHeader(Primary|Secondary)/);
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

  it("does not collapse when clicking hierarchy subtotal text", () => {
    const onCollapseChange = vi.fn();
    const config = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
      show_subtotals: true,
      row_layout: "hierarchy",
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={onCollapseChange}
      />,
    );
    const firstSubtotalRow = screen.getAllByTestId("pivot-subtotal-row")[0];
    fireEvent.click(within(firstSubtotalRow).getByText("EU"));
    expect(onCollapseChange).not.toHaveBeenCalled();
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

  it("expanding a deeper hierarchy breadcrumb clears collapsed parents", () => {
    const onCollapseChange = vi.fn();
    const onConfigChange = vi.fn();
    const config = makeConfig({
      rows: ["region", "category", "product"],
      columns: [],
      values: ["revenue"],
      row_layout: "hierarchy",
      show_subtotals: true,
      collapsed_groups: ["EU", "US"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={onCollapseChange}
        onConfigChange={onConfigChange}
      />,
    );
    fireEvent.click(screen.getByTestId("pivot-dim-toggle-row-1-category"));
    expect(onConfigChange).toHaveBeenCalledTimes(1);
    expect(onConfigChange.mock.calls[0][0].collapsed_groups).toEqual([]);
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

  it("shows deeper hierarchy breadcrumbs as collapsed when an ancestor is collapsed", () => {
    const config = makeConfig({
      rows: ["region", "category", "product"],
      columns: [],
      values: ["revenue"],
      row_layout: "hierarchy",
      show_subtotals: true,
      collapsed_groups: ["EU", "US"],
    });
    const pd = new PivotData(MULTI_DIM_DATA, config);
    render(
      <TableRenderer
        pivotData={pd}
        config={config}
        onCollapseChange={vi.fn()}
        onConfigChange={vi.fn()}
      />,
    );
    const btn = screen.getByTestId("pivot-dim-toggle-row-1-category");
    expect(btn).toHaveAttribute("aria-expanded", "false");
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

  it("excluded measure in row total is omitted", () => {
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
    expect(
      screen.queryByTestId("pivot-excluded-total"),
    ).not.toBeInTheDocument();
  });

  it("remaining row total cells stay interactive when a measure is omitted", () => {
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

    const cell = screen.getByTestId("pivot-row-total");
    expect(cell).toHaveAttribute("role", "gridcell");
    expect(cell).toHaveAttribute("tabindex", "0");
  });

  it("excluded measure in subtotal row total is omitted", () => {
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
    expect(
      screen.queryByTestId("pivot-excluded-total"),
    ).not.toBeInTheDocument();
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
