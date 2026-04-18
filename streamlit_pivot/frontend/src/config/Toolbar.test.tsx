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
import {
  render,
  screen,
  fireEvent,
  waitFor,
  act,
  within,
} from "@testing-library/react";
import Toolbar, { applyDragMove } from "./Toolbar";
import { canDropFieldToZone, resolveDragEnd } from "./SettingsPanel";
import { PivotData, type DataRecord } from "../engine/PivotData";
import { makeConfig } from "../test-utils";

const ALL_COLUMNS = ["region", "year", "revenue", "profit", "category"];
const NUMERIC_COLUMNS = ["revenue", "profit"];
const MANY_FIELDS = [
  "region",
  "year",
  "category",
  "segment",
  "state",
  "city",
  "country",
  "quarter",
  "month",
  "channel",
];
const MANY_NUMERIC_COLUMNS = [
  "revenue",
  "profit",
  "cost",
  "margin",
  "units",
  "discount",
  "tax",
  "freight",
  "returns",
];

describe("Toolbar - rendering", () => {
  it("renders toolbar with data-testid", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("pivot-toolbar")).toBeInTheDocument();
  });

  it("renders selected row chips", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("toolbar-rows-chips")).toHaveTextContent(
      "region",
    );
    expect(screen.getByTestId("toolbar-rows-chips")).toHaveTextContent(
      "category",
    );
  });

  it("renders aggregation controls for values inside the settings panel", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const aggTrigger = within(
      screen.getByTestId("settings-values-chip-revenue"),
    ).getByTestId("settings-agg-revenue");
    expect(aggTrigger).toHaveTextContent("▾");
  });

  it("shows count badge with selected count", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    const sectionHeader = screen.getByTestId("toolbar-rows-header");
    expect(sectionHeader).toHaveTextContent("2");
  });
});

describe("Toolbar - column_config.label rendering", () => {
  it("renders row chips using the label override", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region"],
          field_labels: { region: "Area" },
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    const chip = screen.getByTestId("toolbar-rows-chip-region");
    expect(chip).toHaveTextContent("Area");
    expect(chip).not.toHaveTextContent(/^region/);
  });

  it("renders value chips using the label override", () => {
    render(
      <Toolbar
        config={makeConfig({
          values: ["revenue"],
          field_labels: { revenue: "Rev" },
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("toolbar-values-chip-revenue")).toHaveTextContent(
      "Rev",
    );
  });

  it("empty-string label falls back to field id", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region"],
          field_labels: { region: "" },
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("toolbar-rows-chip-region")).toHaveTextContent(
      "region",
    );
  });

  it("whitespace-only label falls back to field id", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region"],
          field_labels: { region: "   " },
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("toolbar-rows-chip-region")).toHaveTextContent(
      "region",
    );
  });

  it("settings panel chips also use the label override", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region"],
          values: ["revenue"],
          field_labels: { region: "Area", revenue: "Rev" },
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-rows-chip-region")).toHaveTextContent(
      "Area",
    );
    expect(
      screen.getByTestId("settings-values-chip-revenue"),
    ).toHaveTextContent("Rev");
  });
});

describe("Toolbar - interactions", () => {
  it("fires onConfigChange when a value aggregation changes via settings panel", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue", "profit"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-agg-revenue"));
    fireEvent.mouseDown(screen.getByText("Avg"));
    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].aggregation).toEqual({
      revenue: "avg",
      profit: "sum",
    });
  });

  it("does not fire when settings panel is cancelled", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-cancel"));
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("adds a row dimension via settings panel", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ rows: ["region"], columns: [] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-category"));
    fireEvent.click(screen.getByText("Add to Rows"));
    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].rows).toEqual(["region", "category"]);
  });

  it("removes a row dimension via settings panel", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"], columns: [] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-rows-remove-region"));
    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].rows).toEqual(["category"]);
  });

  it("toolbar zone chips render immediate remove buttons", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          columns: ["year"],
          values: ["revenue", "profit"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(
      screen.getByTestId("toolbar-rows-remove-region"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("toolbar-columns-remove-year"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("toolbar-values-remove-revenue"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("toolbar-values-agg-revenue"),
    ).toBeInTheDocument();
  });

  it("removes a row dimension immediately from the toolbar", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"], columns: [] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-rows-remove-region"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].rows).toEqual(["category"]);
  });

  it("removes a value immediately from the toolbar with cleanup", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
          values: ["revenue", "profit"],
          aggregation: { revenue: "avg", profit: "sum" },
          show_values_as: { revenue: "pct_of_row", profit: "raw" },
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-values-remove-revenue"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].values).toEqual(["profit"]);
    expect(handleChange.mock.calls[0][0].aggregation).toEqual({
      profit: "sum",
    });
    expect(handleChange.mock.calls[0][0].show_values_as).toEqual({
      profit: "raw",
    });
  });

  it("changes value aggregation immediately from the toolbar values section", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue", "profit"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-values-agg-revenue"));
    fireEvent.mouseDown(screen.getByText("Avg"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].aggregation).toEqual({
      revenue: "avg",
      profit: "sum",
    });
  });

  it("creates a synthetic measure via settings panel", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-add-synthetic"));
    const builder = screen.getByTestId("settings-synthetic-builder");
    const nameInput = builder.querySelector("input");
    expect(nameInput).toBeTruthy();
    fireEvent.change(nameInput!, { target: { value: "Rev / Profit" } });
    fireEvent.change(screen.getByPlaceholderText("e.g. .1%, $,.0f, ,.2f"), {
      target: { value: ".1%" },
    });
    fireEvent.click(screen.getByTestId("settings-synthetic-save"));
    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].synthetic_measures).toHaveLength(1);
    expect(handleChange.mock.calls[0][0].synthetic_measures[0].format).toBe(
      ".1%",
    );
  });

  it("synthetic builder cancel stays in settings panel and footer is hidden while editing", () => {
    vi.useFakeTimers();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-add-synthetic"));
    expect(
      screen.getByTestId("settings-synthetic-builder"),
    ).toBeInTheDocument();
    expect(screen.queryByTestId("settings-cancel")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("settings-synthetic-cancel"));
    expect(
      screen.queryByTestId("settings-synthetic-builder"),
    ).not.toBeInTheDocument();
    expect(screen.getByTestId("settings-panel")).toBeInTheDocument();
    expect(screen.getByTestId("settings-cancel")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("settings-cancel"));
    act(() => {
      vi.runAllTimers();
    });
    expect(screen.queryByTestId("settings-panel")).not.toBeInTheDocument();
    vi.useRealTimers();
  });

  it("applies a format preset in the settings synthetic builder", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-add-synthetic"));
    const builder = screen.getByTestId("settings-synthetic-builder");
    const nameInput = builder.querySelector("input");
    expect(nameInput).toBeTruthy();
    fireEvent.change(nameInput!, { target: { value: "Margin" } });
    fireEvent.click(screen.getByText("Percent"));
    fireEvent.click(screen.getByTestId("settings-synthetic-save"));
    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].synthetic_measures[0].format).toBe(
      ".1%",
    );
  });

  it("shows a validation error for invalid synthetic format and blocks save", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-add-synthetic"));
    const builder = screen.getByTestId("settings-synthetic-builder");
    const nameInput = builder.querySelector("input");
    expect(nameInput).toBeTruthy();
    fireEvent.change(nameInput!, { target: { value: "Bad Format Metric" } });
    fireEvent.change(screen.getByPlaceholderText("e.g. .1%, $,.0f, ,.2f"), {
      target: { value: "abc" },
    });
    fireEvent.click(screen.getByTestId("settings-synthetic-save"));
    expect(screen.getByText(/Format is invalid/i)).toBeInTheDocument();
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("shows formula preview and updates when operation changes", () => {
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-add-synthetic"));
    const builder = screen.getByTestId("settings-synthetic-builder");
    expect(builder).toHaveTextContent("sum(revenue) / sum(profit)");
    fireEvent.click(screen.getByTestId("settings-synthetic-operation"));
    fireEvent.click(
      screen.getByTestId("settings-synthetic-operation-difference"),
    );
    expect(screen.getByTestId("settings-panel")).toBeInTheDocument();
    expect(
      screen.getByTestId("settings-synthetic-builder"),
    ).toBeInTheDocument();
    expect(builder).toHaveTextContent("sum(revenue) - sum(profit)");
  });

  it("blocks save when generated synthetic id collides with a value field", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["synthetic_margin"] })}
        allColumns={[...ALL_COLUMNS, "synthetic_margin"]}
        numericColumns={[...NUMERIC_COLUMNS, "synthetic_margin"]}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-add-synthetic"));
    const builder = screen.getByTestId("settings-synthetic-builder");
    const nameInput = builder.querySelector("input");
    expect(nameInput).toBeTruthy();
    fireEvent.change(nameInput!, { target: { value: "Margin" } });
    fireEvent.click(screen.getByTestId("settings-synthetic-save"));
    expect(
      screen.getByText(/collides with an existing value field/i),
    ).toBeInTheDocument();
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("closes settings panel when clicking outside", async () => {
    vi.useFakeTimers();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-panel")).toBeInTheDocument();
    fireEvent.mouseDown(document.body);
    act(() => {
      vi.advanceTimersByTime(200);
    });
    expect(screen.queryByTestId("settings-panel")).not.toBeInTheDocument();
    vi.useRealTimers();
  });

  it("allows hidden-from-aggregators numeric fields as synthetic sources", () => {
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue"]}
        syntheticSourceColumns={["revenue", "profit"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-add-synthetic"));
    fireEvent.click(screen.getByTestId("settings-synthetic-numerator"));
    const panel = screen.getByTestId("settings-synthetic-numerator-panel");
    expect(within(panel).getByText("profit")).toBeInTheDocument();
  });

  it("displays existing synthetic measures in the settings panel", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
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
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const synChip = screen.getByTestId("settings-synthetic-rev_minus_profit");
    expect(synChip).toBeInTheDocument();
  });

  it("preserves explicit date_grains when removing a column via settings panel", () => {
    const handleChange = vi.fn();
    const allColumns = [...ALL_COLUMNS, "order_date"];
    const config = makeConfig({
      rows: ["region"],
      columns: ["order_date"],
      date_grains: { order_date: "quarter" },
    });
    render(
      <Toolbar
        config={config}
        allColumns={allColumns}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-columns-remove-order_date"));
    fireEvent.click(screen.getByTestId("settings-apply"));
    const removedConfig = handleChange.mock.calls[0][0];
    expect(removedConfig.columns).toEqual([]);
    expect(removedConfig.date_grains).toEqual({ order_date: "quarter" });
  });

  it("does not drop a non-numeric field when moving to values is rejected", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const menuBtn = screen.queryByTestId("settings-rows-menu-region");
    if (menuBtn) {
      fireEvent.click(menuBtn);
      expect(screen.queryByText("Move to Values")).not.toBeInTheDocument();
    }
    expect(screen.getByTestId("settings-rows-chip-region")).toBeInTheDocument();
    const applyBtn = screen.getByTestId("settings-apply");
    expect(applyBtn).toBeDisabled();
  });
});

describe("Toolbar - field search in settings panel", () => {
  it("does not render search input when available fields are at or below threshold", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "year", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(
      screen.queryByTestId("settings-field-search"),
    ).not.toBeInTheDocument();
  });

  it("renders search input above threshold and filters available fields", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: [] })}
        allColumns={MANY_FIELDS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const search = screen.getByTestId("settings-field-search");
    expect(search).toBeInTheDocument();

    fireEvent.change(search, { target: { value: "cat" } });
    expect(
      screen.getByTestId("settings-available-category"),
    ).toBeInTheDocument();
    expect(
      screen.queryByTestId("settings-available-region"),
    ).not.toBeInTheDocument();

    fireEvent.change(search, { target: { value: "" } });
    expect(screen.getByTestId("settings-available-region")).toBeInTheDocument();
  });

  it("restores all chips when search query is cleared", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: [] })}
        allColumns={MANY_FIELDS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const search = screen.getByTestId("settings-field-search");
    fireEvent.change(search, { target: { value: "reg" } });
    expect(screen.getByTestId("settings-available-region")).toBeInTheDocument();
    fireEvent.change(search, { target: { value: "" } });
    expect(screen.getByTestId("settings-available-region")).toBeInTheDocument();
  });
});

describe("Toolbar - reset", () => {
  it("shows reset button when config differs from initial", () => {
    const initial = makeConfig({ aggregation: "sum" });
    const current = makeConfig({ aggregation: "avg" });
    render(
      <Toolbar
        config={current}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        initialConfig={initial}
      />,
    );
    expect(screen.getByTestId("toolbar-reset")).toBeInTheDocument();
  });

  it("hides reset button when config matches initial", () => {
    const cfg = makeConfig();
    render(
      <Toolbar
        config={cfg}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        initialConfig={cfg}
      />,
    );
    expect(screen.queryByTestId("toolbar-reset")).not.toBeInTheDocument();
  });

  it("fires onConfigChange with initialConfig when reset clicked", () => {
    const initial = makeConfig({ aggregation: "sum" });
    const current = makeConfig({ aggregation: "avg" });
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={current}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
        initialConfig={initial}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-reset"));
    expect(handleChange).toHaveBeenCalledWith(initial);
  });

  it("hides reset button when no initialConfig provided", () => {
    render(
      <Toolbar
        config={makeConfig({ aggregation: "avg" })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.queryByTestId("toolbar-reset")).not.toBeInTheDocument();
  });
});

describe("Toolbar - display checkboxes (inside settings panel)", () => {
  it("renders row totals and column totals checkboxes inside the settings panel", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-row-totals")).toBeInTheDocument();
    expect(screen.getByTestId("settings-col-totals")).toBeInTheDocument();
  });

  it("toggles row totals off when checkbox unchecked and Apply clicked", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ show_totals: true })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const checkbox = screen
      .getByTestId("settings-row-totals")
      .querySelector("input")!;
    fireEvent.click(checkbox);
    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].show_row_totals).toBe(false);
  });

  it("toggles column totals off when checkbox unchecked and Apply clicked", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ show_totals: true })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const checkbox = screen
      .getByTestId("settings-col-totals")
      .querySelector("input")!;
    fireEvent.click(checkbox);
    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].show_column_totals).toBe(false);
  });
});

describe("Toolbar - value aggregation labels", () => {
  it("does not render the old global aggregation control", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.queryByTestId("toolbar-aggregation")).not.toBeInTheDocument();
  });

  it("shows an aggregation label on each raw value chip", () => {
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue", "profit"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(
      screen.getByTestId("toolbar-values-aggregation-label-revenue"),
    ).toHaveTextContent("(Sum)");
    expect(
      screen.getByTestId("toolbar-values-aggregation-label-profit"),
    ).toHaveTextContent("(Sum)");
    expect(
      screen.getByTestId("toolbar-values-chip-label-revenue"),
    ).toHaveTextContent(/revenue\s*\(sum\)/i);
  });

  it("updates only the targeted measure's aggregation via settings panel", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue", "profit"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-agg-profit"));
    fireEvent.mouseDown(screen.getByText("Count"));
    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].aggregation).toEqual({
      revenue: "sum",
      profit: "count",
    });
  });
});

describe("Toolbar - column exclusion in settings panel", () => {
  it("keeps assigned fields visible in available fields", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region"], columns: [] })}
        allColumns={["region", "year", "category"]}
        numericColumns={[]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-available-region")).toBeInTheDocument();
    expect(screen.getByTestId("settings-available-year")).toBeInTheDocument();
    expect(
      screen.getByTestId("settings-available-category"),
    ).toBeInTheDocument();
  });
});

describe("Toolbar - settings panel DnD constraint logic", () => {
  it("zone chips no longer render action menus", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region"],
          columns: ["year"],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(
      screen.queryByTestId("settings-rows-menu-region"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("settings-columns-menu-year"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("settings-values-menu-revenue"),
    ).not.toBeInTheDocument();
  });

  it("non-numeric assigned row field offers move action from available fields", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-region"));
    expect(screen.getByText("Move to Columns")).toBeInTheDocument();
    expect(screen.queryByText("Also add to Values")).not.toBeInTheDocument();
  });

  it("non-numeric assigned column field offers move action from available fields", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: [],
          columns: ["region"],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-region"));
    expect(screen.getByText("Move to Rows")).toBeInTheDocument();
    expect(screen.queryByText("Also add to Values")).not.toBeInTheDocument();
  });

  it("moves field from Rows to Columns via available-fields menu and Apply commits it", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          columns: ["year"],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-category"));
    fireEvent.click(screen.getByText("Move to Columns"));

    expect(
      screen.queryByTestId("settings-rows-chip-category"),
    ).not.toBeInTheDocument();
    expect(
      screen.getByTestId("settings-columns-chip-category"),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    const appliedConfig = handleChange.mock.calls[0][0];
    expect(appliedConfig.rows).not.toContain("category");
    expect(appliedConfig.columns).toContain("category");
  });

  it("adds numeric field from Rows also to Values via available-fields menu", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
          rows: ["revenue"],
          columns: ["year"],
          values: [],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-revenue"));
    fireEvent.click(screen.getByText("Also add to Values"));

    expect(
      screen.getByTestId("settings-rows-chip-revenue"),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("settings-apply"));
    const appliedConfig = handleChange.mock.calls[0][0];
    expect(appliedConfig.rows).toContain("revenue");
    expect(appliedConfig.values).toContain("revenue");
  });

  it("field in rows+values: available-fields menu offers only row move action", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["revenue"],
          columns: [],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-revenue"));
    expect(screen.queryByText("Also add to Rows")).not.toBeInTheDocument();
    expect(screen.queryByText("Also add to Columns")).not.toBeInTheDocument();
    expect(screen.getByText("Move to Columns")).toBeInTheDocument();
    expect(screen.queryByText("Move to Rows")).not.toBeInTheDocument();
  });

  it("field in columns+values: available-fields menu offers only column move action", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: [],
          columns: ["revenue"],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-revenue"));
    expect(screen.queryByText("Also add to Rows")).not.toBeInTheDocument();
    expect(screen.queryByText("Also add to Columns")).not.toBeInTheDocument();
    expect(screen.getByText("Move to Rows")).toBeInTheDocument();
    expect(screen.queryByText("Move to Columns")).not.toBeInTheDocument();
  });

  it("moving a rows+values field to columns preserves its values membership", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
          rows: ["revenue"],
          columns: [],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-revenue"));
    fireEvent.click(screen.getByText("Move to Columns"));

    expect(
      screen.queryByTestId("settings-rows-chip-revenue"),
    ).not.toBeInTheDocument();
    expect(
      screen.getByTestId("settings-columns-chip-revenue"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("settings-values-chip-revenue"),
    ).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("settings-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    const appliedConfig = handleChange.mock.calls[0][0];
    expect(appliedConfig.rows).not.toContain("revenue");
    expect(appliedConfig.columns).toContain("revenue");
    expect(appliedConfig.values).toContain("revenue");
  });

  it("allows 'Also add to Values' for numeric field in rows from available fields", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["revenue"],
          columns: ["year"],
          values: [],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-revenue"));
    expect(screen.getByText("Also add to Values")).toBeInTheDocument();
  });

  it("does not offer 'Also add to Values' for non-numeric assigned fields", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region"],
          columns: ["year"],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-region"));
    expect(screen.queryByText("Also add to Values")).not.toBeInTheDocument();
  });

  it("available fields add menu respects numeric constraint for values", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: [], columns: [], values: [] })}
        allColumns={["region", "revenue"]}
        numericColumns={["revenue"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const regionChip = screen.getByTestId("settings-available-region");
    fireEvent.click(regionChip);
    expect(screen.getByText("Add to Rows")).toBeInTheDocument();
    expect(screen.getByText("Add to Columns")).toBeInTheDocument();
    expect(screen.queryByText("Add to Values")).not.toBeInTheDocument();
  });

  it("frozen field chip cannot be moved between zones", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region"],
          columns: ["year"],
          values: ["revenue"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={["revenue", "profit"]}
        onConfigChange={vi.fn()}
        frozenColumns={new Set(["region"])}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const chip = screen.getByTestId("settings-rows-chip-region");
    expect(chip).toBeInTheDocument();
    expect(
      screen.queryByTestId("settings-rows-menu-region"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("settings-rows-remove-region"),
    ).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("settings-available-region"));
    expect(screen.queryByText("Move to Columns")).not.toBeInTheDocument();
  });
});

describe("Toolbar - config import/export", () => {
  it("renders export and import buttons", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("toolbar-export")).toBeInTheDocument();
    expect(screen.getByTestId("toolbar-import-toggle")).toBeInTheDocument();
  });

  it("shows import panel on import button click", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(
      screen.queryByTestId("toolbar-import-panel"),
    ).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("toolbar-import-toggle"));
    expect(screen.getByTestId("toolbar-import-panel")).toBeInTheDocument();
    expect(screen.getByTestId("toolbar-import-textarea")).toBeInTheDocument();
    expect(screen.getByTestId("toolbar-import-apply")).toBeInTheDocument();
  });

  it("applies valid JSON config on import", () => {
    const handleChange = vi.fn();
    const imported = makeConfig({ aggregation: "avg", rows: ["category"] });
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-import-toggle"));
    fireEvent.change(screen.getByTestId("toolbar-import-textarea"), {
      target: { value: JSON.stringify(imported) },
    });
    fireEvent.click(screen.getByTestId("toolbar-import-apply"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].aggregation).toEqual({
      revenue: "avg",
    });
    expect(handleChange.mock.calls[0][0].rows).toEqual(["category"]);
  });

  it("shows error for invalid JSON on import", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-import-toggle"));
    fireEvent.change(screen.getByTestId("toolbar-import-textarea"), {
      target: { value: "not json" },
    });
    fireEvent.click(screen.getByTestId("toolbar-import-apply"));
    expect(screen.getByTestId("toolbar-import-error")).toBeInTheDocument();
  });

  it("shows error for structurally invalid config on import", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-import-toggle"));
    fireEvent.change(screen.getByTestId("toolbar-import-textarea"), {
      target: { value: JSON.stringify({ version: 2, rows: [] }) },
    });
    fireEvent.click(screen.getByTestId("toolbar-import-apply"));
    expect(screen.getByTestId("toolbar-import-error")).toHaveTextContent(
      "version must be 1",
    );
  });

  it("shows error for period comparisons on non-temporal imported configs", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        columnTypes={
          new Map([
            ["region", "string"],
            ["year", "integer"],
            ["revenue", "float"],
            ["profit", "float"],
            ["category", "string"],
          ])
        }
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-import-toggle"));
    fireEvent.change(screen.getByTestId("toolbar-import-textarea"), {
      target: {
        value: JSON.stringify(
          makeConfig({
            rows: ["region"],
            columns: ["year"],
            show_values_as: { revenue: "diff_from_prev" },
          }),
        ),
      },
    });
    fireEvent.click(screen.getByTestId("toolbar-import-apply"));
    expect(screen.getByTestId("toolbar-import-error")).toHaveTextContent(
      "period comparison show_values_as modes require a grouped date/datetime field on rows or columns",
    );
  });
});

describe("Toolbar - locked mode", () => {
  it("hides dropdown toggles when locked", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
      />,
    );
    expect(screen.queryByTestId("toolbar-rows-select")).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("toolbar-columns-select"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("toolbar-values-select"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("toolbar-values-aggregation-controls"),
    ).not.toBeInTheDocument();
  });

  it("toolbar chips have no remove buttons when locked", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
      />,
    );
    expect(
      screen.queryByTestId("toolbar-rows-remove-region"),
    ).not.toBeInTheDocument();
  });

  it("hides authoring controls when locked but keeps viewer actions", () => {
    render(
      <Toolbar
        config={makeConfig({ aggregation: "avg" })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        initialConfig={makeConfig()}
        locked={true}
        pivotData={makePivotData()}
      />,
    );
    expect(screen.queryByTestId("toolbar-reset")).not.toBeInTheDocument();
    expect(screen.queryByTestId("toolbar-swap")).not.toBeInTheDocument();
    expect(screen.getByTestId("toolbar-settings")).toBeInTheDocument();
    expect(screen.getByTestId("toolbar-export-data")).toBeInTheDocument();
  });

  it("shows read-only status rows instead of disabled checkboxes when locked", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-row-totals-status")).toHaveTextContent(
      "Row Totals",
    );
    expect(screen.getByTestId("settings-col-totals-status")).toHaveTextContent(
      "Column Totals",
    );
    expect(screen.queryByTestId("settings-row-totals")).not.toBeInTheDocument();
  });

  it("shows N/A for totals that are not applicable in locked mode", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region"], columns: [] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-row-totals-status")).toHaveTextContent(
      "N/A",
    );
  });
});

describe("Toolbar - frozen columns", () => {
  it("toolbar chips have no remove buttons for frozen columns", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        frozenColumns={new Set(["region"])}
      />,
    );
    expect(
      screen.queryByTestId("toolbar-rows-remove-region"),
    ).not.toBeInTheDocument();
  });

  it("shows frozen indicator on frozen field chips in settings panel", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        frozenColumns={new Set(["region"])}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const chip = screen.getByTestId("settings-rows-chip-region");
    expect(chip.className).toContain("Frozen");
  });

  // Contract: Python-side column_config.pinned unions into
  // hidden_from_drag_drop, which PivotRoot passes to Toolbar as
  // frozenColumns. This test anchors the frontend side of that pipeline:
  // fields delivered via frozenColumns (whether from explicit
  // frozen_columns kwarg or column_config.pinned) lock equally.
  it("column_config.pinned entries lock the chip like frozen_columns", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        frozenColumns={new Set(["category"])}
      />,
    );
    expect(
      screen.queryByTestId("toolbar-rows-remove-category"),
    ).not.toBeInTheDocument();
    expect(
      screen.getByTestId("toolbar-rows-remove-region"),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const lockedChip = screen.getByTestId("settings-rows-chip-category");
    const unlockedChip = screen.getByTestId("settings-rows-chip-region");
    expect(lockedChip.className).toContain("Frozen");
    expect(unlockedChip.className).not.toContain("Frozen");
  });
});

describe("Toolbar - status indicators", () => {
  it("shows sort indicator on row chips when row_sort is set", () => {
    render(
      <Toolbar
        config={makeConfig({ row_sort: { by: "key", direction: "asc" } })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(
      screen.getByTestId("toolbar-rows-sort-indicator"),
    ).toBeInTheDocument();
  });

  it("shows filter dot on chips when dimension is filtered", () => {
    render(
      <Toolbar
        config={makeConfig({ filters: { region: { exclude: ["US"] } } })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(
      screen.getByTestId("toolbar-rows-filter-indicator-region"),
    ).toBeInTheDocument();
  });

  it("does not show sort indicator when no sort config", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(
      screen.queryByTestId("toolbar-rows-sort-indicator"),
    ).not.toBeInTheDocument();
  });

  it("does not show filter dot when no filter applied", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(
      screen.queryByTestId("toolbar-rows-filter-indicator-region"),
    ).not.toBeInTheDocument();
  });
});

describe("Toolbar - hidden attribute variants", () => {
  it("hidden_attributes: excluded column does not appear in settings panel", () => {
    const allColumnsFiltered = ALL_COLUMNS.filter((c) => c !== "category");
    const numericFiltered = NUMERIC_COLUMNS.filter((c) => c !== "category");
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={allColumnsFiltered}
        numericColumns={numericFiltered}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(
      screen.queryByTestId("settings-available-category"),
    ).not.toBeInTheDocument();
  });

  it("hidden_from_aggregators: non-numeric column has no Values option in add menu", () => {
    const numericFiltered = NUMERIC_COLUMNS.filter((c) => c !== "profit");
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={numericFiltered}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-available-category"));
    expect(screen.getByText("Add to Rows")).toBeInTheDocument();
    expect(screen.getByText("Add to Columns")).toBeInTheDocument();
    expect(screen.queryByText("Add to Values")).not.toBeInTheDocument();
  });

  it("hidden_from_drag_drop: frozen column has no remove button in settings panel", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        frozenColumns={new Set(["region"])}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(
      screen.queryByTestId("settings-rows-remove-region"),
    ).not.toBeInTheDocument();
    expect(
      screen.getByTestId("settings-rows-remove-category"),
    ).toBeInTheDocument();
  });
});

describe("Toolbar - Expand All / Collapse All regression", () => {
  it("Collapse All sets collapsed_groups to __ALL__", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          show_subtotals: true,
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onCollapseChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("pivot-group-toggle-collapse-all"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange).toHaveBeenCalledWith("row", ["__ALL__"]);
  });

  it("Expand All clears collapsed_groups to empty array", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          show_subtotals: true,
          collapsed_groups: ["__ALL__"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onCollapseChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("pivot-group-toggle-expand-all"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange).toHaveBeenCalledWith("row", []);
  });

  it("Collapse Columns sets collapsed_col_groups to __ALL__", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ columns: ["year", "quarter"] })}
        allColumns={[...ALL_COLUMNS, "quarter"]}
        numericColumns={NUMERIC_COLUMNS}
        onCollapseChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("pivot-col-group-collapse-all"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange).toHaveBeenCalledWith("col", ["__ALL__"]);
  });

  it("Expand Columns clears collapsed_col_groups", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
          columns: ["year", "quarter"],
          collapsed_col_groups: ["__ALL__"],
        })}
        allColumns={[...ALL_COLUMNS, "quarter"]}
        numericColumns={NUMERIC_COLUMNS}
        onCollapseChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("pivot-col-group-expand-all"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange).toHaveBeenCalledWith("col", []);
  });
});

describe("Toolbar - locked mode + filtering integration", () => {
  it("locked mode disables config controls but toolbar still renders", () => {
    const onConfigChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ filters: { region: { exclude: ["US"] } } })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={onConfigChange}
        locked={true}
      />,
    );
    expect(screen.getByTestId("pivot-toolbar")).toBeInTheDocument();
    expect(
      screen.getByTestId("toolbar-rows-filter-indicator-region"),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(
      screen.getByTestId("settings-row-totals-status"),
    ).toBeInTheDocument();
    expect(
      screen.queryByTestId("toolbar-rows-remove-region"),
    ).not.toBeInTheDocument();
    expect(screen.queryByTestId("toolbar-reset")).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Settings Panel
// ---------------------------------------------------------------------------

describe("Toolbar - settings panel", () => {
  it("renders settings button", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("toolbar-settings")).toBeInTheDocument();
  });

  it("opens settings panel on settings button click", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.queryByTestId("settings-panel")).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-panel")).toBeInTheDocument();
  });

  it("closes settings panel on settings button click when already open", () => {
    vi.useFakeTimers();
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-panel")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    act(() => {
      vi.advanceTimersByTime(200);
    });
    expect(screen.queryByTestId("settings-panel")).not.toBeInTheDocument();
    vi.useRealTimers();
  });

  it("closes settings panel on Escape", async () => {
    vi.useFakeTimers();
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const panel = screen.getByTestId("settings-panel");
    expect(panel).toBeInTheDocument();
    fireEvent.keyDown(panel, { key: "Escape" });
    act(() => {
      vi.advanceTimersByTime(200);
    });
    expect(screen.queryByTestId("settings-panel")).not.toBeInTheDocument();
    vi.useRealTimers();
  });

  it("contains all display checkboxes", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-row-totals")).toBeInTheDocument();
    expect(screen.getByTestId("settings-col-totals")).toBeInTheDocument();
  });

  it("shows subtotals checkbox when 2+ row dims", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-subtotals")).toBeInTheDocument();
  });

  it("shows row layout control when rows are configured", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-row-layout")).toBeInTheDocument();
    expect(screen.getByTestId("settings-row-layout-table")).toHaveAttribute(
      "aria-pressed",
      "true",
    );
  });

  it("applies hierarchy row layout and clears repeat labels", () => {
    const onConfigChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          repeat_row_labels: true,
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={onConfigChange}
      />,
    );

    fireEvent.click(screen.getByTestId("toolbar-settings"));
    fireEvent.click(screen.getByTestId("settings-row-layout-hierarchy"));
    fireEvent.click(screen.getByTestId("settings-apply"));

    const nextConfig = onConfigChange.mock.calls[0]?.[0];
    expect(nextConfig).toEqual(
      expect.objectContaining({
        row_layout: "hierarchy",
      }),
    );
    expect(nextConfig).not.toHaveProperty("repeat_row_labels");
  });

  it("marks subtotals as always on in hierarchy mode", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          row_layout: "hierarchy",
          show_subtotals: false,
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-subtotals")).toHaveTextContent(
      "Subtotals (always on in hierarchy)",
    );
    const subtotalsCheckbox = screen
      .getByTestId("settings-subtotals")
      .querySelector("input");
    expect(subtotalsCheckbox).toBeDisabled();
  });

  it("hides subtotals checkbox when < 2 row dims", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.queryByTestId("settings-subtotals")).not.toBeInTheDocument();
  });

  it("settings button is visible in locked mode", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
      />,
    );
    expect(screen.getByTestId("toolbar-settings")).toBeInTheDocument();
  });

  it("does not pin the utility menu open just because locked mode is enabled", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
      />,
    );
    expect(
      screen.getByRole("toolbar", { name: "Table actions" }).className,
    ).not.toContain("utilGroupPinned");
  });

  it("locked mode settings panel shows status rows", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          show_subtotals: ["region"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
        onCollapseChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-subtotals-status")).toHaveTextContent(
      "region",
    );
    expect(screen.getByTestId("settings-row-layout-status")).toHaveTextContent(
      "Table",
    );
    expect(screen.queryByTestId("settings-row-totals")).not.toBeInTheDocument();
  });

  it("omits group actions in settings panel when collapse callbacks are not provided", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region", "category"],
          show_subtotals: ["region"],
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(
      screen.queryByTestId("pivot-group-toggle-collapse-all"),
    ).not.toBeInTheDocument();
  });

  it("closes settings panel when config changes externally", () => {
    vi.useFakeTimers();
    const initialConfig = makeConfig({ rows: ["region"], values: ["revenue"] });
    const { rerender } = render(
      <Toolbar
        config={initialConfig}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("settings-panel")).toBeInTheDocument();

    const externalConfig = makeConfig({
      rows: ["region", "category"],
      values: ["revenue"],
    });
    rerender(
      <Toolbar
        config={externalConfig}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    act(() => {
      vi.advanceTimersByTime(200);
    });
    expect(screen.queryByTestId("settings-panel")).not.toBeInTheDocument();
    vi.useRealTimers();
  });

  it("Apply button is disabled when no changes have been made", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region"], values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const applyBtn = screen.getByTestId("settings-apply");
    expect(applyBtn).toBeDisabled();
  });

  it("Apply button enables after a change and disables again on undo", () => {
    render(
      <Toolbar
        config={makeConfig({
          rows: ["region"],
          values: ["revenue"],
          show_row_totals: true,
        })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const applyBtn = screen.getByTestId("settings-apply");
    expect(applyBtn).toBeDisabled();

    const rowTotalsCheckbox = screen
      .getByTestId("settings-row-totals")
      .querySelector("input");
    fireEvent.click(rowTotalsCheckbox!);
    expect(applyBtn).not.toBeDisabled();

    fireEvent.click(rowTotalsCheckbox!);
    expect(applyBtn).toBeDisabled();
  });
});

// ---------------------------------------------------------------------------
// Export Data Controls
// ---------------------------------------------------------------------------

const EXPORT_DATA: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100 },
  { region: "EU", year: "2023", revenue: 200 },
];

function makePivotData() {
  return new PivotData(EXPORT_DATA, makeConfig());
}

describe("Toolbar - Export Data Controls", () => {
  it("renders export data button when pivotData is provided", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        pivotData={makePivotData()}
      />,
    );
    expect(screen.getByTestId("toolbar-export-data")).toBeInTheDocument();
  });

  it("does not render export data button when pivotData is missing", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.queryByTestId("toolbar-export-data")).not.toBeInTheDocument();
  });

  it("opens export popover on button click", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        pivotData={makePivotData()}
      />,
    );
    expect(
      screen.queryByTestId("toolbar-export-data-panel"),
    ).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("toolbar-export-data"));
    expect(screen.getByTestId("toolbar-export-data-panel")).toBeInTheDocument();
  });

  it("renders format and content toggle buttons including Excel", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        pivotData={makePivotData()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-export-data"));
    expect(screen.getByTestId("export-format-xlsx")).toBeInTheDocument();
    expect(screen.getByTestId("export-format-csv")).toBeInTheDocument();
    expect(screen.getByTestId("export-format-tsv")).toBeInTheDocument();
    expect(screen.getByTestId("export-format-clipboard")).toBeInTheDocument();
    expect(screen.getByTestId("export-content-formatted")).toBeInTheDocument();
    expect(screen.getByTestId("export-content-raw")).toBeInTheDocument();
  });

  it("defaults to Excel format with Export Excel label", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        pivotData={makePivotData()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-export-data"));
    expect(screen.getByTestId("toolbar-export-data-action")).toHaveTextContent(
      "Export Excel",
    );
    expect(
      screen.getByTestId("export-format-xlsx").getAttribute("aria-checked"),
    ).toBe("true");
  });

  it("switches format selection on toggle click", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        pivotData={makePivotData()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-export-data"));
    const tsvButton = screen.getByTestId("export-format-tsv");
    fireEvent.click(tsvButton);
    expect(screen.getByTestId("toolbar-export-data-action")).toHaveTextContent(
      "Export TSV",
    );
  });

  it("shows Copy to Clipboard label when clipboard format selected", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        pivotData={makePivotData()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-export-data"));
    fireEvent.click(screen.getByTestId("export-format-clipboard"));
    expect(screen.getByTestId("toolbar-export-data-action")).toHaveTextContent(
      "Copy to Clipboard",
    );
  });

  it("closes export popover on Escape key", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        pivotData={makePivotData()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-export-data"));
    const panel = screen.getByTestId("toolbar-export-data-panel");
    expect(panel).toBeInTheDocument();
    fireEvent.keyDown(panel, { key: "Escape" });
    expect(
      screen.queryByTestId("toolbar-export-data-panel"),
    ).not.toBeInTheDocument();
  });
});

describe("Toolbar - fullscreen toggle", () => {
  it("renders fullscreen button when onToggleFullscreen is provided", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        isFullscreen={false}
        onToggleFullscreen={vi.fn()}
      />,
    );
    const btn = screen.getByTestId("toolbar-fullscreen");
    expect(btn).toBeInTheDocument();
    expect(btn).toHaveAttribute("aria-label", "Enter fullscreen");
  });

  it("does not render fullscreen button when onToggleFullscreen is absent", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(screen.queryByTestId("toolbar-fullscreen")).not.toBeInTheDocument();
  });

  it("fires onToggleFullscreen callback when clicked", () => {
    const handleToggle = vi.fn();
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        isFullscreen={false}
        onToggleFullscreen={handleToggle}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-fullscreen"));
    expect(handleToggle).toHaveBeenCalledTimes(1);
  });

  it("shows exit label when isFullscreen is true", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        isFullscreen={true}
        onToggleFullscreen={vi.fn()}
      />,
    );
    const btn = screen.getByTestId("toolbar-fullscreen");
    expect(btn).toHaveAttribute("aria-label", "Exit fullscreen");
  });
});

// ---------------------------------------------------------------------------
// applyDragMove – pure function unit tests
// ---------------------------------------------------------------------------

describe("applyDragMove – reordering within zones", () => {
  it("reorders rows and clears collapsed_groups", () => {
    const cfg = makeConfig({
      rows: ["region", "category"],
      collapsed_groups: ["region|US"],
    });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "rows",
      field: "region",
      overField: "category",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.rows).toEqual(["category", "region"]);
    expect(result!.collapsed_groups).toBeUndefined();
  });

  it("reorders columns and clears collapsed_col_groups", () => {
    const cfg = makeConfig({
      columns: ["year", "category"],
      collapsed_col_groups: ["year|2023"],
    });
    const result = applyDragMove({
      sourceZone: "columns",
      targetZone: "columns",
      field: "year",
      overField: "category",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.columns).toEqual(["category", "year"]);
    expect(result!.collapsed_col_groups).toBeUndefined();
  });

  it("reorders values without altering aggregation", () => {
    const cfg = makeConfig({
      values: ["revenue", "profit"],
      aggregation: { revenue: "sum", profit: "avg" },
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "values",
      field: "revenue",
      overField: "profit",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.values).toEqual(["profit", "revenue"]);
    expect(result!.aggregation).toEqual({ revenue: "sum", profit: "avg" });
  });

  it("returns null when reordering to same position", () => {
    const cfg = makeConfig({ rows: ["region", "category"] });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "rows",
      field: "region",
      overField: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).toBeNull();
  });

  it("returns null when overField is missing", () => {
    const cfg = makeConfig({ rows: ["region", "category"] });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "rows",
      field: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).toBeNull();
  });
});

describe("applyDragMove – cross-zone moves", () => {
  it("moves Rows -> Columns", () => {
    const cfg = makeConfig({ rows: ["region", "category"], columns: ["year"] });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "columns",
      field: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.rows).toEqual(["category"]);
    expect(result!.columns).toEqual(["year", "region"]);
  });

  it("moves Rows -> Values (numeric field)", () => {
    const cfg = makeConfig({
      rows: ["region", "revenue"],
      values: ["profit"],
      aggregation: { profit: "sum" },
    });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "values",
      field: "revenue",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.rows).toEqual(["region"]);
    expect(result!.values).toEqual(["profit", "revenue"]);
    expect(result!.aggregation.revenue).toBe("sum");
  });

  it("rejects non-numeric field to Values", () => {
    const cfg = makeConfig({ rows: ["region", "category"] });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "values",
      field: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).toBeNull();
  });

  it("moves Values -> Rows and prunes aggregation + show_values_as", () => {
    const cfg = makeConfig({
      rows: ["region"],
      values: ["revenue", "profit"],
      aggregation: { revenue: "sum", profit: "avg" },
      show_values_as: { revenue: "pct_of_total" },
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "rows",
      field: "revenue",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.values).toEqual(["profit"]);
    expect(result!.rows).toEqual(["region", "revenue"]);
    expect(result!.aggregation).toEqual({ profit: "avg" });
    expect(result!.show_values_as?.revenue).toBeUndefined();
  });

  it("moves Columns -> Rows", () => {
    const cfg = makeConfig({ rows: ["region"], columns: ["year", "category"] });
    const result = applyDragMove({
      sourceZone: "columns",
      targetZone: "rows",
      field: "year",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.columns).toEqual(["category"]);
    expect(result!.rows).toEqual(["region", "year"]);
  });
});

describe("applyDragMove – config cleanup: sort", () => {
  it("clears row_sort when dimension leaves rows", () => {
    const cfg = makeConfig({
      rows: ["region", "category"],
      row_sort: { by: "key", direction: "asc", dimension: "region" },
    });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "columns",
      field: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.row_sort).toBeUndefined();
  });

  it("clears col_sort when dimension leaves columns", () => {
    const cfg = makeConfig({
      columns: ["year", "category"],
      col_sort: { by: "key", direction: "desc", dimension: "year" },
    });
    const result = applyDragMove({
      sourceZone: "columns",
      targetZone: "rows",
      field: "year",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.col_sort).toBeUndefined();
  });

  it("clears row_sort when value_field leaves values", () => {
    const cfg = makeConfig({
      values: ["revenue", "profit"],
      row_sort: { by: "value", direction: "asc", value_field: "revenue" },
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "rows",
      field: "revenue",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.row_sort).toBeUndefined();
  });

  it("clears col_sort when value_field leaves values", () => {
    const cfg = makeConfig({
      values: ["revenue", "profit"],
      col_sort: { by: "value", direction: "desc", value_field: "profit" },
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "columns",
      field: "profit",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.col_sort).toBeUndefined();
  });

  it("preserves row_sort when a different dimension leaves rows", () => {
    const cfg = makeConfig({
      rows: ["region", "category"],
      row_sort: { by: "key", direction: "asc", dimension: "region" },
    });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "columns",
      field: "category",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.row_sort).toEqual({
      by: "key",
      direction: "asc",
      dimension: "region",
    });
  });
});

describe("applyDragMove – config cleanup: totals", () => {
  it("prunes show_row_totals string array when value field leaves", () => {
    const cfg = makeConfig({
      values: ["revenue", "profit"],
      show_row_totals: ["revenue", "profit"],
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "rows",
      field: "revenue",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.show_row_totals).toEqual(["profit"]);
  });

  it("sets show_row_totals to false when last value field is removed", () => {
    const cfg = makeConfig({
      values: ["revenue", "profit"],
      show_row_totals: ["revenue"],
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "rows",
      field: "revenue",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.show_row_totals).toBe(false);
  });

  it("prunes show_column_totals string array when value field leaves", () => {
    const cfg = makeConfig({
      values: ["revenue", "profit"],
      show_column_totals: ["revenue", "profit"],
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "columns",
      field: "profit",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.show_column_totals).toEqual(["revenue"]);
  });
});

describe("applyDragMove – config cleanup: show_subtotals", () => {
  it("prunes show_subtotals string array when row dimension leaves", () => {
    const cfg = makeConfig({
      rows: ["region", "category"],
      show_subtotals: ["region", "category"],
    });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "columns",
      field: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.show_subtotals).toEqual(["category"]);
  });

  it("sets show_subtotals to false when last entry is removed", () => {
    const cfg = makeConfig({
      rows: ["region", "category"],
      show_subtotals: ["region"],
    });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "columns",
      field: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.show_subtotals).toBe(false);
  });
});

describe("applyDragMove – config cleanup: collapsed_groups", () => {
  it("clears collapsed_groups when field leaves rows", () => {
    const cfg = makeConfig({
      rows: ["region", "category"],
      collapsed_groups: ["region|US"],
    });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "columns",
      field: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.collapsed_groups).toBeUndefined();
  });

  it("clears collapsed_col_groups when field leaves columns", () => {
    const cfg = makeConfig({
      columns: ["year", "category"],
      collapsed_col_groups: ["year|2023"],
    });
    const result = applyDragMove({
      sourceZone: "columns",
      targetZone: "rows",
      field: "year",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.collapsed_col_groups).toBeUndefined();
  });
});

describe("applyDragMove – config cleanup: conditional_formatting", () => {
  it("prunes conditional_formatting apply_to when value field leaves", () => {
    const cfg = makeConfig({
      values: ["revenue", "profit"],
      conditional_formatting: [
        {
          type: "color_scale",
          apply_to: ["revenue", "profit"],
          min_color: "#ffffff",
          max_color: "#ff0000",
        },
      ],
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "rows",
      field: "revenue",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.conditional_formatting).toEqual([
      {
        type: "color_scale",
        apply_to: ["profit"],
        min_color: "#ffffff",
        max_color: "#ff0000",
      },
    ]);
  });

  it("removes rule entirely when apply_to becomes empty", () => {
    const cfg = makeConfig({
      values: ["revenue", "profit"],
      conditional_formatting: [
        {
          type: "color_scale",
          apply_to: ["revenue"],
          min_color: "#ffffff",
          max_color: "#ff0000",
        },
      ],
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "rows",
      field: "revenue",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result!.conditional_formatting).toEqual([]);
  });
});

describe("applyDragMove – mutual exclusion", () => {
  it("rejects dragging a field to rows when it is already in columns (not from columns)", () => {
    const cfg = makeConfig({
      rows: [],
      columns: ["year"],
      values: ["revenue", "year"] as string[],
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "rows",
      field: "year",
      config: cfg,
      numericColumns: [...NUMERIC_COLUMNS, "year"],
    });
    expect(result).toBeNull();
  });

  it("allows moving from columns to rows (mutual exclusion satisfied by removal)", () => {
    const cfg = makeConfig({ rows: ["region"], columns: ["year", "category"] });
    const result = applyDragMove({
      sourceZone: "columns",
      targetZone: "rows",
      field: "year",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.rows).toEqual(["region", "year"]);
    expect(result!.columns).toEqual(["category"]);
  });
});

// ---------------------------------------------------------------------------
// Drag-and-drop rendering tests
// ---------------------------------------------------------------------------

describe("Toolbar – DnD rendering", () => {
  it("renders drag handles on non-frozen, non-disabled chips", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    const chips = screen.getByTestId("toolbar-rows-chips");
    const handles = chips.querySelectorAll("svg");
    expect(handles.length).toBeGreaterThanOrEqual(2);
  });

  it("does not render drag handles on frozen chips", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        frozenColumns={new Set(["region"])}
      />,
    );
    const regionChip = screen.getByTestId("toolbar-rows-chip-region");
    const handleInFrozen = regionChip.querySelectorAll("svg");
    expect(handleInFrozen.length).toBe(0);

    const categoryChip = screen.getByTestId("toolbar-rows-chip-category");
    const handleInNormal = categoryChip.querySelectorAll("svg");
    expect(handleInNormal.length).toBeGreaterThanOrEqual(1);
  });

  it("does not render drag handles when locked", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
      />,
    );
    const chips = screen.getByTestId("toolbar-rows-chips");
    const handles = chips.querySelectorAll("svg");
    expect(handles.length).toBe(0);
  });

  it("shows empty drop zone placeholder when no chips are selected", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: [] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(
      screen.getAllByText("Apply fields in settings menu").length,
    ).toBeGreaterThanOrEqual(1);
  });

  it("hides the empty drop zone placeholder in locked mode", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: [], columns: [], values: [] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        locked={true}
      />,
    );
    expect(
      screen.queryByText("Apply fields in settings menu"),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Drag fields here")).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// resolveDragEnd – DnD routing logic
// ---------------------------------------------------------------------------

describe("resolveDragEnd – DnD routing logic", () => {
  it("returns null when no field in active data", () => {
    expect(
      resolveDragEnd({
        activeData: undefined,
        overId: "sp-zone-rows",
        overData: { type: "container", zone: "rows" },
      }),
    ).toBeNull();
  });

  it("returns null when no target zone", () => {
    expect(
      resolveDragEnd({
        activeData: { field: "region" },
        overId: "unknown",
        overData: undefined,
      }),
    ).toBeNull();
  });

  it("returns add-from-available when source has no zone", () => {
    const result = resolveDragEnd({
      activeData: { field: "category" },
      overId: "sp-zone-rows",
      overData: { type: "container", zone: "rows" },
    });
    expect(result).toEqual({
      type: "add-from-available",
      field: "category",
      toZone: "rows",
    });
  });

  it("returns add-from-available when dropping on a zone chip", () => {
    const result = resolveDragEnd({
      activeData: { field: "category" },
      overId: "rows::region",
      overData: { zone: "rows", field: "region" },
    });
    expect(result).toEqual({
      type: "add-from-available",
      field: "category",
      toZone: "rows",
    });
  });

  it("returns cross-zone when source zone differs from target zone", () => {
    const result = resolveDragEnd({
      activeData: { zone: "rows", field: "region" },
      overId: "sp-zone-columns",
      overData: { type: "container", zone: "columns" },
    });
    expect(result).toEqual({
      type: "cross-zone",
      field: "region",
      fromZone: "rows",
      toZone: "columns",
    });
  });

  it("returns cross-zone when dropping zone chip onto another zone's chip", () => {
    const result = resolveDragEnd({
      activeData: { zone: "rows", field: "region" },
      overId: "columns::year",
      overData: { zone: "columns", field: "year" },
    });
    expect(result).toEqual({
      type: "cross-zone",
      field: "region",
      fromZone: "rows",
      toZone: "columns",
    });
  });

  it("returns reorder when same-zone with different overField", () => {
    const result = resolveDragEnd({
      activeData: { zone: "rows", field: "region" },
      overId: "rows::category",
      overData: { zone: "rows", field: "category" },
    });
    expect(result).toEqual({
      type: "reorder",
      zone: "rows",
      field: "region",
      overField: "category",
    });
  });

  it("returns null for same-zone reorder when overField equals field", () => {
    expect(
      resolveDragEnd({
        activeData: { zone: "rows", field: "region" },
        overId: "rows::region",
        overData: { zone: "rows", field: "region" },
      }),
    ).toBeNull();
  });

  it("returns null for same-zone reorder when no overField", () => {
    expect(
      resolveDragEnd({
        activeData: { zone: "rows", field: "region" },
        overId: "sp-zone-rows",
        overData: { type: "container", zone: "rows" },
      }),
    ).toBeNull();
  });

  it("resolves target zone from container overId", () => {
    const result = resolveDragEnd({
      activeData: { field: "profit" },
      overId: "sp-zone-values",
      overData: { type: "container" },
    });
    expect(result).toEqual({
      type: "add-from-available",
      field: "profit",
      toZone: "values",
    });
  });

  it("resolves target zone from overData.zone when not container", () => {
    const result = resolveDragEnd({
      activeData: { zone: "values", field: "revenue" },
      overId: "rows::region",
      overData: { zone: "rows", field: "region" },
    });
    expect(result).toEqual({
      type: "cross-zone",
      field: "revenue",
      fromZone: "values",
      toZone: "rows",
    });
  });
});

// ---------------------------------------------------------------------------
// DnD drop validation – canDropFieldToZone via applyDragMove
// ---------------------------------------------------------------------------

describe("canDropFieldToZone", () => {
  it("rejects non-numeric fields for values", () => {
    expect(
      canDropFieldToZone({
        field: "region",
        toZone: "values",
        numericFields: new Set(NUMERIC_COLUMNS),
        rowFields: [],
        columnFields: [],
      }),
    ).toBe(false);
  });

  it("rejects dragging a column field into rows from outside columns", () => {
    expect(
      canDropFieldToZone({
        field: "year",
        fromZone: "values",
        toZone: "rows",
        numericFields: new Set([...NUMERIC_COLUMNS, "year"]),
        rowFields: [],
        columnFields: ["year"],
      }),
    ).toBe(false);
  });

  it("allows moving a field directly from columns to rows", () => {
    expect(
      canDropFieldToZone({
        field: "year",
        fromZone: "columns",
        toZone: "rows",
        numericFields: new Set([...NUMERIC_COLUMNS, "year"]),
        rowFields: [],
        columnFields: ["year"],
      }),
    ).toBe(true);
  });
});

describe("applyDragMove – DnD drop validation", () => {
  it("rejects non-numeric field dragged from available to values", () => {
    const cfg = makeConfig({ rows: [], columns: [], values: [] });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "values",
      field: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).toBeNull();
  });

  it("allows numeric field dragged from available to values", () => {
    const cfg = makeConfig({ rows: [], columns: [], values: [] });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "values",
      field: "revenue",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.values).toContain("revenue");
  });

  it("rejects field in columns dragged to rows from non-columns zone", () => {
    const cfg = makeConfig({
      rows: [],
      columns: ["year"],
      values: ["revenue", "year"] as string[],
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "rows",
      field: "year",
      config: cfg,
      numericColumns: [...NUMERIC_COLUMNS, "year"],
    });
    expect(result).toBeNull();
  });

  it("rejects field in rows dragged to columns from non-rows zone", () => {
    const cfg = makeConfig({
      rows: ["region"],
      columns: [],
      values: ["revenue", "region"] as string[],
    });
    const result = applyDragMove({
      sourceZone: "values",
      targetZone: "columns",
      field: "region",
      config: cfg,
      numericColumns: [...NUMERIC_COLUMNS, "region"],
    });
    expect(result).toBeNull();
  });

  it("allows field moved directly from rows to columns", () => {
    const cfg = makeConfig({
      rows: ["region", "category"],
      columns: ["year"],
      values: ["revenue"],
    });
    const result = applyDragMove({
      sourceZone: "rows",
      targetZone: "columns",
      field: "region",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.rows).not.toContain("region");
    expect(result!.columns).toContain("region");
  });

  it("allows field moved directly from columns to rows", () => {
    const cfg = makeConfig({
      rows: ["region"],
      columns: ["year", "category"],
      values: ["revenue"],
    });
    const result = applyDragMove({
      sourceZone: "columns",
      targetZone: "rows",
      field: "year",
      config: cfg,
      numericColumns: NUMERIC_COLUMNS,
    });
    expect(result).not.toBeNull();
    expect(result!.columns).not.toContain("year");
    expect(result!.rows).toContain("year");
  });
});

// ---------------------------------------------------------------------------
// Settings panel – DnD invalid zone visual feedback
// ---------------------------------------------------------------------------

describe("Toolbar - settings panel DnD invalid zone rendering", () => {
  it("shows invalid message on values zone when non-numeric field is assigned via menu", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: [], columns: [], values: [] })}
        allColumns={["region", "revenue"]}
        numericColumns={["revenue"]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const regionChip = screen.getByTestId("settings-available-region");
    fireEvent.click(regionChip);
    expect(screen.queryByText("Add to Values")).not.toBeInTheDocument();
    expect(screen.getByText("Add to Rows")).toBeInTheDocument();
  });
});
