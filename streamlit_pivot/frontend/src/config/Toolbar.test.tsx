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
import Toolbar, { applyDragMove } from "./Toolbar";
import { PivotData, type DataRecord } from "../engine/PivotData";
import { makeConfig } from "../test-utils";

const ALL_COLUMNS = ["region", "year", "revenue", "profit", "category"];
const NUMERIC_COLUMNS = ["revenue", "profit"];

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

  it("renders measure aggregation controls inside the Values dropdown", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    expect(
      screen.getByTestId("toolbar-values-aggregation-controls"),
    ).toBeInTheDocument();
    expect(
      screen.getByTestId("toolbar-values-aggregation-revenue-trigger"),
    ).toHaveTextContent("Sum");
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

describe("Toolbar - interactions", () => {
  it("fires onConfigChange when a value aggregation changes", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue", "profit"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(
      screen.getByTestId("toolbar-values-aggregation-revenue-trigger"),
    );
    fireEvent.click(
      screen.getByTestId("toolbar-values-aggregation-revenue-option-avg"),
    );
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].aggregation).toEqual({
      revenue: "avg",
      profit: "sum",
    });
  });

  it("does not fire when the same value aggregation is selected", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(
      screen.getByTestId("toolbar-values-aggregation-revenue-trigger"),
    );
    fireEvent.click(
      screen.getByTestId("toolbar-values-aggregation-revenue-option-sum"),
    );
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("toggles a row dimension via dropdown checkbox", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ rows: ["region"], columns: [] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-rows-select"));
    fireEvent.click(screen.getByTestId("toolbar-rows-option-category"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].rows).toEqual(["region", "category"]);
  });

  it("unchecks a row dimension via dropdown checkbox", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"], columns: [] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-rows-select"));
    fireEvent.click(screen.getByTestId("toolbar-rows-option-region"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].rows).toEqual(["category"]);
  });

  it("removes a row dimension when chip remove is clicked", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-rows-remove-region"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].rows).toEqual(["category"]);
  });

  it("removes a column dimension when chip remove is clicked", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ columns: ["year"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-columns-remove-year"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].columns).toEqual([]);
  });

  it("removes a value when chip remove is clicked", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue", "profit"] })}
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
  });

  it("creates a synthetic measure from Values builder", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(screen.getByTestId("toolbar-values-add-synthetic"));
    const builder = screen.getByTestId("toolbar-values-synthetic-builder");
    const nameInput = builder.querySelector("input");
    expect(nameInput).toBeTruthy();
    fireEvent.change(nameInput!, { target: { value: "Rev / Profit" } });
    fireEvent.change(screen.getByPlaceholderText("e.g. .1%, $,.0f, ,.2f"), {
      target: { value: ".1%" },
    });
    fireEvent.click(screen.getByText("Save"));
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].synthetic_measures).toHaveLength(1);
    expect(handleChange.mock.calls[0][0].synthetic_measures[0].format).toBe(
      ".1%",
    );
  });

  it("applies a format preset in the synthetic builder", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(screen.getByTestId("toolbar-values-add-synthetic"));
    const builder = screen.getByTestId("toolbar-values-synthetic-builder");
    const nameInput = builder.querySelector("input");
    expect(nameInput).toBeTruthy();
    fireEvent.change(nameInput!, { target: { value: "Margin" } });
    fireEvent.click(screen.getByTestId("toolbar-values-format-preset-percent"));
    expect(
      screen.getByTestId("toolbar-values-format-preview"),
    ).toHaveTextContent("Example:");
    expect(
      screen.getByTestId("toolbar-values-format-preview"),
    ).toHaveTextContent("%");
    fireEvent.click(screen.getByText("Save"));
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
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(screen.getByTestId("toolbar-values-add-synthetic"));
    const builder = screen.getByTestId("toolbar-values-synthetic-builder");
    const nameInput = builder.querySelector("input");
    expect(nameInput).toBeTruthy();
    fireEvent.change(nameInput!, { target: { value: "Bad Format Metric" } });
    fireEvent.change(screen.getByPlaceholderText("e.g. .1%, $,.0f, ,.2f"), {
      target: { value: "abc" },
    });
    expect(
      screen.getByTestId("toolbar-values-format-preview"),
    ).toHaveTextContent("invalid format");
    fireEvent.click(screen.getByText("Save"));
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
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(screen.getByTestId("toolbar-values-add-synthetic"));
    expect(
      screen.getByTestId("toolbar-values-formula-preview"),
    ).toHaveTextContent("sum(revenue) / sum(profit)");
    const builder = screen.getByTestId("toolbar-values-synthetic-builder");
    const selects = builder.querySelectorAll("select");
    fireEvent.change(selects[0], { target: { value: "difference" } });
    expect(
      screen.getByTestId("toolbar-values-formula-preview"),
    ).toHaveTextContent("sum(revenue) - sum(profit)");
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
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(screen.getByTestId("toolbar-values-add-synthetic"));
    const builder = screen.getByTestId("toolbar-values-synthetic-builder");
    const nameInput = builder.querySelector("input");
    expect(nameInput).toBeTruthy();
    fireEvent.change(nameInput!, { target: { value: "Margin" } });
    fireEvent.click(screen.getByText("Save"));
    expect(
      screen.getByText(/collides with an existing value field/i),
    ).toBeInTheDocument();
    expect(handleChange).not.toHaveBeenCalled();
  });

  it("closes synthetic builder when clicking outside", () => {
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(screen.getByTestId("toolbar-values-add-synthetic"));
    expect(
      screen.getByTestId("toolbar-values-synthetic-builder"),
    ).toBeInTheDocument();
    fireEvent.mouseDown(document.body);
    expect(
      screen.queryByTestId("toolbar-values-synthetic-builder"),
    ).not.toBeInTheDocument();
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
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(screen.getByTestId("toolbar-values-add-synthetic"));
    const builder = screen.getByTestId("toolbar-values-synthetic-builder");
    const selects = builder.querySelectorAll("select");
    expect(selects.length).toBeGreaterThanOrEqual(3);
    const numeratorOptions = Array.from(
      selects[1].querySelectorAll("option"),
    ).map((opt) => opt.value);
    expect(numeratorOptions).toContain("profit");
  });

  it("edits and removes a synthetic measure chip", () => {
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
    fireEvent.click(
      screen.getByTestId("toolbar-values-edit-synthetic-rev_minus_profit"),
    );
    const builder = screen.getByTestId("toolbar-values-synthetic-builder");
    const nameInput = builder.querySelector("input");
    expect(nameInput).toBeTruthy();
    fireEvent.change(nameInput!, {
      target: { value: "Rev - Profit (Edited)" },
    });
    fireEvent.click(screen.getByText("Save"));
    expect(handleChange).toHaveBeenCalled();

    fireEvent.click(
      screen.getByTestId("toolbar-values-remove-synthetic-rev_minus_profit"),
    );
    expect(
      handleChange.mock.calls[handleChange.mock.calls.length - 1][0]
        .synthetic_measures,
    ).toEqual([]);
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

describe("Toolbar - options checkboxes (inside settings popover)", () => {
  it("renders row totals and column totals checkboxes inside the settings popover", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("toolbar-row-totals")).toBeInTheDocument();
    expect(screen.getByTestId("toolbar-col-totals")).toBeInTheDocument();
  });

  it("toggles row totals off when checkbox unchecked", () => {
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
      .getByTestId("toolbar-row-totals")
      .querySelector("input")!;
    fireEvent.click(checkbox);
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].show_row_totals).toBe(false);
  });

  it("toggles column totals off when checkbox unchecked", () => {
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
      .getByTestId("toolbar-col-totals")
      .querySelector("input")!;
    fireEvent.click(checkbox);
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

  it("updates only the targeted measure's aggregation", () => {
    const handleChange = vi.fn();
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue", "profit"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={handleChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    fireEvent.click(
      screen.getByTestId("toolbar-values-aggregation-profit-trigger"),
    );
    fireEvent.click(
      screen.getByTestId("toolbar-values-aggregation-profit-option-count"),
    );
    expect(handleChange).toHaveBeenCalledTimes(1);
    expect(handleChange.mock.calls[0][0].aggregation).toEqual({
      revenue: "sum",
      profit: "count",
    });
  });
});

describe("Toolbar - column exclusion", () => {
  it("excludes columns already used as rows from column options", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region"], columns: [] })}
        allColumns={["region", "year", "category"]}
        numericColumns={[]}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-columns-select"));
    const panel = screen.getByTestId("toolbar-columns-panel");
    const items = Array.from(panel.querySelectorAll("[data-testid]"))
      .map((el) => el.getAttribute("data-testid") ?? "")
      .filter((id) => id.startsWith("toolbar-columns-option-"));
    expect(items).not.toContain("toolbar-columns-option-region");
    expect(items).toContain("toolbar-columns-option-year");
    expect(items).toContain("toolbar-columns-option-category");
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

  it("hides chip remove buttons when locked", () => {
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
    expect(screen.getByTestId("toolbar-row-totals-status")).toHaveTextContent(
      "Row Totals",
    );
    expect(screen.getByTestId("toolbar-col-totals-status")).toHaveTextContent(
      "Column Totals",
    );
    expect(screen.queryByTestId("toolbar-row-totals")).not.toBeInTheDocument();
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
    expect(screen.getByTestId("toolbar-row-totals-status")).toHaveTextContent(
      "N/A",
    );
  });
});

describe("Toolbar - frozen columns", () => {
  it("hides remove button for frozen columns", () => {
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

  it("disables checkbox for frozen columns that are selected in dropdown", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        frozenColumns={new Set(["region"])}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-rows-select"));
    const regionCheckbox = screen
      .getByTestId("toolbar-rows-option-region")
      .querySelector("input")!;
    expect(regionCheckbox.disabled).toBe(true);
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
  it("hidden_attributes: excluded column does not appear in any dropdown", () => {
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
    // Open Rows dropdown and verify "category" is not present
    fireEvent.click(screen.getByTestId("toolbar-rows-select"));
    expect(screen.queryByText("category")).not.toBeInTheDocument();
  });

  it("hidden_from_aggregators: column available for rows/cols but not values", () => {
    const numericFiltered = NUMERIC_COLUMNS.filter((c) => c !== "profit");
    render(
      <Toolbar
        config={makeConfig({ values: ["revenue"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={numericFiltered}
        onConfigChange={vi.fn()}
      />,
    );
    // Open Values dropdown — "profit" should not appear
    fireEvent.click(screen.getByTestId("toolbar-values-select"));
    expect(
      screen.queryByTestId("toolbar-values-option-profit"),
    ).not.toBeInTheDocument();
    // But "revenue" should be there
    expect(
      screen.getByTestId("toolbar-values-option-revenue"),
    ).toBeInTheDocument();
  });

  it("hidden_from_drag_drop: frozen column has no remove button", () => {
    render(
      <Toolbar
        config={makeConfig({ rows: ["region", "category"] })}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
        frozenColumns={new Set(["region"])}
      />,
    );
    expect(
      screen.queryByTestId("toolbar-rows-remove-region"),
    ).not.toBeInTheDocument();
    expect(
      screen.getByTestId("toolbar-rows-remove-category"),
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
    expect(screen.getByTestId("toolbar-row-totals-status")).toBeInTheDocument();
    expect(
      screen.queryByTestId("toolbar-rows-remove-region"),
    ).not.toBeInTheDocument();
    expect(screen.queryByTestId("toolbar-reset")).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Settings Popover
// ---------------------------------------------------------------------------

describe("Toolbar - settings popover", () => {
  it("renders gear settings button", () => {
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

  it("opens settings popover on gear click", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    expect(
      screen.queryByTestId("toolbar-settings-panel"),
    ).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("toolbar-settings-panel")).toBeInTheDocument();
  });

  it("closes settings popover on Escape", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    const panel = screen.getByTestId("toolbar-settings-panel");
    expect(panel).toBeInTheDocument();
    fireEvent.keyDown(panel, { key: "Escape" });
    expect(
      screen.queryByTestId("toolbar-settings-panel"),
    ).not.toBeInTheDocument();
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
    expect(screen.getByTestId("toolbar-row-totals")).toBeInTheDocument();
    expect(screen.getByTestId("toolbar-col-totals")).toBeInTheDocument();
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
    expect(screen.getByTestId("toolbar-subtotals")).toBeInTheDocument();
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
    expect(screen.queryByTestId("toolbar-subtotals")).not.toBeInTheDocument();
  });

  it("gear icon is visible in locked mode", () => {
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

  it("locked mode gear popover shows status rows and group actions", () => {
    const onCollapseChange = vi.fn();
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
        onCollapseChange={onCollapseChange}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("toolbar-subtotals-status")).toHaveTextContent(
      "region",
    );
    expect(screen.queryByTestId("toolbar-row-totals")).not.toBeInTheDocument();
    expect(
      screen.getByTestId("pivot-group-toggle-collapse-all"),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("pivot-group-toggle-collapse-all"));
    expect(onCollapseChange).toHaveBeenCalledWith("row", ["__ALL__"]);
  });

  it("omits group actions when collapse callbacks are not provided", () => {
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

  it("toggles closed when gear clicked again", () => {
    render(
      <Toolbar
        config={makeConfig()}
        allColumns={ALL_COLUMNS}
        numericColumns={NUMERIC_COLUMNS}
        onConfigChange={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(screen.getByTestId("toolbar-settings-panel")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("toolbar-settings"));
    expect(
      screen.queryByTestId("toolbar-settings-panel"),
    ).not.toBeInTheDocument();
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
        { type: "color_scale", apply_to: ["revenue", "profit"] },
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
      { type: "color_scale", apply_to: ["profit"] },
    ]);
  });

  it("removes rule entirely when apply_to becomes empty", () => {
    const cfg = makeConfig({
      values: ["revenue", "profit"],
      conditional_formatting: [{ type: "color_scale", apply_to: ["revenue"] }],
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
      screen.getAllByText("Drag fields here").length,
    ).toBeGreaterThanOrEqual(1);
  });
});
