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
import HeaderMenu from "./HeaderMenu";

const baseProps = {
  dimension: "Year",
  axis: "col" as const,
  onSortChange: vi.fn(),
  uniqueValues: ["2022", "2023", "2024"],
  onFilterChange: vi.fn(),
  onClose: vi.fn(),
};

describe("HeaderMenu - rendering", () => {
  it("renders with dimension title", () => {
    render(<HeaderMenu {...baseProps} />);
    expect(screen.getByTestId("header-menu-Year")).toBeInTheDocument();
    expect(screen.getByTestId("header-menu-title")).toHaveTextContent("Year");
  });

  it("renders sort section with key-based options", () => {
    render(<HeaderMenu {...baseProps} />);
    expect(screen.getByTestId("header-sort-key-asc")).toBeInTheDocument();
    expect(screen.getByTestId("header-sort-key-desc")).toBeInTheDocument();
  });

  it("renders value sort options when valueFields provided", () => {
    render(<HeaderMenu {...baseProps} valueFields={["Revenue", "Profit"]} />);
    expect(screen.getByTestId("header-sort-value-asc")).toBeInTheDocument();
    expect(screen.getByTestId("header-sort-value-desc")).toBeInTheDocument();
  });

  it("hides value sort options when no valueFields", () => {
    render(<HeaderMenu {...baseProps} />);
    expect(
      screen.queryByTestId("header-sort-value-asc"),
    ).not.toBeInTheDocument();
  });

  it("renders filter section with search and checkboxes", () => {
    render(<HeaderMenu {...baseProps} />);
    expect(screen.getByTestId("header-filter-search-Year")).toBeInTheDocument();
    expect(screen.getByText("2022")).toBeInTheDocument();
    expect(screen.getByText("2023")).toBeInTheDocument();
    expect(screen.getByText("2024")).toBeInTheDocument();
  });

  it("uses role=menu on the container", () => {
    render(<HeaderMenu {...baseProps} />);
    expect(screen.getByTestId("header-menu-Year")).toHaveAttribute(
      "role",
      "menu",
    );
  });

  it("sort buttons use role=menuitem, filter items use role=menuitemcheckbox", () => {
    render(<HeaderMenu {...baseProps} />);
    expect(screen.getByTestId("header-sort-key-asc")).toHaveAttribute(
      "role",
      "menuitem",
    );
    const filterItem = screen.getByText("2022").closest("label")!;
    expect(filterItem).toHaveAttribute("role", "menuitemcheckbox");
  });
});

describe("HeaderMenu - sort interactions", () => {
  it("calls onSortChange with key_asc when Sort A→Z clicked", () => {
    const onSortChange = vi.fn();
    render(<HeaderMenu {...baseProps} onSortChange={onSortChange} />);
    fireEvent.click(screen.getByTestId("header-sort-key-asc"));
    expect(onSortChange).toHaveBeenCalledWith({ by: "key", direction: "asc" });
  });

  it("calls onSortChange with key_desc when Sort Z→A clicked", () => {
    const onSortChange = vi.fn();
    render(<HeaderMenu {...baseProps} onSortChange={onSortChange} />);
    fireEvent.click(screen.getByTestId("header-sort-key-desc"));
    expect(onSortChange).toHaveBeenCalledWith({ by: "key", direction: "desc" });
  });

  it("clears sort when clicking already-active sort option", () => {
    const onSortChange = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        onSortChange={onSortChange}
        sortConfig={{ by: "key", direction: "asc" }}
      />,
    );
    fireEvent.click(screen.getByTestId("header-sort-key-asc"));
    expect(onSortChange).toHaveBeenCalledWith(undefined);
  });

  it("calls onSortChange with value sort when Sort by value clicked", () => {
    const onSortChange = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        onSortChange={onSortChange}
        valueFields={["Revenue"]}
      />,
    );
    fireEvent.click(screen.getByTestId("header-sort-value-desc"));
    expect(onSortChange).toHaveBeenCalledWith({
      by: "value",
      direction: "desc",
      value_field: "Revenue",
      col_key: undefined,
    });
  });

  it("highlights active sort option", () => {
    render(
      <HeaderMenu
        {...baseProps}
        sortConfig={{ by: "key", direction: "desc" }}
      />,
    );
    const btn = screen.getByTestId("header-sort-key-desc");
    expect(btn.className).toContain("Active");
  });

  it("shows value field selector when sort by value is active", () => {
    render(
      <HeaderMenu
        {...baseProps}
        sortConfig={{ by: "value", direction: "desc", value_field: "Revenue" }}
        valueFields={["Revenue", "Profit"]}
      />,
    );
    expect(screen.getByTestId("header-sort-value-field")).toBeInTheDocument();
  });
});

describe("HeaderMenu - filter interactions", () => {
  it("toggles a filter value off immediately (live filtering)", () => {
    const onFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        onFilterChange={onFilterChange}
        onClose={onClose}
      />,
    );
    const checkbox2022 = screen
      .getByText("2022")
      .closest("label")!
      .querySelector("input")!;
    fireEvent.click(checkbox2022);
    expect(onFilterChange).toHaveBeenCalledWith("Year", { exclude: ["2022"] });
    expect(onClose).not.toHaveBeenCalled();
  });

  it("selects all immediately (live filtering)", () => {
    const onFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        onFilterChange={onFilterChange}
        onClose={onClose}
        filter={{ exclude: ["2022"] }}
      />,
    );
    fireEvent.click(screen.getByText("Select All"));
    expect(onFilterChange).toHaveBeenCalledWith("Year", undefined);
    expect(onClose).not.toHaveBeenCalled();
  });

  it("clears all immediately (live filtering)", () => {
    const onFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        onFilterChange={onFilterChange}
        onClose={onClose}
      />,
    );
    fireEvent.click(screen.getByText("Clear All"));
    expect(onFilterChange).toHaveBeenCalledWith("Year", {
      exclude: ["2022", "2023", "2024"],
    });
    expect(onClose).not.toHaveBeenCalled();
  });

  it("filters values by search text", () => {
    render(<HeaderMenu {...baseProps} />);
    const search = screen.getByTestId("header-filter-search-Year");
    fireEvent.change(search, { target: { value: "202" } });
    expect(screen.getByText("2022")).toBeInTheDocument();
    expect(screen.getByText("2023")).toBeInTheDocument();
    expect(screen.getByText("2024")).toBeInTheDocument();
    fireEvent.change(search, { target: { value: "2024" } });
    expect(screen.queryByText("2022")).not.toBeInTheDocument();
    expect(screen.getByText("2024")).toBeInTheDocument();
  });
});

describe("HeaderMenu - date grouping", () => {
  it("renders date grain controls and comparison display modes when enabled", () => {
    render(
      <HeaderMenu
        {...baseProps}
        title="Order Date (Month)"
        dateGrain="month"
        onDateGrainChange={vi.fn()}
        onDateDrill={vi.fn()}
        supportsPeriodComparison={true}
        onShowValuesAsChange={vi.fn()}
        showValuesAs="diff_from_prev"
      />,
    );
    expect(screen.getByTestId("header-menu-title")).toHaveTextContent(
      "Order Date (Month)",
    );
    expect(screen.getByTestId("header-date-grain")).toHaveValue("month");
    expect(
      screen.getByTestId("header-display-diff_from_prev_year"),
    ).toBeInTheDocument();
  });

  it("calls drill and grain change handlers", () => {
    const onDateGrainChange = vi.fn();
    const onDateDrill = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        dateGrain="month"
        onDateGrainChange={onDateGrainChange}
        onDateDrill={onDateDrill}
      />,
    );
    fireEvent.change(screen.getByTestId("header-date-grain"), {
      target: { value: "quarter" },
    });
    expect(onDateGrainChange).toHaveBeenCalledWith("quarter");
    fireEvent.click(screen.getByTestId("header-date-drill-up"));
    expect(onDateDrill).toHaveBeenCalledWith("up");
  });
});

describe("HeaderMenu - keyboard", () => {
  it("calls onClose on Escape", () => {
    const onClose = vi.fn();
    render(<HeaderMenu {...baseProps} onClose={onClose} />);
    fireEvent.keyDown(screen.getByTestId("header-menu-Year"), {
      key: "Escape",
    });
    expect(onClose).toHaveBeenCalled();
  });

  it("ArrowDown from sort section reaches filter items", () => {
    render(<HeaderMenu {...baseProps} />);
    const menu = screen.getByTestId("header-menu-Year");
    const allNavItems = menu.querySelectorAll("[data-menu-nav]");
    // Sort A→Z, Sort Z→A, search input, Select All, Clear, 2022, 2023, 2024
    expect(allNavItems.length).toBe(8);

    // Focus Sort Z→A (index 1), press ArrowDown repeatedly to reach filter items
    const sortZA = screen.getByTestId("header-sort-key-desc");
    sortZA.focus();

    // ArrowDown → search input (index 2)
    fireEvent.keyDown(menu, { key: "ArrowDown" });
    expect(document.activeElement).toBe(allNavItems[2]); // search input

    // ArrowDown → Select All (index 3)
    fireEvent.keyDown(menu, { key: "ArrowDown" });
    expect(document.activeElement).toBe(allNavItems[3]);

    // ArrowDown → Clear (index 4)
    fireEvent.keyDown(menu, { key: "ArrowDown" });
    expect(document.activeElement).toBe(allNavItems[4]);

    // ArrowDown → 2022 filter item (index 5)
    fireEvent.keyDown(menu, { key: "ArrowDown" });
    expect(document.activeElement).toBe(allNavItems[5]);
  });

  it("ArrowUp from filter section returns to sort section", () => {
    render(<HeaderMenu {...baseProps} />);
    const menu = screen.getByTestId("header-menu-Year");
    const allNavItems = menu.querySelectorAll("[data-menu-nav]");

    // Focus 2022 filter item (index 5), ArrowUp until we reach sort
    (allNavItems[5] as HTMLElement).focus();
    expect(document.activeElement).toBe(allNavItems[5]);

    // ArrowUp → Clear (index 4)
    fireEvent.keyDown(menu, { key: "ArrowUp" });
    expect(document.activeElement).toBe(allNavItems[4]);

    // ArrowUp → Select All (index 3)
    fireEvent.keyDown(menu, { key: "ArrowUp" });
    expect(document.activeElement).toBe(allNavItems[3]);

    // ArrowUp → search input (index 2)
    fireEvent.keyDown(menu, { key: "ArrowUp" });
    expect(document.activeElement).toBe(allNavItems[2]);

    // ArrowUp → Sort Z→A (index 1)
    fireEvent.keyDown(menu, { key: "ArrowUp" });
    expect(document.activeElement).toBe(allNavItems[1]);
  });

  it("Home focuses first item, End focuses last item", () => {
    render(<HeaderMenu {...baseProps} />);
    const menu = screen.getByTestId("header-menu-Year");
    const allNavItems = menu.querySelectorAll("[data-menu-nav]");

    (allNavItems[3] as HTMLElement).focus();
    fireEvent.keyDown(menu, { key: "Home" });
    expect(document.activeElement).toBe(allNavItems[0]);

    fireEvent.keyDown(menu, { key: "End" });
    expect(document.activeElement).toBe(allNavItems[allNavItems.length - 1]);
  });

  it("Enter/Space toggles checkbox on filter items (live filtering)", () => {
    const onFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        onFilterChange={onFilterChange}
        onClose={onClose}
      />,
    );
    const filterLabel2022 = screen.getByText("2022").closest("label")!;
    filterLabel2022.focus();
    fireEvent.keyDown(filterLabel2022, { key: "Enter" });
    expect(onFilterChange).toHaveBeenCalledWith("Year", { exclude: ["2022"] });
    expect(onClose).not.toHaveBeenCalled();
  });

  it("ArrowDown wraps from last item to first", () => {
    render(<HeaderMenu {...baseProps} />);
    const menu = screen.getByTestId("header-menu-Year");
    const allNavItems = menu.querySelectorAll("[data-menu-nav]");
    const last = allNavItems[allNavItems.length - 1] as HTMLElement;
    last.focus();
    fireEvent.keyDown(menu, { key: "ArrowDown" });
    expect(document.activeElement).toBe(allNavItems[0]);
  });

  it("ArrowUp wraps from first item to last", () => {
    render(<HeaderMenu {...baseProps} />);
    const menu = screen.getByTestId("header-menu-Year");
    const allNavItems = menu.querySelectorAll("[data-menu-nav]");
    (allNavItems[0] as HTMLElement).focus();
    fireEvent.keyDown(menu, { key: "ArrowUp" });
    expect(document.activeElement).toBe(allNavItems[allNavItems.length - 1]);
  });
});

describe("HeaderMenu - sort-disabled mode (no onSortChange)", () => {
  it("hides the sort section when onSortChange is undefined", () => {
    render(<HeaderMenu {...baseProps} onSortChange={undefined} />);
    expect(screen.queryByTestId("header-sort-key-asc")).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("header-sort-key-desc"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("header-sort-value-asc"),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("header-sort-value-desc"),
    ).not.toBeInTheDocument();
  });

  it("still renders filter section when onSortChange is undefined", () => {
    render(<HeaderMenu {...baseProps} onSortChange={undefined} />);
    expect(screen.getByTestId("header-filter-search-Year")).toBeInTheDocument();
    expect(screen.getByText("2022")).toBeInTheDocument();
  });
});

describe("HeaderMenu - Tab/Shift-Tab focus trap", () => {
  it("Tab wraps forward instead of leaving the menu", () => {
    render(<HeaderMenu {...baseProps} />);
    const menu = screen.getByTestId("header-menu-Year");
    const allNavItems = menu.querySelectorAll("[data-menu-nav]");
    const last = allNavItems[allNavItems.length - 1] as HTMLElement;
    last.focus();
    fireEvent.keyDown(menu, { key: "Tab" });
    expect(document.activeElement).toBe(allNavItems[0]);
  });

  it("Shift-Tab wraps backward instead of leaving the menu", () => {
    render(<HeaderMenu {...baseProps} />);
    const menu = screen.getByTestId("header-menu-Year");
    const allNavItems = menu.querySelectorAll("[data-menu-nav]");
    (allNavItems[0] as HTMLElement).focus();
    fireEvent.keyDown(menu, { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(allNavItems[allNavItems.length - 1]);
  });

  it("Tab does not call onClose", () => {
    const onClose = vi.fn();
    render(<HeaderMenu {...baseProps} onClose={onClose} />);
    const menu = screen.getByTestId("header-menu-Year");
    const allNavItems = menu.querySelectorAll("[data-menu-nav]");
    (allNavItems[0] as HTMLElement).focus();
    fireEvent.keyDown(menu, { key: "Tab" });
    expect(onClose).not.toHaveBeenCalled();
  });
});

describe("HeaderMenu - Display section (Show Values As)", () => {
  it("does not render display section by default", () => {
    render(<HeaderMenu {...baseProps} />);
    expect(screen.queryByTestId("header-menu-display")).not.toBeInTheDocument();
  });

  it("renders display section when onShowValuesAsChange is provided", () => {
    render(
      <HeaderMenu
        {...baseProps}
        showValuesAs="raw"
        onShowValuesAsChange={vi.fn()}
      />,
    );
    expect(screen.getByTestId("header-menu-display")).toBeInTheDocument();
    expect(screen.getByTestId("header-display-raw")).toBeInTheDocument();
    expect(
      screen.getByTestId("header-display-pct_of_total"),
    ).toBeInTheDocument();
    expect(screen.getByTestId("header-display-pct_of_row")).toBeInTheDocument();
    expect(screen.getByTestId("header-display-pct_of_col")).toBeInTheDocument();
  });

  it("highlights the active display mode", () => {
    render(
      <HeaderMenu
        {...baseProps}
        showValuesAs="pct_of_row"
        onShowValuesAsChange={vi.fn()}
      />,
    );
    const btn = screen.getByTestId("header-display-pct_of_row");
    expect(btn.className).toContain("Active");
    expect(btn).toHaveAttribute("aria-checked", "true");
  });

  it("calls onShowValuesAsChange when a display mode is clicked", () => {
    const onShowValuesAsChange = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        showValuesAs="raw"
        onShowValuesAsChange={onShowValuesAsChange}
      />,
    );
    fireEvent.click(screen.getByTestId("header-display-pct_of_total"));
    expect(onShowValuesAsChange).toHaveBeenCalledWith("pct_of_total");
  });

  it("display items use role=menuitemradio", () => {
    render(
      <HeaderMenu
        {...baseProps}
        showValuesAs="raw"
        onShowValuesAsChange={vi.fn()}
      />,
    );
    const btn = screen.getByTestId("header-display-raw");
    expect(btn).toHaveAttribute("role", "menuitemradio");
  });
});

describe("HeaderMenu - showFilter prop", () => {
  it("hides the filter section when showFilter=false", () => {
    render(
      <HeaderMenu
        {...baseProps}
        showFilter={false}
        showValuesAs="raw"
        onShowValuesAsChange={vi.fn()}
      />,
    );
    expect(screen.queryByTestId("header-menu-filter")).not.toBeInTheDocument();
    expect(
      screen.queryByTestId("header-filter-search-Year"),
    ).not.toBeInTheDocument();
  });

  it("shows the filter section by default (showFilter=true)", () => {
    render(<HeaderMenu {...baseProps} />);
    expect(screen.getByTestId("header-menu-filter")).toBeInTheDocument();
  });
});

describe("HeaderMenu - menu stays open after filter toggle", () => {
  it("menu remains mounted after toggling a filter checkbox", () => {
    const onFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        onFilterChange={onFilterChange}
        onClose={onClose}
      />,
    );

    const checkbox2022 = screen
      .getByText("2022")
      .closest("label")!
      .querySelector("input")!;
    fireEvent.click(checkbox2022);

    expect(onFilterChange).toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByTestId("header-menu-Year")).toBeInTheDocument();
  });

  it("menu remains mounted after Select All", () => {
    const onFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        onFilterChange={onFilterChange}
        onClose={onClose}
        filter={{ exclude: ["2022"] }}
      />,
    );

    fireEvent.click(screen.getByText("Select All"));

    expect(onFilterChange).toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByTestId("header-menu-Year")).toBeInTheDocument();
  });

  it("menu remains mounted after Clear All", () => {
    const onFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...baseProps}
        onFilterChange={onFilterChange}
        onClose={onClose}
      />,
    );

    fireEvent.click(screen.getByText("Clear All"));

    expect(onFilterChange).toHaveBeenCalled();
    expect(onClose).not.toHaveBeenCalled();
    expect(screen.getByTestId("header-menu-Year")).toBeInTheDocument();
  });
});

describe("HeaderMenu - sort config rendering", () => {
  it("highlights sort when sortConfig is passed", () => {
    render(
      <HeaderMenu
        {...baseProps}
        dimension="Year"
        sortConfig={{ by: "key", direction: "asc" }}
      />,
    );
    const btn = screen.getByTestId("header-sort-key-asc");
    expect(btn.className).toContain("Active");
  });

  it("does not highlight sort when no sortConfig passed", () => {
    render(
      <HeaderMenu {...baseProps} dimension="Year" sortConfig={undefined} />,
    );
    const btnAsc = screen.getByTestId("header-sort-key-asc");
    const btnDesc = screen.getByTestId("header-sort-key-desc");
    expect(btnAsc.className).not.toContain("Active");
    expect(btnDesc.className).not.toContain("Active");
  });
});

describe("HeaderMenu - menuLimit & overflow", () => {
  it("caps visible items at 50 and shows overflow indicator", () => {
    const manyValues = Array.from(
      { length: 75 },
      (_, i) => `item-${String(i).padStart(3, "0")}`,
    );
    render(<HeaderMenu {...baseProps} uniqueValues={manyValues} />);
    const labels = screen.getAllByRole("menuitemcheckbox");
    expect(labels.length).toBe(50);
    expect(screen.getByText("and 25 more...")).toBeInTheDocument();
  });

  it("does not show overflow indicator when items fit within limit", () => {
    render(<HeaderMenu {...baseProps} />);
    expect(screen.queryByText(/and \d+ more/)).not.toBeInTheDocument();
  });

  it("respects custom menuLimit prop", () => {
    const values = Array.from({ length: 20 }, (_, i) => `val-${i}`);
    render(<HeaderMenu {...baseProps} uniqueValues={values} menuLimit={10} />);
    const labels = screen.getAllByRole("menuitemcheckbox");
    expect(labels.length).toBe(10);
    expect(screen.getByText("and 10 more...")).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// 0.5.0 — Top N / Value Filter sections
// ---------------------------------------------------------------------------

describe("HeaderMenu - Top N / Bottom N section", () => {
  const topNProps = {
    ...baseProps,
    dimension: "Region",
    axis: "row" as const,
    valueFields: ["Revenue", "Profit"],
    onTopNFilterChange: vi.fn(),
  };

  it("renders Top N section when valueFields and onTopNFilterChange are provided", () => {
    render(<HeaderMenu {...topNProps} />);
    expect(screen.getByTestId("header-menu-top-n")).toBeInTheDocument();
    // Direction is now two segmented buttons instead of a <select>
    expect(screen.getByTestId("header-top-n-dir-top")).toBeInTheDocument();
    expect(screen.getByTestId("header-top-n-dir-bottom")).toBeInTheDocument();
    expect(screen.getByTestId("header-top-n-count")).toBeInTheDocument();
    expect(screen.getByTestId("header-top-n-by")).toBeInTheDocument();
    // Clear is always present, disabled when no filter active
    expect(screen.getByTestId("header-top-n-clear")).toBeInTheDocument();
    expect(screen.getByTestId("header-top-n-clear")).toBeDisabled();
  });

  it("does not render Top N section when onTopNFilterChange is absent", () => {
    render(<HeaderMenu {...baseProps} valueFields={["Revenue"]} />);
    expect(screen.queryByTestId("header-menu-top-n")).not.toBeInTheDocument();
  });

  it("does not render Top N section when valueFields is empty", () => {
    render(<HeaderMenu {...topNProps} valueFields={[]} />);
    expect(screen.queryByTestId("header-menu-top-n")).not.toBeInTheDocument();
  });

  it("Apply button calls onTopNFilterChange with correct filter object and closes menu", () => {
    const onTopNFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...topNProps}
        onTopNFilterChange={onTopNFilterChange}
        onClose={onClose}
        topNFilter={undefined}
      />,
    );

    // Direction is now a segmented button — click the "Bottom" button
    fireEvent.click(screen.getByTestId("header-top-n-dir-bottom"));
    fireEvent.change(screen.getByTestId("header-top-n-count"), {
      target: { value: "5" },
    });
    fireEvent.change(screen.getByTestId("header-top-n-by"), {
      target: { value: "Profit" },
    });
    fireEvent.click(screen.getByTestId("header-top-n-apply"));

    expect(onTopNFilterChange).toHaveBeenCalledWith(
      expect.objectContaining({
        field: "Region",
        n: 5,
        by: "Profit",
        direction: "bottom",
      }),
    );
    expect(onClose).toHaveBeenCalled();
  });

  it("Clear button is enabled when topNFilter is active, disabled otherwise", () => {
    const { rerender } = render(
      <HeaderMenu {...topNProps} topNFilter={undefined} />,
    );
    expect(screen.getByTestId("header-top-n-clear")).toBeDisabled();

    rerender(
      <HeaderMenu
        {...topNProps}
        topNFilter={{ field: "Region", n: 3, by: "Revenue", direction: "top" }}
      />,
    );
    expect(screen.getByTestId("header-top-n-clear")).not.toBeDisabled();
  });

  it("Clear button calls onTopNFilterChange(undefined), resets inputs, and keeps menu open", () => {
    const onTopNFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...topNProps}
        onTopNFilterChange={onTopNFilterChange}
        onClose={onClose}
        topNFilter={{
          field: "Region",
          n: 3,
          by: "Revenue",
          direction: "bottom",
        }}
      />,
    );
    fireEvent.click(screen.getByTestId("header-top-n-clear"));
    expect(onTopNFilterChange).toHaveBeenCalledWith(undefined);
    expect(onClose).not.toHaveBeenCalled();
    // Inputs reset to defaults
    expect(screen.getByTestId("header-top-n-count")).toHaveValue(10);
    expect(screen.getByTestId("header-top-n-by")).toHaveValue("");
    expect(screen.getByTestId("header-top-n-dir-top")).toHaveAttribute(
      "aria-pressed",
      "true",
    );
  });
});

describe("HeaderMenu - Value Filter section", () => {
  const vfProps = {
    ...baseProps,
    dimension: "Region",
    axis: "row" as const,
    valueFields: ["Revenue", "Profit"],
    onValueFilterChange: vi.fn(),
  };

  it("renders value filter section when valueFields and onValueFilterChange are provided", () => {
    render(<HeaderMenu {...vfProps} />);
    expect(screen.getByTestId("header-menu-value-filter")).toBeInTheDocument();
    expect(screen.getByTestId("header-value-filter-by")).toBeInTheDocument();
    // Operator is now a button group — check the group container and one known button
    expect(
      screen.getByTestId("header-value-filter-operator-group"),
    ).toBeInTheDocument();
    expect(screen.getByTestId("header-value-filter-op-gt")).toBeInTheDocument();
    expect(screen.getByTestId("header-value-filter-value")).toBeInTheDocument();
  });

  it("does not render value filter section when onValueFilterChange is absent", () => {
    render(<HeaderMenu {...baseProps} valueFields={["Revenue"]} />);
    expect(
      screen.queryByTestId("header-menu-value-filter"),
    ).not.toBeInTheDocument();
  });

  it("Apply calls onValueFilterChange with correct filter and closes menu", () => {
    const onValueFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...vfProps}
        onValueFilterChange={onValueFilterChange}
        onClose={onClose}
      />,
    );

    fireEvent.change(screen.getByTestId("header-value-filter-by"), {
      target: { value: "Revenue" },
    });
    // Operator is now a button group — click the ≥ (gte) button
    fireEvent.click(screen.getByTestId("header-value-filter-op-gte"));
    fireEvent.change(screen.getByTestId("header-value-filter-value"), {
      target: { value: "1000" },
    });
    fireEvent.click(screen.getByTestId("header-value-filter-apply"));

    expect(onValueFilterChange).toHaveBeenCalledWith(
      expect.objectContaining({
        field: "Region",
        by: "Revenue",
        operator: "gte",
        value: 1000,
      }),
    );
    expect(onClose).toHaveBeenCalled();
  });

  it("between operator shows second value input", () => {
    render(<HeaderMenu {...vfProps} />);
    expect(
      screen.queryByTestId("header-value-filter-value2"),
    ).not.toBeInTheDocument();

    // Click the "btw" (between) operator button
    fireEvent.click(screen.getByTestId("header-value-filter-op-between"));
    expect(
      screen.getByTestId("header-value-filter-value2"),
    ).toBeInTheDocument();
  });

  it("Clear button is enabled when valueFilter is active, disabled otherwise", () => {
    const { rerender } = render(
      <HeaderMenu {...vfProps} valueFilter={undefined} />,
    );
    expect(screen.getByTestId("header-value-filter-clear")).toBeDisabled();

    rerender(
      <HeaderMenu
        {...vfProps}
        valueFilter={{
          field: "Region",
          by: "Revenue",
          operator: "gt",
          value: 500,
        }}
      />,
    );
    expect(screen.getByTestId("header-value-filter-clear")).not.toBeDisabled();
  });

  it("Clear button calls onValueFilterChange(undefined), resets inputs, and keeps menu open", () => {
    const onValueFilterChange = vi.fn();
    const onClose = vi.fn();
    render(
      <HeaderMenu
        {...vfProps}
        onValueFilterChange={onValueFilterChange}
        onClose={onClose}
        valueFilter={{
          field: "Region",
          by: "Revenue",
          operator: "lte",
          value: 500,
        }}
      />,
    );
    fireEvent.click(screen.getByTestId("header-value-filter-clear"));
    expect(onValueFilterChange).toHaveBeenCalledWith(undefined);
    expect(onClose).not.toHaveBeenCalled();
    // Inputs reset to defaults
    expect(screen.getByTestId("header-value-filter-by")).toHaveValue("");
    // type="number" with empty state returns null, not ""
    expect(screen.getByTestId("header-value-filter-value")).toHaveValue(null);
    // Default operator (gt) button should be active
    expect(screen.getByTestId("header-value-filter-op-gt")).toHaveAttribute(
      "aria-pressed",
      "true",
    );
  });
});
