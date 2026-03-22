# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""E2E tests for the pivot table component.

These tests use Playwright via pytest to test the full component end-to-end.
Run with:
    pytest e2e_playwright/ --browser chromium

Covers: rendering, toolbar, header menus, drilldown, conditional formatting,
number formatting, locked mode, data export, config I/O, aggregation types,
auto-detection, column groups, sticky headers, alignment, and edge cases.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from e2e_utils import StreamlitRunner
from playwright.sync_api import Locator, Page, expect

SCRIPT = Path(__file__).parent / "pivot_table.py"

PIVOT_KEYS = [
    "test_pivot",
    "test_pivot_subtotals",
    "test_pivot_locked",
    "test_pivot_locked_groups",
    "test_pivot_cond_fmt",
    "test_pivot_readonly",
    "test_pivot_number_fmt",
    "test_pivot_drilldown",
    "test_pivot_empty",
    "test_pivot_single_row",
    "test_pivot_no_cols",
    "test_pivot_count_distinct",
    "test_pivot_median",
    "test_pivot_auto",
    "test_pivot_threshold",
    "test_pivot_col_groups",
    "test_pivot_alignment",
    "test_pivot_tall",
    "test_pivot_null_separate",
    "test_pivot_null_zero",
    "test_pivot_dim_toggle",
    "test_pivot_no_drilldown",
    "test_pivot_per_dim_subtotals",
    "test_pivot_per_measure_row_totals",
    "test_pivot_per_measure_col_totals",
    "test_pivot_sparse_drilldown",
    "test_pivot_synthetic",
    "test_pivot_scalar_roundtrip",
]


def get_pivot(page: Page, key: str) -> Locator:
    """Return a Locator scoped to the pivot-container for *key*."""
    idx = PIVOT_KEYS.index(key)
    container = page.get_by_test_id("pivot-container").nth(idx)
    container.evaluate("el => el.scrollIntoView({ block: 'center' })")
    return container


def open_settings_popover(container: Locator):
    """Open the gear settings popover in the toolbar."""
    button = container.get_by_test_id("toolbar-settings")
    button.scroll_into_view_if_needed()
    button.evaluate("el => el.click()")
    expect(container.get_by_test_id("toolbar-settings-panel")).to_be_visible(
        timeout=5000
    )


@pytest.fixture(scope="module")
def app():
    """Start the Streamlit app once for the test module."""
    with StreamlitRunner(SCRIPT) as runner:
        yield runner


@pytest.fixture
def page_at_app(app, page: Page):
    """Navigate to the app and wait for it to be ready."""
    page.goto(app.server_url)
    page.wait_for_selector("text=Pivot Table E2E Test App", timeout=30000)
    page.add_style_tag(
        content="header[data-testid='stHeader'] { display: none !important; }"
    )
    return page


# =====================================================================
# Existing smoke / basic tests
# =====================================================================


def test_app_starts(app):
    """Smoke test: the Streamlit app starts and health check passes."""
    assert app.server_port is not None
    assert app.server_url.startswith("http://")


def test_pivot_table_renders(page_at_app: Page):
    """The pivot table renders a real HTML table with data."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container).to_be_visible(timeout=15000)

    table = container.get_by_test_id("pivot-table")
    expect(table).to_be_visible(timeout=10000)

    data_cells = container.get_by_test_id("pivot-data-cell")
    expect(data_cells.first).to_be_visible(timeout=5000)
    assert data_cells.count() > 0


def test_table_has_headers(page_at_app: Page):
    """Table renders column and row headers."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    headers = container.get_by_test_id("pivot-header-cell")
    expect(headers.first).to_be_visible(timeout=5000)
    assert headers.count() >= 1

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers.first).to_be_visible(timeout=5000)
    assert row_headers.count() >= 1


def test_totals_row_visible(page_at_app: Page):
    """Grand total row is rendered when show_totals is True."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    expect(container.get_by_test_id("pivot-totals-row")).to_be_visible(timeout=5000)


def test_toolbar_visible_in_interactive_mode(page_at_app: Page):
    """Toolbar is rendered when interactive mode is on."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container).to_be_visible(timeout=15000)
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=5000)


def test_aggregation_change_via_toolbar(page_at_app: Page):
    """Changing aggregation via toolbar fires config change and updates table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)
    expect(page.get_by_text("Config change count: 0")).to_be_visible()

    container.get_by_test_id("toolbar-values-select").click()
    container.get_by_test_id("toolbar-values-aggregation-Revenue-trigger").click()
    container.get_by_test_id("toolbar-values-aggregation-Revenue-option-avg").click()

    expect(page.get_by_text("Config change count: 1")).to_be_visible(timeout=10000)


def test_cell_click_on_data_cell(page_at_app: Page):
    """Clicking a data cell fires the cell_click trigger."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    expect(page.get_by_text("Cell click count: 0")).to_be_visible()

    container.get_by_test_id("pivot-data-cell").first.click()

    expect(page.get_by_text("Cell click count: 1")).to_be_visible(timeout=10000)
    expect(page.get_by_text("Last cell click")).to_be_visible()


def test_state_persists_across_rerun(page_at_app: Page):
    """Config changes via toolbar persist across unrelated reruns."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-values-select").click()
    container.get_by_test_id("toolbar-values-aggregation-Revenue-trigger").click()
    container.get_by_test_id("toolbar-values-aggregation-Revenue-option-min").click()
    expect(page.get_by_text("Config change count: 1")).to_be_visible(timeout=10000)

    page.get_by_role("button", name="Trigger rerun").scroll_into_view_if_needed()
    page.get_by_role("button", name="Trigger rerun").click()
    expect(page.get_by_text("Reruns:")).to_be_visible(timeout=10000)

    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Min)"
    )


def test_wide_table_horizontal_scroll_alignment(page_at_app: Page):
    """Horizontal scroll of a wide pivot preserves header/data column alignment."""
    page = page_at_app

    page.set_viewport_size({"width": 400, "height": 720})
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    expect(container.get_by_test_id("pivot-data-cell").first).to_be_visible(
        timeout=5000
    )

    wrapper = container.locator("[class*='tableWrapper']")
    scroll_width = wrapper.evaluate("el => el.scrollWidth")
    client_width = wrapper.evaluate("el => el.clientWidth")
    assert scroll_width > client_width, (
        f"Expected table to overflow horizontally: scrollWidth={scroll_width}, "
        f"clientWidth={client_width}"
    )

    def get_column_positions():
        headers = container.get_by_test_id("pivot-header-cell").all()
        data_cells = container.locator(
            "[data-testid='pivot-data-row']:first-of-type "
            "[data-testid='pivot-data-cell']"
        ).all()
        h_boxes = [h.bounding_box() for h in headers[:5]]
        d_boxes = [d.bounding_box() for d in data_cells[:5]]
        return h_boxes, d_boxes

    h_before, d_before = get_column_positions()
    assert len(h_before) > 0
    assert len(d_before) > 0

    scroll_amount = (scroll_width - client_width) // 2
    wrapper.evaluate(f"el => el.scrollLeft = {scroll_amount}")
    page.evaluate(
        "() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))"
    )

    h_after, d_after = get_column_positions()

    assert (
        h_after[0]["x"] != h_before[0]["x"] or h_after[0] != h_before[0]
    ), "Expected headers to shift after horizontal scroll"

    num_compare = min(len(h_after), len(d_after), 3)
    for i in range(num_compare):
        h_left = h_after[i]["x"]
        d_left = d_after[i]["x"]
        delta = abs(h_left - d_left)
        assert delta < 5, (
            f"Column {i} misaligned after scroll: header x={h_left}, "
            f"data x={d_left}, delta={delta}px"
        )


# =====================================================================
# 1. Toolbar -- Rows/Columns/Values dropdowns (4 tests)
# =====================================================================


def test_toolbar_add_row_dimension(page_at_app: Page):
    """Adding a row dimension via toolbar dropdown updates the table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    initial_row_count = container.get_by_test_id("pivot-row-header").count()

    container.get_by_test_id("toolbar-rows-select").click()
    container.get_by_test_id("toolbar-rows-option-Category").click()

    chips = container.get_by_test_id("toolbar-rows-chips")
    expect(chips).to_contain_text("Category", timeout=10000)

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers).not_to_have_count(initial_row_count, timeout=10000)
    assert row_headers.count() > initial_row_count


def test_toolbar_remove_row_chip(page_at_app: Page):
    """Removing a row dimension chip updates the table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    chips_before = container.get_by_test_id("toolbar-rows-chips")
    expect(chips_before).to_contain_text("Region")

    container.get_by_test_id("toolbar-rows-remove-Region").click()

    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=10000)


def test_toolbar_add_value_field(page_at_app: Page):
    """Adding a second value field shows two value label headers."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    initial_labels = container.get_by_test_id("pivot-value-label").count()

    container.get_by_test_id("toolbar-values-select").click()
    container.get_by_test_id("toolbar-values-option-Profit").click()

    value_labels = container.get_by_test_id("pivot-value-label")
    expect(value_labels).not_to_have_count(initial_labels, timeout=10000)
    assert value_labels.count() > initial_labels


def test_toolbar_per_measure_aggregation_controls(page_at_app: Page):
    """Values UI supports setting different aggregations per raw measure."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cond_fmt")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-values-select").click()
    expect(
        container.get_by_test_id("toolbar-values-aggregation-controls")
    ).to_be_visible(timeout=5000)
    profit_trigger = container.get_by_test_id(
        "toolbar-values-aggregation-Profit-trigger"
    )
    profit_trigger.scroll_into_view_if_needed()
    profit_trigger.evaluate("el => el.click()")
    container.get_by_test_id("toolbar-values-aggregation-Profit-option-count").click()

    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Sum)", timeout=10000
    )
    expect(container.get_by_test_id("toolbar-values-chip-label-Profit")).to_have_text(
        "Profit (Count)", timeout=10000
    )


def test_scalar_aggregation_roundtrip_persists_and_python_override_wins(
    page_at_app: Page,
):
    """Scalar Python aggregation hydrates to a map, persists, then yields to Python changes."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_scalar_roundtrip")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-values-select").click()
    expect(
        container.get_by_test_id("toolbar-values-aggregation-controls")
    ).to_be_visible(timeout=5000)
    profit_trigger = container.get_by_test_id(
        "toolbar-values-aggregation-Profit-trigger"
    )
    profit_trigger.scroll_into_view_if_needed()
    profit_trigger.evaluate("el => el.click()")
    container.get_by_test_id("toolbar-values-aggregation-Profit-option-count").click()

    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Sum)", timeout=10000
    )
    expect(container.get_by_test_id("toolbar-values-chip-label-Profit")).to_have_text(
        "Profit (Count)", timeout=10000
    )

    page.get_by_role("button", name="Trigger scalar roundtrip rerun").click()
    expect(page.get_by_text("Reruns:")).to_be_visible(timeout=10000)

    container = get_pivot(page, "test_pivot_scalar_roundtrip")
    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Sum)", timeout=10000
    )
    expect(container.get_by_test_id("toolbar-values-chip-label-Profit")).to_have_text(
        "Profit (Count)", timeout=10000
    )

    page.get_by_role("button", name="Set scalar aggregation to avg").click()
    expect(page.get_by_text("Scalar roundtrip aggregation: avg")).to_be_visible(
        timeout=10000
    )

    container = get_pivot(page, "test_pivot_scalar_roundtrip")
    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Avg)", timeout=10000
    )
    expect(container.get_by_test_id("toolbar-values-chip-label-Profit")).to_have_text(
        "Profit (Avg)", timeout=10000
    )


def test_toolbar_swap_rows_columns(page_at_app: Page):
    """Swapping rows and columns transposes the pivot layout."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)
    expect(page.get_by_text("Config change count: 0")).to_be_visible()

    container.get_by_test_id("toolbar-swap").click(force=True)

    expect(container.get_by_test_id("toolbar-columns-chips")).to_contain_text(
        "Region", timeout=10000
    )


# =====================================================================
# 2. Toolbar -- Options checkboxes (4 tests)
# =====================================================================


def test_toggle_row_totals_off(page_at_app: Page):
    """Unchecking row totals hides the row-total column, re-check restores."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-row-total").first).to_be_visible()

    open_settings_popover(container)
    container.get_by_test_id("toolbar-row-totals").locator("input").click()
    expect(container.get_by_test_id("pivot-row-total")).to_have_count(0, timeout=10000)

    container.get_by_test_id("toolbar-row-totals").locator("input").click()
    expect(container.get_by_test_id("pivot-row-total").first).to_be_visible(
        timeout=10000
    )


def test_toggle_column_totals_off(page_at_app: Page):
    """Unchecking column totals hides the totals row."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-totals-row")).to_be_visible()

    open_settings_popover(container)
    container.get_by_test_id("toolbar-col-totals").locator("input").click()
    expect(container.get_by_test_id("pivot-totals-row")).to_have_count(0, timeout=10000)


def test_toggle_subtotals(page_at_app: Page):
    """Enabling subtotals on a 2-row-dim pivot shows subtotal rows."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-subtotal-row").first).to_be_visible()

    open_settings_popover(container)
    container.get_by_test_id("toolbar-subtotals").locator("input").click()
    expect(container.get_by_test_id("pivot-subtotal-row")).to_have_count(
        0, timeout=10000
    )

    open_settings_popover(container)
    container.get_by_test_id("toolbar-subtotals").locator("input").click()
    expect(container.get_by_test_id("pivot-subtotal-row").first).to_be_visible(
        timeout=10000
    )


def test_toggle_repeat_labels(page_at_app: Page):
    """Toggling repeat labels changes row header rendering."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    headers_before = [
        h.inner_text() for h in container.get_by_test_id("pivot-row-header").all()
    ]

    open_settings_popover(container)
    container.get_by_test_id("toolbar-repeat-labels").locator("input").click()

    first_blank_idx = next(
        (i for i, h in enumerate(headers_before) if h.strip() == ""), None
    )
    if first_blank_idx is not None:
        expect(
            container.get_by_test_id("pivot-row-header").nth(first_blank_idx)
        ).not_to_have_text("", timeout=10000)

    headers_after = [
        h.inner_text() for h in container.get_by_test_id("pivot-row-header").all()
    ]
    assert headers_after != headers_before


# =====================================================================
# 3. Toolbar -- Config Reset (1 test)
# =====================================================================


def test_toolbar_reset_config(page_at_app: Page):
    """Reset button appears after config change and reverts to initial."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("toolbar-reset")).to_have_count(0)

    container.get_by_test_id("toolbar-values-select").click()
    container.get_by_test_id("toolbar-values-aggregation-Revenue-trigger").click()
    container.get_by_test_id("toolbar-values-aggregation-Revenue-option-avg").click()

    expect(container.get_by_test_id("toolbar-reset")).to_be_visible(timeout=10000)

    container.get_by_test_id("toolbar-values-select").click()
    container.get_by_test_id("toolbar-reset").click(force=True)

    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Sum)", timeout=10000
    )
    expect(container.get_by_test_id("toolbar-reset")).to_have_count(0, timeout=10000)


# =====================================================================
# 4. Header Menu -- Sorting (3 tests)
# =====================================================================


def test_header_menu_sort_key_asc(page_at_app: Page):
    """Sorting A->Z via header menu reorders rows alphabetically."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("header-menu-trigger-Region").click()
    menu = page.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    menu.get_by_test_id("header-sort-key-asc").click()

    expect(container.get_by_test_id("pivot-row-header").first).to_have_text(
        "East", timeout=10000
    )

    row_headers = [
        h.inner_text() for h in container.get_by_test_id("pivot-row-header").all()
    ]
    assert row_headers == sorted(row_headers), f"Rows not sorted A->Z: {row_headers}"


def test_header_menu_sort_key_desc(page_at_app: Page):
    """Sorting Z->A via header menu reorders rows in reverse."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("header-menu-trigger-Region").click()
    menu = page.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    menu.get_by_test_id("header-sort-key-desc").evaluate("el => el.click()")

    expect(container.get_by_test_id("pivot-row-header").first).to_have_text(
        "West", timeout=10000
    )

    row_headers = [
        h.inner_text() for h in container.get_by_test_id("pivot-row-header").all()
    ]
    assert row_headers == sorted(
        row_headers, reverse=True
    ), f"Rows not sorted Z->A: {row_headers}"


def test_header_menu_sort_by_value(page_at_app: Page):
    """Sorting by value via header menu reorders rows by aggregated measure."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("header-menu-trigger-Region").click()
    menu = container.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    menu.get_by_test_id("header-sort-value-desc").click()

    expect(container.get_by_test_id("sort-indicator-desc")).to_be_visible(timeout=10000)


# =====================================================================
# 5. Header Menu -- Filtering (3 tests)
# =====================================================================


def test_header_menu_filter_uncheck_value(page_at_app: Page):
    """Unchecking a filter value hides that row from the table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    rows_before = container.get_by_test_id("pivot-row-header").count()

    container.get_by_test_id("header-menu-trigger-Region").click()
    menu = container.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    filter_section = menu.get_by_test_id("header-menu-filter")
    first_checkbox = filter_section.locator("label").first.locator("input")
    first_checkbox.click()

    page.keyboard.press("Escape")

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers).not_to_have_count(rows_before, timeout=10000)
    assert (
        row_headers.count() < rows_before
    ), f"Expected fewer rows after filtering: before={rows_before}, after={row_headers.count()}"


def test_header_menu_filter_search(page_at_app: Page):
    """Typing in the filter search box filters the checklist."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("header-menu-trigger-Region").click()
    menu = container.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    all_items = menu.get_by_test_id("header-menu-filter").locator("label").count()

    search_input = menu.get_by_test_id("header-filter-search-Region")
    search_input.fill("North")

    filter_labels = menu.get_by_test_id("header-menu-filter").locator("label")
    expect(filter_labels).not_to_have_count(all_items, timeout=5000)
    assert filter_labels.count() < all_items


def test_header_menu_filter_select_all_clear_all(page_at_app: Page):
    """Select All / Clear All buttons toggle all filter checkboxes."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("header-menu-trigger-Region").click()
    menu = container.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    menu.get_by_role("button", name="Clear All").click()
    checkboxes = menu.get_by_test_id("header-menu-filter").locator(
        "input[type=checkbox]"
    )
    expect(checkboxes.first).not_to_be_checked(timeout=5000)
    for cb in checkboxes.all():
        expect(cb).not_to_be_checked()

    menu.get_by_role("button", name="Select All").click()
    expect(checkboxes.first).to_be_checked(timeout=5000)
    for cb in checkboxes.all():
        expect(cb).to_be_checked()


# =====================================================================
# 6. Header Menu -- Show Values As (1 test)
# =====================================================================


def test_header_menu_show_values_as_pct(page_at_app: Page):
    """Changing display mode to % of Grand Total alters cell values."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cond_fmt")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    trigger = container.get_by_test_id("header-menu-trigger-Revenue").first
    expect(trigger).to_be_visible(timeout=5000)
    trigger.click()

    display_group = container.get_by_test_id("header-menu-display")
    expect(display_group).to_be_visible(timeout=5000)

    container.get_by_test_id("header-display-pct_of_total").click()

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_contain_text("%", timeout=10000)


# =====================================================================
# 7. Drilldown Panel (3 tests)
# =====================================================================


def test_drilldown_opens_on_cell_click(page_at_app: Page):
    """Clicking a data cell opens the drilldown panel with a detail table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("pivot-data-cell").first.click()

    expect(container.get_by_test_id("drilldown-panel")).to_be_visible(timeout=10000)
    expect(container.get_by_test_id("drilldown-table")).to_be_visible()


def test_drilldown_close_button(page_at_app: Page):
    """The drilldown close button dismisses the panel."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("pivot-data-cell").first.click()
    expect(container.get_by_test_id("drilldown-panel")).to_be_visible(timeout=5000)

    container.get_by_test_id("drilldown-close").click()
    expect(container.get_by_test_id("drilldown-panel")).to_have_count(0, timeout=5000)


def test_drilldown_escape_closes(page_at_app: Page):
    """Pressing Escape closes the drilldown panel."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("pivot-data-cell").first.click()
    expect(container.get_by_test_id("drilldown-panel")).to_be_visible(timeout=5000)

    container.get_by_test_id("drilldown-panel").press("Escape")
    expect(container.get_by_test_id("drilldown-panel")).to_have_count(0, timeout=5000)


# =====================================================================
# 8. Row Group Collapse/Expand (3 tests)
# =====================================================================


def test_subtotal_group_collapse(page_at_app: Page):
    """Collapsing a group hides child rows but keeps the subtotal row."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    rows_before = container.get_by_test_id("pivot-data-row").count()

    toggle = container.locator(
        "button[data-testid^='pivot-group-toggle-']"
        ":not([data-testid$='-expand-all'])"
        ":not([data-testid$='-collapse-all'])"
    ).first
    expect(toggle).to_have_attribute("aria-expanded", "true")
    toggle.click()

    expect(toggle).to_have_attribute("aria-expanded", "false", timeout=10000)

    rows_after = container.get_by_test_id("pivot-data-row").count()
    assert (
        rows_after < rows_before
    ), f"Expected fewer rows after collapse: before={rows_before}, after={rows_after}"

    expect(container.get_by_test_id("pivot-subtotal-row").first).to_be_visible()


def test_subtotal_expand_all_collapse_all(page_at_app: Page):
    """Collapse All hides all child rows; Expand All restores them."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_rows = container.get_by_test_id("pivot-data-row")
    rows_expanded = data_rows.count()

    open_settings_popover(container)
    container.get_by_test_id("pivot-group-toggle-collapse-all").click()

    expect(data_rows).not_to_have_count(rows_expanded, timeout=10000)
    rows_collapsed = data_rows.count()
    assert rows_collapsed < rows_expanded

    open_settings_popover(container)
    container.get_by_test_id("pivot-group-toggle-expand-all").click()

    expect(data_rows).to_have_count(rows_expanded, timeout=10000)


def test_subtotal_row_values(page_at_app: Page):
    """Subtotal cells contain numeric values (not empty)."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    subtotal_cells = container.get_by_test_id("pivot-subtotal-cell")
    expect(subtotal_cells.first).to_be_visible(timeout=5000)

    for cell in subtotal_cells.all()[:4]:
        text = cell.inner_text().strip()
        assert text and text != "-", f"Subtotal cell should have a value, got: {text!r}"


# =====================================================================
# 9. Conditional Formatting (2 tests)
# =====================================================================


def test_conditional_formatting_color_scale(page_at_app: Page):
    """Color scale formatting applies background-color to data cells."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cond_fmt")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)

    has_bg = False
    for cell in cells.all()[:12]:
        style = cell.get_attribute("style") or ""
        if "background-color" in style or "background" in style:
            has_bg = True
            break
    assert (
        has_bg
    ), "Expected at least one data cell to have background-color from color scale"


def test_conditional_formatting_data_bars(page_at_app: Page):
    """Data bars formatting applies background-image (gradient) to cells."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cond_fmt")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)

    has_gradient = False
    for cell in cells.all():
        style = cell.get_attribute("style") or ""
        if "linear-gradient" in style or "background-image" in style:
            has_gradient = True
            break
    assert (
        has_gradient
    ), "Expected at least one data cell to have linear-gradient from data bars"


# =====================================================================
# 10. Number Formatting (1 test)
# =====================================================================


def test_number_format_currency(page_at_app: Page):
    """Currency number format renders values with $ and commas."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_number_fmt")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)

    has_currency = False
    for cell in cells.all()[:8]:
        text = cell.inner_text().strip()
        if text.startswith("$"):
            has_currency = True
            break
    assert has_currency, "Expected at least one cell to display currency format ($)"


# =====================================================================
# 11. Locked Mode (2 tests)
# =====================================================================


def test_locked_mode_toolbar_disabled(page_at_app: Page):
    """In locked mode, authoring controls are hidden but viewer actions remain."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_locked")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    toolbar = container.get_by_test_id("pivot-toolbar")
    expect(toolbar).to_be_visible()

    toolbar_class = toolbar.get_attribute("class") or ""
    assert (
        "Locked" in toolbar_class or "locked" in toolbar_class.lower()
    ), f"Expected toolbar to have locked class, got: {toolbar_class}"

    expect(container.get_by_test_id("toolbar-rows-select")).to_have_count(0)
    expect(container.get_by_test_id("toolbar-swap")).to_have_count(0)
    expect(container.get_by_test_id("toolbar-export-data")).to_be_visible()
    expect(container.get_by_test_id("toolbar-settings")).to_be_visible()


def test_locked_mode_header_sort_and_filter_still_work(page_at_app: Page):
    """In locked mode, header-menu exploration remains available."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_locked")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    trigger = container.get_by_test_id("header-menu-trigger-Region")
    expect(trigger).to_be_visible()

    trigger.evaluate("el => el.click()")
    menu = container.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    sort_section = menu.get_by_test_id("header-menu-sort")
    expect(sort_section).to_be_visible()
    filter_section = menu.get_by_test_id("header-menu-filter")
    expect(filter_section).to_be_visible()
    checkboxes = filter_section.locator("input[type=checkbox]")
    assert checkboxes.count() > 0

    menu.get_by_test_id("header-sort-key-desc").evaluate("el => el.click()")
    expect(container.get_by_test_id("pivot-row-header").first).to_have_text("West")


def test_locked_mode_show_values_as_still_works(page_at_app: Page):
    """In locked mode, value-header display modes remain available."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_locked")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    trigger = container.get_by_test_id("header-menu-trigger-Revenue").first
    expect(trigger).to_be_visible(timeout=5000)
    trigger.click()

    display_group = container.get_by_test_id("header-menu-display")
    expect(display_group).to_be_visible(timeout=5000)
    container.get_by_test_id("header-display-pct_of_total").click()

    expect(container.get_by_test_id("pivot-data-cell").first).to_contain_text(
        "%", timeout=10000
    )


def test_locked_mode_export_data_panel_opens(page_at_app: Page):
    """In locked mode, export remains available as a viewer action."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_locked")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    button = container.get_by_test_id("toolbar-export-data")
    button.scroll_into_view_if_needed()
    button.evaluate("el => el.click()")
    panel = page.get_by_test_id("toolbar-export-data-panel")
    expect(panel).to_be_visible(timeout=5000)
    expect(panel.get_by_test_id("export-format-csv")).to_be_visible()
    expect(panel.get_by_test_id("export-content-formatted")).to_be_visible()


def test_locked_mode_csv_download_content(page_at_app: Page):
    """Locked mode can complete a CSV export successfully."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_locked")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    button = container.get_by_test_id("toolbar-export-data")
    button.scroll_into_view_if_needed()
    button.evaluate("el => el.click()")
    panel = page.get_by_test_id("toolbar-export-data-panel")
    expect(panel).to_be_visible(timeout=5000)

    panel.get_by_test_id("export-format-csv").click()
    panel.get_by_test_id("export-content-raw").click()

    with page.expect_download() as dl_info:
        panel.get_by_test_id("toolbar-export-data-action").click()

    download = dl_info.value
    path = download.path()
    assert path is not None

    content = Path(path).read_text()
    assert "Region" in content
    assert len(content.strip().splitlines()) > 1


def test_readonly_mode_hides_toolbar_and_menu_actions(page_at_app: Page):
    """interactive=False hides the toolbar and removes header-menu config actions."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_readonly")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-toolbar")).to_have_count(0)
    expect(container.get_by_test_id("header-menu-trigger-Region")).to_have_count(0)

    container.get_by_test_id("pivot-data-cell").first.click()
    expect(container.get_by_test_id("drilldown-panel")).to_be_visible(timeout=5000)


# =====================================================================
# 12. Empty / Edge Cases (3 tests)
# =====================================================================


def test_empty_dataframe(page_at_app: Page):
    """An empty DataFrame renders the empty state, not a crash."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_empty")
    expect(container).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-table-empty")).to_be_visible(timeout=5000)


def test_single_row_dataset(page_at_app: Page):
    """A single-row dataset renders a table with one data row."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_single_row")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_rows = container.get_by_test_id("pivot-data-row")
    assert data_rows.count() >= 1

    expect(container.get_by_test_id("pivot-totals-row")).to_be_visible()


def test_no_column_dimension(page_at_app: Page):
    """With no column dimension, the table renders as a flat list."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_no_cols")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers.first).to_be_visible()
    assert row_headers.count() >= 1

    data_cells = container.get_by_test_id("pivot-data-cell")
    assert data_cells.count() >= 1


# =====================================================================
# 13. Data Export (1 test)
# =====================================================================


def test_export_data_panel_opens(page_at_app: Page):
    """The export data button opens a panel with format/content options."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    button = container.get_by_test_id("toolbar-export-data")
    button.scroll_into_view_if_needed()
    button.evaluate("el => el.click()")

    panel = page.get_by_test_id("toolbar-export-data-panel")
    expect(panel).to_be_visible(timeout=5000)

    expect(panel.get_by_test_id("export-format-csv")).to_be_visible()
    expect(panel.get_by_test_id("export-format-tsv")).to_be_visible()
    expect(panel.get_by_test_id("export-content-formatted")).to_be_visible()
    expect(panel.get_by_test_id("export-content-raw")).to_be_visible()


# =====================================================================
# 14. Advanced Aggregation Types (3 tests)
# =====================================================================


def test_aggregation_count_distinct(page_at_app: Page):
    """Count distinct aggregation produces small integer values."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_count_distinct")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)

    for cell in cells.all()[:6]:
        text = cell.inner_text().strip()
        val = float(text.replace(",", ""))
        assert val == int(val), f"Count distinct should be integer, got: {text}"
        assert 1 <= val <= 10, f"Count distinct value out of expected range: {val}"


def test_aggregation_median(page_at_app: Page):
    """Median aggregation produces numeric values different from sum."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_median")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)

    median_vals = []
    for cell in cells.all()[:6]:
        text = cell.inner_text().strip().replace(",", "")
        if text and text != "-":
            median_vals.append(float(text))
    assert len(median_vals) > 0, "Expected numeric median values"

    primary = get_pivot(page, "test_pivot")
    primary_cells = primary.get_by_test_id("pivot-data-cell")
    sum_vals = []
    for cell in primary_cells.all()[:6]:
        text = cell.inner_text().strip().replace(",", "")
        if text and text != "-":
            sum_vals.append(float(text))

    assert median_vals != sum_vals, "Median values should differ from sum values"


# =====================================================================
# 15. Auto-Detection of Dimensions vs. Measures (1 test)
# =====================================================================


def test_auto_detect_dimensions_measures(page_at_app: Page):
    """Auto-detection renders a table with detected rows, columns, and values."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_auto")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    assert container.get_by_test_id("pivot-row-header").count() >= 1
    assert container.get_by_test_id("pivot-data-cell").count() >= 1

    chips = container.get_by_test_id("toolbar-rows-chips")
    expect(chips).to_be_visible()
    assert chips.inner_text().strip() != ""


# =====================================================================
# 16. Config Import/Export (2 tests)
# =====================================================================


@pytest.mark.chromium_only
def test_config_export_copies_json(page_at_app: Page):
    """Clicking export config copies valid JSON to the clipboard."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-export").click(force=True)

    page.wait_for_function(
        "async () => { try { const t = await navigator.clipboard.readText();"
        " return t.startsWith('{'); } catch { return false; } }",
        timeout=5000,
    )
    clip = page.evaluate("() => navigator.clipboard.readText()")
    data = json.loads(clip)
    assert "version" in data
    assert "rows" in data
    assert "columns" in data
    assert "values" in data
    assert "aggregation" in data


@pytest.mark.chromium_only
def test_config_import_apply(page_at_app: Page):
    """Importing a JSON config changes the pivot configuration."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    new_json = json.dumps(
        {
            "version": 1,
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "max"},
        }
    )

    import_toggle = container.get_by_test_id("toolbar-import-toggle")
    import_toggle.scroll_into_view_if_needed()
    import_toggle.evaluate("el => el.click()")
    expect(container.get_by_test_id("toolbar-import-panel")).to_be_visible(timeout=5000)

    textarea = container.get_by_test_id("toolbar-import-textarea")
    textarea.fill(new_json)

    container.get_by_test_id("toolbar-import-apply").click()

    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Max)", timeout=10000
    )


# =====================================================================
# 17. Threshold Conditional Formatting (1 test)
# =====================================================================


def test_conditional_formatting_threshold(page_at_app: Page):
    """Threshold rule applies bold + background to cells above the threshold."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_threshold")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)

    styled_count = 0
    unstyled_count = 0
    for cell in cells.all():
        style = cell.get_attribute("style") or ""
        has_bg = "background-color" in style
        # Component uses fontWeight: 600
        has_bold = "font-weight: 600" in style or "font-weight: 700" in style
        if has_bg and has_bold:
            styled_count += 1
        elif not has_bg and not has_bold:
            unstyled_count += 1

    assert styled_count > 0, "Expected at least one cell to have threshold styling"
    assert unstyled_count > 0, "Expected at least one cell without threshold styling"


# =====================================================================
# 18. Column Group Collapse/Expand (2 tests)
# =====================================================================


def test_col_group_collapse(page_at_app: Page):
    """Collapsing a column group reduces visible header cells."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_col_groups")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    headers_before = container.get_by_test_id("pivot-header-cell").count()

    toggle = container.locator("[data-testid^='pivot-col-group-toggle-']").first
    toggle.click()

    header_cells = container.get_by_test_id("pivot-header-cell")
    expect(header_cells).not_to_have_count(headers_before, timeout=10000)
    assert (
        header_cells.count() < headers_before
    ), f"Expected fewer headers after column collapse: before={headers_before}, after={header_cells.count()}"


def test_col_group_expand_collapse_all(page_at_app: Page):
    """Collapse All / Expand All for column groups toggles all groups."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_col_groups")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    headers_expanded = container.get_by_test_id("pivot-header-cell").count()

    header_cells = container.get_by_test_id("pivot-header-cell")

    open_settings_popover(container)
    container.get_by_test_id("pivot-col-group-collapse-all").click()
    expect(header_cells).not_to_have_count(headers_expanded, timeout=10000)
    headers_collapsed = header_cells.count()
    assert headers_collapsed < headers_expanded

    open_settings_popover(container)
    container.get_by_test_id("pivot-col-group-expand-all").click()
    expect(header_cells).to_have_count(headers_expanded, timeout=10000)


# =====================================================================
# 19. CSV Download Verification (1 test)
# =====================================================================


def test_csv_download_content(page_at_app: Page):
    """Downloading CSV produces a file with expected headers and data."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-export-data").click(force=True)
    panel = container.get_by_test_id("toolbar-export-data-panel")
    expect(panel).to_be_visible(timeout=5000)

    panel.get_by_test_id("export-format-csv").click()
    panel.get_by_test_id("export-content-raw").click()

    with page.expect_download() as dl_info:
        panel.get_by_test_id("toolbar-export-data-action").click()

    download = dl_info.value
    path = download.path()
    assert path is not None

    content = Path(path).read_text()
    assert (
        "Region" in content
    ), f"CSV should contain 'Region' header, got: {content[:200]}"
    assert len(content.strip().splitlines()) > 1, "CSV should have data rows"


# =====================================================================
# 20. Sticky Headers (1 test)
# =====================================================================


def test_sticky_headers_during_scroll(page_at_app: Page):
    """Sticky headers remain visible during vertical scroll."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_tall")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    wrapper = container.locator("[class*='tableWrapper']")
    scroll_height = wrapper.evaluate("el => el.scrollHeight")
    client_height = wrapper.evaluate("el => el.clientHeight")

    if scroll_height <= client_height:
        pytest.skip("Table not tall enough to scroll vertically")

    header = container.get_by_test_id("pivot-header-cell").first
    header_box_before = header.bounding_box()
    assert header_box_before is not None

    wrapper.evaluate("el => el.scrollTop = 100")
    page.evaluate(
        "() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))"
    )

    header_box_after = header.bounding_box()
    assert header_box_after is not None

    wrapper_box = wrapper.bounding_box()
    assert (
        header_box_after["y"] >= wrapper_box["y"] - 2
    ), "Sticky header should remain within the wrapper viewport"

    open_settings_popover(container)
    container.get_by_test_id("toolbar-sticky-headers").locator("input").click()

    table = container.get_by_test_id("pivot-table")
    page.wait_for_function(
        "(el) => el.classList.contains('noSticky') || el.className.includes('noSticky')",
        arg=table.element_handle(),
        timeout=10000,
    )

    wrapper.evaluate("el => el.scrollTop = 100")
    page.evaluate(
        "() => new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)))"
    )

    has_no_sticky = table.evaluate(
        "el => el.classList.contains('noSticky') || el.className.includes('noSticky')"
    )
    assert (
        has_no_sticky
    ), "Table should have noSticky class after disabling sticky headers"


# =====================================================================
# 21. Column Alignment and Empty Cell Display (1 test)
# =====================================================================


def test_column_alignment_and_empty_cell(page_at_app: Page):
    """Right-aligned cells and N/A display for empty cells."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_alignment")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)

    has_right_align = False
    has_na = False
    for cell in cells.all():
        style = cell.get_attribute("style") or ""
        text = cell.inner_text().strip()
        if "text-align" in style and "right" in style:
            has_right_align = True
        if text == "N/A":
            has_na = True

    assert has_right_align, "Expected at least one cell with text-align: right"
    assert has_na, "Expected at least one cell displaying 'N/A' for missing data"


# =====================================================================
# 22. Null Handling (2 tests)
# =====================================================================


def test_null_handling_separate_shows_null_bucket(page_at_app: Page):
    """null_handling='separate' renders a (null) row for records with missing Region."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_null_separate")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers.first).to_be_visible(timeout=5000)

    labels = [h.inner_text().strip() for h in row_headers.all()]
    assert (
        "(null)" in labels
    ), f"Expected a '(null)' row header with null_handling='separate', got: {labels}"


def test_null_handling_zero_no_null_bucket(page_at_app: Page):
    """null_handling='zero' folds nulls into existing groups (no (null) row)."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_null_zero")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers.first).to_be_visible(timeout=5000)

    labels = [h.inner_text().strip() for h in row_headers.all()]
    assert (
        "(null)" not in labels
    ), f"Expected no '(null)' row header with null_handling='zero', got: {labels}"


# =====================================================================
# 23. Drilldown Disabled (1 test)
# =====================================================================


def test_drilldown_disabled_no_panel(page_at_app: Page):
    """With enable_drilldown=False, clicking a data cell does not open the drilldown panel."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_no_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("pivot-data-cell").first.click()

    page.wait_for_timeout(1000)
    expect(container.get_by_test_id("drilldown-panel")).to_have_count(0)


# =====================================================================
# 24. Invalid Config Import (1 test)
# =====================================================================


def test_config_import_invalid_json_no_crash(page_at_app: Page):
    """Pasting invalid JSON into the import textarea does not crash the table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    import_toggle = container.get_by_test_id("toolbar-import-toggle")
    import_toggle.scroll_into_view_if_needed()
    import_toggle.evaluate("el => el.click()")
    expect(container.get_by_test_id("toolbar-import-panel")).to_be_visible(timeout=5000)

    textarea = container.get_by_test_id("toolbar-import-textarea")
    textarea.fill("{ this is not valid json !!! }")

    container.get_by_test_id("toolbar-import-apply").click()

    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=5000)

    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Sum)"
    )


# =====================================================================
# 25. Settings Popover (4 tests)
# =====================================================================


def test_settings_popover_opens_and_closes(page_at_app: Page):
    """Clicking the gear button opens the settings popover; clicking outside closes it."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("toolbar-settings")).to_be_visible()
    expect(container.get_by_test_id("toolbar-settings-panel")).to_have_count(0)

    open_settings_popover(container)

    container.get_by_test_id("pivot-table").click()
    page.wait_for_timeout(500)
    expect(container.get_by_test_id("toolbar-settings-panel")).to_have_count(0)


def test_settings_popover_escape_closes(page_at_app: Page):
    """Pressing Escape closes the settings popover."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    open_settings_popover(container)

    container.get_by_test_id("toolbar-settings-panel").press("Escape")
    expect(container.get_by_test_id("toolbar-settings-panel")).to_have_count(
        0, timeout=5000
    )


def test_action_bar_always_visible(page_at_app: Page):
    """The action bar (swap, settings) is always visible without hover."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("toolbar-swap")).to_be_visible()
    expect(container.get_by_test_id("toolbar-settings")).to_be_visible()


def test_locked_mode_gear_visible_and_functional(page_at_app: Page):
    """In locked mode, the settings gear opens a viewer-oriented popover."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_locked")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("toolbar-settings")).to_be_visible()

    open_settings_popover(container)
    expect(container.get_by_test_id("toolbar-row-totals-status")).to_be_visible()
    expect(container.get_by_test_id("toolbar-col-totals-status")).to_be_visible()


def test_locked_mode_group_actions_still_work(page_at_app: Page):
    """In locked mode, settings popover group actions still collapse/expand rows."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_locked_groups")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    button = container.get_by_test_id("toolbar-settings")
    button.scroll_into_view_if_needed()
    button.evaluate("el => el.click()")
    panel = page.get_by_test_id("toolbar-settings-panel")
    expect(panel).to_be_visible(timeout=5000)
    panel.get_by_test_id("pivot-group-toggle-expand-all").click()

    data_rows = container.get_by_test_id("pivot-data-row")
    rows_expanded = data_rows.count()
    assert rows_expanded > 0

    expect(panel.get_by_test_id("toolbar-subtotals-status")).to_be_visible()
    panel.get_by_test_id("pivot-group-toggle-collapse-all").click()
    expect(data_rows).not_to_have_count(rows_expanded, timeout=10000)
    rows_collapsed = data_rows.count()
    assert rows_collapsed < rows_expanded

    panel.get_by_test_id("pivot-group-toggle-expand-all").click()
    expect(data_rows).to_have_count(rows_expanded, timeout=10000)


def test_locked_mode_inline_row_dim_toggle_still_works(page_at_app: Page):
    """Locked mode still allows inline row dimension toggles for group exploration."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_locked_groups")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    rows_before = container.get_by_test_id("pivot-data-row").count()
    toggle = container.get_by_test_id("pivot-dim-toggle-row-0-region")
    expect(toggle).to_have_attribute("aria-expanded", "true")

    toggle.click()
    expect(toggle).to_have_attribute("aria-expanded", "false", timeout=10000)

    rows_after = container.get_by_test_id("pivot-data-row").count()
    assert rows_after < rows_before


# =====================================================================
# 26. Dimension-level collapse toggles (tests)
# =====================================================================


def test_row_dim_toggle_visible(page_at_app: Page):
    """Row dimension toggles appear for non-innermost dims with subtotals on."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_dim_toggle")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-dim-toggle-row-0-region")).to_be_visible()
    expect(container.get_by_test_id("pivot-dim-toggle-row-1-category")).to_be_visible()
    expect(container.locator("[data-testid^='pivot-dim-toggle-row-2']")).to_have_count(
        0
    )


def test_col_dim_toggle_visible(page_at_app: Page):
    """Column dimension toggle appears for non-last column dims."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_dim_toggle")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-dim-toggle-col-0-year")).to_be_visible()


def test_row_dim_toggle_collapses_groups(page_at_app: Page):
    """Clicking a row dim toggle collapses all groups at that level."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_dim_toggle")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    toggle = container.get_by_test_id("pivot-dim-toggle-row-0-region")
    expect(toggle).to_have_attribute("aria-expanded", "true")

    toggle.click()
    page.wait_for_timeout(1500)

    expect(toggle).to_have_attribute("aria-expanded", "false")


def test_col_dim_toggle_collapses_groups(page_at_app: Page):
    """Clicking a column dim toggle collapses all column groups at that level."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_dim_toggle")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    toggle = container.get_by_test_id("pivot-dim-toggle-col-0-year")
    expect(toggle).to_have_attribute("aria-expanded", "true")

    toggle.click()
    page.wait_for_timeout(1500)

    expect(toggle).to_have_attribute("aria-expanded", "false")


def test_dim_toggle_hidden_single_row(page_at_app: Page):
    """No row dim toggle when only one row dimension."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.locator("[data-testid^='pivot-dim-toggle-row']")).to_have_count(0)


def test_dim_toggle_hidden_single_col(page_at_app: Page):
    """No column dim toggle when only one column dimension."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.locator("[data-testid^='pivot-dim-toggle-col']")).to_have_count(0)


def test_subtotals_pivot_has_row_toggle(page_at_app: Page):
    """The subtotals pivot (2 row dims + subtotals) shows row dim toggle."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-dim-toggle-row-0-region")).to_be_visible()


# =====================================================================
# 27. Per-dimension subtotals and per-measure totals (3 tests)
# =====================================================================


def test_per_dimension_subtotals(page_at_app: Page):
    """Only Region subtotals appear, not Category subtotals."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_per_dim_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    subtotal_rows = container.locator('[data-testid="pivot-subtotal-row"]')
    # Region subtotals should exist
    expect(subtotal_rows.first).to_be_visible()
    # All subtotals should be Region-level (level 0)
    for i in range(subtotal_rows.count()):
        expect(subtotal_rows.nth(i)).to_have_attribute("data-level", "0")


def test_per_measure_row_totals_excluded_shows_dash(page_at_app: Page):
    """Profit row totals show dash when excluded via show_row_totals=['Revenue']."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_per_measure_row_totals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    excluded = container.locator('[data-testid="pivot-excluded-total"]')
    expect(excluded.first).to_be_visible()
    expect(excluded.first).to_have_text("–")


def test_per_measure_column_totals_excluded_shows_dash(page_at_app: Page):
    """Profit column totals show dash when excluded via show_column_totals=['Revenue']."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_per_measure_col_totals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    excluded = container.locator('[data-testid="pivot-excluded-total"]')
    expect(excluded.first).to_be_visible()
    expect(excluded.first).to_have_text("–")


# =====================================================================
# 28. Hierarchical sort — child dim sort preserves parent groups
# =====================================================================


def test_hierarchical_sort_preserves_parent_groups(page_at_app: Page):
    """Sorting by Category A→Z reorders within Region groups without reordering Regions."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    region_headers = container.locator(
        '[data-testid="pivot-row-header"][data-dim-index="0"]'
    )
    expect(region_headers.first).to_be_visible(timeout=5000)
    regions_before = [h.inner_text() for h in region_headers.all()]

    container.get_by_test_id("header-menu-trigger-Category").click()
    menu = container.get_by_test_id("header-menu-Category")
    expect(menu).to_be_visible(timeout=5000)

    menu.get_by_test_id("header-sort-key-asc").click()
    page.wait_for_timeout(1500)

    regions_after = [h.inner_text() for h in region_headers.all()]
    assert regions_before == regions_after, (
        f"Parent (Region) order should not change when sorting by Category. "
        f"Before: {regions_before}, After: {regions_after}"
    )


# =====================================================================
# 29. Live filter empty state — Clear All shows message, Select All recovers
# =====================================================================


def test_filter_empty_state_and_recovery(page_at_app: Page):
    """Clear All filters out all data showing an empty message; Select All recovers."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    rows_before = container.get_by_test_id("pivot-data-row").count()
    assert rows_before > 0

    container.get_by_test_id("header-menu-trigger-Region").click()
    menu = container.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    menu.get_by_role("button", name="Clear All").click()
    page.wait_for_timeout(1000)

    expect(container.get_by_test_id("pivot-empty-filter-row")).to_be_visible(
        timeout=5000
    )
    expect(container.get_by_test_id("pivot-data-row")).to_have_count(0, timeout=5000)

    menu.get_by_role("button", name="Select All").click()
    page.wait_for_timeout(1000)

    expect(container.get_by_test_id("pivot-empty-filter-row")).to_have_count(
        0, timeout=5000
    )
    expect(container.get_by_test_id("pivot-data-row")).to_have_count(
        rows_before, timeout=5000
    )


# =====================================================================
# 30. Disabled child toggle when parent dimension is collapsed
# =====================================================================


def test_child_toggle_disabled_when_parent_collapsed(page_at_app: Page):
    """Collapsing Region disables the Category toggle (no role=button, shows tooltip)."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_dim_toggle")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    region_toggle = container.get_by_test_id("pivot-dim-toggle-row-0-region")
    category_toggle = container.get_by_test_id("pivot-dim-toggle-row-1-category")

    expect(category_toggle).to_have_attribute("role", "button")

    region_toggle.click()
    page.wait_for_timeout(1500)
    expect(region_toggle).to_have_attribute("aria-expanded", "false")

    expect(category_toggle).not_to_have_attribute("role", "button", timeout=5000)
    expect(category_toggle).to_have_attribute("title", "Expand Region first")


# =====================================================================
# 31. Empty (null) data cells are non-interactive
# =====================================================================


def test_empty_data_cells_non_interactive(page_at_app: Page):
    """Cells displaying the empty_cell_value have no tabindex and clicking does not open drilldown."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_sparse_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell")
    expect(data_cells.first).to_be_visible(timeout=5000)

    empty_cell = None
    for cell in data_cells.all():
        if cell.inner_text().strip() == "-":
            empty_cell = cell
            break

    assert empty_cell is not None, "Expected at least one empty cell with '-'"

    assert (
        empty_cell.get_attribute("tabindex") is None
    ), "Empty cell should not have tabindex"
    assert (
        empty_cell.get_attribute("role") is None
    ), "Empty cell should not have role=gridcell"

    empty_cell.click(force=True)
    page.wait_for_timeout(500)
    expect(container.get_by_test_id("drilldown-panel")).to_have_count(0)


# =====================================================================
# 32. Synthetic measures (3 tests)
# =====================================================================


def test_synthetic_measure_columns_render(page_at_app: Page):
    """Synthetic measure labels are rendered in the value header row."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_synthetic")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    labels = container.get_by_test_id("pivot-value-label")
    expect(labels.first).to_be_visible(timeout=5000)
    text_blob = " ".join([label.inner_text() for label in labels.all()])
    assert "PRs / Person" in text_blob
    assert "PRs - People" in text_blob


def test_synthetic_denominator_zero_shows_dash(page_at_app: Page):
    """Synthetic ratio shows '-' when denominator is zero."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_synthetic")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    cells = container.get_by_test_id("pivot-data-cell")
    texts = [c.inner_text().strip() for c in cells.all()]
    assert "-" in texts


def test_synthetic_menu_hides_show_values_as(page_at_app: Page):
    """Synthetic value header menu omits show-values-as controls."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_synthetic")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    container.get_by_test_id("header-menu-trigger-prs_per_person").click()
    expect(container.get_by_test_id("header-menu-prs_per_person")).to_be_visible(
        timeout=5000
    )
    expect(container.get_by_test_id("header-menu-display")).to_have_count(0)
