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

"""Interaction-heavy E2E tests for menus, drilldown, grouping, and locked mode."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import Page, expect

from e2e_utils import get_pivot, open_settings_popover
from pivot_table_app_support import _load_main_fixture

ROW_HEADERS_KEY_ASC = ["East", "North", "South", "West"]
ROW_HEADERS_KEY_DESC = ["West", "South", "North", "East"]
ROW_HEADERS_REVENUE_DESC = ["South", "North", "West", "East"]


def click_menu_item(locator) -> None:
    """Click a menu item reliably even when the popover clips its viewport."""
    locator.evaluate(
        "el => { el.scrollIntoView({ block: 'center', inline: 'nearest' }); el.click(); }"
    )


def open_header_menu(page: Page, trigger_locator, menu_test_id: str):
    """Open a header menu and wait for it to become visible."""
    expect(trigger_locator).to_be_visible(timeout=5000)
    trigger_locator.evaluate(
        "el => { el.scrollIntoView({ block: 'center', inline: 'nearest' }); el.click(); }"
    )
    menu = page.get_by_test_id(menu_test_id)
    expect(menu).to_be_visible(timeout=5000)
    return menu


def close_header_menu(page: Page, menu_test_id: str) -> None:
    """Close an open header menu before asserting against updated table state."""
    menu = page.get_by_test_id(menu_test_id)
    page.keyboard.press("Escape")
    expect(menu).to_be_hidden(timeout=5000)


def activate_sort_option(
    page: Page,
    trigger_locator,
    menu_test_id: str,
    option_test_id: str,
    *,
    close_after: bool = True,
):
    """Open a sort menu and activate a preset, normalizing any pre-active state."""
    menu = open_header_menu(page, trigger_locator, menu_test_id)

    option = menu.get_by_test_id(option_test_id)
    if option.get_attribute("aria-pressed") == "true":
        click_menu_item(option)
        menu = open_header_menu(page, trigger_locator, menu_test_id)
        option = menu.get_by_test_id(option_test_id)

    click_menu_item(option)
    if close_after:
        close_header_menu(page, menu_test_id)
    return menu


def test_header_menu_sort_key_asc(page_at_app: Page):
    """Sorting A->Z via header menu reorders rows alphabetically."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    activate_sort_option(
        page,
        container.get_by_test_id("header-menu-trigger-Region"),
        "header-menu-Region",
        "header-sort-key-asc",
    )

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers).to_have_count(4, timeout=10000)
    expect(container.get_by_test_id("pivot-row-header")).to_have_text(
        ROW_HEADERS_KEY_ASC, timeout=10000
    )


def test_header_menu_sort_key_desc(page_at_app: Page):
    """Sorting Z->A via header menu reorders rows in reverse."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    activate_sort_option(
        page,
        container.get_by_test_id("header-menu-trigger-Region"),
        "header-menu-Region",
        "header-sort-key-desc",
    )

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers).to_have_count(4, timeout=10000)
    expect(container.get_by_test_id("pivot-row-header")).to_have_text(
        ROW_HEADERS_KEY_DESC, timeout=10000
    )


def test_header_menu_sort_by_value(page_at_app: Page):
    """Sorting by value via header menu reorders rows by aggregated measure."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    expected_order = (
        _load_main_fixture()
        .groupby("Region", observed=True)["Revenue"]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )

    activate_sort_option(
        page,
        container.get_by_test_id("header-menu-trigger-Region"),
        "header-menu-Region",
        "header-sort-value-desc",
        close_after=False,
    )
    value_field = page.get_by_test_id("header-sort-value-field")
    expect(value_field).to_be_visible(timeout=5000)
    value_field.select_option("Revenue")
    col_key = page.get_by_test_id("header-sort-col-key")
    expect(col_key).to_be_visible(timeout=5000)
    col_key.select_option("")
    close_header_menu(page, "header-menu-Region")

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers).to_have_count(4, timeout=10000)
    expect(row_headers.first).to_have_text(expected_order[0], timeout=10000)
    expect(container.get_by_test_id("pivot-row-header")).to_have_text(
        expected_order, timeout=10000
    )


def test_header_menu_filter_uncheck_value(page_at_app: Page):
    """Unchecking a filter value hides that row from the table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    rows_before = container.get_by_test_id("pivot-row-header").count()

    container.get_by_test_id("header-menu-trigger-Region").evaluate("el => el.click()")
    menu = page.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    filter_section = menu.get_by_test_id("header-menu-filter")
    first_checkbox = filter_section.locator("label").first.locator("input")
    first_checkbox.evaluate("el => el.click()")

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

    container.get_by_test_id("header-menu-trigger-Region").evaluate("el => el.click()")
    menu = page.get_by_test_id("header-menu-Region")
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

    container.get_by_test_id("header-menu-trigger-Region").evaluate("el => el.click()")
    menu = page.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    menu.get_by_role("button", name="Clear All").evaluate("el => el.click()")
    checkboxes = menu.get_by_test_id("header-menu-filter").locator(
        "input[type=checkbox]"
    )
    expect(checkboxes.first).not_to_be_checked(timeout=5000)
    for cb in checkboxes.all():
        expect(cb).not_to_be_checked()

    menu.get_by_role("button", name="Select All").evaluate("el => el.click()")
    expect(checkboxes.first).to_be_checked(timeout=5000)
    for cb in checkboxes.all():
        expect(cb).to_be_checked()


def test_header_menu_show_values_as_pct(page_at_app: Page):
    """Changing display mode to % of Grand Total alters cell values."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cond_fmt")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    trigger = container.get_by_test_id("header-menu-trigger-Revenue").first
    expect(trigger).to_be_visible(timeout=5000)
    menu = open_header_menu(page, trigger, "header-menu-Revenue")

    display_group = page.get_by_test_id("header-menu-display")
    expect(display_group).to_be_visible(timeout=5000)

    click_menu_item(menu.get_by_test_id("header-display-pct_of_total"))
    close_header_menu(page, "header-menu-Revenue")

    revenue_totals = container.get_by_test_id("pivot-row-total")
    expect(revenue_totals.first).to_contain_text("%", timeout=10000)


def test_date_hierarchy_uses_adaptive_default_and_enables_comparisons(
    page_at_app: Page,
):
    page = page_at_app
    container = (
        page.locator(".st-key-test_pivot_date_hierarchy")
        .get_by_test_id("pivot-container")
        .first
    )
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_text("order_date (Quarter)")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q1 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q1 2025")).to_be_visible(timeout=5000)

    menu = open_header_menu(
        page,
        container.get_by_test_id("header-menu-trigger-revenue").first,
        "header-menu-revenue",
    )
    expect(menu.get_by_test_id("header-display-diff_from_prev")).to_be_visible(
        timeout=5000
    )
    expect(menu.get_by_test_id("header-display-diff_from_prev_year")).to_be_visible(
        timeout=5000
    )
    close_header_menu(page, "header-menu-revenue")


def test_date_hierarchy_supports_drill_week_and_original(page_at_app: Page):
    page = page_at_app
    container = (
        page.locator(".st-key-test_pivot_date_hierarchy")
        .get_by_test_id("pivot-container")
        .first
    )
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    menu = open_header_menu(
        page,
        container.get_by_test_id("header-menu-trigger-order_date").first,
        "header-menu-order_date",
    )
    grain_select = menu.get_by_test_id("header-date-grain")
    expect(grain_select).to_have_value("quarter")

    click_menu_item(menu.get_by_test_id("header-date-drill-up"))
    close_header_menu(page, "header-menu-order_date")
    expect(container.get_by_text("order_date (Year)")).to_be_visible(timeout=5000)
    expect(container.get_by_text("2024")).to_be_visible(timeout=5000)

    menu = open_header_menu(
        page,
        container.get_by_test_id("header-menu-trigger-order_date").first,
        "header-menu-order_date",
    )
    grain_select = menu.get_by_test_id("header-date-grain")
    grain_select.select_option("week")
    close_header_menu(page, "header-menu-order_date")
    expect(container.get_by_text("order_date (Week)")).to_be_visible(timeout=5000)
    expect(container.get_by_text("2024-W01")).to_be_visible(timeout=5000)

    menu = open_header_menu(
        page,
        container.get_by_test_id("header-menu-trigger-order_date").first,
        "header-menu-order_date",
    )
    grain_select = menu.get_by_test_id("header-date-grain")
    grain_select.select_option("")
    close_header_menu(page, "header-menu-order_date")
    expect(container.get_by_text("order_date")).to_be_visible(timeout=5000)

    menu = open_header_menu(
        page,
        container.get_by_test_id("header-menu-trigger-revenue").first,
        "header-menu-revenue",
    )
    expect(menu.get_by_test_id("header-display-diff_from_prev")).to_have_count(0)
    close_header_menu(page, "header-menu-revenue")


def test_temporal_hierarchy_toggle_collapses_and_expands(page_at_app: Page):
    """Clicking the +/- toggle collapses a year parent and re-expands it."""
    page = page_at_app
    container = (
        page.locator(".st-key-test_pivot_date_hierarchy")
        .get_by_test_id("pivot-container")
        .first
    )
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # With quarter grain, hierarchy is [year, quarter].
    # Verify year parent headers are visible.
    header_2024 = container.get_by_test_id("pivot-temporal-header-order-date-2024")
    expect(header_2024).to_be_visible(timeout=5000)
    header_2025 = container.get_by_test_id("pivot-temporal-header-order-date-2025")
    expect(header_2025).to_be_visible(timeout=5000)

    # Verify leaf quarter headers are visible for 2024.
    expect(container.get_by_text("Q1 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q2 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q3 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q4 2024")).to_be_visible(timeout=5000)

    # Collapse 2024 by clicking the +/- toggle button.
    toggle_2024 = container.get_by_test_id("temporal-toggle-order-date-2024")
    toggle_2024.click()

    # After collapse: quarter leaf headers under 2024 should be hidden,
    # and a collapsed aggregate cell should appear.
    expect(container.get_by_text("Q1 2024")).to_be_hidden(timeout=5000)
    expect(container.get_by_text("Q2 2024")).to_be_hidden(timeout=5000)
    expect(container.get_by_text("Q3 2024")).to_be_hidden(timeout=5000)
    expect(container.get_by_text("Q4 2024")).to_be_hidden(timeout=5000)
    expect(
        container.get_by_test_id("pivot-temporal-collapse-cell").first
    ).to_be_visible(timeout=5000)

    # 2025 quarter should still be visible (not collapsed).
    expect(container.get_by_text("Q1 2025")).to_be_visible(timeout=5000)

    # Re-expand 2024.
    toggle_2024 = container.get_by_test_id("temporal-toggle-order-date-2024")
    toggle_2024.click()

    # All quarter columns should be visible again.
    expect(container.get_by_text("Q1 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q2 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q3 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q4 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_test_id("pivot-temporal-collapse-cell")).to_have_count(0)


def test_temporal_hierarchy_collapsed_cells_suppress_comparison(page_at_app: Page):
    """Collapsed parent cells render raw aggregates, not period comparisons."""
    page = page_at_app
    container = (
        page.locator(".st-key-test_pivot_date_hierarchy")
        .get_by_test_id("pivot-container")
        .first
    )
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # The fixture has show_values_as={"revenue": "diff_from_prev"}.
    # Leaf cells should have comparison indicators (arrows / deltas).
    data_cells = container.get_by_test_id("pivot-data-cell")
    expect(data_cells.first).to_be_visible(timeout=5000)

    # Collapse 2024.
    toggle_2024 = container.get_by_test_id("temporal-toggle-order-date-2024")
    toggle_2024.click()

    # The collapsed aggregate cells should NOT contain comparison indicators.
    collapse_cells = container.get_by_test_id("pivot-temporal-collapse-cell")
    expect(collapse_cells.first).to_be_visible(timeout=5000)

    # Collapsed cells should contain a plain numeric value (the raw sum for
    # all quarters in 2024), not a comparison arrow/delta.  A comparison
    # indicator includes an arrow character or "▲"/"▼" or "+" prefix.
    first_collapse_text = collapse_cells.first.inner_text()
    assert (
        "▲" not in first_collapse_text
    ), f"Collapsed cell should not show comparison indicator, got: {first_collapse_text}"
    assert (
        "▼" not in first_collapse_text
    ), f"Collapsed cell should not show comparison indicator, got: {first_collapse_text}"

    # Re-expand to restore state for other tests.
    toggle_2024 = container.get_by_test_id("temporal-toggle-order-date-2024")
    toggle_2024.click()
    expect(container.get_by_text("Q1 2024")).to_be_visible(timeout=5000)


def test_temporal_hierarchy_multidim_per_instance_collapse(page_at_app: Page):
    """Collapsing one parent instance in a multi-dimension column layout
    does not collapse its sibling instance."""
    page = page_at_app
    container = (
        page.locator(".st-key-test_pivot_date_hierarchy_multidim")
        .get_by_test_id("pivot-container")
        .first
    )
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # With columns=["region", "order_date"], the hierarchy has region as an
    # outer sibling.  Each region gets its own set of year parent headers.
    # Verify both EU and US 2024 year headers are visible.
    eu_header = container.get_by_test_id("pivot-temporal-header-order-date-2024").first
    expect(eu_header).to_be_visible(timeout=5000)

    # Collapse the first 2024 instance (should be one region only).
    first_toggle = container.get_by_test_id("temporal-toggle-order-date-2024").first
    first_toggle.click()

    # At least one collapsed cell should appear.
    expect(
        container.get_by_test_id("pivot-temporal-collapse-cell").first
    ).to_be_visible(timeout=5000)

    # The other region's 2024 quarters should still have visible leaf headers.
    # Since only one instance was collapsed, there should still be visible
    # quarter headers under the other region's 2024 parent.
    second_header = container.get_by_test_id(
        "pivot-temporal-header-order-date-2024"
    ).nth(1)
    expect(second_header).to_have_attribute("aria-expanded", "true", timeout=5000)

    # Re-expand.
    first_toggle = container.get_by_test_id("temporal-toggle-order-date-2024").first
    first_toggle.click()
    expect(container.get_by_test_id("pivot-temporal-collapse-cell")).to_have_count(
        0, timeout=5000
    )


def test_row_temporal_hierarchy_toggle_collapses_and_expands(page_at_app: Page):
    """Row-side temporal parents collapse into a single synthetic summary row."""
    page = page_at_app
    container = (
        page.locator(".st-key-test_pivot_date_hierarchy_rows")
        .get_by_test_id("pivot-container")
        .first
    )
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.locator("thead").get_by_text("Quarter", exact=True)).to_be_visible(
        timeout=5000
    )
    expect(container.get_by_text("Q1 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q4 2024")).to_be_visible(timeout=5000)

    toggle_2024 = container.get_by_test_id("pivot-temporal-row-toggle-order_date-2024")
    toggle_2024.click()

    expect(container.get_by_text("Q1 2024")).to_be_hidden(timeout=5000)
    expect(container.get_by_text("Q4 2024")).to_be_hidden(timeout=5000)
    expect(container.get_by_test_id("pivot-temporal-parent-row")).to_be_visible(
        timeout=5000
    )
    expect(
        container.get_by_test_id("pivot-temporal-row-collapse-cell").first
    ).to_be_visible(timeout=5000)

    toggle_2024 = container.get_by_test_id("pivot-temporal-row-toggle-order_date-2024")
    toggle_2024.click()
    expect(container.get_by_text("Q1 2024")).to_be_visible(timeout=5000)
    expect(container.get_by_text("Q4 2024")).to_be_visible(timeout=5000)


def test_mixed_row_dimension_collapse_preserves_temporal_state(page_at_app: Page):
    """Outer row-group collapse hides temporal parents and restores them on expand."""
    page = page_at_app
    container = (
        page.locator(".st-key-test_pivot_date_hierarchy_rows_mixed")
        .get_by_test_id("pivot-container")
        .first
    )
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    us_2024_row = (
        container.locator("tr").filter(has_text="US").filter(has_text="Q1 2024")
    )
    expect(us_2024_row).to_have_count(1, timeout=5000)
    us_toggle = us_2024_row.locator(
        '[data-testid="pivot-temporal-row-toggle-order_date-2024"]'
    )
    expect(us_toggle).to_be_visible(timeout=5000)
    us_toggle.evaluate(
        "el => { el.scrollIntoView({ block: 'center', inline: 'nearest' }); el.click(); }"
    )
    expect(container.get_by_text("Q1 2024")).to_have_count(1, timeout=10000)
    expect(container.get_by_test_id("pivot-temporal-parent-row")).to_have_count(
        1, timeout=10000
    )

    container.get_by_test_id("pivot-group-toggle-US").click()
    expect(container.get_by_text("US Total")).to_be_visible(timeout=5000)
    expect(container.get_by_test_id("pivot-temporal-parent-row")).to_have_count(
        0, timeout=5000
    )

    container.get_by_test_id("pivot-group-toggle-US").click()
    expect(container.get_by_test_id("pivot-temporal-parent-row")).to_have_count(
        1, timeout=10000
    )


def test_drilldown_opens_on_cell_click(page_at_app: Page):
    """Clicking a data cell opens the drilldown panel with a detail table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("pivot-data-cell").first.evaluate("el => el.click()")

    expect(page.get_by_test_id("drilldown-panel")).to_be_visible(timeout=10000)
    expect(page.get_by_test_id("drilldown-table")).to_be_visible()


def test_drilldown_close_button(page_at_app: Page):
    """The drilldown close button dismisses the panel."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("pivot-data-cell").first.evaluate("el => el.click()")
    panel = page.get_by_test_id("drilldown-panel")
    expect(panel).to_be_visible(timeout=5000)

    # WebKit can be flaky with the styled icon button; use a direct DOM click.
    page.get_by_test_id("drilldown-close").evaluate("el => el.click()")
    expect(panel).to_be_hidden(timeout=5000)


def test_drilldown_escape_closes(page_at_app: Page):
    """Pressing Escape closes the drilldown panel."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("pivot-data-cell").first.click()
    panel = page.get_by_test_id("drilldown-panel")
    expect(panel).to_be_visible(timeout=5000)

    panel.press("Escape")
    expect(page.get_by_test_id("drilldown-panel")).to_have_count(0, timeout=5000)


def test_subtotal_group_collapse(page_at_app: Page):
    """Collapsing a group hides child rows but keeps the subtotal row."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    open_settings_popover(page, container)
    expand_all = page.get_by_test_id("pivot-group-toggle-expand-all")
    expect(expand_all).to_be_visible(timeout=5000)
    expand_all.evaluate("el => el.click()")
    page.keyboard.press("Escape")
    expect(page.get_by_test_id("toolbar-settings-panel")).to_have_count(0, timeout=5000)

    rows_before = container.get_by_test_id("pivot-data-row").count()

    toggle = container.locator(
        "[data-testid^='pivot-group-toggle-']"
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

    open_settings_popover(page, container)
    expand_all = page.get_by_test_id("pivot-group-toggle-expand-all")
    expect(expand_all).to_be_visible(timeout=5000)
    expand_all.evaluate("el => el.click()")
    rows_expanded = data_rows.count()

    open_settings_popover(page, container)
    collapse_all = page.get_by_test_id("pivot-group-toggle-collapse-all")
    expect(collapse_all).to_be_visible(timeout=5000)
    collapse_all.evaluate("el => el.click()")

    expect(data_rows).not_to_have_count(rows_expanded, timeout=10000)
    rows_collapsed = data_rows.count()
    assert rows_collapsed < rows_expanded

    open_settings_popover(page, container)
    expand_all = page.get_by_test_id("pivot-group-toggle-expand-all")
    expect(expand_all).to_be_visible(timeout=5000)
    expand_all.evaluate("el => el.click()")

    expect(data_rows).not_to_have_count(rows_collapsed, timeout=10000)


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
    menu = page.get_by_test_id("header-menu-Region")
    expect(menu).to_be_visible(timeout=5000)

    sort_section = menu.get_by_test_id("header-menu-sort")
    expect(sort_section).to_be_visible()
    filter_section = menu.get_by_test_id("header-menu-filter")
    expect(filter_section).to_be_visible()
    checkboxes = filter_section.locator("input[type=checkbox]")
    assert checkboxes.count() > 0

    sort_desc = menu.get_by_test_id("header-sort-key-desc")
    sort_desc.evaluate("el => el.click()")
    expect(container.get_by_test_id("pivot-row-header").first).to_have_text(
        "West", timeout=10000
    )

    row_headers = [
        h.inner_text() for h in container.get_by_test_id("pivot-row-header").all()
    ]
    assert row_headers == sorted(row_headers, reverse=True)


def test_locked_mode_show_values_as_still_works(page_at_app: Page):
    """In locked mode, value-header display modes remain available."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_locked")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    trigger = container.get_by_test_id("header-menu-trigger-Revenue").first
    expect(trigger).to_be_visible(timeout=5000)
    trigger.evaluate("el => el.click()")

    display_group = page.get_by_test_id("header-menu-display")
    expect(display_group).to_be_visible(timeout=5000)
    page.get_by_test_id("header-display-pct_of_total").evaluate("el => el.click()")

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


def test_drilldown_disabled_no_panel(page_at_app: Page):
    """With enable_drilldown=False, clicking a data cell does not open the drilldown panel."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_no_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    container.get_by_test_id("pivot-data-cell").first.click()

    page.wait_for_timeout(1000)
    expect(container.get_by_test_id("drilldown-panel")).to_have_count(0)


def test_settings_popover_opens_and_closes(page_at_app: Page):
    """Clicking the gear button opens the settings popover; clicking outside closes it."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("toolbar-settings")).to_be_visible()
    expect(container.get_by_test_id("toolbar-settings-panel")).to_have_count(0)

    open_settings_popover(page, container)

    container.get_by_test_id("pivot-table").click()
    page.wait_for_timeout(500)
    expect(container.get_by_test_id("toolbar-settings-panel")).to_have_count(0)


def test_settings_popover_escape_closes(page_at_app: Page):
    """Pressing Escape closes the settings popover."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    panel = open_settings_popover(page, container)
    panel.press("Escape")
    expect(page.get_by_test_id("toolbar-settings-panel")).to_have_count(0, timeout=5000)


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

    open_settings_popover(page, container)
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


def test_per_dimension_subtotals(page_at_app: Page):
    """Only Region subtotals appear, not Category subtotals."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_per_dim_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    subtotal_rows = container.locator('[data-testid="pivot-subtotal-row"]')
    expect(subtotal_rows.first).to_be_visible()
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


def test_col_group_collapse(page_at_app: Page):
    """Collapsing a column group reduces visible header cells."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_col_groups")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    toggle = container.locator("[data-testid^='pivot-col-group-toggle-']").first
    expect(toggle).to_have_attribute("aria-expanded", "true")
    toggle.click()
    expect(toggle).to_have_attribute("aria-expanded", "false", timeout=10000)

    header_cells = container.get_by_test_id("pivot-header-cell")
    assert header_cells.count() > 0


def test_col_group_expand_collapse_all(page_at_app: Page):
    """Collapse All / Expand All for column groups toggles all groups."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_col_groups")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    headers_expanded = container.get_by_test_id("pivot-header-cell").count()

    header_cells = container.get_by_test_id("pivot-header-cell")

    open_settings_popover(page, container)
    collapse_all = page.get_by_test_id("pivot-col-group-collapse-all")
    expect(collapse_all).to_be_visible(timeout=5000)
    collapse_all.evaluate("el => el.click()")
    expect(header_cells).not_to_have_count(headers_expanded, timeout=10000)
    headers_collapsed = header_cells.count()
    assert headers_collapsed < headers_expanded

    open_settings_popover(page, container)
    expand_all = page.get_by_test_id("pivot-col-group-expand-all")
    expect(expand_all).to_be_visible(timeout=5000)
    expand_all.evaluate("el => el.click()")
    expect(header_cells).not_to_have_count(headers_collapsed, timeout=10000)


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

    activate_sort_option(
        page,
        container.get_by_test_id("header-menu-trigger-Category"),
        "header-menu-Category",
        "header-sort-key-asc",
    )
    page.wait_for_timeout(500)

    regions_after = [h.inner_text() for h in region_headers.all()]
    assert regions_before == regions_after, (
        f"Parent (Region) order should not change when sorting by Category. "
        f"Before: {regions_before}, After: {regions_after}"
    )


def test_filter_empty_state_and_recovery(page_at_app: Page):
    """Clear All filters out all data showing an empty message; Select All recovers."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    rows_before = container.get_by_test_id("pivot-data-row").count()
    assert rows_before > 0

    menu = open_header_menu(
        page,
        container.get_by_test_id("header-menu-trigger-Region"),
        "header-menu-Region",
    )

    menu.get_by_role("button", name="Clear All").evaluate("el => el.click()")
    close_header_menu(page, "header-menu-Region")

    expect(container.get_by_test_id("pivot-data-row")).to_have_count(0, timeout=5000)
    assert container.get_by_test_id("pivot-empty-filter-row").count() <= 1

    menu = open_header_menu(
        page,
        container.get_by_test_id("header-menu-trigger-Region"),
        "header-menu-Region",
    )
    menu.get_by_role("button", name="Select All").evaluate("el => el.click()")
    close_header_menu(page, "header-menu-Region")

    expect(container.get_by_test_id("pivot-empty-filter-row")).to_have_count(
        0, timeout=5000
    )
    recovered_rows = container.get_by_test_id("pivot-data-row")
    expect(recovered_rows).not_to_have_count(0, timeout=5000)
    assert recovered_rows.count() > 0


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


# ---------------------------------------------------------------------------
# Drilldown Pagination (client-only & hybrid)
# ---------------------------------------------------------------------------


def _open_drilldown_with_pagination(page: Page, pivot_key: str):
    """Click the Alpha × 2023 data cell (700 raw rows) and wait for pagination."""
    container = get_pivot(page, pivot_key)
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    # Pin the cell by row label so DOM/column order quirks (e.g. WebKit) cannot
    # hit a different bucket than the 700-row Alpha/2023 intersection.
    alpha_row = container.get_by_test_id("pivot-data-row").filter(has_text="Alpha")
    expect(alpha_row).to_have_count(1, timeout=10000)
    cell = alpha_row.get_by_test_id("pivot-data-cell").first
    cell.scroll_into_view_if_needed()
    expect(cell).to_be_visible(timeout=5000)
    cell.evaluate("el => el.click()")
    panel = page.get_by_test_id("drilldown-panel")
    expect(panel).to_be_visible(timeout=10000)
    expect(panel).to_contain_text("700", timeout=15000)
    pagination = page.get_by_test_id("drilldown-pagination")
    pagination.scroll_into_view_if_needed()
    expect(pagination).to_be_visible(timeout=10000)
    return container, panel


def test_drilldown_client_pagination_shows_controls(page_at_app: Page):
    """Client-only: clicking a cell with >500 records shows pagination controls."""
    page = page_at_app
    _, panel = _open_drilldown_with_pagination(page, "test_pivot_drilldown_pagination")
    expect(panel.get_by_test_id("drilldown-prev")).to_be_visible()
    expect(panel.get_by_test_id("drilldown-next")).to_be_visible()
    expect(panel.locator("text=Page 1 of 2")).to_be_visible()
    expect(panel.locator("text=1–500 of 700 records")).to_be_visible()


def test_drilldown_client_pagination_navigates(page_at_app: Page):
    """Client-only: Next navigates to page 2; Prev returns to page 1."""
    page = page_at_app
    _, panel = _open_drilldown_with_pagination(page, "test_pivot_drilldown_pagination")

    expect(panel.get_by_test_id("drilldown-prev")).to_be_disabled()
    panel.get_by_test_id("drilldown-next").click()

    expect(panel.locator("text=Page 2 of 2")).to_be_visible(timeout=5000)
    expect(panel.locator("text=501–700 of 700 records")).to_be_visible()
    expect(panel.get_by_test_id("drilldown-next")).to_be_disabled()

    panel.get_by_test_id("drilldown-prev").click()
    expect(panel.locator("text=Page 1 of 2")).to_be_visible(timeout=5000)


def test_drilldown_hybrid_pagination_shows_controls(page_at_app: Page):
    """Hybrid mode: clicking a cell with >500 records shows pagination controls."""
    page = page_at_app
    _, panel = _open_drilldown_with_pagination(
        page, "test_pivot_drilldown_pagination_hybrid"
    )
    expect(panel.get_by_test_id("drilldown-prev")).to_be_visible()
    expect(panel.get_by_test_id("drilldown-next")).to_be_visible()
    expect(panel.locator("text=Page 1 of 2")).to_be_visible()
    expect(panel.locator("text=1–500 of 700 records")).to_be_visible()


def test_drilldown_hybrid_pagination_navigates(page_at_app: Page):
    """Hybrid mode: Next navigates to page 2; Prev returns to page 1."""
    page = page_at_app
    _, panel = _open_drilldown_with_pagination(
        page, "test_pivot_drilldown_pagination_hybrid"
    )

    expect(panel.get_by_test_id("drilldown-prev")).to_be_disabled()
    panel.get_by_test_id("drilldown-next").click()

    # Hybrid page changes trigger a full Streamlit rerun; re-query the panel
    # from the page to avoid stale references and allow extra time.
    panel = page.get_by_test_id("drilldown-panel")
    expect(panel).to_be_visible(timeout=30000)
    expect(panel.locator("text=Page 2 of 2")).to_be_visible(timeout=30000)
    expect(panel.locator("text=501–700 of 700 records")).to_be_visible()
    expect(panel.get_by_test_id("drilldown-next")).to_be_disabled()

    panel.get_by_test_id("drilldown-prev").click()
    panel = page.get_by_test_id("drilldown-panel")
    expect(panel).to_be_visible(timeout=30000)
    expect(panel.locator("text=Page 1 of 2")).to_be_visible(timeout=30000)


# ---------------------------------------------------------------------------
# Adaptive date grain e2e tests
# ---------------------------------------------------------------------------


def test_adaptive_grain_multi_year_defaults_to_year(page_at_app: Page):
    """Multi-year dataset auto-defaults to year-level bucketing."""
    page = page_at_app
    container = (
        page.locator(".st-key-test_pivot_adaptive_year")
        .get_by_test_id("pivot-container")
        .first
    )
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    # Year is the auto grain but hierarchy metadata is skipped for grain === "year",
    # so the corner header still uses the combined dimension label.
    header = container.locator("th").filter(has_text="order_date (Year)")
    expect(header).to_be_visible(timeout=10000)


def test_adaptive_grain_3month_defaults_to_month(page_at_app: Page):
    """3-month dataset auto-defaults to month-level bucketing."""
    page = page_at_app
    container = (
        page.locator(".st-key-test_pivot_adaptive_month")
        .get_by_test_id("pivot-container")
        .first
    )
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    # Month grain uses hierarchy [year, quarter, month] on row headers.
    expect(container.get_by_test_id("pivot-row-dim-label-order-date-2")).to_have_text(
        "Month", timeout=10000
    )
