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

"""Smoke and toolbar-focused E2E tests for the pivot table component."""

from __future__ import annotations

import re

from playwright.sync_api import Page, expect

from e2e_utils import get_pivot, open_settings_popover


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

    container.get_by_test_id("toolbar-values-select").evaluate("el => el.click()")
    trigger = page.get_by_test_id("toolbar-values-aggregation-Revenue-trigger")
    expect(trigger).to_be_visible(timeout=5000)
    trigger.evaluate("el => el.click()")
    option = page.get_by_test_id("toolbar-values-aggregation-Revenue-option-avg")
    expect(option).to_be_visible(timeout=5000)
    option.scroll_into_view_if_needed()
    option.click(force=True)

    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Avg)", timeout=10000
    )
    expect(page.get_by_text("Config change count: 1")).to_be_visible(timeout=10000)


def test_cell_click_on_data_cell(page_at_app: Page):
    """Clicking a data cell fires the cell_click trigger."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    expect(page.get_by_text("Cell click count: 0")).to_be_visible()

    # Wait for the component's initial perf-metrics render cycle to settle
    # so that any state-driven Streamlit reruns complete before we click.
    expect(container).to_have_attribute(
        "data-perf-metrics", re.compile(r".+"), timeout=10000
    )
    page.wait_for_timeout(500)

    cell = container.get_by_test_id("pivot-data-cell").first
    cell.scroll_into_view_if_needed()
    cell.click(force=True)

    expect(page.get_by_text("Cell click count: 1")).to_be_visible(timeout=10000)
    expect(page.get_by_text("Last cell click")).to_be_visible()


def test_state_persists_across_rerun(page_at_app: Page):
    """Config changes via toolbar persist across unrelated reruns."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-swap").evaluate("el => el.click()")
    expect(container.get_by_test_id("toolbar-columns-chips")).to_contain_text(
        "Region", timeout=10000
    )

    rerun_button = page.get_by_role("button", name="Trigger rerun")
    expect(rerun_button).to_be_visible(timeout=10000)
    rerun_button.click()
    expect(page.get_by_text("Reruns:")).to_be_visible(timeout=10000)

    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("toolbar-columns-chips")).to_contain_text(
        "Region", timeout=10000
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


def test_toolbar_add_row_dimension(page_at_app: Page):
    """Adding a row dimension via toolbar dropdown updates the table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    initial_row_count = container.get_by_test_id("pivot-row-header").count()

    container.get_by_test_id("toolbar-rows-select").evaluate("el => el.click()")
    option = page.get_by_test_id("toolbar-rows-option-Category")
    expect(option).to_be_visible(timeout=5000)
    option.scroll_into_view_if_needed()
    option.click(force=True)

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

    container.get_by_test_id("toolbar-values-select").evaluate("el => el.click()")
    option = page.get_by_test_id("toolbar-values-option-Profit")
    expect(option).to_be_visible(timeout=5000)
    option.evaluate("el => el.click()")

    expect(container.get_by_test_id("toolbar-values-chip-label-Profit")).to_be_visible(
        timeout=10000
    )


def test_toolbar_per_measure_aggregation_controls(page_at_app: Page):
    """Values UI supports setting different aggregations per raw measure."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cond_fmt")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-values-select").evaluate("el => el.click()")
    controls = page.get_by_test_id("toolbar-values-aggregation-controls")
    expect(controls).to_be_visible(timeout=5000)
    profit_trigger = page.get_by_test_id("toolbar-values-aggregation-Profit-trigger")
    profit_trigger.scroll_into_view_if_needed()
    profit_trigger.evaluate("el => el.click()")
    page.get_by_test_id("toolbar-values-aggregation-Profit-option-count").click()

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
    container.get_by_test_id("toolbar-swap").evaluate("el => el.click()")
    expect(container.get_by_test_id("toolbar-columns-chips")).to_contain_text(
        "Region", timeout=10000
    )
    expect(container.get_by_test_id("toolbar-rows-chips")).not_to_contain_text(
        "Region", timeout=10000
    )


def test_toggle_row_totals_off(page_at_app: Page):
    """Unchecking row totals hides the row-total column, re-check restores."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-row-total").first).to_be_visible()

    panel = open_settings_popover(page, container)
    panel.get_by_test_id("toolbar-row-totals").locator("input").evaluate(
        "el => el.click()"
    )
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

    panel = open_settings_popover(page, container)
    panel.get_by_test_id("toolbar-col-totals").locator("input").evaluate(
        "el => el.click()"
    )
    expect(container.get_by_test_id("pivot-totals-row")).to_have_count(0, timeout=10000)


def test_toggle_subtotals(page_at_app: Page):
    """Enabling subtotals on a 2-row-dim pivot shows subtotal rows."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-subtotal-row").first).to_be_visible()

    panel = open_settings_popover(page, container)
    panel.get_by_test_id("toolbar-subtotals").locator("input").click()
    expect(container.get_by_test_id("pivot-subtotal-row")).to_have_count(
        0, timeout=10000
    )

    panel = open_settings_popover(page, container)
    panel.get_by_test_id("toolbar-subtotals").locator("input").click()
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

    panel = open_settings_popover(page, container)
    panel.get_by_test_id("toolbar-repeat-labels").locator("input").click()

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


def test_toolbar_reset_config(page_at_app: Page):
    """Reset button appears after config change and reverts to initial."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("toolbar-reset")).to_have_count(0)

    container.get_by_test_id("toolbar-swap").evaluate("el => el.click()")
    expect(container.get_by_test_id("toolbar-columns-chips")).to_contain_text(
        "Region", timeout=10000
    )
    expect(container.get_by_test_id("toolbar-reset")).to_be_visible(timeout=10000)

    container.get_by_test_id("toolbar-reset").evaluate("el => el.click()")

    expect(container.get_by_test_id("toolbar-rows-chips")).to_contain_text(
        "Region", timeout=10000
    )
    expect(container.get_by_test_id("toolbar-columns-chips")).not_to_contain_text(
        "Region", timeout=10000
    )
    expect(container.get_by_test_id("toolbar-reset")).to_have_count(0, timeout=10000)
