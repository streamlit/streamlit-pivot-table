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

from e2e_utils import get_pivot, open_settings_panel


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


def test_aggregation_change_via_settings_panel(page_at_app: Page):
    """Changing aggregation in the settings panel fires config change and updates table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)
    expect(page.get_by_text("Config change count: 0")).to_be_visible()

    panel = open_settings_panel(page, container)

    trigger = panel.get_by_test_id("settings-agg-Revenue")
    expect(trigger).to_be_visible(timeout=5000)
    trigger.click()

    option = page.get_by_test_id("settings-agg-panel-Revenue").locator(
        "button", has_text="Avg"
    )
    expect(option).to_be_visible(timeout=5000)
    option.dispatch_event("mousedown")

    panel.get_by_test_id("settings-apply").click()

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


def test_add_row_dimension_via_settings_panel(page_at_app: Page):
    """Adding a row dimension via settings panel updates the table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    initial_row_count = container.get_by_test_id("pivot-row-header").count()

    panel = open_settings_panel(page, container)

    chip = panel.get_by_test_id("settings-available-Category")
    expect(chip).to_be_visible(timeout=5000)
    chip.click()

    menu_item = page.locator("text=Add to Rows")
    expect(menu_item).to_be_visible(timeout=5000)
    menu_item.click()

    panel.get_by_test_id("settings-apply").click()

    chips = container.get_by_test_id("toolbar-rows-chips")
    expect(chips).to_contain_text("Category", timeout=10000)

    row_headers = container.get_by_test_id("pivot-row-header")
    expect(row_headers).not_to_have_count(initial_row_count, timeout=10000)
    assert row_headers.count() > initial_row_count


def test_remove_row_via_settings_panel(page_at_app: Page):
    """Removing a row dimension via settings panel updates the table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    chips_before = container.get_by_test_id("toolbar-rows-chips")
    expect(chips_before).to_contain_text("Region")

    panel = open_settings_panel(page, container)
    panel.get_by_test_id("settings-rows-remove-Region").click()
    panel.get_by_test_id("settings-apply").click()

    expect(container.get_by_test_id("toolbar-rows-chips")).to_have_count(
        0, timeout=10000
    )


def test_add_value_field_via_settings_panel(page_at_app: Page):
    """Adding a second value field via settings panel shows two value label headers."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    panel = open_settings_panel(page, container)

    chip = panel.get_by_test_id("settings-available-Profit")
    expect(chip).to_be_visible(timeout=5000)
    chip.click()

    menu_item = page.locator("text=Add to Values")
    expect(menu_item).to_be_visible(timeout=5000)
    menu_item.click()

    panel.get_by_test_id("settings-apply").click()

    expect(container.get_by_test_id("toolbar-values-chip-label-Profit")).to_be_visible(
        timeout=10000
    )


def test_per_measure_aggregation_via_settings_panel(page_at_app: Page):
    """Settings panel supports setting different aggregations per raw measure."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cond_fmt")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    panel = open_settings_panel(page, container)

    profit_trigger = panel.get_by_test_id("settings-agg-Profit")
    expect(profit_trigger).to_be_visible(timeout=5000)
    profit_trigger.click()

    option = page.get_by_test_id("settings-agg-panel-Profit").get_by_role(
        "button", name="# Count", exact=True
    )
    expect(option).to_be_visible(timeout=5000)
    option.dispatch_event("mousedown")

    panel.get_by_test_id("settings-apply").click()

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

    panel = open_settings_panel(page, container)

    profit_trigger = panel.get_by_test_id("settings-agg-Profit")
    expect(profit_trigger).to_be_visible(timeout=5000)
    profit_trigger.click()

    option = page.get_by_test_id("settings-agg-panel-Profit").get_by_role(
        "button", name="# Count", exact=True
    )
    expect(option).to_be_visible(timeout=5000)
    option.dispatch_event("mousedown")

    panel.get_by_test_id("settings-apply").click()

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

    panel = open_settings_panel(page, container)
    panel.get_by_test_id("settings-row-totals").locator("input").evaluate(
        "el => el.click()"
    )
    panel.get_by_test_id("settings-apply").click()
    expect(container.get_by_test_id("pivot-row-total")).to_have_count(0, timeout=10000)

    panel = open_settings_panel(page, container)
    panel.get_by_test_id("settings-row-totals").locator("input").evaluate(
        "el => el.click()"
    )
    panel.get_by_test_id("settings-apply").click()
    expect(container.get_by_test_id("pivot-row-total").first).to_be_visible(
        timeout=10000
    )


def test_toggle_column_totals_off(page_at_app: Page):
    """Unchecking column totals hides the totals row."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-totals-row")).to_be_visible()

    panel = open_settings_panel(page, container)
    panel.get_by_test_id("settings-col-totals").locator("input").evaluate(
        "el => el.click()"
    )
    panel.get_by_test_id("settings-apply").click()
    expect(container.get_by_test_id("pivot-totals-row")).to_have_count(0, timeout=10000)


def test_toggle_subtotals(page_at_app: Page):
    """Enabling subtotals on a 2-row-dim pivot shows subtotal rows."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expect(container.get_by_test_id("pivot-subtotal-row").first).to_be_visible()

    panel = open_settings_panel(page, container)
    panel.get_by_test_id("settings-subtotals").locator("input").click()
    panel.get_by_test_id("settings-apply").click()
    expect(container.get_by_test_id("pivot-subtotal-row")).to_have_count(
        0, timeout=10000
    )

    panel = open_settings_panel(page, container)
    panel.get_by_test_id("settings-subtotals").locator("input").click()
    panel.get_by_test_id("settings-apply").click()
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

    panel = open_settings_panel(page, container)
    panel.get_by_test_id("settings-repeat-labels").locator("input").click()
    panel.get_by_test_id("settings-apply").click()

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


# ---------------------------------------------------------------------------
# Drag-and-drop E2E tests
# ---------------------------------------------------------------------------


def _drag_chip(page: Page, source, target):
    """Simulate a dnd-kit drag from source chip to target chip/zone.

    dnd-kit uses PointerSensor with 5px activation distance, so we need real
    mouse-move events over at least that distance.
    """
    source_box = source.bounding_box()
    target_box = target.bounding_box()
    if not source_box or not target_box:
        raise RuntimeError("Could not get bounding boxes for drag elements")

    sx = source_box["x"] + source_box["width"] / 2
    sy = source_box["y"] + source_box["height"] / 2
    tx = target_box["x"] + target_box["width"] / 2
    ty = target_box["y"] + target_box["height"] / 2

    page.mouse.move(sx, sy)
    page.mouse.down()
    # Move past the 5px activation distance in small steps
    steps = max(10, int(((tx - sx) ** 2 + (ty - sy) ** 2) ** 0.5 / 5))
    for i in range(1, steps + 1):
        page.mouse.move(
            sx + (tx - sx) * i / steps,
            sy + (ty - sy) * i / steps,
        )
        if i == 1:
            page.wait_for_timeout(50)
    page.mouse.up()


def test_drag_reorder_rows(page_at_app: Page):
    """Drag-and-drop reorder within Rows zone updates the pivot grouping order."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_subtotals")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    chips = container.get_by_test_id("toolbar-rows-chips")
    expect(chips).to_contain_text("Region", timeout=5000)
    expect(chips).to_contain_text("Category", timeout=5000)

    region_chip = container.get_by_test_id("toolbar-rows-chip-Region")
    category_chip = container.get_by_test_id("toolbar-rows-chip-Category")
    expect(region_chip).to_be_visible(timeout=5000)
    expect(category_chip).to_be_visible(timeout=5000)

    _drag_chip(page, region_chip, category_chip)

    # After reorder: Category should come first in the chips
    page.wait_for_timeout(1000)
    chip_text = chips.inner_text()
    cat_pos = chip_text.find("Category")
    reg_pos = chip_text.find("Region")
    assert (
        cat_pos < reg_pos
    ), f"Expected Category before Region after drag reorder, got: {chip_text}"


def test_drag_handle_visible_on_chips(page_at_app: Page):
    """Drag handles (SVG grip dots) are visible on non-frozen chips."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    chip = container.get_by_test_id("toolbar-rows-chip-Region")
    expect(chip).to_be_visible(timeout=5000)

    handle = chip.locator("svg")
    expect(handle).to_be_visible(timeout=5000)


def test_empty_zone_shows_placeholder(page_at_app: Page):
    """Zones with no chips display an 'Apply fields in settings menu' placeholder."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    panel = open_settings_panel(page, container)
    panel.get_by_test_id("settings-rows-remove-Region").click()
    panel.get_by_test_id("settings-apply").click()

    expect(container.locator("text=Apply fields in settings menu").first).to_be_visible(
        timeout=10000
    )
