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

"""E2E tests for 0.6.0 member grouping feature.

Backed by pivot_table_member_groups_app.py.

Data fixture:
    Region      Category  Revenue
    Northeast   A          100
    Northeast   B          120
    Southeast   A          150
    Southeast   B           80
    West        A          200
    West        B           60

East Coast group = Northeast + Southeast:
    East Coast  A = 100 + 150 = 250
    East Coast  B = 120 +  80 = 200
    East Coast total = 450
    West        A = 200
    West        B =  60
    West total  = 260
"""

from __future__ import annotations

import re

from playwright.sync_api import Page, expect

from e2e_utils import get_pivot


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _parse_cell_number(text: str) -> float | None:
    cleaned = re.sub(r"[%$,\s]", "", (text or "").strip())
    try:
        return float(cleaned)
    except ValueError:
        return None


def _row_header_texts(container) -> list[str]:
    """Return text of every pivot-row-header element in DOM order."""
    return [
        (h.text_content() or "").strip()
        for h in container.get_by_test_id("pivot-row-header").all()
    ]


def _open_header_menu(page: Page, cell_locator, menu_test_id: str):
    """Open a header menu by clicking the header cell."""
    expect(cell_locator).to_be_visible(timeout=5000)
    menu = page.get_by_test_id(menu_test_id)
    for attempt in range(2):
        cell_locator.click()
        try:
            expect(menu).to_be_visible(timeout=5000)
            return menu
        except AssertionError:
            if attempt == 1:
                raise


def _close_header_menu(page: Page, menu_test_id: str) -> None:
    page.keyboard.press("Escape")
    expect(page.get_by_test_id(menu_test_id)).to_be_hidden(timeout=5000)


def _open_group_manager_via_menu(page: Page, cell_locator, menu_test_id: str):
    """Open the GroupManagerDialog via the 'Groups…' nav row in a header menu."""
    menu = _open_header_menu(page, cell_locator, menu_test_id)
    groups_nav = menu.get_by_test_id("header-menu-groups-nav")
    expect(groups_nav).to_be_visible(timeout=5000)
    groups_nav.click()
    dialog = page.get_by_test_id("group-manager-dialog")
    expect(dialog).to_be_visible(timeout=5000)
    return dialog


def _close_group_manager(page: Page) -> None:
    page.get_by_test_id("group-manager-done").click()
    expect(page.get_by_test_id("group-manager-dialog")).to_be_hidden(timeout=5000)


# ---------------------------------------------------------------------------
# Static group tests
# ---------------------------------------------------------------------------


def test_static_group_renders_group_name(page_at_app: Page):
    """Row headers show "East Coast" instead of "Northeast"/"Southeast"."""
    container = get_pivot(page_at_app, "test_pivot_member_groups_static")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    headers = _row_header_texts(container)
    assert (
        "East Coast" in headers
    ), f"Expected 'East Coast' in row headers, got: {headers}"
    assert (
        "Northeast" not in headers
    ), f"'Northeast' should not appear when grouped, got: {headers}"
    assert (
        "Southeast" not in headers
    ), f"'Southeast' should not appear when grouped, got: {headers}"
    assert "West" in headers, f"Expected 'West' in row headers, got: {headers}"


def test_static_group_aggregated_value_correct(page_at_app: Page):
    """East Coast / A cell = Northeast A + Southeast A = 100 + 150 = 250."""
    container = get_pivot(page_at_app, "test_pivot_member_groups_static")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell").all()
    values = [_parse_cell_number(c.text_content() or "") for c in data_cells]
    numeric = [v for v in values if v is not None]

    # Expect East Coast total = 450 (first row total with show_totals=True)
    assert 250 in numeric, f"Expected 250 (East Coast/A) in cell values, got: {numeric}"
    assert (
        200 in numeric
    ), f"Expected 200 (East Coast/B or West/A) in cell values, got: {numeric}"


# ---------------------------------------------------------------------------
# Interactive group creation
# ---------------------------------------------------------------------------


def test_create_group_interactively(page_at_app: Page):
    """User can create a group via the header menu Groups… nav row:
    open Region menu → click Groups… → type name → check 2 members → click Add Group → Done.
    The group name then appears as a row header.
    """
    container = get_pivot(page_at_app, "test_pivot_member_groups_interactive")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # Before grouping, all raw members should be present.
    headers_before = _row_header_texts(container)
    assert "Northeast" in headers_before
    assert "Southeast" in headers_before
    assert "West" in headers_before

    # Open header menu then click "Groups…" nav row → opens GroupManagerDialog.
    region_header = container.locator("[data-testid='pivot-row-header']").first
    dialog = _open_group_manager_via_menu(
        page_at_app, region_header, "header-menu-Region"
    )

    # Type group name.
    dialog.get_by_test_id("group-manager-name-input").fill("East Coast")

    # Check "Northeast" and "Southeast".
    dialog.locator("label").filter(has_text="Northeast").click()
    dialog.locator("label").filter(has_text="Southeast").click()

    # "Add Group" button should now be enabled.
    add_btn = dialog.get_by_test_id("group-manager-add-btn")
    expect(add_btn).to_be_enabled(timeout=3000)
    add_btn.click()

    # Close the dialog.
    _close_group_manager(page_at_app)

    # Check updated row headers.
    expect(
        container.get_by_test_id("pivot-row-header").filter(has_text="East Coast")
    ).to_be_visible(timeout=10000)
    headers_after = _row_header_texts(container)
    assert (
        "East Coast" in headers_after
    ), f"Group name missing after creation: {headers_after}"
    assert (
        "Northeast" not in headers_after
    ), f"Raw member visible after grouping: {headers_after}"
    assert (
        "Southeast" not in headers_after
    ), f"Raw member visible after grouping: {headers_after}"


# ---------------------------------------------------------------------------
# Ungroup
# ---------------------------------------------------------------------------


def test_ungroup_interactively(page_at_app: Page):
    """Clicking the remove × button in GroupManagerDialog removes the group."""
    container = get_pivot(page_at_app, "test_pivot_member_groups_ungroup")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # Initially "East Coast" group is present.
    headers_before = _row_header_texts(container)
    assert (
        "East Coast" in headers_before
    ), f"Expected East Coast group, got: {headers_before}"

    # Open GroupManagerDialog via groups chip in FilterBar.
    groups_chip = page_at_app.get_by_test_id("groups-chip-Region")
    expect(groups_chip).to_be_visible(timeout=5000)
    groups_chip.click()
    dialog = page_at_app.get_by_test_id("group-manager-dialog")
    expect(dialog).to_be_visible(timeout=5000)

    # Click the remove button for the "East Coast" group.
    remove_btn = dialog.get_by_test_id("group-manager-remove-East Coast")
    expect(remove_btn).to_be_visible(timeout=5000)
    remove_btn.click()

    # Close dialog.
    _close_group_manager(page_at_app)

    # After ungrouping, raw members reappear.
    expect(
        container.get_by_test_id("pivot-row-header").filter(has_text="Northeast")
    ).to_be_visible(timeout=10000)
    headers_after = _row_header_texts(container)
    assert (
        "Northeast" in headers_after
    ), f"Northeast should reappear after ungroup: {headers_after}"
    assert (
        "Southeast" in headers_after
    ), f"Southeast should reappear after ungroup: {headers_after}"
    assert (
        "East Coast" not in headers_after
    ), f"Group name should be gone after ungroup: {headers_after}"


# ---------------------------------------------------------------------------
# Group Selected button requires ≥ 2 selections
# ---------------------------------------------------------------------------


def test_group_selected_requires_two_members(page_at_app: Page):
    """Add Group button stays disabled when fewer than 2 members are checked."""
    container = get_pivot(page_at_app, "test_pivot_member_groups_interactive")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    region_header = container.locator("[data-testid='pivot-row-header']").first
    dialog = _open_group_manager_via_menu(
        page_at_app, region_header, "header-menu-Region"
    )

    # Type a group name so that's not the limiting factor.
    dialog.get_by_test_id("group-manager-name-input").fill("Test Group")

    add_btn = dialog.get_by_test_id("group-manager-add-btn")

    # With 0 checked → disabled.
    expect(add_btn).to_be_disabled()

    # Check exactly 1 member → still disabled.
    dialog.locator("label").filter(has_text="Northeast").click()
    expect(add_btn).to_be_disabled()

    # Check a second member → now enabled.
    dialog.locator("label").filter(has_text="Southeast").click()
    expect(add_btn).to_be_enabled(timeout=3000)

    _close_group_manager(page_at_app)


# ---------------------------------------------------------------------------
# Drilldown shows raw member values
# ---------------------------------------------------------------------------


def test_drilldown_shows_raw_members(page_at_app: Page):
    """Clicking an 'East Coast' cell opens drilldown with raw Region values."""
    container = get_pivot(page_at_app, "test_pivot_member_groups_drilldown")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # Find and click the "East Coast" / "A" data cell.
    # The first data cell in the East Coast row corresponds to Category A.
    east_coast_row = container.locator("[data-testid='pivot-row-header']").filter(
        has_text="East Coast"
    )
    expect(east_coast_row).to_be_visible(timeout=5000)

    # Click the first data cell in the table (East Coast / A).
    data_cells = container.get_by_test_id("pivot-data-cell").all()
    assert len(data_cells) > 0, "No data cells found"
    data_cells[0].click()

    # Drilldown panel should open.
    drilldown = page_at_app.get_by_test_id("drilldown-panel")
    expect(drilldown).to_be_visible(timeout=10000)

    # Panel rows must contain raw Region values, not the group name.
    drilldown_text = drilldown.text_content() or ""
    assert (
        "Northeast" in drilldown_text or "Southeast" in drilldown_text
    ), f"Drilldown should show raw Region values; got: {drilldown_text[:200]}"
    assert (
        "East Coast" not in drilldown_text
    ), f"Drilldown should not show group name 'East Coast'; got: {drilldown_text[:200]}"
