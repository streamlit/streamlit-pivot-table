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

"""E2E tests for Top N / Bottom N and Value-predicate analytical filters (0.5.0)."""

from __future__ import annotations

from playwright.sync_api import Page, expect

from e2e_utils import get_pivot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row_headers(container):
    """Return a Locator for all leaf row-header cells in the pivot."""
    return container.get_by_test_id("pivot-row-header")


def _row_texts(container) -> list[str]:
    """Collect all row-header text values currently visible in *container*."""
    headers = _row_headers(container)
    return [h.inner_text().strip() for h in headers.all()]


def _open_header_menu(page: Page, container, dim: str):
    """Open the header-menu popover for dimension *dim*."""
    trigger = container.get_by_test_id(f"header-menu-trigger-{dim}")
    trigger.evaluate("el => el.click()")
    menu = page.get_by_test_id(f"header-menu-{dim}")
    expect(menu).to_be_visible(timeout=8000)
    return menu


def _close_header_menu(page: Page, dim: str) -> None:
    menu = page.get_by_test_id(f"header-menu-{dim}")
    page.keyboard.press("Escape")
    expect(menu).to_be_hidden(timeout=5000)


# ---------------------------------------------------------------------------
# API-configured filters — static rendering
# ---------------------------------------------------------------------------


class TestTopNFilterAPI:
    """Top N / Bottom N filters specified via the Python API."""

    def test_top2_shows_correct_member_count(self, page_at_app: Page):
        """Top-2 filter keeps exactly 2 Products per Region in row headers."""
        container = get_pivot(page_at_app, "test_pivot_top_n")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        texts = _row_texts(container)
        product_names = {"Alpha", "Beta", "Gamma", "Delta"}
        product_rows = [t for t in texts if t in product_names]
        # 3 regions × 2 kept products = 6 product rows
        assert (
            len(product_rows) == 6
        ), f"Expected 6 product rows (3 regions × top 2) but got {len(product_rows)}: {product_rows}"

    def test_top2_keeps_highest_revenue_products(self, page_at_app: Page):
        """Top-2 keeps the two highest-revenue products, not the lowest."""
        container = get_pivot(page_at_app, "test_pivot_top_n")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        texts = _row_texts(container)
        product_names = {"Alpha", "Beta", "Gamma", "Delta"}
        product_rows = [t for t in texts if t in product_names]
        # Gamma (offset 50) is the lowest-revenue product; it must be absent.
        assert (
            "Gamma" not in product_rows
        ), f"Lowest-revenue product 'Gamma' should be filtered out by Top-2 but found in: {product_rows}"

    def test_top_n_subtotals_still_present(self, page_at_app: Page):
        """Region subtotals are still rendered even when product rows are filtered."""
        container = get_pivot(page_at_app, "test_pivot_top_n")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        texts = _row_texts(container)
        regions = {"East", "West", "North"}
        assert any(
            t in regions for t in texts
        ), f"Region subtotal rows must still appear when child products are filtered: {texts}"

    def test_bottom2_shows_lowest_revenue_products(self, page_at_app: Page):
        """Bottom-2 keeps the two lowest-revenue products."""
        container = get_pivot(page_at_app, "test_pivot_bottom_n")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        texts = _row_texts(container)
        product_names = {"Alpha", "Beta", "Gamma", "Delta"}
        product_rows = [t for t in texts if t in product_names]
        # Delta (offset 300) is the highest — must be absent from Bottom-2
        assert (
            "Delta" not in product_rows
        ), f"Highest-revenue product 'Delta' should be absent from Bottom-2 but found: {product_rows}"
        # Gamma (offset 50) is the lowest — must appear
        assert (
            "Gamma" in product_rows
        ), f"Lowest-revenue product 'Gamma' must be kept by Bottom-2 but missing: {product_rows}"


class TestValueFilterAPI:
    """Value-predicate filters specified via the Python API."""

    def test_gte_filter_removes_low_revenue_products(self, page_at_app: Page):
        """gte-500 filter hides products whose per-parent revenue < 500.

        Data layout (sum over years per region × product):
          East:  Alpha=250, Beta=450, Delta=650, Gamma=150
          West:  all > 2000
          North: all > 4000

        East-specific: Alpha, Beta, Gamma each < 500 → excluded for East.
        Delta (650) is kept for East; all 4 products kept for West and North.
        Total kept product rows: 1 (East) + 4 (West) + 4 (North) = 9 out of 12.
        """
        container = get_pivot(page_at_app, "test_pivot_value_filter")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        texts = _row_texts(container)
        product_names = {"Alpha", "Beta", "Gamma", "Delta"}
        product_rows = [t for t in texts if t in product_names]

        # Delta appears in all 3 regions (East, West, North)
        assert "Delta" in product_rows, "High-revenue Delta must be kept in all regions"
        # The filter removes Alpha/Beta/Gamma from East → fewer than 12 product rows total
        assert (
            len(product_rows) < 12
        ), f"Expected fewer than 12 product rows after gte-500 filter, got {len(product_rows)}: {product_rows}"

    def test_value_filter_subtotals_still_present(self, page_at_app: Page):
        """Region subtotals remain visible even when some East product rows are filtered."""
        container = get_pivot(page_at_app, "test_pivot_value_filter")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        texts = _row_texts(container)
        regions = {"East", "West", "North"}
        region_rows = [t for t in texts if t in regions]
        assert (
            len(region_rows) == 3
        ), f"All 3 Region subtotals must remain after value filter, got: {region_rows}"


# ---------------------------------------------------------------------------
# Interactive header-menu filters
# ---------------------------------------------------------------------------


class TestTopNInteractiveMenu:
    """Top N filter set via the column header menu."""

    def test_top_n_section_visible_in_menu(self, page_at_app: Page):
        """Header menu for a row-dimension shows the Top / Bottom N section."""
        container = get_pivot(page_at_app, "test_pivot_top_n_interactive")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        menu = _open_header_menu(page_at_app, container, "Product")
        top_n_section = menu.get_by_test_id("header-menu-top-n")
        expect(top_n_section).to_be_visible(timeout=5000)
        _close_header_menu(page_at_app, "Product")

    def test_top_n_apply_reduces_row_count(self, page_at_app: Page):
        """Applying Top-2 via the header menu reduces the number of product rows."""
        page = page_at_app
        container = get_pivot(page, "test_pivot_top_n_interactive")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        rows_before = _row_headers(container).count()

        menu = _open_header_menu(page, container, "Product")

        # Set n=2
        n_input = menu.get_by_test_id("header-top-n-count")
        n_input.fill("2")

        # Set "by" measure
        by_select = menu.get_by_test_id("header-top-n-by")
        by_select.select_option("Revenue")

        # Apply
        apply_btn = menu.get_by_test_id("header-top-n-apply")
        apply_btn.evaluate("el => el.click()")

        # Menu should close; table should have fewer rows
        expect(page.get_by_test_id("header-menu-Product")).to_be_hidden(timeout=8000)
        expect(_row_headers(container)).not_to_have_count(rows_before, timeout=10000)
        assert _row_headers(container).count() < rows_before

    def test_top_n_clear_restores_row_count(self, page_at_app: Page):
        """Clearing the Top N filter via the header menu restores all rows."""
        page = page_at_app
        container = get_pivot(page, "test_pivot_top_n_interactive")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        # Capture baseline
        rows_all = _row_headers(container).count()

        # Apply top-2
        menu = _open_header_menu(page, container, "Product")
        menu.get_by_test_id("header-top-n-count").fill("2")
        menu.get_by_test_id("header-top-n-by").select_option("Revenue")
        menu.get_by_test_id("header-top-n-apply").evaluate("el => el.click()")
        expect(page.get_by_test_id("header-menu-Product")).to_be_hidden(timeout=8000)
        expect(_row_headers(container)).not_to_have_count(rows_all, timeout=10000)

        # Now clear
        menu2 = _open_header_menu(page, container, "Product")
        clear_btn = menu2.get_by_test_id("header-top-n-clear")
        clear_btn.evaluate("el => el.click()")
        expect(page.get_by_test_id("header-menu-Product")).to_be_hidden(timeout=8000)

        expect(_row_headers(container)).to_have_count(rows_all, timeout=10000)


class TestValueFilterInteractiveMenu:
    """Value filter set via the column header menu."""

    def test_value_filter_section_visible_in_menu(self, page_at_app: Page):
        """Header menu for a row-dimension shows the 'Filter by value' section."""
        container = get_pivot(page_at_app, "test_pivot_value_filter_interactive")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        menu = _open_header_menu(page_at_app, container, "Product")
        section = menu.get_by_test_id("header-menu-value-filter")
        expect(section).to_be_visible(timeout=5000)
        _close_header_menu(page_at_app, "Product")

    def test_value_filter_apply_reduces_rows(self, page_at_app: Page):
        """Applying a 'Revenue > 7000' filter via header menu reduces product rows.

        Grand-total revenues across all regions and years:
          Alpha=6750, Beta=7350, Gamma=6450, Delta=7950
        Only Beta and Delta exceed 7000 → 4 rows become 2.
        """
        page = page_at_app
        container = get_pivot(page, "test_pivot_value_filter_interactive")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        rows_before = _row_headers(container).count()

        menu = _open_header_menu(page, container, "Product")

        # Set "by" measure to Revenue
        menu.get_by_test_id("header-value-filter-by").select_option("Revenue")

        # Operator: click the ">" (gt) button in the operator strip
        menu.get_by_test_id("header-value-filter-op-gt").evaluate("el => el.click()")

        # Value: 7000 — Alpha (6750) and Gamma (6450) fall below this threshold
        menu.get_by_test_id("header-value-filter-value").fill("7000")

        # Apply
        menu.get_by_test_id("header-value-filter-apply").evaluate("el => el.click()")

        expect(page.get_by_test_id("header-menu-Product")).to_be_hidden(timeout=8000)
        expect(_row_headers(container)).not_to_have_count(rows_before, timeout=10000)
        assert _row_headers(container).count() < rows_before

    def test_value_filter_clear_restores_rows(self, page_at_app: Page):
        """Clearing the value filter via the header menu restores all product rows."""
        page = page_at_app
        container = get_pivot(page, "test_pivot_value_filter_interactive")
        expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

        rows_all = _row_headers(container).count()

        # Apply filter first (same threshold as apply test: 7000)
        menu = _open_header_menu(page, container, "Product")
        menu.get_by_test_id("header-value-filter-by").select_option("Revenue")
        menu.get_by_test_id("header-value-filter-op-gt").evaluate("el => el.click()")
        menu.get_by_test_id("header-value-filter-value").fill("7000")
        menu.get_by_test_id("header-value-filter-apply").evaluate("el => el.click()")
        expect(page.get_by_test_id("header-menu-Product")).to_be_hidden(timeout=8000)
        expect(_row_headers(container)).not_to_have_count(rows_all, timeout=10000)

        # Clear the filter
        menu2 = _open_header_menu(page, container, "Product")
        menu2.get_by_test_id("header-value-filter-clear").evaluate("el => el.click()")
        expect(page.get_by_test_id("header-menu-Product")).to_be_hidden(timeout=8000)

        expect(_row_headers(container)).to_have_count(rows_all, timeout=10000)
