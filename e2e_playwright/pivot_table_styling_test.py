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

"""E2E tests for the style= parameter (Phase 5 styling API).

Conventions:
  - Each test maps to a single pivot in pivot_table_styling_app.py.
  - Wrapper div is located via [class*='tableWrapper'] — CSS modules hash class
    names but keep the original token as a substring (e.g. "TableRenderer_tableWrapper__abc").
  - Modifier classes (densityCompact, bordersRows, stripesOff, hoverOff) are
    likewise matched with [class*='...'].
  - Inline --pivot-* vars are read from the wrapper's "style" attribute string.
  - Cell inline styles are read from the element's "style" attribute.
"""

from __future__ import annotations

from playwright.sync_api import Page, expect

from e2e_utils import get_pivot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_wrapper(page: Page, key: str):
    """Return a Locator for the .tableWrapper div inside the pivot container."""
    container = get_pivot(page, key)
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    return container.locator("[class*='tableWrapper']").first


def _wrapper_style(page: Page, key: str) -> str:
    """Return the inline style attribute string of the wrapper div."""
    wrapper = _get_wrapper(page, key)
    return wrapper.get_attribute("style") or ""


def _wrapper_class(page: Page, key: str) -> str:
    """Return the class attribute string of the wrapper div."""
    wrapper = _get_wrapper(page, key)
    return wrapper.get_attribute("class") or ""


# ---------------------------------------------------------------------------
# 1. Zero-style baseline: style=None produces no --pivot-* inline vars
# ---------------------------------------------------------------------------


def test_style_none_no_pivot_vars(page_at_app: Page):
    """style=None must not set any --pivot-* CSS custom property on the wrapper."""
    style = _wrapper_style(page_at_app, "style_none")
    assert (
        "--pivot-" not in style
    ), f"Expected no --pivot-* vars for style=None, got style={style!r}"


# ---------------------------------------------------------------------------
# 2. Custom background_color sets --pivot-bg on the wrapper
# ---------------------------------------------------------------------------


def test_style_custom_bg_sets_pivot_var(page_at_app: Page):
    """PivotStyle(background_color=...) must set --pivot-bg in the wrapper inline style."""
    style = _wrapper_style(page_at_app, "style_custom_bg")
    assert "--pivot-bg" in style, f"Expected --pivot-bg in wrapper style, got {style!r}"
    assert (
        "200" in style or "rgb" in style
    ), f"Expected rgb(200, 100, 50) value in wrapper style, got {style!r}"


# ---------------------------------------------------------------------------
# 3. Density compact adds the densityCompact modifier class
# ---------------------------------------------------------------------------


def test_style_density_compact_class(page_at_app: Page):
    """PivotStyle(density='compact') must add the densityCompact class to the wrapper."""
    classes = _wrapper_class(page_at_app, "style_density_compact")
    assert (
        "densityCompact" in classes
    ), f"Expected densityCompact class on wrapper, got {classes!r}"


# ---------------------------------------------------------------------------
# 4. borders="rows" adds the bordersRows modifier class
# ---------------------------------------------------------------------------


def test_style_borders_rows_class(page_at_app: Page):
    """PivotStyle(borders='rows') must add the bordersRows class to the wrapper."""
    classes = _wrapper_class(page_at_app, "style_borders_rows")
    assert (
        "bordersRows" in classes
    ), f"Expected bordersRows class on wrapper, got {classes!r}"


# ---------------------------------------------------------------------------
# 5. stripe_color=None adds the stripesOff modifier class
# ---------------------------------------------------------------------------


def test_style_stripes_off_class(page_at_app: Page):
    """PivotStyle(stripe_color=None) must add the stripesOff class to the wrapper."""
    classes = _wrapper_class(page_at_app, "style_stripes_off")
    assert (
        "stripesOff" in classes
    ), f"Expected stripesOff class on wrapper, got {classes!r}"


def test_style_stripes_off_no_stripe_var(page_at_app: Page):
    """stripe_color=None must NOT set --pivot-stripe-color (disable via class, not var)."""
    style = _wrapper_style(page_at_app, "style_stripes_off")
    assert (
        "--pivot-stripe-color" not in style
    ), f"Expected --pivot-stripe-color absent for stripe_color=None, got {style!r}"


# ---------------------------------------------------------------------------
# 6. row_hover_color=None adds the hoverOff modifier class
# ---------------------------------------------------------------------------


def test_style_hover_off_class(page_at_app: Page):
    """PivotStyle(row_hover_color=None) must add the hoverOff class to the wrapper."""
    classes = _wrapper_class(page_at_app, "style_hover_off")
    assert "hoverOff" in classes, f"Expected hoverOff class on wrapper, got {classes!r}"


def test_style_hover_off_no_hover_var(page_at_app: Page):
    """row_hover_color=None must NOT set --pivot-row-hover-bg."""
    style = _wrapper_style(page_at_app, "style_hover_off")
    assert (
        "--pivot-row-hover-bg" not in style
    ), f"Expected --pivot-row-hover-bg absent for row_hover_color=None, got {style!r}"


# ---------------------------------------------------------------------------
# 7. column_header region override sets --pivot-column-header-bg on wrapper
# ---------------------------------------------------------------------------


def test_style_column_header_bg_var(page_at_app: Page):
    """column_header.background_color must set --pivot-column-header-bg on wrapper."""
    style = _wrapper_style(page_at_app, "style_column_header_bg")
    assert (
        "--pivot-column-header-bg" in style
    ), f"Expected --pivot-column-header-bg in wrapper style, got {style!r}"
    # Value should contain the rgb color we set
    assert (
        "10" in style
    ), f"Expected color value (10, 20, 30) in wrapper style, got {style!r}"


# ---------------------------------------------------------------------------
# 8 & 9. row_total/column_total region mapping to correct CSS variables
# ---------------------------------------------------------------------------


def test_style_row_total_maps_to_row_total_var(page_at_app: Page):
    """row_total.background_color must set --pivot-row-total-bg (not --pivot-column-total-bg)."""
    style = _wrapper_style(page_at_app, "style_row_total_mapping")
    assert (
        "--pivot-row-total-bg" in style
    ), f"Expected --pivot-row-total-bg in wrapper style, got {style!r}"
    assert (
        "--pivot-column-total-bg" not in style
    ), f"--pivot-column-total-bg must NOT be set by row_total override, got {style!r}"


def test_style_column_total_maps_to_column_total_var(page_at_app: Page):
    """column_total.background_color must set --pivot-column-total-bg (not --pivot-row-total-bg)."""
    style = _wrapper_style(page_at_app, "style_column_total_mapping")
    assert (
        "--pivot-column-total-bg" in style
    ), f"Expected --pivot-column-total-bg in wrapper style, got {style!r}"
    assert (
        "--pivot-row-total-bg" not in style
    ), f"--pivot-row-total-bg must NOT be set by column_total override, got {style!r}"


# ---------------------------------------------------------------------------
# 10. data_cell_by_measure: Revenue cells get inline background-color
# ---------------------------------------------------------------------------


def test_style_per_measure_inline_style_on_data_cells(page_at_app: Page):
    """data_cell_by_measure Revenue override must appear as inline style on data cells."""
    container = get_pivot(page_at_app, "style_per_measure")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell").all()
    assert data_cells, "Expected at least one pivot-data-cell"

    # All Revenue data cells should have the per-measure background-color.
    # Revenue is always the first of the two measures per column slot, so
    # at least some data cells will have the inline style.
    styled_count = sum(
        1
        for cell in data_cells
        if "background-color" in (cell.get_attribute("style") or "")
    )
    assert styled_count > 0, (
        "Expected at least one data cell with inline background-color from "
        "data_cell_by_measure, but none found"
    )


# ---------------------------------------------------------------------------
# 11. data_cell_by_measure must not leak to row-total or column-total cells
# ---------------------------------------------------------------------------


def test_style_per_measure_no_leak_to_row_totals(page_at_app: Page):
    """data_cell_by_measure must not apply inline style to row-total cells (.totalsCol)."""
    container = get_pivot(page_at_app, "style_per_measure")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    row_total_cells = container.get_by_test_id("pivot-row-total").all()
    for cell in row_total_cells:
        cell_style = cell.get_attribute("style") or ""
        assert "background-color" not in cell_style, (
            f"data_cell_by_measure must not leak to pivot-row-total cells, "
            f"but found style={cell_style!r}"
        )


def test_style_per_measure_no_leak_to_column_totals(page_at_app: Page):
    """data_cell_by_measure must not apply inline style to column-total cells (.totalsRow).

    Column-total cells use the test ID "pivot-grand-total" (renderTotalsRow).
    """
    container = get_pivot(page_at_app, "style_per_measure")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    grand_total_cells = container.get_by_test_id("pivot-grand-total").all()
    assert grand_total_cells, "Expected at least one pivot-grand-total cell"
    for cell in grand_total_cells:
        cell_style = cell.get_attribute("style") or ""
        assert "background-color" not in cell_style, (
            f"data_cell_by_measure must not leak to pivot-grand-total cells, "
            f"but found style={cell_style!r}"
        )


# ---------------------------------------------------------------------------
# 12. CF wins over per-measure: color_scale background overrides the red bg
# ---------------------------------------------------------------------------


def test_style_cf_wins_over_per_measure(page_at_app: Page):
    """Conditional formatting background must override per-measure background-color.

    The app sets data_cell_by_measure Revenue → red, and a color_scale CF that
    sets all Revenue cells to rgb(0,200,0). CF is spread after measureStyle in
    renderDataRow, so CF wins. We assert no cell has the per-measure red (255, 0, 0).
    """
    container = get_pivot(page_at_app, "style_cf_precedence")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell").all()
    assert data_cells, "Expected at least one pivot-data-cell"

    for cell in data_cells:
        cell_style = cell.get_attribute("style") or ""
        # The per-measure red (255, 0, 0) must not appear; CF green should win.
        assert (
            "255, 0, 0" not in cell_style and "255,0,0" not in cell_style
        ), f"Per-measure red must not win over CF: found {cell_style!r}"


# ---------------------------------------------------------------------------
# 13. Composition: list of preset + PivotStyle merges correctly
# ---------------------------------------------------------------------------


def test_style_composition_applies_both_layers(page_at_app: Page):
    """A [preset, PivotStyle] list must apply both layers.

    The app uses ["compact", PivotStyle(background_color=..., borders="rows")].
    The wrapper must have:
      - densityCompact class (from "compact" preset)
      - bordersRows class (from explicit PivotStyle borders override)
      - --pivot-bg in the inline style (from explicit PivotStyle bg override)
    """
    classes = _wrapper_class(page_at_app, "style_composition")
    style = _wrapper_style(page_at_app, "style_composition")

    assert (
        "densityCompact" in classes
    ), f"Expected densityCompact from 'compact' preset, got classes={classes!r}"
    assert (
        "bordersRows" in classes
    ), f"Expected bordersRows from PivotStyle override, got classes={classes!r}"
    assert (
        "--pivot-bg" in style
    ), f"Expected --pivot-bg from PivotStyle override, got style={style!r}"
