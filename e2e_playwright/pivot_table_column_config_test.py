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

"""End-to-end coverage for Tier 1 column_config keys: label, help, width, pinned."""

from __future__ import annotations

from playwright.sync_api import Page, expect

from e2e_utils import get_pivot


def test_column_config_label_row_dim(page_at_app: Page):
    """column_config.label renames a row dimension header without changing the
    underlying field id."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_label")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    row_dim = container.get_by_test_id("pivot-row-dim-label-Region")
    expect(row_dim).to_contain_text("Area")
    expect(row_dim).not_to_contain_text("Region (")


def test_column_config_label_measure(page_at_app: Page):
    """column_config.label renames a measure header."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_label")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    thead = container.locator("thead")
    expect(thead).to_contain_text("Rev")


def test_column_config_help_tooltip_row_dim(page_at_app: Page):
    """column_config.help flows through as a title attribute on the row dim header."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_help")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    row_dim = container.get_by_test_id("pivot-row-dim-label-Region")
    expect(row_dim).to_have_attribute("title", "Geographic region")


def test_column_config_help_tooltip_measure(page_at_app: Page):
    """column_config.help flows through as a title attribute on a measure header."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_help")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    measure_cell = container.locator(
        '[data-testid="pivot-header-cell"][title="Revenue in USD"]'
    ).first
    expect(measure_cell).to_be_visible()


def test_column_config_width_pixel(page_at_app: Page):
    """column_config.width with a pixel integer applies to the row dim header."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_width_px")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    row_dim = container.get_by_test_id("pivot-row-dim-label-Region")
    style = row_dim.get_attribute("style") or ""
    assert "width: 180px" in style, f"expected 180px width, got style={style!r}"


def test_column_config_width_preset(page_at_app: Page):
    """column_config.width with a preset string maps to documented pixel values."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_width_preset")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    row_dim = container.get_by_test_id("pivot-row-dim-label-Region")
    style = row_dim.get_attribute("style") or ""
    assert "width: 200px" in style, f"expected 'large' -> 200px, got style={style!r}"


def test_column_config_pinned_locks_in_config_ui(page_at_app: Page):
    """column_config.pinned=True locks the field in the config UI (same behavior
    as frozen_columns): the chip renders but its remove (×) button is absent."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_pinned")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    chip = container.get_by_test_id("toolbar-rows-chip-Region")
    expect(chip).to_be_visible(timeout=5000)
    remove_btn = container.get_by_test_id("toolbar-rows-remove-Region")
    expect(remove_btn).to_have_count(0)


# ---------------------------------------------------------------------------
# Tier 2 cell renderers: LinkColumn / ImageColumn / CheckboxColumn /
# TextColumn.max_chars.
# ---------------------------------------------------------------------------


def test_column_config_link_renderer(page_at_app: Page):
    """column_config={"type": "link"} renders row-dim values as <a> tags
    with href=<raw value> and text substituted from display_text='Visit {}'."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_link")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    anchors = container.get_by_test_id("pivot-link-cell")
    expect(anchors.first).to_be_visible(timeout=10000)
    # One anchor per distinct Website value in the fixture.
    expect(anchors).to_have_count(4)
    first = anchors.first
    expect(first).to_have_attribute("target", "_blank")
    expect(first).to_contain_text("Visit https://example.com/")


def test_column_config_image_renderer(page_at_app: Page):
    """column_config={"type": "image"} renders row-dim values as <img> tags."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_image")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    imgs = container.get_by_test_id("pivot-image-cell")
    expect(imgs.first).to_be_visible(timeout=10000)
    expect(imgs).to_have_count(4)
    expect(imgs.first).to_have_attribute("loading", "lazy")


def test_column_config_checkbox_renderer(page_at_app: Page):
    """column_config={"type": "checkbox"} renders boolean row-dim values as
    ☑ / ☐ glyphs with data-checked attributes."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_checkbox")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    boxes = container.get_by_test_id("pivot-checkbox-cell")
    expect(boxes.first).to_be_visible(timeout=10000)
    # Two distinct boolean values (True / False) after aggregation.
    expect(boxes).to_have_count(2)
    checked = container.locator(
        '[data-testid="pivot-checkbox-cell"][data-checked="true"]'
    )
    unchecked = container.locator(
        '[data-testid="pivot-checkbox-cell"][data-checked="false"]'
    )
    expect(checked).to_have_count(1)
    expect(unchecked).to_have_count(1)


def test_column_config_text_max_chars(page_at_app: Page):
    """column_config={"type": "text", "max_chars": N} truncates long dim-cell
    values with an ellipsis; the full text remains available in the title attr."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_text_max")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    truncs = container.get_by_test_id("pivot-text-cell-truncated")
    expect(truncs.first).to_be_visible(timeout=10000)
    # Two rows have long values (>12 chars); one is "Short"; one is
    # "Medium length note here." (24 chars, also long).
    expect(truncs).to_have_count(3)
    first = truncs.first
    text = first.inner_text()
    assert text.endswith("\u2026"), f"expected ellipsis suffix, got {text!r}"
    assert len(text) <= 12, f"expected len<=12, got {len(text)} ({text!r})"
    title = first.get_attribute("title") or ""
    assert len(title) > 12, f"expected full text in title, got {title!r}"


def test_column_config_link_renderer_subtotal_fallback(page_at_app: Page):
    """Subtotal / Total rows render plain text (no anchor) even when the field
    has a link renderer configured."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_renderer_totals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # Subtotal rows have class containing "subtotalHeaderCell"; no link in those.
    subtotal_links = container.locator(
        'tr:has([class*="subtotalHeaderCell"]) [data-testid="pivot-link-cell"]'
    )
    expect(subtotal_links).to_have_count(0)
    # Data rows should still render anchors (one per distinct Website).
    data_anchors = container.get_by_test_id("pivot-link-cell")
    expect(data_anchors.first).to_be_visible(timeout=10000)


# ---------------------------------------------------------------------------
# column_config.help propagation: column-dimension header cells
# ---------------------------------------------------------------------------


def test_column_config_help_single_col_dim_slot_headers(page_at_app: Page):
    """column_config.help flows through as a title attribute on the slot-based
    column-dimension value headers when there is a single column dimension.
    With one col dim there is no col-dim-label; the only attach-point is the
    individual value cells (e.g. "2023", "2024")."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_help_col_dim_single")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # At least one slot header cell for the "Year" dim should carry the title.
    slot_with_title = container.locator(
        '[data-testid="pivot-header-cell"][title="Fiscal year"]'
    ).first
    expect(slot_with_title).to_be_visible()


def test_column_config_help_col_dim_label(page_at_app: Page):
    """column_config.help flows through as a title attribute on the col-dim-label
    corner cell that shows the outer column dimension name when there are 2+
    column dimensions."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_help_col_dim_label")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # With 2+ col dims and interactive=True the outer dimension always renders
    # as a dim-toggle corner cell.  slugify("Year") → "year".  The selector
    # requires BOTH the correct testid AND the expected title so the assertion
    # actually exercises the col-dim-label code path rather than falling back
    # to any other cell that happens to carry the same title text.
    col_dim_label = container.locator(
        '[data-testid="pivot-dim-toggle-col-0-year"][title="Fiscal year"]'
    )
    expect(col_dim_label).to_be_visible()


def test_column_config_help_temporal_parent_headers(page_at_app: Page):
    """column_config.help flows through as a title attribute on temporal parent
    column headers (year-level buckets) when auto_date_hierarchy=True is used
    with a real date-typed column."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_cc_help_temporal")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # Temporal parent headers carry testid pivot-temporal-header-orderdate-<year>
    # (slugify lowercases the field name: "OrderDate" → "orderdate")
    temporal_with_title = container.locator(
        '[data-testid^="pivot-temporal-header-orderdate-"][title="Date of order"]'
    ).first
    expect(temporal_with_title).to_be_visible()
