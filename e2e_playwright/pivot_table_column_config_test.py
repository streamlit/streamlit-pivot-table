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
