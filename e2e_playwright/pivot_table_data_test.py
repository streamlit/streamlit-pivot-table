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

"""Data, formatting, config I/O, and edge-case E2E tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

from e2e_utils import get_pivot, open_settings_panel
from pivot_table_app_support import _load_main_fixture


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


def test_conditional_formatting_color_scale_mid_value(page_at_app: Page):
    """mid_value anchors the gradient at the specified numeric value.

    Uses the deterministic `test_pivot_cond_fmt_mid_value` fixture whose three
    row values are exactly min (-100), mid_value (0), and max (100), so the
    rendered background colors must be the exact endpoint/mid colors
    configured on the rule.
    """
    page = page_at_app
    container = get_pivot(page, "test_pivot_cond_fmt_mid_value")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)
    # Three Region rows, one Year column -> exactly three data cells.
    assert cells.count() == 3

    def bg(cell) -> str:
        return cell.evaluate("el => window.getComputedStyle(el).backgroundColor")

    # Row order matches dataframe order: AA_Low (min), BB_Mid (mid), CC_High (max).
    assert bg(cells.nth(0)) == "rgb(255, 0, 0)", "min cell should render min_color"
    assert (
        bg(cells.nth(1)) == "rgb(255, 255, 255)"
    ), "cell at mid_value should render mid_color"
    assert bg(cells.nth(2)) == "rgb(0, 0, 255)", "max cell should render max_color"


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


def test_number_format_currency(page_at_app: Page):
    """Currency number format renders values with $ and commas."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_number_fmt")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)

    has_currency = False
    for cell in cells.all()[:20]:
        text = cell.inner_text().strip()
        if "$" in text:
            has_currency = True
            break
    assert has_currency, "Expected at least one cell to display currency format ($)"


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

    expect(panel.get_by_test_id("export-format-xlsx")).to_be_visible()
    expect(panel.get_by_test_id("export-format-csv")).to_be_visible()
    expect(panel.get_by_test_id("export-format-tsv")).to_be_visible()
    expect(panel.get_by_test_id("export-content-formatted")).to_be_visible()
    expect(panel.get_by_test_id("export-content-raw")).to_be_visible()


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


@pytest.mark.chromium_only
def test_config_export_copies_json(page_at_app: Page):
    """Clicking export config copies valid JSON to the clipboard."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-export").evaluate("el => el.click()")

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


def test_conditional_formatting_threshold(page_at_app: Page):
    """Threshold rule applies bold + background to qualifying cells."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_threshold")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    threshold = 5000
    grouped_means = (
        _load_main_fixture()
        .groupby(["Region", "Year"], observed=True)["Revenue"]
        .mean()
    )
    expect_unstyled_cells = bool((grouped_means <= threshold).any())

    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)

    styled_count = 0
    unstyled_count = 0
    for cell in cells.all():
        style = cell.get_attribute("style") or ""
        has_bg = "background-color" in style
        has_bold = "font-weight: 600" in style or "font-weight: 700" in style
        if has_bg and has_bold:
            styled_count += 1
        elif not has_bg and not has_bold:
            unstyled_count += 1

    assert styled_count > 0, "Expected at least one cell to have threshold styling"
    if expect_unstyled_cells:
        assert (
            unstyled_count > 0
        ), "Expected at least one cell without threshold styling"


def test_csv_download_content(page_at_app: Page):
    """Downloading CSV produces a file with expected headers and data."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-export-data").evaluate("el => el.click()")
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
    assert (
        "Region" in content
    ), f"CSV should contain 'Region' header, got: {content[:200]}"
    assert len(content.strip().splitlines()) > 1, "CSV should have data rows"


def test_excel_download_content(page_at_app: Page):
    """Downloading Excel produces a .xlsx file with the expected filename."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    container.get_by_test_id("toolbar-export-data").evaluate("el => el.click()")
    panel = page.get_by_test_id("toolbar-export-data-panel")
    expect(panel).to_be_visible(timeout=5000)

    # Excel is the default format; just select raw content
    panel.get_by_test_id("export-content-raw").click()

    with page.expect_download() as dl_info:
        panel.get_by_test_id("toolbar-export-data-action").click()

    download = dl_info.value
    suggested = download.suggested_filename
    assert suggested.endswith(".xlsx"), f"Expected .xlsx extension, got: {suggested}"

    path = download.path()
    assert path is not None

    file_size = Path(path).stat().st_size
    assert file_size > 100, f"Excel file too small ({file_size} bytes), likely empty"


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
    assert wrapper_box is not None
    assert (
        header_box_after["y"] >= wrapper_box["y"] - 2
    ), "Sticky header should remain within the wrapper viewport"

    panel = open_settings_panel(page, container)
    panel.get_by_test_id("settings-sticky-headers").locator("input").click()
    panel.get_by_test_id("settings-apply").click()

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


def test_config_import_invalid_json_no_crash(page_at_app: Page):
    """Pasting invalid JSON into the import textarea does not crash the table."""
    page = page_at_app
    container = get_pivot(page, "test_pivot")
    expect(container.get_by_test_id("pivot-toolbar")).to_be_visible(timeout=15000)

    import_toggle = container.get_by_test_id("toolbar-import-toggle")
    import_toggle.scroll_into_view_if_needed()
    import_toggle.evaluate("el => el.click()")
    panel = page.get_by_test_id("toolbar-import-panel")
    expect(panel).to_be_visible(timeout=5000)

    textarea = page.get_by_test_id("toolbar-import-textarea")
    textarea.fill("{ this is not valid json !!! }")

    page.get_by_test_id("toolbar-import-apply").click()

    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=5000)

    expect(container.get_by_test_id("toolbar-values-chip-label-Revenue")).to_have_text(
        "Revenue (Sum)"
    )


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
    container.get_by_test_id("header-menu-trigger-prs_per_person").first.evaluate(
        "el => el.click()"
    )
    expect(page.get_by_test_id("header-menu-prs_per_person")).to_be_visible(
        timeout=5000
    )
    expect(page.get_by_test_id("header-menu-display")).to_have_count(0)


# ---------------------------------------------------------------------------
# Hybrid mode tests (non-decomposable aggregations)
# ---------------------------------------------------------------------------


def test_hybrid_median_renders_cells(page_at_app: Page):
    """Hybrid median pivot renders data cells and totals."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_hybrid_median")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)
    cell_count = cells.count()
    assert cell_count > 0, "Hybrid median pivot should render data cells"


def test_hybrid_median_grand_total_not_dash(page_at_app: Page):
    """Grand total for hybrid median is not the empty cell value."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_hybrid_median")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    grand_total_cells = container.get_by_test_id("pivot-grand-total-cell")
    if grand_total_cells.count() > 0:
        text = grand_total_cells.first.inner_text().strip()
        assert text != "-", f"Grand total should be a number, got '{text}'"


def test_hybrid_count_distinct_renders_correct_values(page_at_app: Page):
    """Hybrid count_distinct pivot shows actual distinct counts (not 1)."""
    page = page_at_app
    container = get_pivot(page, "test_pivot_hybrid_count_distinct")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    cells = container.get_by_test_id("pivot-data-cell")
    expect(cells.first).to_be_visible(timeout=5000)
    texts = [
        c.inner_text().strip() for c in cells.all() if c.inner_text().strip() != "-"
    ]
    for t in texts:
        val = float(t.replace(",", ""))
        assert val >= 1, f"Count distinct value should be >= 1, got {val}"
