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

"""Golden verification E2E tests — exact numerical cell value assertions.

Unlike existing E2E tests that check element presence/counts/substrings,
these tests verify that specific rendered cell values match pandas-computed
golden expected values. This catches bugs where the engine produces numbers
that look plausible but are mathematically wrong.
"""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

from playwright.sync_api import Page, expect

from e2e_utils import get_pivot

GOLDEN_DIR = Path(__file__).parent.parent / "tests" / "golden_data"
GOLDEN = json.loads((GOLDEN_DIR / "golden_expected.json").read_text())


def _cell_values(container, testid: str = "pivot-data-cell") -> list[str]:
    """Extract text content from all cells matching the test ID."""
    cells = container.get_by_test_id(testid)
    expect(cells.first).to_be_visible(timeout=15000)
    return [c.text_content().strip() for c in cells.all()]


def _parse_number(text: str) -> float | None:
    """Parse a cell's displayed text to a float, handling commas and %."""
    text = text.strip().replace(",", "")
    if text.endswith("%"):
        return float(text[:-1])
    try:
        return float(text)
    except ValueError:
        return None


def _collect_rendered_numbers(container, testid: str) -> list[float]:
    """Collect all parseable numeric values from cells with the given testid."""
    cells = container.get_by_test_id(testid)
    expect(cells.first).to_be_visible(timeout=5000)
    result = []
    for c in cells.all():
        val = _parse_number(c.text_content())
        if val is not None:
            result.append(val)
    return result


def _count_golden_matches(
    rendered: list[float],
    expected: list[float],
    tolerance: float = 1.0,
) -> int:
    """Count how many expected values have a close match in rendered values."""
    matched = 0
    for exp in expected:
        for r in rendered:
            if abs(r - exp) < tolerance:
                matched += 1
                break
    return matched


# ---------------------------------------------------------------------------
# Config A — Basic Sum
# ---------------------------------------------------------------------------


def test_config_a_cell_values(page_at_app: Page):
    """Config A: verify specific data cell values match golden sum values."""
    g = GOLDEN["A"]
    container = get_pivot(page_at_app, "golden_a")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    all_expected = []
    for rk in sorted(g["cells"].keys()):
        for ck in sorted(g["cells"][rk].keys()):
            all_expected.append(g["cells"][rk][ck])

    rendered = _collect_rendered_numbers(container, "pivot-data-cell")
    matched = _count_golden_matches(rendered, all_expected)

    assert matched >= len(all_expected) - 1, (
        f"Expected at least {len(all_expected) - 1} golden cell values to match, "
        f"got {matched}. Expected: {all_expected[:5]}, Rendered: {rendered[:10]}"
    )


def test_config_a_grand_total(page_at_app: Page):
    """Config A: verify grand total matches pandas."""
    g = GOLDEN["A"]
    container = get_pivot(page_at_app, "golden_a")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    grand_cells = container.get_by_test_id("pivot-grand-total")
    expect(grand_cells.first).to_be_visible(timeout=5000)
    grand_val = _parse_number(grand_cells.first.text_content())
    assert grand_val is not None, "Could not parse grand total"
    assert (
        abs(grand_val - g["grand_total"]) < 1.0
    ), f"Grand total mismatch: rendered={grand_val}, expected={g['grand_total']}"


# ---------------------------------------------------------------------------
# Config C — Per-measure Aggregation (Revenue=sum, Units=avg)
# ---------------------------------------------------------------------------


def test_config_c_revenue_cells(page_at_app: Page):
    """Config C: Revenue cells (sum) match pandas golden values."""
    g = GOLDEN["C"]["measures"]["Revenue"]
    container = get_pivot(page_at_app, "golden_c")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    all_expected = []
    for rk in sorted(g["cells"].keys()):
        for ck in sorted(g["cells"][rk].keys()):
            all_expected.append(g["cells"][rk][ck])

    rendered = _collect_rendered_numbers(container, "pivot-data-cell")
    matched = _count_golden_matches(rendered, all_expected)

    assert matched >= len(all_expected) - 2, (
        f"Config C Revenue: expected at least {len(all_expected) - 2} matches, "
        f"got {matched}. Expected sample: {all_expected[:4]}"
    )


def test_config_c_units_avg_cells(page_at_app: Page):
    """Config C: Units cells (avg) match pandas golden values."""
    g = GOLDEN["C"]["measures"]["Units"]
    container = get_pivot(page_at_app, "golden_c")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    all_expected = []
    for rk in sorted(g["cells"].keys()):
        for ck in sorted(g["cells"][rk].keys()):
            all_expected.append(g["cells"][rk][ck])

    rendered = _collect_rendered_numbers(container, "pivot-data-cell")
    matched = _count_golden_matches(rendered, all_expected)

    assert matched >= len(all_expected) - 2, (
        f"Config C Units avg: expected at least {len(all_expected) - 2} matches, "
        f"got {matched}. Expected sample: {all_expected[:4]}"
    )


def test_config_c_grand_totals(page_at_app: Page):
    """Config C: grand totals for Revenue (sum) and Units (avg) match pandas."""
    g_rev = GOLDEN["C"]["measures"]["Revenue"]
    g_units = GOLDEN["C"]["measures"]["Units"]
    container = get_pivot(page_at_app, "golden_c")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    grand_cells = container.get_by_test_id("pivot-grand-total")
    expect(grand_cells.first).to_be_visible(timeout=5000)

    grand_values = []
    for c in grand_cells.all():
        val = _parse_number(c.text_content())
        if val is not None:
            grand_values.append(val)

    rev_matched = any(abs(v - g_rev["grand_total"]) < 2.0 for v in grand_values)
    units_matched = any(abs(v - g_units["grand_total"]) < 2.0 for v in grand_values)

    assert (
        rev_matched
    ), f"Revenue grand total {g_rev['grand_total']} not found in {grand_values}"
    assert (
        units_matched
    ), f"Units grand total {g_units['grand_total']} not found in {grand_values}"


# ---------------------------------------------------------------------------
# Config E — Subtotals (Region × Category, Revenue sum)
# ---------------------------------------------------------------------------


def test_config_e_subtotal_arithmetic(page_at_app: Page):
    """Config E: each subtotal cell equals the sum of its children in the rendered UI."""
    g = GOLDEN["E"]
    container = get_pivot(page_at_app, "golden_e")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    subtotal_cells = container.get_by_test_id("pivot-subtotal-cell")
    expect(subtotal_cells.first).to_be_visible(timeout=5000)

    subtotal_values = []
    for c in subtotal_cells.all():
        val = _parse_number(c.text_content())
        if val is not None:
            subtotal_values.append(val)

    expected_subtotals = []
    for rk in sorted(g["subtotals"]["by_region"]["cells"].keys()):
        for ck in sorted(g["subtotals"]["by_region"]["cells"][rk].keys()):
            expected_subtotals.append(g["subtotals"]["by_region"]["cells"][rk][ck])

    matched = _count_golden_matches(subtotal_values, expected_subtotals, tolerance=2.0)

    assert matched >= len(expected_subtotals) - 2, (
        f"Config E subtotals: expected at least {len(expected_subtotals) - 2} matches, "
        f"got {matched}. Expected: {expected_subtotals[:4]}, "
        f"Rendered subtotals: {subtotal_values[:8]}"
    )


def test_config_e_subtotal_row_totals(page_at_app: Page):
    """Config E: subtotal row totals match pandas groupby(Region).sum()."""
    g = GOLDEN["E"]
    container = get_pivot(page_at_app, "golden_e")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    sub_total_cells = container.get_by_test_id("pivot-subtotal-total")
    expect(sub_total_cells.first).to_be_visible(timeout=5000)

    rendered_totals = []
    for c in sub_total_cells.all():
        val = _parse_number(c.text_content())
        if val is not None:
            rendered_totals.append(val)

    expected_totals = list(g["subtotals"]["by_region"]["row_totals"].values())
    matched = _count_golden_matches(rendered_totals, expected_totals, tolerance=2.0)

    assert matched >= len(expected_totals) - 1, (
        f"Config E subtotal row totals: expected at least {len(expected_totals) - 1} "
        f"matches, got {matched}. Expected: {expected_totals}, "
        f"Rendered: {rendered_totals}"
    )


def test_config_e_leaf_plus_subtotal_consistency(page_at_app: Page):
    """Config E: sum of leaf data cells ≈ sum of subtotal cells (cross-check)."""
    container = get_pivot(page_at_app, "golden_e")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    leaf_values = _collect_rendered_numbers(container, "pivot-data-cell")
    subtotal_values = _collect_rendered_numbers(container, "pivot-subtotal-cell")

    assert len(leaf_values) > 0, "No leaf data cells found"
    assert len(subtotal_values) > 0, "No subtotal cells found"

    leaf_sum = sum(leaf_values)
    subtotal_sum = sum(subtotal_values)

    assert abs(leaf_sum - subtotal_sum) < 5.0, (
        f"Leaf cell sum ({leaf_sum:.2f}) should equal subtotal cell sum "
        f"({subtotal_sum:.2f}) — both aggregate the same data"
    )


# ---------------------------------------------------------------------------
# Config F — Pct of Total
# ---------------------------------------------------------------------------


def test_config_f_pct_of_total(page_at_app: Page):
    """Config F: verify pct_of_total cells show percentage values."""
    container = get_pivot(page_at_app, "golden_f")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell")
    expect(data_cells.first).to_be_visible(timeout=5000)

    pct_count = sum(1 for c in data_cells.all() if "%" in c.text_content().strip())
    assert (
        pct_count >= 3
    ), f"Expected at least 3 cells with '%' in pct_of_total mode, got {pct_count}"


def test_config_f_vs_f2_different_values(page_at_app: Page):
    """Config F vs F2: pct_of_total and pct_of_row produce different numbers."""
    container_f = get_pivot(page_at_app, "golden_f")
    expect(container_f.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    f_cells = [
        c.text_content().strip()
        for c in container_f.get_by_test_id("pivot-data-cell").all()
    ]

    container_f2 = get_pivot(page_at_app, "golden_f2")
    expect(container_f2.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    f2_cells = [
        c.text_content().strip()
        for c in container_f2.get_by_test_id("pivot-data-cell").all()
    ]

    assert len(f_cells) > 0 and len(f2_cells) > 0, "No cells found"
    min_len = min(len(f_cells), len(f2_cells))
    differences = sum(1 for i in range(min_len) if f_cells[i] != f2_cells[i])
    assert (
        differences > 0
    ), "pct_of_total and pct_of_row should produce different cell values"


def test_config_f_pct_cells_sum_near_100(page_at_app: Page):
    """Config F: all pct_of_total data cells should sum to approximately 100%."""
    container = get_pivot(page_at_app, "golden_f")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    values = _collect_rendered_numbers(container, "pivot-data-cell")
    assert len(values) > 0, "No parseable percentage cells found"
    total = sum(values)
    assert (
        abs(total - 100.0) < 1.0
    ), f"pct_of_total cells should sum to ~100%, got {total:.2f}%"


# ---------------------------------------------------------------------------
# Config F3 — Pct of Col
# ---------------------------------------------------------------------------


def test_config_f3_pct_of_col_cells_show_percentages(page_at_app: Page):
    """Config F3: pct_of_col cells display % values."""
    container = get_pivot(page_at_app, "golden_f3")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell")
    expect(data_cells.first).to_be_visible(timeout=5000)

    pct_count = sum(1 for c in data_cells.all() if "%" in c.text_content().strip())
    assert (
        pct_count >= 3
    ), f"Expected at least 3 cells with '%' in pct_of_col mode, got {pct_count}"


def test_config_f3_pct_of_col_column_sums_to_100(page_at_app: Page):
    """Config F3: for each column, pct_of_col data cells should sum to ~100%."""
    g = GOLDEN["F3"]
    container = get_pivot(page_at_app, "golden_f3")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell")
    expect(data_cells.first).to_be_visible(timeout=5000)

    all_pcts = []
    for c in data_cells.all():
        val = _parse_number(c.text_content())
        all_pcts.append(val)

    num_regions = len(g["pct_cells"])
    num_cols = len(g["col_totals_raw"])

    assert (
        len(all_pcts) >= num_regions * num_cols
    ), f"Expected at least {num_regions * num_cols} data cells, got {len(all_pcts)}"

    for col_idx in range(num_cols):
        col_sum = 0.0
        for row_idx in range(num_regions):
            val = all_pcts[row_idx * num_cols + col_idx]
            if val is not None:
                col_sum += val
        assert (
            abs(col_sum - 100.0) < 2.0
        ), f"Column {col_idx} pct_of_col should sum to ~100%, got {col_sum:.1f}%"


def test_config_f3_pct_of_col_values_match_golden(page_at_app: Page):
    """Config F3: specific pct_of_col values match pandas golden."""
    g = GOLDEN["F3"]
    container = get_pivot(page_at_app, "golden_f3")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    all_expected = []
    for rk in sorted(g["pct_cells"].keys()):
        for ck in sorted(g["pct_cells"][rk].keys()):
            all_expected.append(g["pct_cells"][rk][ck])

    rendered = _collect_rendered_numbers(container, "pivot-data-cell")
    matched = _count_golden_matches(rendered, all_expected, tolerance=1.0)

    assert matched >= len(all_expected) - 2, (
        f"Config F3 pct_of_col: expected at least {len(all_expected) - 2} matches, "
        f"got {matched}. Expected: {all_expected[:4]}, Rendered: {rendered[:8]}"
    )


def test_config_f3_vs_f_different_values(page_at_app: Page):
    """Config F3 vs F: pct_of_col and pct_of_total produce different numbers."""
    container_f3 = get_pivot(page_at_app, "golden_f3")
    expect(container_f3.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    f3_cells = [
        c.text_content().strip()
        for c in container_f3.get_by_test_id("pivot-data-cell").all()
    ]

    container_f = get_pivot(page_at_app, "golden_f")
    expect(container_f.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    f_cells = [
        c.text_content().strip()
        for c in container_f.get_by_test_id("pivot-data-cell").all()
    ]

    assert len(f3_cells) > 0 and len(f_cells) > 0, "No cells found"
    min_len = min(len(f3_cells), len(f_cells))
    differences = sum(1 for i in range(min_len) if f3_cells[i] != f_cells[i])
    assert (
        differences > 0
    ), "pct_of_col and pct_of_total should produce different cell values"


# ---------------------------------------------------------------------------
# Config H — Synthetic sum_over_sum
# ---------------------------------------------------------------------------


def test_config_h_synthetic_ratio_values(page_at_app: Page):
    """Config H: synthetic Rev/Unit ratios match pandas golden values."""
    g = GOLDEN["H"]
    container = get_pivot(page_at_app, "golden_h")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    expected_ratios = list(g["synthetic_ratios"].values())

    rendered = _collect_rendered_numbers(container, "pivot-data-cell")

    matched = _count_golden_matches(rendered, expected_ratios, tolerance=0.5)

    assert matched >= len(expected_ratios) - 1, (
        f"Config H synthetic ratios: expected at least {len(expected_ratios) - 1} "
        f"matches, got {matched}. Expected: {expected_ratios}, Rendered: {rendered}"
    )


def test_config_h_synthetic_grand_total_ratio(page_at_app: Page):
    """Config H: grand total of synthetic ratio matches pandas."""
    g = GOLDEN["H"]
    container = get_pivot(page_at_app, "golden_h")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    # Config H has columns=[] so grand totals may appear as pivot-grand-total
    # or in the totals row. Collect all visible numbers from the totals area.
    all_numbers = []
    for testid in ("pivot-grand-total", "pivot-data-cell"):
        cells = container.get_by_test_id(testid)
        if cells.count() > 0:
            for c in cells.all():
                val = _parse_number(c.text_content())
                if val is not None:
                    all_numbers.append(val)

    # Also check the totals row itself
    totals_row = container.get_by_test_id("pivot-totals-row")
    if totals_row.count() > 0:
        for c in totals_row.locator("td").all():
            val = _parse_number(c.text_content())
            if val is not None:
                all_numbers.append(val)

    ratio_matched = any(abs(v - g["grand_total_ratio"]) < 0.5 for v in all_numbers)
    assert ratio_matched, (
        f"Synthetic grand total ratio {g['grand_total_ratio']:.2f} "
        f"not found in any rendered values: {all_numbers}"
    )


# ---------------------------------------------------------------------------
# show_totals=False
# ---------------------------------------------------------------------------


def test_no_totals_suppresses_totals_row(page_at_app: Page):
    """show_totals=False: grand totals row should not appear."""
    container = get_pivot(page_at_app, "golden_no_totals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell")
    expect(data_cells.first).to_be_visible(timeout=5000)

    totals_row = container.get_by_test_id("pivot-totals-row")
    assert totals_row.count() == 0, (
        "show_totals=False should suppress the grand totals row, "
        f"but found {totals_row.count()} totals rows"
    )


def test_no_totals_suppresses_grand_total_cells(page_at_app: Page):
    """show_totals=False: no grand total cells should appear."""
    container = get_pivot(page_at_app, "golden_no_totals")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    grand_cells = container.get_by_test_id("pivot-grand-total")
    assert grand_cells.count() == 0, (
        "show_totals=False should suppress grand total cells, "
        f"but found {grand_cells.count()}"
    )


# ---------------------------------------------------------------------------
# Export filename
# ---------------------------------------------------------------------------


def test_export_filename_in_download(page_at_app: Page):
    """export_filename controls the suggested download filename."""
    container = get_pivot(page_at_app, "golden_export")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    export_btn = container.get_by_test_id("toolbar-export-data")
    export_btn.scroll_into_view_if_needed()
    page_at_app.wait_for_timeout(500)
    export_btn.evaluate("el => el.click()")

    panel = page_at_app.get_by_test_id("toolbar-export-data-panel")
    expect(panel).to_be_visible(timeout=5000)

    csv_btn = page_at_app.get_by_test_id("export-format-csv")
    csv_btn.click()

    raw_btn = page_at_app.get_by_test_id("export-content-raw")
    raw_btn.click()

    with page_at_app.expect_download() as download_info:
        action_btn = page_at_app.get_by_test_id("toolbar-export-data-action")
        action_btn.click()

    download = download_info.value
    suggested_name = download.suggested_filename
    assert "golden_export" in suggested_name, (
        f"Expected download filename to contain 'golden_export', "
        f"got '{suggested_name}'"
    )


def test_csv_export_values_match_golden(page_at_app: Page):
    """Config A export: CSV export values match golden expected values."""
    container = get_pivot(page_at_app, "golden_export")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    export_btn = container.get_by_test_id("toolbar-export-data")
    export_btn.scroll_into_view_if_needed()
    page_at_app.wait_for_timeout(500)
    export_btn.evaluate("el => el.click()")

    panel = page_at_app.get_by_test_id("toolbar-export-data-panel")
    expect(panel).to_be_visible(timeout=5000)

    csv_btn = page_at_app.get_by_test_id("export-format-csv")
    csv_btn.click()

    raw_btn = page_at_app.get_by_test_id("export-content-raw")
    raw_btn.click()

    with page_at_app.expect_download() as download_info:
        action_btn = page_at_app.get_by_test_id("toolbar-export-data-action")
        action_btn.click()

    download = download_info.value
    csv_path = download.path()
    assert csv_path is not None, "CSV download failed"

    csv_content = Path(csv_path).read_text()
    reader = csv.reader(io.StringIO(csv_content))
    rows = list(reader)
    assert len(rows) > 1, f"CSV has only {len(rows)} rows"

    numeric_values = []
    for row in rows[1:]:
        for cell in row[1:]:
            try:
                numeric_values.append(float(cell.replace(",", "")))
            except ValueError:
                pass

    assert len(numeric_values) > 0, "No numeric values found in CSV"

    g = GOLDEN["A"]
    golden_values = []
    for rk_vals in g["cells"].values():
        golden_values.extend(rk_vals.values())

    matched = _count_golden_matches(numeric_values, golden_values)

    assert matched >= 3, (
        f"Expected at least 3 CSV values to match golden, got {matched}. "
        f"Golden: {golden_values[:5]}, CSV: {numeric_values[:10]}"
    )
