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

"""E2E tests for 0.5.0 features: analytical show_values_as modes and
multi-field sorting.  Backed by the lightweight pivot_table_new_features_app.py
so these tests do not inflate the larger interactions app and cause timeouts.
"""

from __future__ import annotations

import re

from playwright.sync_api import Page, expect

from e2e_utils import get_pivot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_cell_number(text: str) -> float | None:
    """Parse a rendered cell text into a float, returning None if non-numeric."""
    cleaned = re.sub(r"[%$,\s]", "", (text or "").strip())
    try:
        return float(cleaned)
    except ValueError:
        return None


def _get_leaf_row_header_labels(container) -> list[str]:
    """Return text of every pivot-row-header element in DOM order."""
    headers = container.get_by_test_id("pivot-row-header").all()
    return [(h.text_content() or "").strip() for h in headers]


# ---------------------------------------------------------------------------
# Analytical show_values_as: running_total
# ---------------------------------------------------------------------------


def test_running_total_cells_are_cumulative_within_group(page_at_app: Page):
    """running_total: within each parent group the cell values must be
    non-decreasing top-to-bottom, and the last cell equals the group subtotal."""
    container = get_pivot(page_at_app, "test_pivot_running_total")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell").all()
    values = [_parse_cell_number(c.text_content() or "") for c in data_cells]
    numeric = [v for v in values if v is not None]

    assert len(numeric) >= 4, f"Expected at least 4 numeric data cells, got {numeric}"

    # The fixture has 2 groups of 2 rows each (East: A=100, B=150; West: A=200, B=250).
    # Running totals reset per group: [100, 250] and [200, 450].
    mid = len(numeric) // 2
    first_half = numeric[:mid]
    second_half = numeric[mid:]
    assert first_half == sorted(
        first_half
    ), f"First group running totals should be non-decreasing: {first_half}"
    assert second_half == sorted(
        second_half
    ), f"Second group running totals should be non-decreasing: {second_half}"

    # The last value in each group must equal the group subtotal.
    subtotal_cells = container.get_by_test_id("pivot-subtotal-cell").all()
    subtotal_values = [
        _parse_cell_number(c.text_content() or "") for c in subtotal_cells
    ]
    subtotal_numeric = [v for v in subtotal_values if v is not None]

    if subtotal_numeric:
        assert first_half[-1] in subtotal_numeric or any(
            abs(first_half[-1] - s) < 1 for s in subtotal_numeric
        ), (
            f"Last running total in first group ({first_half[-1]}) "
            f"should match a subtotal ({subtotal_numeric})"
        )


# ---------------------------------------------------------------------------
# Analytical show_values_as: rank
# ---------------------------------------------------------------------------


def test_rank_cells_contain_positive_integers(page_at_app: Page):
    """rank mode: all data cells must be positive integers drawn from {1, N}
    where N is the number of dimension members, with no duplicates."""
    container = get_pivot(page_at_app, "test_pivot_rank")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    data_cells = container.get_by_test_id("pivot-data-cell").all()
    values = [_parse_cell_number(c.text_content() or "") for c in data_cells]
    numeric = [v for v in values if v is not None]

    assert len(numeric) >= 2, f"Expected at least 2 rank cells, got {numeric}"
    for v in numeric:
        assert (
            v == int(v) and v >= 1
        ), f"Rank cell value {v!r} is not a positive integer"
    # The fixture has 2 row members (East, West) so ranks must be a subset of {1, 2}.
    assert set(numeric).issubset(
        {1.0, 2.0}
    ), f"Rank values {set(numeric)} are outside the expected set {{1, 2}}"
    assert len(set(numeric)) == len(numeric), f"Expected unique ranks but got {numeric}"


# ---------------------------------------------------------------------------
# Multi-field sort: secondary key tie-breaking
# ---------------------------------------------------------------------------


def test_multi_sort_secondary_key_asc_breaks_ties_alphabetically(page_at_app: Page):
    """Multi-sort with secondary key=asc: tied-value members appear A→B."""
    container = get_pivot(page_at_app, "test_pivot_multi_sort_asc")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    labels = _get_leaf_row_header_labels(container)
    ab_labels = [lbl for lbl in labels if lbl in ("A", "B")]
    assert (
        len(ab_labels) >= 4
    ), f"Expected at least 4 Category labels (A/B × 2 groups), got {ab_labels}"
    for i in range(0, len(ab_labels), 2):
        pair = ab_labels[i : i + 2]
        assert pair == ["A", "B"], (
            f"Expected [A, B] pair at position {i}, got {pair}. "
            f"Full label list: {ab_labels}"
        )


def test_multi_sort_secondary_key_desc_breaks_ties_reverse(page_at_app: Page):
    """Multi-sort with secondary key=desc: tied-value members appear B→A."""
    container = get_pivot(page_at_app, "test_pivot_multi_sort_desc")
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)

    labels = _get_leaf_row_header_labels(container)
    ab_labels = [lbl for lbl in labels if lbl in ("A", "B")]
    assert (
        len(ab_labels) >= 4
    ), f"Expected at least 4 Category labels (A/B × 2 groups), got {ab_labels}"
    for i in range(0, len(ab_labels), 2):
        pair = ab_labels[i : i + 2]
        assert pair == ["B", "A"], (
            f"Expected [B, A] pair at position {i}, got {pair}. "
            f"Full label list: {ab_labels}"
        )
