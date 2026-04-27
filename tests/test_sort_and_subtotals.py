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

"""Pytest tests for multi-field sorting and subtotal_position (Commit 4)."""

import pytest


# ---------------------------------------------------------------------------
# row_sort / col_sort — list form
# ---------------------------------------------------------------------------


def test_row_sort_list_passes_through(sample_df, pivot_module, mount_recorder):
    """A list of SortConfig dicts is accepted and forwarded verbatim."""
    calls = mount_recorder()
    sort_list = [
        {"by": "value", "direction": "desc", "value_field": "Revenue"},
        {"by": "key", "direction": "asc"},
    ]
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        row_sort=sort_list,
    )
    sent_config = calls[0]["data"]["config"]
    assert sent_config["row_sort"] == sort_list


def test_col_sort_list_passes_through(sample_df, pivot_module, mount_recorder):
    """A list form for col_sort is accepted and forwarded verbatim."""
    calls = mount_recorder()
    sort_list = [
        {"by": "key", "direction": "asc"},
        {"by": "value", "direction": "desc"},
    ]
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        col_sort=sort_list,
    )
    sent_config = calls[0]["data"]["config"]
    assert sent_config["col_sort"] == sort_list


def test_row_sort_single_dict_still_works(sample_df, pivot_module, mount_recorder):
    """Single-dict form is still accepted (backward-compatible)."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        row_sort={"by": "key", "direction": "desc"},
    )
    sent_config = calls[0]["data"]["config"]
    assert sent_config["row_sort"] == {"by": "key", "direction": "desc"}


@pytest.mark.parametrize(
    ("row_sort", "match"),
    [
        # invalid 'by' in a list element
        ([{"by": "bogus", "direction": "asc"}], r"row_sort\[0\]\['by'\]"),
        # invalid 'direction' in a list element
        ([{"by": "key", "direction": "sideways"}], r"row_sort\[0\]\['direction'\]"),
        # non-dict in list
        ([{"by": "key", "direction": "asc"}, "bad"], r"row_sort\[1\]"),
        # empty list
        ([], "must not be empty"),
    ],
)
def test_row_sort_list_invalid_raises(sample_df, pivot_module, row_sort, match):
    with pytest.raises((ValueError, TypeError), match=match):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            row_sort=row_sort,
        )


def test_row_sort_invalid_by_raises(sample_df, pivot_module):
    """Invalid 'by' in a single-dict sort raises ValueError."""
    with pytest.raises(ValueError, match=r"row_sort\['by'\]"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            row_sort={"by": "bogus", "direction": "asc"},
        )


def test_row_sort_invalid_direction_raises(sample_df, pivot_module):
    with pytest.raises(ValueError, match=r"row_sort\['direction'\]"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            row_sort={"by": "key", "direction": "sideways"},
        )


# ---------------------------------------------------------------------------
# subtotal_position
# ---------------------------------------------------------------------------


def test_subtotal_position_top_passes_through(sample_df, pivot_module, mount_recorder):
    """subtotal_position='top' is forwarded in the config."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        show_subtotals=True,
        subtotal_position="top",
    )
    sent_config = calls[0]["data"]["config"]
    assert sent_config.get("subtotal_position") == "top"


def test_subtotal_position_bottom_is_default_and_omitted(
    sample_df, pivot_module, mount_recorder
):
    """subtotal_position='bottom' is the default and is NOT included in the
    config payload (omitting it means the frontend falls back to 'bottom')."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region", "Category"],
        values=["Revenue"],
        show_subtotals=True,
        # subtotal_position defaults to "bottom"
    )
    sent_config = calls[0]["data"]["config"]
    # "bottom" is the default — it should NOT be present in the payload
    assert "subtotal_position" not in sent_config


def test_subtotal_position_invalid_raises(sample_df, pivot_module):
    with pytest.raises(ValueError, match="subtotal_position must be"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            subtotal_position="middle",
        )
