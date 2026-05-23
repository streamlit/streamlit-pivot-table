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

"""Unit tests for member grouping aggregation, drilldown, and sidecar fingerprint."""

import logging

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def region_df():
    """Simple string-dimension DataFrame with Region, Category, Revenue."""
    return pd.DataFrame(
        {
            "Region": ["Northeast", "Southeast", "West", "West", "Northeast"],
            "Category": ["A", "A", "A", "B", "B"],
            "Revenue": [100, 150, 200, 80, 120],
        }
    )


@pytest.fixture
def base_cfg():
    """Minimal valid pivot config (no member_groups)."""
    return {
        "rows": ["Region"],
        "columns": ["Category"],
        "values": ["Revenue"],
        "aggregation": {"Revenue": "sum"},
        "null_handling": None,
    }


@pytest.fixture
def group_cfg(base_cfg):
    """Config with East Coast group covering Northeast + Southeast."""
    return {
        **base_cfg,
        "member_groups": [
            {
                "field": "Region",
                "name": "East Coast",
                "members": ["Northeast", "Southeast"],
            }
        ],
    }


# ---------------------------------------------------------------------------
# _apply_member_groups helper tests
# ---------------------------------------------------------------------------


def test_apply_member_groups_remaps_grouped_values(pivot_module, region_df, base_cfg):
    """Grouped members are replaced with the group name."""
    groups = [
        {"field": "Region", "name": "East Coast", "members": ["Northeast", "Southeast"]}
    ]
    remapped, remapped_fields = pivot_module._apply_member_groups(
        region_df,
        groups,
        column_types={},
        null_handling=None,
        rows=["Region"],
        columns=["Category"],
        date_grains=None,
        adaptive_grains=None,
    )
    assert "Region" in remapped_fields
    region_values = set(remapped["Region"].tolist())
    assert "East Coast" in region_values
    assert "Northeast" not in region_values
    assert "Southeast" not in region_values
    assert "West" in region_values


def test_apply_member_groups_does_not_mutate_original(pivot_module, region_df):
    """_apply_member_groups must not modify the input DataFrame."""
    original_values = region_df["Region"].tolist()
    groups = [
        {"field": "Region", "name": "East Coast", "members": ["Northeast", "Southeast"]}
    ]
    pivot_module._apply_member_groups(
        region_df,
        groups,
        column_types={},
        null_handling=None,
        rows=["Region"],
        columns=["Category"],
        date_grains=None,
        adaptive_grains=None,
    )
    assert region_df["Region"].tolist() == original_values


def test_apply_member_groups_empty_groups_is_noop(pivot_module, region_df):
    """Empty member_groups list returns original data unchanged."""
    remapped, remapped_fields = pivot_module._apply_member_groups(
        region_df,
        [],
        column_types={},
        null_handling=None,
        rows=["Region"],
        columns=["Category"],
        date_grains=None,
        adaptive_grains=None,
    )
    assert remapped_fields == set()
    pd.testing.assert_frame_equal(remapped, region_df)


def test_apply_member_groups_unrecognized_member_is_noop(pivot_module, region_df):
    """Members not present in the data are silently ignored."""
    groups = [
        {
            "field": "Region",
            "name": "East Coast",
            "members": ["Northeast", "Midwest"],  # Midwest doesn't exist
        }
    ]
    remapped, _ = pivot_module._apply_member_groups(
        region_df,
        groups,
        column_types={},
        null_handling=None,
        rows=["Region"],
        columns=["Category"],
        date_grains=None,
        adaptive_grains=None,
    )
    # Midwest was not in data so no row gets "East Coast" for that reason;
    # but Northeast rows should still be remapped.
    assert "East Coast" in set(remapped["Region"].tolist())


def test_apply_member_groups_null_member_key(pivot_module):
    """The '(null)' sentinel maps null values into the group when null_handling is 'separate'."""
    df = pd.DataFrame(
        {
            "Region": ["Northeast", None, "West"],
            "Revenue": [100, 50, 200],
        }
    )
    # "(null)" sentinel is used only when null_handling_mode == "separate", which
    # is enabled by passing null_handling={"Region": "separate"} or a global
    # dict that triggers separate mode for this field.
    groups = [{"field": "Region", "name": "Unknown", "members": ["(null)"]}]
    remapped, _ = pivot_module._apply_member_groups(
        df,
        groups,
        column_types={},
        null_handling={"Region": "separate"},
        rows=["Region"],
        columns=[],
        date_grains=None,
        adaptive_grains=None,
    )
    values = remapped["Region"].tolist()
    assert "Unknown" in values
    assert "(null)" not in values  # raw sentinel should be replaced


def test_apply_member_groups_integer_field_uses_resolved_string(pivot_module):
    """Integer dimension values must match via resolved-string keys."""
    df = pd.DataFrame(
        {
            "Year": [2023, 2024, 2025],
            "Revenue": [100, 200, 300],
        }
    )
    # The resolved key for integer 2024 is "2024" (string).
    groups = [{"field": "Year", "name": "Recent", "members": ["2024", "2025"]}]
    remapped, _ = pivot_module._apply_member_groups(
        df,
        groups,
        column_types={"Year": "integer"},
        null_handling=None,
        rows=["Year"],
        columns=[],
        date_grains=None,
        adaptive_grains=None,
    )
    values = set(remapped["Year"].tolist())
    assert "Recent" in values
    assert "2023" in values


# ---------------------------------------------------------------------------
# Aggregation correctness tests
# ---------------------------------------------------------------------------


def test_member_groups_aggregation_matches_sum(pivot_module, region_df, group_cfg):
    """East Coast Revenue == Northeast Revenue + Southeast Revenue."""
    northeast_rev = int(
        region_df[region_df["Region"] == "Northeast"]["Revenue"].sum()
    )  # 220
    southeast_rev = int(
        region_df[region_df["Region"] == "Southeast"]["Revenue"].sum()
    )  # 150
    expected_east_coast = northeast_rev + southeast_rev  # 370

    groups = [
        {
            "field": "Region",
            "name": "East Coast",
            "members": ["Northeast", "Southeast"],
        }
    ]
    remapped, _ = pivot_module._apply_member_groups(
        region_df,
        groups,
        column_types={},
        null_handling=None,
        rows=["Region"],
        columns=["Category"],
        date_grains=None,
        adaptive_grains=None,
    )
    east_coast_rev = int(remapped[remapped["Region"] == "East Coast"]["Revenue"].sum())
    assert east_coast_rev == expected_east_coast


def test_member_groups_ungrouped_members_unaffected(pivot_module, region_df):
    """Ungrouped members remain as individual resolved-string entries."""
    groups = [
        {"field": "Region", "name": "East Coast", "members": ["Northeast", "Southeast"]}
    ]
    remapped, _ = pivot_module._apply_member_groups(
        region_df,
        groups,
        column_types={},
        null_handling=None,
        rows=["Region"],
        columns=["Category"],
        date_grains=None,
        adaptive_grains=None,
    )
    values = set(remapped["Region"].tolist())
    assert "West" in values


# ---------------------------------------------------------------------------
# Sidecar fingerprint tests
# ---------------------------------------------------------------------------


def test_fingerprint_changes_when_member_groups_added(pivot_module):
    base = {
        "rows": ["Region"],
        "columns": ["Category"],
        "values": ["Revenue"],
        "aggregation": {"Revenue": "sum"},
    }
    with_groups = {
        **base,
        "member_groups": [
            {"field": "Region", "name": "East Coast", "members": ["Northeast"]}
        ],
    }
    assert pivot_module._build_sidecar_fingerprint(
        base, None
    ) != pivot_module._build_sidecar_fingerprint(with_groups, None)


def test_fingerprint_unchanged_for_unrelated_config_change(pivot_module):
    base = {
        "rows": ["Region"],
        "columns": ["Category"],
        "values": ["Revenue"],
        "aggregation": {"Revenue": "sum"},
        "member_groups": [
            {"field": "Region", "name": "East Coast", "members": ["Northeast"]}
        ],
    }
    with_format = {**base, "number_format": ",.0f"}
    assert pivot_module._build_sidecar_fingerprint(
        base, None
    ) == pivot_module._build_sidecar_fingerprint(with_format, None)


def test_fingerprint_is_order_independent(pivot_module):
    cfg_a = {
        "rows": ["Region"],
        "columns": ["Category"],
        "values": ["Revenue"],
        "aggregation": {"Revenue": "sum"},
        "member_groups": [
            {
                "field": "Region",
                "name": "East Coast",
                "members": ["Northeast", "Southeast"],
            }
        ],
    }
    cfg_b = {
        **cfg_a,
        "member_groups": [
            {
                "field": "Region",
                "name": "East Coast",
                "members": ["Southeast", "Northeast"],  # reversed
            }
        ],
    }
    assert pivot_module._build_sidecar_fingerprint(
        cfg_a, None
    ) == pivot_module._build_sidecar_fingerprint(cfg_b, None)


def test_fingerprint_changes_when_group_member_changes(pivot_module):
    cfg_a = {
        "rows": ["Region"],
        "columns": ["Category"],
        "values": ["Revenue"],
        "aggregation": {"Revenue": "sum"},
        "member_groups": [
            {"field": "Region", "name": "East Coast", "members": ["Northeast"]}
        ],
    }
    cfg_b = {
        **cfg_a,
        "member_groups": [
            {
                "field": "Region",
                "name": "East Coast",
                "members": ["Northeast", "Southeast"],
            }
        ],
    }
    assert pivot_module._build_sidecar_fingerprint(
        cfg_a, None
    ) != pivot_module._build_sidecar_fingerprint(cfg_b, None)


# ---------------------------------------------------------------------------
# Drilldown tests
# ---------------------------------------------------------------------------


def test_drilldown_returns_raw_members_for_grouped_cell(pivot_module, region_df):
    """Clicking an 'East Coast' cell must return rows with raw Region values."""
    member_groups = [
        {
            "field": "Region",
            "name": "East Coast",
            "members": ["Northeast", "Southeast"],
        }
    ]
    # drilldown_request uses "filters" dict for cell-path filters
    drilldown_request = {
        "filters": {"Region": "East Coast", "Category": "A"},
    }

    records, _cols, total_count, _page = pivot_module._compute_hybrid_drilldown(
        region_df,
        drilldown_request,
        rows=["Region"],
        columns=["Category"],
        member_groups=member_groups,
    )
    assert total_count > 0
    # Raw Region values must appear, not group name
    raw_regions = {r["Region"] for r in records}
    assert "East Coast" not in raw_regions
    assert raw_regions.issubset({"Northeast", "Southeast"})


def test_drilldown_not_mutated_after_call(pivot_module, region_df):
    """filtered_data must not be mutated after _compute_hybrid_drilldown returns."""
    original = region_df.copy()
    member_groups = [
        {
            "field": "Region",
            "name": "East Coast",
            "members": ["Northeast", "Southeast"],
        }
    ]
    pivot_module._compute_hybrid_drilldown(
        region_df,
        {"filters": {"Region": "East Coast", "Category": "A"}},
        rows=["Region"],
        columns=["Category"],
        member_groups=member_groups,
    )
    pd.testing.assert_frame_equal(region_df, original)


def test_drilldown_with_nonunique_index(pivot_module):
    """Drilldown must work correctly with a non-unique DataFrame index."""
    df = pd.DataFrame(
        {
            "Region": ["Northeast", "Southeast", "West"],
            "Category": ["A", "A", "A"],
            "Revenue": [100, 150, 200],
        },
        index=[0, 0, 0],  # deliberately non-unique
    )
    member_groups = [
        {
            "field": "Region",
            "name": "East Coast",
            "members": ["Northeast", "Southeast"],
        }
    ]
    records, _cols, total, _page = pivot_module._compute_hybrid_drilldown(
        df,
        {"filters": {"Region": "East Coast", "Category": "A"}},
        rows=["Region"],
        columns=["Category"],
        member_groups=member_groups,
    )
    # Should return exactly the 2 East Coast rows, not 3
    assert total == 2
    raw_regions = {r["Region"] for r in records}
    assert raw_regions.issubset({"Northeast", "Southeast"})


# ---------------------------------------------------------------------------
# debug-log test for unrecognized members
# ---------------------------------------------------------------------------


def test_apply_member_groups_logs_debug_for_unrecognized_members(
    pivot_module, region_df, caplog
):
    """Unrecognized members must emit a debug log and not raise."""
    groups = [
        {
            "field": "Region",
            "name": "East Coast",
            "members": ["Northeast", "Midwest"],  # Midwest not in data
        }
    ]
    with caplog.at_level(logging.DEBUG, logger="streamlit_pivot"):
        pivot_module._apply_member_groups(
            region_df,
            groups,
            column_types={},
            null_handling=None,
            rows=["Region"],
            columns=["Category"],
            date_grains=None,
            adaptive_grains=None,
        )
    # A debug log message must have been emitted for the unrecognized member
    debug_msgs = [r.message for r in caplog.records if r.levelno == logging.DEBUG]
    assert any(
        "Midwest" in msg for msg in debug_msgs
    ), f"Expected debug log for unrecognized member 'Midwest', got: {debug_msgs}"
