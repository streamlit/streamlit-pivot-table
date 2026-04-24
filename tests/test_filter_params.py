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

"""Tests for filter_fields, filters, and show_sections params (report-level filtering)."""

import pytest


# ---------------------------------------------------------------------------
# filters — user-facing dimension filter config
# ---------------------------------------------------------------------------


def test_filters_include_passes_through(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        filters={"Region": {"include": ["East"]}},
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["filters"] == {"Region": {"include": ["East"]}}


def test_filters_exclude_passes_through(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        filters={"Region": {"exclude": ["West"]}},
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["filters"] == {"Region": {"exclude": ["West"]}}


def test_filters_multiple_fields(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        filters={
            "Region": {"include": ["East"]},
            "Category": {"include": ["A"]},
        },
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["filters"]["Region"] == {"include": ["East"]}
    assert cfg["filters"]["Category"] == {"include": ["A"]}


def test_filters_omitted_not_in_config(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
    )
    cfg = calls[0]["default"]["config"]
    assert "filters" not in cfg or cfg.get("filters") is None


def test_filters_none_not_in_config(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        filters=None,
    )
    cfg = calls[0]["default"]["config"]
    assert "filters" not in cfg or cfg.get("filters") is None


# ---------------------------------------------------------------------------
# filter_fields — ordered Filters zone fields
# ---------------------------------------------------------------------------


def test_filter_fields_passes_through(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        filter_fields=["Category", "Region"],
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["filter_fields"] == ["Category", "Region"]


def test_filter_fields_preserves_order(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        filter_fields=["Category", "Year", "Region"],
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["filter_fields"] == ["Category", "Year", "Region"]


def test_filter_fields_omitted_not_in_config(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
    )
    cfg = calls[0]["default"]["config"]
    assert "filter_fields" not in cfg or cfg.get("filter_fields") is None


def test_filter_fields_and_filters_together(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        filter_fields=["Category", "Region"],
        filters={"Category": {"include": ["A"]}},
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["filter_fields"] == ["Category", "Region"]
    assert cfg["filters"] == {"Category": {"include": ["A"]}}


def test_filter_fields_invalid_type_raises(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    with pytest.raises((TypeError, ValueError)):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            filter_fields="Category",  # should be a list, not a string
        )


# ---------------------------------------------------------------------------
# filters — validation (mirrors source_filters depth)
# ---------------------------------------------------------------------------


def test_filters_not_dict_raises(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    with pytest.raises(TypeError, match="filters must be a dict"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            filters=["Region", "include", "East"],  # list instead of dict
        )


def test_filters_non_string_key_raises(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    with pytest.raises(TypeError, match="filters keys must be strings"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            filters={123: {"include": ["East"]}},
        )


def test_filters_non_dict_value_raises(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    with pytest.raises(TypeError, match=r"filters\['Region'\] must be a dict"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            filters={"Region": ["East"]},  # list instead of inner dict
        )


def test_filters_unsupported_key_raises(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    with pytest.raises(ValueError, match="unsupported keys"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            filters={"Region": {"contains": ["East"]}},  # unsupported key
        )


def test_filters_non_list_operand_raises(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    with pytest.raises(
        TypeError, match=r"filters\['Region'\]\['include'\] must be a list"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            filters={"Region": {"include": "East"}},  # string instead of list
        )


def test_filters_non_scalar_value_raises(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    with pytest.raises(
        TypeError, match=r"filters\['Region'\]\['include'\]\[0\] must be a scalar"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            filters={"Region": {"include": [["East"]]}},  # nested list
        )


def test_filters_both_include_and_exclude_passes(
    sample_df, pivot_module, mount_recorder
):
    """Both keys are allowed simultaneously (include takes precedence at runtime)."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        filters={"Region": {"include": ["East"], "exclude": ["West"]}},
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["filters"]["Region"]["include"] == ["East"]


def test_off_axis_filter_auto_promoted_to_filter_fields(
    sample_df, pivot_module, mount_recorder
):
    """A filter on a field not in rows/columns/filter_fields is auto-added to filter_fields."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        # Category is NOT in rows, columns, or filter_fields — must be promoted
        filters={"Category": {"include": ["Electronics"]}},
    )
    cfg = calls[0]["default"]["config"]
    assert "Category" in cfg.get(
        "filter_fields", []
    ), "off-axis filter key should be auto-promoted to filter_fields"


def test_on_axis_filter_not_duplicated_in_filter_fields(
    sample_df, pivot_module, mount_recorder
):
    """A filter on a row/column dimension is NOT added to filter_fields (it's in the layout)."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        filters={"Region": {"include": ["East"]}},  # Region is in rows
    )
    cfg = calls[0]["default"]["config"]
    assert "Region" not in (
        cfg.get("filter_fields") or []
    ), "on-axis filter key must not be duplicated in filter_fields"


def test_explicit_filter_fields_preserved_and_off_axis_appended(
    sample_df, pivot_module, mount_recorder
):
    """Explicit filter_fields order is preserved; off-axis filter keys are appended after."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        filter_fields=["Region"],  # explicit, even though it's on-axis (dual-role)
        filters={
            "Region": {"include": ["East"]},
            "Category": {"include": ["Electronics"]},  # off-axis → auto-promoted
        },
    )
    cfg = calls[0]["default"]["config"]
    ff = cfg.get("filter_fields", [])
    assert "Region" in ff
    assert "Category" in ff
    # Explicitly listed fields come before auto-promoted ones
    assert ff.index("Region") < ff.index("Category")


# ---------------------------------------------------------------------------
# show_sections — toolbar sections expand/collapse
# ---------------------------------------------------------------------------


def test_show_sections_false_passes_through(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        show_sections=False,
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["show_sections"] is False


def test_show_sections_true_passes_through(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        show_sections=True,
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["show_sections"] is True


def test_show_sections_omitted_not_in_config(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
    )
    cfg = calls[0]["default"]["config"]
    # show_sections should not appear when not specified (frontend defaults to True)
    assert "show_sections" not in cfg or cfg.get("show_sections") is None


def test_show_sections_invalid_type_raises(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    with pytest.raises((TypeError, ValueError)):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            show_sections="yes",  # must be bool or None
        )


# ---------------------------------------------------------------------------
# Interaction: filters are applied to source data in client_only mode
# ---------------------------------------------------------------------------


def test_off_axis_filter_applied_in_client_only(
    sample_df, pivot_module, mount_recorder
):
    """Off-axis filter_fields don't affect server-side aggregation in client_only mode;
    the full data should be shipped and filtered by the frontend."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        filter_fields=["Category"],
        filters={"Category": {"include": ["A"]}},
        execution_mode="client_only",
    )
    cfg = calls[0]["default"]["config"]
    assert cfg["filter_fields"] == ["Category"]
    assert cfg["filters"] == {"Category": {"include": ["A"]}}
