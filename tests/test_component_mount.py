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

"""Python-side mount/payload tests for st_pivot_table()."""

import sys
from importlib import import_module
from unittest.mock import patch

import numpy as np
import pandas as pd


def test_component_registers_deterministic_entry_assets():
    sys.modules.pop("streamlit_pivot", None)
    try:
        with patch("streamlit.components.v2.component") as component_factory:
            import_module("streamlit_pivot")

        component_factory.assert_called_once()
        kwargs = component_factory.call_args.kwargs
        assert kwargs["js"] == "index.js"
        assert kwargs["css"] == "index.css"
    finally:
        sys.modules.pop("streamlit_pivot", None)


def test_mount_normalizes_partial_aggregation_map(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()

    result = pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        aggregation={"Revenue": "avg"},
    )

    sent_config = calls[0]["data"]["config"]
    assert sent_config["aggregation"] == {"Revenue": "avg", "Profit": "sum"}
    assert calls[0]["default"]["config"]["aggregation"] == {
        "Revenue": "avg",
        "Profit": "sum",
    }
    assert result["config"]["aggregation"] == {"Revenue": "avg", "Profit": "sum"}


def test_mount_normalizes_numpy_string_lists_to_builtin_strings(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()

    result = pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=np.array(["Region"]),
        columns=np.array(["Year"]),
        values=list(np.array(["Revenue"])),
    )

    sent_config = calls[0]["data"]["config"]
    assert sent_config["rows"] == ["Region"]
    assert sent_config["columns"] == ["Year"]
    assert sent_config["values"] == ["Revenue"]
    assert all(type(field) is str for field in sent_config["rows"])
    assert all(type(field) is str for field in sent_config["columns"])
    assert all(type(field) is str for field in sent_config["values"])
    assert list(sent_config["aggregation"]) == ["Revenue"]
    assert type(next(iter(sent_config["aggregation"]))) is str
    assert result["config"]["values"] == ["Revenue"]


def test_mount_includes_optional_payload_fields_and_callbacks(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()

    def on_cell_click():
        return None

    def on_config_change():
        return None

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        null_handling={"Revenue": "zero"},
        hidden_attributes=["Category"],
        hidden_from_aggregators=["Profit"],
        frozen_columns=["Revenue"],
        sorters={"Region": ["West", "East"]},
        locked=True,
        menu_limit=25,
        enable_drilldown=False,
        export_filename="sales-report",
        on_cell_click=on_cell_click,
        on_config_change=on_config_change,
    )

    mount_kwargs = calls[0]
    payload = mount_kwargs["data"]

    assert payload["null_handling"] == {"Revenue": "zero"}
    assert payload["hidden_attributes"] == ["Category"]
    assert payload["hidden_from_aggregators"] == ["Profit"]
    assert payload["hidden_from_drag_drop"] == ["Revenue"]
    assert payload["sorters"] == {"Region": ["West", "East"]}
    assert payload["locked"] is True
    assert payload["menu_limit"] == 25
    assert payload["enable_drilldown"] is False
    assert payload["export_filename"] == "sales-report"
    assert mount_kwargs["on_cell_click_change"] is on_cell_click
    assert mount_kwargs["on_config_change"] is on_config_change


def test_mount_preserves_sort_dimension_in_config(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        row_sort={
            "by": "key",
            "direction": "asc",
            "dimension": "Category",
        },
    )

    sent_config = calls[0]["data"]["config"]
    assert sent_config["row_sort"] == {
        "by": "key",
        "direction": "asc",
        "dimension": "Category",
    }


def test_hidden_from_drag_drop_alias_flows_to_payload(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        hidden_from_drag_drop=["Revenue"],
    )

    assert calls[0]["data"]["hidden_from_drag_drop"] == ["Revenue"]


def test_missing_on_config_change_uses_noop_callback(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
    )

    assert calls[0]["on_config_change"] is pivot_module._noop_callback


def test_mount_includes_perf_metrics_state_and_callback(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
    )

    mount_kwargs = calls[0]
    assert mount_kwargs["default"]["perf_metrics"] is None
    assert mount_kwargs["on_perf_metrics_change"] is pivot_module._noop_callback


def test_mount_defaults_auto_date_hierarchy_on(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
    )

    sent_config = calls[0]["data"]["config"]
    assert sent_config["auto_date_hierarchy"] is True
    assert calls[0]["default"]["config"]["auto_date_hierarchy"] is True


def test_mount_preserves_explicit_original_date_opt_out(pivot_module, mount_recorder):
    calls = mount_recorder()
    df = pd.DataFrame(
        {
            "Region": ["East", "East"],
            "order_date": pd.to_datetime(["2024-01-03", "2024-02-10"]),
            "Revenue": [100, 150],
        }
    )

    pivot_module.st_pivot_table(
        df,
        key="pivot",
        rows=["Region"],
        columns=["order_date"],
        values=["Revenue"],
        date_grains={"order_date": None},
    )

    sent_config = calls[0]["data"]["config"]
    assert sent_config["date_grains"] == {"order_date": None}


def test_threshold_hybrid_preaggregates_compatible_large_configs(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    assert payload["execution_mode"] == "threshold_hybrid"
    assert (
        "enable_drilldown" not in payload
    )  # drill-down is enabled (server round-trip)
    assert len(payload["dataframe"]) <= len(large_df)
    assert payload["server_mode_reason"]
    assert "Drill-down" in payload["server_mode_reason"]


def test_threshold_hybrid_works_with_synthetic_measures(
    sample_df, pivot_module, mount_recorder
):
    """Synthetic measures are now compatible with threshold_hybrid."""
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        synthetic_measures=[
            {
                "id": "synth1",
                "label": "Ratio",
                "operation": "sum_over_sum",
                "numerator": "Revenue",
                "denominator": "Revenue",
            }
        ],
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    assert payload["execution_mode"] == "threshold_hybrid"


def test_threshold_hybrid_median_no_longer_falls_back(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="median",
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    assert payload["execution_mode"] == "threshold_hybrid"


def test_threshold_hybrid_includes_totals_sidecar(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="median",
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    assert "hybrid_totals" in payload
    assert "sidecar_fingerprint" in payload["hybrid_totals"]


def test_threshold_hybrid_includes_agg_remap(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="count",
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    assert "hybrid_agg_remap" in payload
    assert payload["hybrid_agg_remap"]["Revenue"] == "sum"


def test_threshold_hybrid_omits_sidecar_for_decomposable(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    assert "hybrid_totals" not in payload
    assert "hybrid_agg_remap" not in payload


def test_threshold_hybrid_includes_source_row_count(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    assert payload["source_row_count"] == len(large_df)


def test_threshold_hybrid_source_row_count_reflects_source_filters(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        execution_mode="threshold_hybrid",
        source_filters={"Category": {"include": ["A"]}},
    )

    payload = calls[0]["data"]
    assert payload["execution_mode"] == "threshold_hybrid"
    assert payload["source_row_count"] == int((large_df["Category"] == "A").sum())


def test_client_only_omits_source_row_count(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        execution_mode="client_only",
    )

    payload = calls[0]["data"]
    assert "source_row_count" not in payload


def test_threshold_hybrid_allows_source_filters_on_hidden_field(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        execution_mode="threshold_hybrid",
        source_filters={"Category": {"include": ["A"]}},
    )

    payload = calls[0]["data"]
    assert payload["execution_mode"] == "threshold_hybrid"
    # Hidden-field source_filters should affect the aggregated payload without
    # forcing hybrid to fall back.
    assert len(payload["dataframe"]) > 0
    assert payload["source_row_count"] == int((large_df["Category"] == "A").sum())


def test_threshold_hybrid_source_filters_exclude_nulls_from_payload_and_drilldown(
    pivot_module, mount_recorder
):
    df = {
        "Region": ["East", "East", "West", "West"],
        "Category": ["A", None, "A", None],
        "Year": [2023, 2023, 2023, 2023],
        "Revenue": [100, 150, 200, 250],
        "Profit": [10, 20, 30, 40],
    }
    session_state = {
        "pivot": {"drilldown_request": {"filters": {"Year": "2023"}, "page": 0}}
    }
    calls = mount_recorder(session_state=session_state)

    pivot_module.st_pivot_table(
        df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        execution_mode="threshold_hybrid",
        source_filters={"Category": {"exclude": [None]}},
    )

    payload = calls[0]["data"]
    assert payload["execution_mode"] == "threshold_hybrid"
    assert payload["source_row_count"] == 2
    assert all(r["Category"] == "A" for r in payload["drilldown_records"])


def test_hybrid_drilldown_respects_source_filters(
    sample_df, pivot_module, mount_recorder
):
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)
    session_state = {
        "pivot": {
            "drilldown_request": {
                "filters": {"Region": "East", "Year": "2023"},
                "page": 0,
            }
        }
    }
    calls = mount_recorder(session_state=session_state)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        execution_mode="threshold_hybrid",
        source_filters={"Category": {"include": ["A"]}},
    )

    payload = calls[0]["data"]
    assert payload["drilldown_total_count"] > 0
    assert all(r["Category"] == "A" for r in payload["drilldown_records"])


def test_hybrid_drilldown_sort_fields_order_records(
    sample_df, pivot_module, mount_recorder
):
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)
    session_state = {
        "pivot": {
            "drilldown_request": {
                "filters": {"Region": "East", "Year": "2023"},
                "page": 0,
                "sortColumn": "Revenue",
                "sortDirection": "desc",
            }
        }
    }
    calls = mount_recorder(session_state=session_state)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    revenues = [row["Revenue"] for row in payload["drilldown_records"]]
    assert revenues == sorted(revenues, reverse=True)


def test_source_filters_and_config_filters_intersect_on_same_field(
    sample_df, pivot_module, mount_recorder
):
    session_state: dict[str, object] = {"_seed": True}
    calls = mount_recorder(session_state=session_state)

    # Two calls are required: the first captures the Python-sent config shape,
    # then we inject a persisted interactive filter into session_state to
    # verify the next render intersects it with source_filters.
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        source_filters={"Region": {"include": ["East"]}},
        execution_mode="threshold_hybrid",
    )
    sent_config = calls[0]["data"]["config"]
    session_state["pivot"] = {
        "config": {
            **sent_config,
            "filters": {"Region": {"exclude": ["East"]}},
        }
    }
    calls.clear()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        source_filters={"Region": {"include": ["East"]}},
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    assert len(payload["dataframe"]) == 0


def test_non_dimension_filter_causes_client_fallback(pivot_module):
    """Programmatic filter on non-dimension field makes hybrid incompatible."""
    cfg = {
        "rows": ["Region"],
        "columns": ["Year"],
        "values": ["Revenue"],
        "aggregation": {"Revenue": "sum"},
        "filters": {"Category": {"include": ["A"]}},
    }
    ok, msg = pivot_module._can_use_threshold_hybrid(cfg)
    assert ok is False
    assert "Category" in msg


def test_adaptive_date_grains_in_payload(pivot_module, mount_recorder):
    """adaptive_date_grains appears in data_payload for temporal columns."""
    df = pd.DataFrame(
        {
            "order_date": pd.to_datetime(["2019-01-01", "2024-06-15"]),
            "revenue": [100, 200],
        }
    )
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        df,
        key="adg",
        rows=["order_date"],
        values=["revenue"],
    )
    data = calls[0]["data"]
    assert "adaptive_date_grains" in data
    assert data["adaptive_date_grains"]["order_date"] == "year"


def test_adaptive_grains_computed_from_source_filtered_data(
    pivot_module, mount_recorder
):
    """adaptive_date_grains reflects source_filters, not raw data."""
    df = pd.DataFrame(
        {
            "order_date": pd.to_datetime(
                ["2019-01-01", "2020-01-01", "2024-06-01", "2024-06-10"]
            ),
            "year_col": [2019, 2020, 2024, 2024],
            "revenue": [100, 200, 300, 400],
        }
    )
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        df,
        key="adg_sf",
        rows=["order_date"],
        values=["revenue"],
        source_filters={"year_col": {"include": [2024]}},
    )
    data = calls[0]["data"]
    assert data["adaptive_date_grains"]["order_date"] == "day"


# ---------------------------------------------------------------------------
# row_layout validation
# ---------------------------------------------------------------------------


def test_row_layout_rejects_invalid_value(sample_df, pivot_module, mount_recorder):
    """row_layout must be 'table' or 'hierarchy'."""
    import pytest

    mount_recorder()
    with pytest.raises(ValueError, match="row_layout"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            values=["Revenue"],
            row_layout="tree",
        )


def test_row_layout_hierarchy_forces_show_subtotals(
    sample_df, pivot_module, mount_recorder
):
    """row_layout='hierarchy' auto-enables show_subtotals even when not specified."""
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        row_layout="hierarchy",
    )

    sent_config = calls[0]["data"]["config"]
    assert sent_config["row_layout"] == "hierarchy"
    assert sent_config["show_subtotals"] is True


def test_row_layout_hierarchy_preserves_explicit_subtotals_list(
    pivot_module, mount_recorder
):
    """row_layout='hierarchy' with an explicit show_subtotals list keeps the list."""
    df = pd.DataFrame(
        {
            "Region": ["East", "East", "West", "West"],
            "Category": ["A", "B", "A", "B"],
            "SubCat": ["x", "y", "x", "y"],
            "Year": [2023, 2024, 2023, 2024],
            "Revenue": [100, 150, 200, 250],
        }
    )
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        df,
        key="pivot",
        rows=["Region", "Category", "SubCat"],
        columns=["Year"],
        values=["Revenue"],
        row_layout="hierarchy",
        show_subtotals=["Region"],
    )

    sent_config = calls[0]["data"]["config"]
    assert sent_config["row_layout"] == "hierarchy"
    assert sent_config["show_subtotals"] == ["Region"]


def test_row_layout_table_default_no_subtotals(sample_df, pivot_module, mount_recorder):
    """Default row_layout='table' does not auto-enable subtotals."""
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
    )

    sent_config = calls[0]["data"]["config"]
    assert sent_config.get("row_layout", "table") == "table"
    assert sent_config.get("show_subtotals", False) is False


# ---------------------------------------------------------------------------
# Hybrid subtotal sidecar with hierarchy
# ---------------------------------------------------------------------------


def test_threshold_hybrid_hierarchy_includes_subtotals_sidecar(
    sample_df, pivot_module, mount_recorder
):
    """hierarchy layout in hybrid mode produces subtotal sidecar entries."""
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="median",
        execution_mode="threshold_hybrid",
        row_layout="hierarchy",
    )

    payload = calls[0]["data"]
    assert payload["execution_mode"] == "threshold_hybrid"
    assert "hybrid_totals" in payload
    totals = payload["hybrid_totals"]
    assert "subtotals" in totals
    assert len(totals["subtotals"]) > 0


def test_threshold_hybrid_subtotals_list_generates_all_depths(
    sample_df, pivot_module, mount_recorder
):
    """show_subtotals=['Region'] still generates sidecar entries (truthy list)."""
    calls = mount_recorder()
    large_df = sample_df.loc[sample_df.index.repeat(20000)].reset_index(drop=True)

    pivot_module.st_pivot_table(
        large_df,
        key="pivot",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="median",
        execution_mode="threshold_hybrid",
        show_subtotals=["Region"],
    )

    payload = calls[0]["data"]
    assert payload["execution_mode"] == "threshold_hybrid"
    totals = payload["hybrid_totals"]
    assert "subtotals" in totals
    assert len(totals["subtotals"]) > 0
