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
    assert payload["enable_drilldown"] is False
    assert len(payload["dataframe"]) <= len(large_df)


def test_threshold_hybrid_falls_back_for_incompatible_configs(
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
        aggregation="avg",
        execution_mode="threshold_hybrid",
    )

    payload = calls[0]["data"]
    assert payload["execution_mode"] == "client_only"
