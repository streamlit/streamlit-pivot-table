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

"""Streamlit app backing the data, export, and edge-case Playwright suites."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_pivot_table import st_pivot_table

from pivot_table_app_support import (
    handle_click,
    handle_config,
    init_page,
    load_data,
    noop,
)


def render_app(data):
    df = data["df"]
    df_single = data["df_single"]
    df_nulls = data["df_nulls"]
    sparse_df = data["sparse_df"]
    df_synth = data["df_synth"]

    st.subheader("Primary Pivot")
    st_pivot_table(
        df,
        key="test_pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
        on_cell_click=handle_click,
        on_config_change=handle_config,
    )
    st.write(f"Config change count: {st.session_state.get('config_change_count', 0)}")
    st.write(f"Cell click count: {st.session_state.get('cell_click_count', 0)}")
    st.button("Trigger rerun", key="rerun_trigger")

    st.subheader("Conditional Format Pivot")
    st_pivot_table(
        df,
        key="test_pivot_cond_fmt",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        aggregation="sum",
        conditional_formatting=[
            {
                "type": "color_scale",
                "apply_to": ["Revenue"],
                "min_color": "#1b2e1b",
                "max_color": "#4caf50",
            },
            {
                "type": "data_bars",
                "apply_to": ["Profit"],
                "color": "#1976d2",
                "fill": "gradient",
            },
        ],
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Number Format Pivot")
    st_pivot_table(
        df,
        key="test_pivot_number_fmt",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        number_format={"Revenue": "$,.0f"},
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Empty Pivot")
    st_pivot_table(pd.DataFrame(), key="test_pivot_empty")

    st.subheader("Single Row Pivot")
    st_pivot_table(
        df_single,
        key="test_pivot_single_row",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("No Columns Pivot")
    st_pivot_table(
        df,
        key="test_pivot_no_cols",
        rows=["Region"],
        columns=[],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Count Distinct Pivot")
    st_pivot_table(
        df,
        key="test_pivot_count_distinct",
        rows=["Region"],
        columns=["Year"],
        values=["Category"],
        aggregation="count_distinct",
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Median Pivot")
    st_pivot_table(
        df,
        key="test_pivot_median",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="median",
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Auto Detect Pivot")
    st_pivot_table(
        df,
        key="test_pivot_auto",
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Threshold Pivot")
    st_pivot_table(
        df,
        key="test_pivot_threshold",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="avg",
        conditional_formatting=[
            {
                "type": "threshold",
                "apply_to": ["Revenue"],
                "conditions": [
                    {
                        "operator": "gt",
                        "value": 5000,
                        "background": "#1565c0",
                        "bold": True,
                    },
                ],
            },
        ],
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Tall Pivot")
    st_pivot_table(
        df,
        key="test_pivot_tall",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_subtotals=True,
        height=200,
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Alignment Pivot")
    st_pivot_table(
        sparse_df,
        key="test_pivot_alignment",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        column_alignment={"Revenue": "right"},
        empty_cell_value="N/A",
        show_totals=False,
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Null Handling Pivot")
    st_pivot_table(
        df_nulls,
        key="test_pivot_null_separate",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        null_handling="separate",
        show_totals=True,
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Null Handling Zero Pivot")
    st_pivot_table(
        df_nulls,
        key="test_pivot_null_zero",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        null_handling="zero",
        show_totals=True,
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Sparse Drilldown Pivot")
    st_pivot_table(
        sparse_df,
        key="test_pivot_sparse_drilldown",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        enable_drilldown=True,
        interactive=True,
        on_cell_click=noop,
        on_config_change=noop,
    )

    st.subheader("Synthetic Measures Pivot")
    st_pivot_table(
        df_synth,
        key="test_pivot_synthetic",
        rows=["Region"],
        columns=["Year"],
        values=["Total PRs"],
        synthetic_measures=[
            {
                "id": "prs_per_person",
                "label": "PRs / Person",
                "operation": "sum_over_sum",
                "numerator": "Total PRs",
                "denominator": "People",
            },
            {
                "id": "prs_minus_people",
                "label": "PRs - People",
                "operation": "difference",
                "numerator": "Total PRs",
                "denominator": "People",
            },
        ],
        show_totals=True,
        interactive=True,
        on_config_change=noop,
    )


def main():
    init_page()
    render_app(load_data())


if __name__ == "__main__":
    main()
