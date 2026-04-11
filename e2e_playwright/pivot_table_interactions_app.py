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

"""Streamlit app backing the interactions and locked-mode Playwright suites."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_pivot import st_pivot_table

from pivot_table_app_support import (
    handle_click,
    handle_config,
    init_page,
    load_data,
    noop,
)


def _make_drilldown_pagination_data() -> pd.DataFrame:
    """Generate a dataset where one cell has >500 matching rows to trigger pagination."""
    rows = []
    for i in range(700):
        rows.append({"Region": "Alpha", "Year": "2023", "Revenue": 10 + i})
    for i in range(50):
        rows.append({"Region": "Alpha", "Year": "2024", "Revenue": 100 + i})
    for i in range(30):
        rows.append({"Region": "Beta", "Year": "2023", "Revenue": 200 + i})
    return pd.DataFrame(rows)


def render_app(data):
    df = data["df"]

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

    st.subheader("Subtotals Pivot")
    st_pivot_table(
        df,
        key="test_pivot_subtotals",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_subtotals=True,
        interactive=True,
        on_config_change=noop,
    )

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

    st.subheader("Locked Pivot")
    st_pivot_table(
        df,
        key="test_pivot_locked",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        aggregation={"Revenue": "sum", "Profit": "avg"},
        locked=True,
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Locked Grouped Pivot")
    st_pivot_table(
        df,
        key="test_pivot_locked_groups",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_subtotals=True,
        locked=True,
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Read Only Pivot")
    st_pivot_table(
        df,
        key="test_pivot_readonly",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        interactive=False,
    )

    st.subheader("Drilldown Pivot")
    st_pivot_table(
        df,
        key="test_pivot_drilldown",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        enable_drilldown=True,
        interactive=True,
        on_cell_click=noop,
        on_config_change=noop,
    )

    st.subheader("Drilldown Disabled Pivot")
    st_pivot_table(
        df,
        key="test_pivot_no_drilldown",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        enable_drilldown=False,
        interactive=True,
        on_cell_click=noop,
        on_config_change=noop,
    )

    st.subheader("Dimension Toggle Pivot")
    st_pivot_table(
        df,
        key="test_pivot_dim_toggle",
        rows=["Region", "Category", "Year"],
        columns=["Year", "Category"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_subtotals=True,
        interactive=True,
        on_config_change=noop,
    )

    st.header("Per-Dimension Subtotals")
    st_pivot_table(
        df,
        key="test_pivot_per_dim_subtotals",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        show_subtotals=["Region"],
        show_totals=True,
    )

    st.header("Per-Measure Row Totals")
    st_pivot_table(
        df,
        key="test_pivot_per_measure_row_totals",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        show_totals=True,
        show_row_totals=["Revenue"],
    )

    st.header("Per-Measure Column Totals")
    st_pivot_table(
        df,
        key="test_pivot_per_measure_col_totals",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        show_totals=True,
        show_column_totals=["Revenue"],
    )

    st.subheader("Column Groups Pivot")
    st_pivot_table(
        df,
        key="test_pivot_col_groups",
        rows=["Region"],
        columns=["Year", "Category"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
        on_config_change=noop,
    )

    drill_df = _make_drilldown_pagination_data()

    st.subheader("Drilldown Pagination (Client)")
    st_pivot_table(
        drill_df,
        key="test_pivot_drilldown_pagination",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        enable_drilldown=True,
        execution_mode="client_only",
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Drilldown Pagination (Hybrid)")
    st_pivot_table(
        drill_df,
        key="test_pivot_drilldown_pagination_hybrid",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        enable_drilldown=True,
        execution_mode="threshold_hybrid",
        interactive=True,
        on_config_change=noop,
    )


def main():
    init_page()
    render_app(load_data())


if __name__ == "__main__":
    main()
