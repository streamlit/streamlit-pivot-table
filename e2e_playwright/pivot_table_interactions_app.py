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

import pandas as pd  # type: ignore[import-untyped]
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


def _make_date_hierarchy_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "region": [
                "US",
                "US",
                "US",
                "US",
                "US",
                "EU",
                "EU",
                "EU",
                "EU",
                "EU",
            ],
            "order_date": pd.to_datetime(
                [
                    "2024-01-03",
                    "2024-04-10",
                    "2024-07-12",
                    "2024-10-05",
                    "2025-01-09",
                    "2024-01-04",
                    "2024-04-17",
                    "2024-07-14",
                    "2024-10-08",
                    "2025-01-10",
                ]
            ),
            "revenue": [100, 200, 150, 180, 130, 80, 160, 95, 140, 90],
            "profit": [40, 80, 55, 65, 45, 30, 60, 34, 50, 32],
        }
    )


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

    st.subheader("Date Hierarchy Pivot")
    st_pivot_table(
        _make_date_hierarchy_data(),
        key="test_pivot_date_hierarchy",
        rows=["region"],
        columns=["order_date"],
        values=["revenue", "profit"],
        aggregation="sum",
        show_values_as={"revenue": "diff_from_prev"},
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Date Hierarchy Multi-Dim Columns Pivot")
    st_pivot_table(
        _make_date_hierarchy_data(),
        key="test_pivot_date_hierarchy_multidim",
        rows=["profit"],
        columns=["region", "order_date"],
        values=["revenue"],
        aggregation="sum",
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Date Hierarchy Rows Pivot")
    st_pivot_table(
        _make_date_hierarchy_data(),
        key="test_pivot_date_hierarchy_rows",
        rows=["order_date"],
        columns=["region"],
        values=["revenue"],
        aggregation="sum",
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Date Hierarchy Mixed Rows Pivot")
    st_pivot_table(
        _make_date_hierarchy_data(),
        key="test_pivot_date_hierarchy_rows_mixed",
        rows=["region", "order_date"],
        values=["revenue"],
        aggregation="sum",
        show_subtotals=["region"],
        interactive=True,
        on_config_change=noop,
    )

    # Adaptive date grain: multi-year dataset -> auto-defaults to "year"
    st.subheader("Adaptive Grain (Multi-Year)")
    adaptive_year_df = pd.DataFrame(
        {
            "order_date": pd.to_datetime(
                ["2019-03-01", "2020-06-15", "2021-09-10", "2023-01-20", "2024-11-05"]
            ),
            "revenue": [100, 200, 300, 400, 500],
        }
    )
    st_pivot_table(
        adaptive_year_df,
        key="test_pivot_adaptive_year",
        rows=["order_date"],
        values=["revenue"],
        aggregation="sum",
        interactive=True,
        on_config_change=noop,
    )

    # Adaptive date grain: 3-month dataset -> auto-defaults to "month"
    st.subheader("Adaptive Grain (3 Month)")
    adaptive_month_df = pd.DataFrame(
        {
            "order_date": pd.to_datetime(
                ["2024-06-01", "2024-06-15", "2024-07-10", "2024-08-05", "2024-08-28"]
            ),
            "revenue": [10, 20, 30, 40, 50],
        }
    )
    st_pivot_table(
        adaptive_month_df,
        key="test_pivot_adaptive_month",
        rows=["order_date"],
        values=["revenue"],
        aggregation="sum",
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Hierarchy Layout")
    st_pivot_table(
        df,
        key="test_pivot_hierarchy",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        row_layout="hierarchy",
        show_subtotals=True,
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Hierarchy Layout – Per-Measure Row Totals")
    st_pivot_table(
        df,
        key="test_pivot_hierarchy_totals",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        aggregation="sum",
        row_layout="hierarchy",
        show_totals=True,
        show_row_totals=["Revenue"],
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("Hierarchy Layout – Locked")
    st_pivot_table(
        df,
        key="test_pivot_hierarchy_locked",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        row_layout="hierarchy",
        show_subtotals=True,
        locked=True,
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
