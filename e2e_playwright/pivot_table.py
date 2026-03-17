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

"""E2E test app for the pivot table component.

Run with: streamlit run e2e_playwright/pivot_table.py

Renders multiple pivot table instances, each pre-configured to exercise a
specific feature area.  Tests locate the right instance by scrolling to its
anchor heading.
"""

from pathlib import Path

import pandas as pd
from streamlit_pivot_table import st_pivot_table

import streamlit as st

_DATA_DIR = Path(__file__).parent.parent / "tests" / "golden_data"

st.set_page_config(page_title="Pivot Table E2E Tests", layout="wide")
st.title("Pivot Table E2E Test App")

if "rerun_count" not in st.session_state:
    st.session_state["rerun_count"] = 0
st.session_state["rerun_count"] += 1
st.write(f"Reruns: {st.session_state['rerun_count']}")

df = pd.read_csv(_DATA_DIR / "small.csv")
df_single = pd.read_csv(_DATA_DIR / "edge_single_row.csv")


# ---------------------------------------------------------------------------
# Callbacks
# ---------------------------------------------------------------------------


def handle_click():
    click_data = st.session_state.get("test_pivot", {}).get("cell_click")
    st.session_state["last_cell_click"] = click_data
    st.session_state["cell_click_count"] = (
        st.session_state.get("cell_click_count", 0) + 1
    )


def handle_config():
    config_data = st.session_state.get("test_pivot", {}).get("config")
    st.session_state["last_config_change"] = config_data
    st.session_state["config_change_count"] = (
        st.session_state.get("config_change_count", 0) + 1
    )


# ---------------------------------------------------------------------------
# 1. Primary interactive pivot (used by most existing + new toolbar tests)
# ---------------------------------------------------------------------------
st.subheader("Primary Pivot")

result = st_pivot_table(
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

if "last_config_change" in st.session_state:
    st.subheader("Last config change")
    st.json(st.session_state["last_config_change"])

if "last_cell_click" in st.session_state:
    st.subheader("Last cell click")
    st.json(st.session_state["last_cell_click"])

st.button("Trigger rerun", key="rerun_trigger")


# ---------------------------------------------------------------------------
# 2. Subtotals pivot (2 row dims)
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 3. Locked mode
# ---------------------------------------------------------------------------
st.subheader("Locked Pivot")

st_pivot_table(
    df,
    key="test_pivot_locked",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    aggregation="sum",
    locked=True,
    interactive=True,
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 4. Conditional formatting -- color scale + data bars
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 5. Number formatting
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 6. Drilldown
# ---------------------------------------------------------------------------
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
    on_cell_click=lambda: None,
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 7. Empty dataframe
# ---------------------------------------------------------------------------
st.subheader("Empty Pivot")

st_pivot_table(
    pd.DataFrame(),
    key="test_pivot_empty",
)


# ---------------------------------------------------------------------------
# 8. Single-row dataset
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 9. No column dimension
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 10. Count distinct aggregation
# ---------------------------------------------------------------------------
st.subheader("Count Distinct Pivot")

st_pivot_table(
    df,
    key="test_pivot_count_distinct",
    rows=["Region"],
    columns=["Year"],
    values=["Category"],
    aggregation="count_distinct",
    interactive=True,
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 11. Median aggregation
# ---------------------------------------------------------------------------
st.subheader("Median Pivot")

st_pivot_table(
    df,
    key="test_pivot_median",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    aggregation="median",
    interactive=True,
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 13. Auto-detect (no rows/columns/values specified)
# ---------------------------------------------------------------------------
st.subheader("Auto Detect Pivot")

st_pivot_table(
    df,
    key="test_pivot_auto",
    interactive=True,
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 14. Threshold conditional formatting
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 15. Column group collapse (2 column dims)
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 16. Column alignment + empty cell display
# ---------------------------------------------------------------------------
st.subheader("Alignment Pivot")

sparse_df = pd.DataFrame(
    {
        "Region": ["North", "North", "South"],
        "Year": [2023, 2024, 2023],
        "Revenue": [1000, 2000, 3000],
    }
)

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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 17. Tall pivot for sticky header test
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 18. Null handling -- "separate" mode (null dims become "(null)" bucket)
# ---------------------------------------------------------------------------
st.subheader("Null Handling Pivot")

df_nulls = pd.read_csv(_DATA_DIR / "edge_nulls.csv")

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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 19. Null handling -- "zero" mode (nulls treated as 0)
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 20. Dimension-level collapse toggles (3 row dims + 2 col dims)
# ---------------------------------------------------------------------------
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
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 21. Drilldown disabled
# ---------------------------------------------------------------------------
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
    on_cell_click=lambda: None,
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 22. Per-attribute totals tests
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# 23. Sparse data with drilldown (for empty-cell non-interactive tests)
# ---------------------------------------------------------------------------
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
    on_cell_click=lambda: None,
    on_config_change=lambda: None,
)


# ---------------------------------------------------------------------------
# 24. Synthetic measures (V1)
# ---------------------------------------------------------------------------
st.subheader("Synthetic Measures Pivot")

df_synth = pd.DataFrame(
    {
        "Region": ["East", "East", "West", "West", "North"],
        "Year": [2023, 2024, 2023, 2024, 2024],
        "Total PRs": [20, 30, 10, 25, 5],
        "People": [5, 0, 2, 5, 1],
    }
)

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
    on_config_change=lambda: None,
)
