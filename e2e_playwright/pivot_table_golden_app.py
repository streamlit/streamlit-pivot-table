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

"""Streamlit app for golden verification E2E tests.

Renders a set of pivot table configurations using small.csv so that
Playwright tests can verify rendered cell values match pandas-computed
golden expected values.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from streamlit_pivot import st_pivot_table
from pivot_table_app_support import init_page

_DATA_DIR = Path(__file__).parent.parent / "tests" / "golden_data"


def render_app():
    df = pd.read_csv(_DATA_DIR / "small.csv")

    st.subheader("Config A — Basic Sum")
    st_pivot_table(
        df,
        key="golden_a",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=False,
    )

    st.subheader("Config C — Per-measure Agg")
    st_pivot_table(
        df,
        key="golden_c",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue", "Units"],
        aggregation={"Revenue": "sum", "Units": "avg"},
        show_totals=True,
        interactive=False,
    )

    st.subheader("Config E — Subtotals")
    st_pivot_table(
        df,
        key="golden_e",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_subtotals=True,
        interactive=False,
    )

    st.subheader("Config F — Pct of Total")
    st_pivot_table(
        df,
        key="golden_f",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_values_as={"Revenue": "pct_of_total"},
        interactive=False,
    )

    st.subheader("Config F2 — Pct of Row")
    st_pivot_table(
        df,
        key="golden_f2",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_values_as={"Revenue": "pct_of_row"},
        interactive=False,
    )

    st.subheader("Config H — Synthetic sum_over_sum")
    st_pivot_table(
        df,
        key="golden_h",
        rows=["Region"],
        columns=[],
        values=["Revenue", "Units"],
        aggregation={"Revenue": "sum", "Units": "sum"},
        synthetic_measures=[
            {
                "id": "rev_per_unit",
                "label": "Rev/Unit",
                "operation": "sum_over_sum",
                "numerator": "Revenue",
                "denominator": "Units",
            }
        ],
        show_totals=True,
        interactive=False,
    )

    st.subheader("Config F3 — Pct of Col")
    st_pivot_table(
        df,
        key="golden_f3",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_values_as={"Revenue": "pct_of_col"},
        interactive=False,
    )

    st.subheader("No Totals")
    st_pivot_table(
        df,
        key="golden_no_totals",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=False,
        interactive=False,
    )

    st.subheader("Config A — Export Test")
    st_pivot_table(
        df,
        key="golden_export",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
        export_filename="golden_export",
    )

    st.subheader("Config VA — Values Axis Rows")
    st_pivot_table(
        df,
        key="golden_va",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue", "Units"],
        aggregation={"Revenue": "sum", "Units": "sum"},
        show_totals=True,
        values_axis="rows",
        interactive=False,
    )


if __name__ == "__main__" or True:
    init_page()
    render_app()
