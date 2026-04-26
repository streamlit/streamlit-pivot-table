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

"""Streamlit app backing the Top N / Value Filter E2E Playwright suite."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_pivot import st_pivot_table

from pivot_table_app_support import handle_config, init_page


def _make_sales_df() -> pd.DataFrame:
    """Sales data: 3 regions × 4 products × 2 years."""
    rows = []
    for region in ["East", "West", "North"]:
        for product in ["Alpha", "Beta", "Gamma", "Delta"]:
            for year in ["2023", "2024"]:
                # Deterministic revenue: region_offset + product_offset + year_bump
                r_off = {"East": 0, "West": 1000, "North": 2000}[region]
                p_off = {"Alpha": 100, "Beta": 200, "Gamma": 50, "Delta": 300}[product]
                y_bump = 0 if year == "2023" else 50
                rows.append(
                    {
                        "Region": region,
                        "Product": product,
                        "Year": year,
                        "Revenue": r_off + p_off + y_bump,
                    }
                )
    return pd.DataFrame(rows)


def render_app():
    init_page()
    df = _make_sales_df()

    # ── 1. Top N via API ──────────────────────────────────────────────────────
    st.subheader("Top N Filter (API)")
    st_pivot_table(
        df,
        key="test_pivot_top_n",
        rows=["Region", "Product"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_subtotals=True,
        top_n_filters=[
            {
                "field": "Product",
                "n": 2,
                "by": "Revenue",
                "direction": "top",
                "axis": "rows",
            }
        ],
        on_config_change=handle_config,
    )

    # ── 2. Bottom N via API ───────────────────────────────────────────────────
    st.subheader("Bottom N Filter (API)")
    st_pivot_table(
        df,
        key="test_pivot_bottom_n",
        rows=["Region", "Product"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_subtotals=True,
        top_n_filters=[
            {
                "field": "Product",
                "n": 2,
                "by": "Revenue",
                "direction": "bottom",
                "axis": "rows",
            }
        ],
        on_config_change=handle_config,
    )

    # ── 3. Value Filter gte via API ───────────────────────────────────────────
    st.subheader("Value Filter (API)")
    st_pivot_table(
        df,
        key="test_pivot_value_filter",
        rows=["Region", "Product"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_subtotals=True,
        value_filters=[
            {
                "field": "Product",
                "by": "Revenue",
                "operator": "gte",
                "value": 500,
                "axis": "rows",
            }
        ],
        on_config_change=handle_config,
    )

    # ── 4. Top N via interactive header menu ──────────────────────────────────
    st.subheader("Top N Filter (interactive)")
    st_pivot_table(
        df,
        key="test_pivot_top_n_interactive",
        rows=["Product"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
        on_config_change=handle_config,
    )

    # ── 5. Value Filter via interactive header menu ───────────────────────────
    st.subheader("Value Filter (interactive)")
    st_pivot_table(
        df,
        key="test_pivot_value_filter_interactive",
        rows=["Product"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
        on_config_change=handle_config,
    )

    st.write(f"Config change count: {st.session_state.get('config_change_count', 0)}")


render_app()
