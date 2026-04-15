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

"""Streamlit app backing the smoke and toolbar Playwright suites."""

from __future__ import annotations

import numpy as np
import streamlit as st

from streamlit_pivot import st_pivot_table

from pivot_table_app_support import (
    handle_click,
    handle_config,
    init_page,
    load_data,
    noop,
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

    if "last_config_change" in st.session_state:
        st.subheader("Last config change")
        st.json(st.session_state["last_config_change"])

    if "last_cell_click" in st.session_state:
        st.subheader("Last cell click")
        st.json(st.session_state["last_cell_click"])

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

    st.subheader("Scalar Aggregation Roundtrip Pivot")
    if "scalar_roundtrip_aggregation" not in st.session_state:
        st.session_state["scalar_roundtrip_aggregation"] = "sum"

    st.button("Trigger scalar roundtrip rerun", key="scalar_roundtrip_rerun")
    if st.button("Set scalar aggregation to avg", key="scalar_roundtrip_set_avg"):
        st.session_state["scalar_roundtrip_aggregation"] = "avg"

    st.write(
        "Scalar roundtrip aggregation: "
        f"{st.session_state['scalar_roundtrip_aggregation']}"
    )

    st_pivot_table(
        df,
        key="test_pivot_scalar_roundtrip",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        aggregation=st.session_state["scalar_roundtrip_aggregation"],
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("NumPy list Pivot")
    st_pivot_table(
        df,
        key="test_pivot_numpy_list",
        rows=list(np.array(["Region"])),
        columns=list(np.array(["Year"])),
        values=list(np.array(["Revenue"])),
        aggregation="sum",
        interactive=True,
        on_config_change=noop,
    )


def main():
    init_page()
    render_app(load_data())


if __name__ == "__main__":
    main()
