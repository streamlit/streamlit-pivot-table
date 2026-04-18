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

"""Streamlit app backing the column_config Playwright suite."""

from __future__ import annotations

import streamlit as st

from streamlit_pivot import st_pivot_table

from pivot_table_app_support import init_page, load_data, noop


def render_app(data):
    df = data["df"]

    st.subheader("column_config.label")
    st_pivot_table(
        df,
        key="test_pivot_cc_label",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Region": {"label": "Area"},
            "Revenue": {"label": "Rev"},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config.help")
    st_pivot_table(
        df,
        key="test_pivot_cc_help",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Region": {"help": "Geographic region"},
            "Revenue": {"help": "Revenue in USD"},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config.width (pixel)")
    st_pivot_table(
        df,
        key="test_pivot_cc_width_px",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Region": {"width": 180},
            "Revenue": {"width": 220},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config.width (preset)")
    st_pivot_table(
        df,
        key="test_pivot_cc_width_preset",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Region": {"width": "large"},
            "Revenue": {"width": "small"},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config.pinned (config-UI lock)")
    st_pivot_table(
        df,
        key="test_pivot_cc_pinned",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Region": {"pinned": True},
        },
        interactive=True,
        on_config_change=noop,
    )


def main():
    init_page()
    render_app(load_data())


if __name__ == "__main__":
    main()
