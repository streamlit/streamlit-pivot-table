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

import pandas as pd  # type: ignore[import-untyped]
import streamlit as st

from streamlit_pivot import st_pivot_table

from pivot_table_app_support import init_page, load_data, noop


def _renderer_fixture() -> pd.DataFrame:
    """Small fixture exercising Tier 2 cell renderers.

    Each row has a URL-like, image-like, boolean, and long-text dim value,
    plus a numeric measure so measure cells render through the normal path.
    """
    return pd.DataFrame(
        {
            "Region": ["North", "North", "South", "South"],
            "Website": [
                "https://example.com/n1",
                "https://example.com/n2",
                "https://example.com/s1",
                "https://example.com/s2",
            ],
            "Logo": [
                "https://placehold.co/40x40?text=N1",
                "https://placehold.co/40x40?text=N2",
                "https://placehold.co/40x40?text=S1",
                "https://placehold.co/40x40?text=S2",
            ],
            "Active": [True, False, True, False],
            "Note": [
                "This is a long note that should be truncated with ellipsis.",
                "Another long piece of text that exceeds the configured max.",
                "Short",
                "Medium length note here.",
            ],
            "Revenue": [100.0, 150.0, 200.0, 250.0],
        }
    )


def render_app(data):
    df = data["df"]

    # No col dims here so the single-value measure header (`pivot-header-cell`)
    # renders and can be inspected by the measure label/help assertions. The
    # col-dim + single-value layout does not surface a dedicated measure header
    # (values fold into the grid cells), which would render the measure-level
    # label/help assertions unreachable.
    st.subheader("column_config.label")
    st_pivot_table(
        df,
        key="test_pivot_cc_label",
        rows=["Region"],
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

    # ------------------------------------------------------------------
    # Tier 2 cell renderers (LinkColumn / ImageColumn / CheckboxColumn /
    # TextColumn.max_chars). Each pivot uses a dedicated fixture so the
    # dim field being exercised is a first-class row dimension.
    # ------------------------------------------------------------------
    renderer_df = _renderer_fixture()

    st.subheader("column_config.type=link")
    st_pivot_table(
        renderer_df,
        key="test_pivot_cc_link",
        rows=["Website"],
        columns=[],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Website": {"type": "link", "display_text": "Visit {}"},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config.type=image")
    st_pivot_table(
        renderer_df,
        key="test_pivot_cc_image",
        rows=["Logo"],
        columns=[],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Logo": {"type": "image"},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config.type=checkbox")
    st_pivot_table(
        renderer_df,
        key="test_pivot_cc_checkbox",
        rows=["Active"],
        columns=[],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Active": {"type": "checkbox"},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config.type=text max_chars")
    st_pivot_table(
        renderer_df,
        key="test_pivot_cc_text_max",
        rows=["Note"],
        columns=[],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Note": {"type": "text", "max_chars": 12},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config renderers + Subtotal/Total safety")
    st_pivot_table(
        renderer_df,
        key="test_pivot_cc_renderer_totals",
        rows=["Region", "Website"],
        columns=[],
        values=["Revenue"],
        aggregation="sum",
        show_subtotals=True,
        show_totals=True,
        column_config={
            "Website": {"type": "link", "display_text": "Open"},
        },
        interactive=True,
        on_config_change=noop,
    )

    # ------------------------------------------------------------------
    # column_config.help propagation: column-dimension header cells
    # ------------------------------------------------------------------
    sparse_df = data["sparse_df"]

    st.subheader("column_config.help – single col dim (slot headers)")
    st_pivot_table(
        sparse_df,
        key="test_pivot_cc_help_col_dim_single",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Year": {"help": "Fiscal year"},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config.help – 2-col-dim col-dim-label")
    _two_col_df = pd.DataFrame(
        {
            "Region": ["North", "North", "South", "South"],
            "Year": [2023, 2023, 2024, 2024],
            "Quarter": ["Q1", "Q2", "Q1", "Q2"],
            "Revenue": [100.0, 150.0, 200.0, 250.0],
        }
    )
    st_pivot_table(
        _two_col_df,
        key="test_pivot_cc_help_col_dim_label",
        rows=["Region"],
        columns=["Year", "Quarter"],
        values=["Revenue"],
        aggregation="sum",
        column_config={
            "Year": {"help": "Fiscal year"},
        },
        interactive=True,
        on_config_change=noop,
    )

    st.subheader("column_config.help – temporal parent headers")
    _temporal_df = pd.DataFrame(
        {
            "Region": ["North", "North", "South", "South"],
            "OrderDate": pd.to_datetime(
                ["2023-03-10", "2023-09-20", "2024-02-14", "2024-11-05"]
            ),
            "Revenue": [100.0, 150.0, 200.0, 250.0],
        }
    )
    st_pivot_table(
        _temporal_df,
        key="test_pivot_cc_help_temporal",
        rows=["Region"],
        columns=["OrderDate"],
        values=["Revenue"],
        aggregation="sum",
        auto_date_hierarchy=True,
        column_config={
            "OrderDate": {"help": "Date of order"},
        },
        interactive=True,
        on_config_change=noop,
    )


def main():
    init_page()
    render_app(load_data())


if __name__ == "__main__":
    main()
