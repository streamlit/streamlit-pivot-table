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

"""Streamlit app backing the styling API Playwright test suite.

Each pivot here corresponds to a test in pivot_table_styling_test.py.
Uses deterministic rgb() colors so computed-style assertions are reliable.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from streamlit_pivot import PivotStyle, RegionStyle, st_pivot_table

from pivot_table_app_support import init_page

# Deterministic test data with two measures and two dimensions.
_DF = pd.DataFrame(
    {
        "Region": ["East", "East", "West", "West"],
        "Category": ["A", "B", "A", "B"],
        "Revenue": [100, 150, 200, 250],
        "Profit": [10, 20, 30, 40],
    }
)

_SHARED = dict(
    rows=["Region"],
    columns=["Category"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    show_totals=True,
    interactive=False,
)


def render_app() -> None:
    init_page()

    # 1. Baseline: style=None → no --pivot-* vars on wrapper
    st.subheader("style_none")
    st_pivot_table(_DF, key="style_none", **_SHARED, style=None)

    # 2. Custom background_color → --pivot-bg on wrapper
    st.subheader("style_custom_bg")
    st_pivot_table(
        _DF,
        key="style_custom_bg",
        **_SHARED,
        style=PivotStyle(background_color="rgb(200, 100, 50)"),
    )

    # 3. density="compact" → densityCompact class on wrapper
    st.subheader("style_density_compact")
    st_pivot_table(
        _DF,
        key="style_density_compact",
        **_SHARED,
        style=PivotStyle(density="compact"),
    )

    # 4. borders="rows" → bordersRows class on wrapper
    st.subheader("style_borders_rows")
    st_pivot_table(
        _DF,
        key="style_borders_rows",
        **_SHARED,
        style=PivotStyle(borders="rows"),
    )

    # 5. stripe_color=None → stripesOff class on wrapper
    st.subheader("style_stripes_off")
    st_pivot_table(
        _DF,
        key="style_stripes_off",
        **_SHARED,
        style=PivotStyle(stripe_color=None),
    )

    # 6. row_hover_color=None → hoverOff class on wrapper
    st.subheader("style_hover_off")
    st_pivot_table(
        _DF,
        key="style_hover_off",
        **_SHARED,
        style=PivotStyle(row_hover_color=None),
    )

    # 7. column_header region override → --pivot-column-header-bg on wrapper
    st.subheader("style_column_header_bg")
    st_pivot_table(
        _DF,
        key="style_column_header_bg",
        **_SHARED,
        style=PivotStyle(column_header=RegionStyle(background_color="rgb(10, 20, 30)")),
    )

    # 8. row_total region → --pivot-row-total-bg (not --pivot-column-total-bg)
    st.subheader("style_row_total_mapping")
    st_pivot_table(
        _DF,
        key="style_row_total_mapping",
        **_SHARED,
        style=PivotStyle(row_total=RegionStyle(background_color="rgb(0, 0, 200)")),
    )

    # 9. column_total region → --pivot-column-total-bg (not --pivot-row-total-bg)
    st.subheader("style_column_total_mapping")
    st_pivot_table(
        _DF,
        key="style_column_total_mapping",
        **_SHARED,
        style=PivotStyle(column_total=RegionStyle(background_color="rgb(0, 200, 0)")),
    )

    # 10 & 11. data_cell_by_measure: Revenue cells get inline style;
    # row-total and column-total cells for Revenue must NOT get it.
    st.subheader("style_per_measure")
    st_pivot_table(
        _DF,
        key="style_per_measure",
        **_SHARED,
        style=PivotStyle(
            data_cell_by_measure={
                "Revenue": RegionStyle(background_color="rgb(255, 0, 128)")
            }
        ),
    )

    # 12. CF wins over per-measure: color_scale overrides the red per-measure bg.
    st.subheader("style_cf_precedence")
    st_pivot_table(
        _DF,
        key="style_cf_precedence",
        **_SHARED,
        conditional_formatting=[
            {
                "type": "color_scale",
                "apply_to": ["Revenue"],
                "min_color": "rgb(0,200,0)",
                "max_color": "rgb(0,200,0)",
            }
        ],
        style=PivotStyle(
            data_cell_by_measure={
                "Revenue": RegionStyle(background_color="rgb(255, 0, 0)")
            }
        ),
    )

    # 13. Composition: list of presets + overrides
    st.subheader("style_composition")
    st_pivot_table(
        _DF,
        key="style_composition",
        **_SHARED,
        style=[
            "compact",
            PivotStyle(
                background_color="rgb(240, 240, 255)",
                borders="rows",
            ),
        ],
    )

    # 14. vertical_align: row_header top-aligned
    st.subheader("style_vertical_align_row_header")
    st_pivot_table(
        _DF,
        key="style_vertical_align_row_header",
        **_SHARED,
        style=PivotStyle(row_header=RegionStyle(vertical_align="top")),
    )


render_app()
