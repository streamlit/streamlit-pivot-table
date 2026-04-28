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

"""Minimal Streamlit app backing new-feature E2E tests (0.5.0+).

Kept deliberately small so it does not inflate the shared interactions app
and cause timeout regressions on CI.
"""

from __future__ import annotations

import pandas as pd  # type: ignore[import-untyped]
import streamlit as st

from streamlit_pivot import st_pivot_table


def _make_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Region": ["East", "East", "West", "West"],
            "Category": ["A", "B", "A", "B"],
            "Revenue": [100, 150, 200, 250],
        }
    )


def _make_tied_data() -> pd.DataFrame:
    """Both members of each Region group have identical Revenue.

    The secondary sort key is therefore the only thing that determines
    their relative order — perfect for testing tie-breaking.
    """
    return pd.DataFrame(
        {
            "Region": ["East", "East", "West", "West"],
            "Category": ["A", "B", "A", "B"],
            "Revenue": [100, 100, 200, 200],
        }
    )


def main() -> None:
    st.title("Pivot Table E2E Test App")

    df = _make_data()
    tied = _make_tied_data()

    # ── Analytical show_values_as: running_total ─────────────────────────────
    st.subheader("Analytical – Running Total")
    st_pivot_table(
        df,
        key="test_pivot_running_total",
        rows=["Region", "Category"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        show_subtotals=True,
        show_values_as={"Revenue": "running_total"},
        number_format={"Revenue": ",.0f"},
        interactive=False,
    )

    # ── Analytical show_values_as: rank ─────────────────────────────────────
    st.subheader("Analytical – Rank")
    st_pivot_table(
        df,
        key="test_pivot_rank",
        rows=["Region"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=False,
        show_values_as={"Revenue": "rank"},
        interactive=False,
    )

    # ── Multi-field sort: secondary key asc (A before B) ────────────────────
    st.subheader("Multi-Sort – secondary key asc")
    st_pivot_table(
        tied,
        key="test_pivot_multi_sort_asc",
        rows=["Region", "Category"],
        values=["Revenue"],
        aggregation="sum",
        row_sort=[
            {"by": "value", "direction": "desc"},
            {"by": "key", "direction": "asc"},
        ],
        interactive=False,
    )

    # ── Multi-field sort: secondary key desc (B before A) ───────────────────
    st.subheader("Multi-Sort – secondary key desc")
    st_pivot_table(
        tied,
        key="test_pivot_multi_sort_desc",
        rows=["Region", "Category"],
        values=["Revenue"],
        aggregation="sum",
        row_sort=[
            {"by": "value", "direction": "desc"},
            {"by": "key", "direction": "desc"},
        ],
        interactive=False,
    )


if __name__ == "__main__":
    main()
