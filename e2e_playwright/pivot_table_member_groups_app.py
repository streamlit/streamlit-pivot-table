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

"""Minimal Streamlit app backing member-groups E2E tests (0.6.0).

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
            "Region": [
                "Northeast",
                "Northeast",
                "Southeast",
                "Southeast",
                "West",
                "West",
            ],
            "Category": ["A", "B", "A", "B", "A", "B"],
            "Revenue": [100, 120, 150, 80, 200, 60],
        }
    )


def main() -> None:
    st.title("Pivot Table E2E Test App")

    df = _make_data()

    # ── Static group set in Python ────────────────────────────────────────────
    st.subheader("Static member_groups")
    st_pivot_table(
        df,
        key="test_pivot_member_groups_static",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=False,
        member_groups=[
            {
                "field": "Region",
                "name": "East Coast",
                "members": ["Northeast", "Southeast"],
            }
        ],
    )

    # ── Interactive pivot (no initial groups) ─────────────────────────────────
    st.subheader("Interactive – no initial groups")
    st_pivot_table(
        df,
        key="test_pivot_member_groups_interactive",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
    )

    # ── Interactive pivot with an initial group (for ungroup test) ────────────
    st.subheader("Interactive – with initial group (for ungroup test)")
    st_pivot_table(
        df,
        key="test_pivot_member_groups_ungroup",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
        member_groups=[
            {
                "field": "Region",
                "name": "East Coast",
                "members": ["Northeast", "Southeast"],
            }
        ],
    )

    # ── Drilldown-enabled pivot with groups ───────────────────────────────────
    st.subheader("Drilldown – East Coast group")
    st_pivot_table(
        df,
        key="test_pivot_member_groups_drilldown",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
        enable_drilldown=True,
        member_groups=[
            {
                "field": "Region",
                "name": "East Coast",
                "members": ["Northeast", "Southeast"],
            }
        ],
    )


if __name__ == "__main__":
    main()
else:
    main()
