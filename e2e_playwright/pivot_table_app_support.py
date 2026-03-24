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

"""Small shared helpers for Playwright Streamlit app scripts."""

from __future__ import annotations

from pathlib import Path

import streamlit as st
import pandas as pd

_DATA_DIR = Path(__file__).parent.parent / "tests" / "golden_data"


def init_page() -> None:
    """Render the common E2E app shell and rerun counter."""
    st.set_page_config(page_title="Pivot Table E2E Tests", layout="wide")
    st.title("Pivot Table E2E Test App")

    if "rerun_count" not in st.session_state:
        st.session_state["rerun_count"] = 0
    st.session_state["rerun_count"] += 1
    st.write(f"Reruns: {st.session_state['rerun_count']}")


def load_data() -> dict[str, pd.DataFrame]:
    """Load shared fixture dataframes for the E2E apps."""
    df = pd.read_csv(_DATA_DIR / "small.csv")
    df_single = pd.read_csv(_DATA_DIR / "edge_single_row.csv")
    df_nulls = pd.read_csv(_DATA_DIR / "edge_nulls.csv")
    sparse_df = pd.DataFrame(
        {
            "Region": ["North", "North", "South"],
            "Year": [2023, 2024, 2023],
            "Revenue": [1000, 2000, 3000],
        }
    )
    df_synth = pd.DataFrame(
        {
            "Region": ["East", "East", "West", "West", "North"],
            "Year": [2023, 2024, 2023, 2024, 2024],
            "Total PRs": [20, 30, 10, 25, 5],
            "People": [5, 0, 2, 5, 1],
        }
    )
    return {
        "df": df,
        "df_single": df_single,
        "df_nulls": df_nulls,
        "sparse_df": sparse_df,
        "df_synth": df_synth,
    }


def noop() -> None:
    """No-op callback used by interactive test pivots."""


def handle_click() -> None:
    """Track primary pivot cell clicks in session state."""
    click_data = st.session_state.get("test_pivot", {}).get("cell_click")
    st.session_state["last_cell_click"] = click_data
    st.session_state["cell_click_count"] = (
        st.session_state.get("cell_click_count", 0) + 1
    )


def handle_config() -> None:
    """Track primary pivot config changes in session state."""
    config_data = st.session_state.get("test_pivot", {}).get("config")
    st.session_state["last_config_change"] = config_data
    st.session_state["config_change_count"] = (
        st.session_state.get("config_change_count", 0) + 1
    )
