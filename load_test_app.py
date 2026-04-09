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

"""Interactive load playground for large pivot-table datasets.

Run locally:
    uv run streamlit run load_test_app.py
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from math import prod
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd  # type: ignore[import-untyped]
import streamlit as st

APP_ROOT = str(Path(__file__).resolve().parent)
if APP_ROOT in sys.path:
    sys.path.remove(APP_ROOT)

from streamlit_pivot import PivotConfig, SortConfig, st_pivot_table  # noqa: E402


@dataclass(frozen=True)
class Profile:
    description: str
    cardinalities: dict[str, int]
    default_rows: tuple[str, ...]
    default_columns: tuple[str, ...]
    default_values: tuple[str, ...]


PROFILES: dict[str, Profile] = {
    "balanced": Profile(
        description="Reasonable grouping depth with a moderate result grid.",
        cardinalities={
            "Region": 8,
            "Country": 40,
            "City": 500,
            "Segment": 6,
            "Channel": 4,
            "Product": 150,
            "Year": 5,
            "Quarter": 4,
            "Month": 12,
        },
        default_rows=("Region", "Country"),
        default_columns=("Year", "Quarter"),
        default_values=("Revenue", "Profit"),
    ),
    "high_row_cardinality": Profile(
        description="Many distinct row groups. Useful for finding when row expansion gets heavy.",
        cardinalities={
            "Region": 12,
            "Country": 120,
            "City": 6000,
            "Segment": 8,
            "Channel": 5,
            "Product": 200,
            "Year": 5,
            "Quarter": 4,
            "Month": 12,
        },
        default_rows=("Country", "City"),
        default_columns=("Year",),
        default_values=("Revenue",),
    ),
    "wide_columns": Profile(
        description="Intentionally wide pivot. Useful for seeing column truncation and virtualization.",
        cardinalities={
            "Region": 8,
            "Country": 40,
            "City": 500,
            "Segment": 6,
            "Channel": 8,
            "Product": 150,
            "Year": 10,
            "Quarter": 4,
            "Month": 12,
        },
        default_rows=("Region",),
        default_columns=("Year", "Month", "Channel"),
        default_values=("Revenue",),
    ),
    "stress_mix": Profile(
        description="High-cardinality rows plus a wide column layout.",
        cardinalities={
            "Region": 20,
            "Country": 200,
            "City": 10000,
            "Segment": 10,
            "Channel": 8,
            "Product": 250,
            "Year": 10,
            "Quarter": 4,
            "Month": 12,
        },
        default_rows=("Country", "City"),
        default_columns=("Year", "Quarter", "Channel"),
        default_values=("Revenue", "Profit"),
    ),
}

ROW_OPTIONS = {
    "10k": 10_000,
    "50k": 50_000,
    "100k": 100_000,
    "250k": 250_000,
    "500k": 500_000,
    "1M": 1_000_000,
}
DIMENSION_FIELDS = [
    "Region",
    "Country",
    "City",
    "Segment",
    "Channel",
    "Product",
    "Year",
    "Quarter",
    "Month",
]
VALUE_FIELDS = ["Revenue", "Profit", "Units"]

ROW_LABEL_KEY = "load_playground_row_label"
PROFILE_NAME_KEY = "load_playground_profile_name"
SEED_KEY = "load_playground_seed"
ROWS_KEY = "load_playground_rows"
COLUMNS_KEY = "load_playground_columns"
VALUES_KEY = "load_playground_values"
HEIGHT_KEY = "load_playground_height"
SUBTOTALS_KEY = "load_playground_show_subtotals"
TOTALS_KEY = "load_playground_show_totals"
EXECUTION_MODE_KEY = "load_playground_execution_mode"
LAST_SYNCED_CONFIG_KEY = "_load_playground_last_synced_config_json"
PIVOT_WIDGET_KEY = "load_playground_pivot"


def _make_categories(prefix: str, count: int) -> list[str]:
    width = len(str(max(count - 1, 0)))
    return [f"{prefix}_{i:0{width}d}" for i in range(count)]


def _make_categorical_series(
    prefix: str,
    count: int,
    n_rows: int,
    rng: np.random.Generator,
) -> pd.Categorical:
    codes = rng.integers(0, count, size=n_rows)
    return pd.Categorical.from_codes(codes, categories=_make_categories(prefix, count))


@st.cache_data(show_spinner=False, max_entries=16)
def generate_dataset(n_rows: int, profile_name: str, seed: int) -> pd.DataFrame:
    profile = PROFILES[profile_name]
    rng = np.random.default_rng(seed)
    card = profile.cardinalities

    revenue = rng.gamma(shape=4.5, scale=220.0, size=n_rows)
    margin = np.clip(rng.normal(loc=0.22, scale=0.08, size=n_rows), 0.02, 0.65)
    units = rng.integers(1, 40, size=n_rows)

    return pd.DataFrame(
        {
            "Region": _make_categorical_series("Region", card["Region"], n_rows, rng),
            "Country": _make_categorical_series(
                "Country", card["Country"], n_rows, rng
            ),
            "City": _make_categorical_series("City", card["City"], n_rows, rng),
            "Segment": _make_categorical_series(
                "Segment", card["Segment"], n_rows, rng
            ),
            "Channel": _make_categorical_series(
                "Channel", card["Channel"], n_rows, rng
            ),
            "Product": _make_categorical_series(
                "Product", card["Product"], n_rows, rng
            ),
            "Year": _make_categorical_series("Year", card["Year"], n_rows, rng),
            "Quarter": _make_categorical_series(
                "Quarter", card["Quarter"], n_rows, rng
            ),
            "Month": _make_categorical_series("Month", card["Month"], n_rows, rng),
            "Revenue": revenue.round(2),
            "Profit": (revenue * margin).round(2),
            "Units": units.astype("int16"),
        }
    )


def estimate_groups(
    n_rows: int, selected_fields: list[str], cardinalities: dict[str, int]
) -> int:
    if not selected_fields:
        return 1
    return min(n_rows, prod(cardinalities[field] for field in selected_fields))


def _sync_profile_defaults() -> None:
    default_row_label = os.getenv("LOAD_TEST_ROWS", next(iter(ROW_OPTIONS)))
    default_profile_name = os.getenv("LOAD_TEST_PROFILE", next(iter(PROFILES)))
    default_execution_mode = os.getenv("LOAD_TEST_EXECUTION_MODE", "auto")
    if default_row_label not in ROW_OPTIONS:
        default_row_label = next(iter(ROW_OPTIONS))
    if default_profile_name not in PROFILES:
        default_profile_name = next(iter(PROFILES))
    if default_execution_mode not in {"auto", "client_only", "threshold_hybrid"}:
        default_execution_mode = "auto"

    profile_name = st.session_state.get(PROFILE_NAME_KEY, default_profile_name)
    profile = PROFILES[profile_name]

    st.session_state.setdefault(ROW_LABEL_KEY, default_row_label)
    st.session_state.setdefault(PROFILE_NAME_KEY, profile_name)
    st.session_state.setdefault(SEED_KEY, 42)
    st.session_state.setdefault(ROWS_KEY, list(profile.default_rows))
    st.session_state.setdefault(COLUMNS_KEY, list(profile.default_columns))
    st.session_state.setdefault(VALUES_KEY, list(profile.default_values))
    st.session_state.setdefault(HEIGHT_KEY, 560)
    st.session_state.setdefault(SUBTOTALS_KEY, False)
    st.session_state.setdefault(TOTALS_KEY, True)
    st.session_state.setdefault(EXECUTION_MODE_KEY, default_execution_mode)


def _reset_layout_to_profile_defaults() -> None:
    profile = PROFILES[st.session_state[PROFILE_NAME_KEY]]
    st.session_state[ROWS_KEY] = list(profile.default_rows)
    st.session_state[COLUMNS_KEY] = list(profile.default_columns)
    st.session_state[VALUES_KEY] = list(profile.default_values)
    st.session_state.pop(PIVOT_WIDGET_KEY, None)
    st.session_state.pop(LAST_SYNCED_CONFIG_KEY, None)


def _sync_sidebar_from_pivot_state() -> None:
    pivot_state = st.session_state.get(PIVOT_WIDGET_KEY, {})
    pivot_config = pivot_state.get("config")
    if not pivot_config:
        return

    config_json = json.dumps(pivot_config, sort_keys=True)
    if config_json == st.session_state.get(LAST_SYNCED_CONFIG_KEY):
        return

    st.session_state[ROWS_KEY] = list(pivot_config.get("rows", []))
    st.session_state[COLUMNS_KEY] = list(pivot_config.get("columns", []))
    st.session_state[VALUES_KEY] = list(pivot_config.get("values", []))
    st.session_state[SUBTOTALS_KEY] = bool(pivot_config.get("show_subtotals"))
    st.session_state[TOTALS_KEY] = bool(pivot_config.get("show_totals", True))
    st.session_state[LAST_SYNCED_CONFIG_KEY] = config_json


def _get_pivot_config() -> dict[str, object]:
    pivot_state = st.session_state.get(PIVOT_WIDGET_KEY, {})
    pivot_config = pivot_state.get("config")
    if isinstance(pivot_config, dict):
        return pivot_config
    return {}


st.set_page_config(page_title="Pivot Table Load Playground", layout="wide")
st.title("Pivot Table Load Playground")
st.caption(
    "Use this app to interactively test larger datasets. Raw row count matters, "
    "but the bigger limit is how many unique row and column groups the pivot has to render."
)

_sync_profile_defaults()
_sync_sidebar_from_pivot_state()

with st.sidebar:
    st.header("Dataset")
    row_label = st.radio("Rows", list(ROW_OPTIONS), key=ROW_LABEL_KEY)
    profile_name = st.selectbox(
        "Shape preset",
        list(PROFILES),
        key=PROFILE_NAME_KEY,
        format_func=lambda name: name.replace("_", " ").title(),
        on_change=_reset_layout_to_profile_defaults,
    )
    seed = st.number_input("Seed", min_value=1, max_value=9999, step=1, key=SEED_KEY)

    profile = PROFILES[profile_name]
    st.caption(profile.description)

    st.header("Pivot Setup")
    selected_rows = st.multiselect(
        "Row dimensions",
        DIMENSION_FIELDS,
        key=ROWS_KEY,
    )
    selected_columns = st.multiselect(
        "Column dimensions",
        DIMENSION_FIELDS,
        key=COLUMNS_KEY,
    )
    selected_values = st.multiselect(
        "Measures",
        VALUE_FIELDS,
        key=VALUES_KEY,
    )
    table_height = st.slider(
        "Table height", min_value=300, max_value=900, step=20, key=HEIGHT_KEY
    )
    show_subtotals = st.checkbox("Show subtotals", key=SUBTOTALS_KEY)
    show_totals = st.checkbox("Show grand totals", key=TOTALS_KEY)
    execution_mode = st.selectbox(
        "Execution mode",
        ["auto", "client_only", "threshold_hybrid"],
        key=EXECUTION_MODE_KEY,
    )

if not selected_values:
    st.error("Select at least one measure.")
    st.stop()

n_rows = ROW_OPTIONS[row_label]
df = generate_dataset(n_rows, profile_name, int(seed))
cardinalities = PROFILES[profile_name].cardinalities

estimated_row_groups = estimate_groups(n_rows, selected_rows, cardinalities)
estimated_col_groups = estimate_groups(n_rows, selected_columns, cardinalities)
estimated_visible_cells = (
    estimated_row_groups * min(estimated_col_groups, 200) * len(selected_values)
)
memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

metric_cols = st.columns(4)
metric_cols[0].metric("Rows", f"{n_rows:,}")
metric_cols[1].metric("Approx. row groups", f"{estimated_row_groups:,}")
metric_cols[2].metric("Approx. column groups", f"{estimated_col_groups:,}")
metric_cols[3].metric("DataFrame memory", f"{memory_mb:.1f} MB")

if estimated_col_groups > 200:
    st.warning(
        "This setup likely exceeds the 200-column cardinality budget, so the UI may "
        "truncate visible columns."
    )

if estimated_visible_cells > 5_000:
    st.warning(
        "This setup likely exceeds the 5,000 visible-cell budget, so virtualization "
        "should kick in."
    )

with st.expander("Dataset profile", expanded=False):
    st.write(
        {
            "shape_preset": profile_name,
            "row_dimensions": selected_rows,
            "column_dimensions": selected_columns,
            "measures": selected_values,
            "field_cardinalities": cardinalities,
        }
    )

pivot_config = cast(PivotConfig, _get_pivot_config())
synthetic_measures = cast(
    list[dict[str, Any]] | None, pivot_config.get("synthetic_measures")
)
aggregation = cast(
    str | dict[str, str],
    pivot_config.get("aggregation", {field: "sum" for field in selected_values}),
)
show_row_totals = cast(bool | list[str] | None, pivot_config.get("show_row_totals"))
show_column_totals = cast(
    bool | list[str] | None, pivot_config.get("show_column_totals")
)
row_sort = cast(SortConfig | None, pivot_config.get("row_sort"))
col_sort = cast(SortConfig | None, pivot_config.get("col_sort"))
show_subtotals_config = cast(
    bool | list[str], pivot_config.get("show_subtotals", show_subtotals)
)
show_values_as = cast(dict[str, str] | None, pivot_config.get("show_values_as"))
conditional_formatting = cast(
    list[dict[str, Any]] | None, pivot_config.get("conditional_formatting")
)
number_format = cast(str | dict[str, str] | None, pivot_config.get("number_format"))
column_alignment = cast(dict[str, str] | None, pivot_config.get("column_alignment"))

result = st_pivot_table(
    df,
    key=PIVOT_WIDGET_KEY,
    rows=selected_rows,
    columns=selected_columns,
    values=selected_values,
    synthetic_measures=synthetic_measures,
    aggregation=aggregation,
    show_totals=bool(pivot_config.get("show_totals", show_totals)),
    show_row_totals=show_row_totals,
    show_column_totals=show_column_totals,
    row_sort=row_sort,
    col_sort=col_sort,
    sticky_headers=bool(pivot_config.get("sticky_headers", True)),
    show_subtotals=show_subtotals_config,
    repeat_row_labels=bool(pivot_config.get("repeat_row_labels", False)),
    show_values_as=show_values_as,
    conditional_formatting=conditional_formatting,
    number_format=number_format,
    column_alignment=column_alignment,
    interactive=True,
    height=table_height,
    enable_drilldown=True,
    execution_mode=execution_mode,
)

with st.expander("Current config", expanded=False):
    st.json(result["config"])

perf_metrics = cast(
    dict[str, Any] | None,
    st.session_state.get(PIVOT_WIDGET_KEY, {}).get("perf_metrics"),
)
if perf_metrics:
    comparison_row = {
        "rows": n_rows,
        "shape": profile_name,
        "row_groups": estimated_row_groups,
        "col_groups": estimated_col_groups,
        "visible_cells": estimated_visible_cells,
        "first_render_ms": perf_metrics.get("firstMountMs"),
        "sort_ms": (
            perf_metrics.get("lastAction", {}).get("elapsedMs")
            if perf_metrics.get("lastAction", {}).get("kind") == "sort"
            else None
        ),
        "filter_ms": (
            perf_metrics.get("lastAction", {}).get("elapsedMs")
            if perf_metrics.get("lastAction", {}).get("kind") == "filter"
            else None
        ),
        "drilldown_ms": (
            perf_metrics.get("lastAction", {}).get("elapsedMs")
            if perf_metrics.get("lastAction", {}).get("kind") == "drilldown"
            else None
        ),
        "virtualized": perf_metrics.get("needsVirtualization"),
        "truncated": perf_metrics.get("columnsTruncated"),
        "warnings": len(perf_metrics.get("warnings", [])),
        "execution_mode": perf_metrics.get("executionMode"),
    }
    with st.expander("Current performance metrics", expanded=True):
        st.json(perf_metrics)
    with st.expander("Comparison row", expanded=True):
        st.write(comparison_row)
        st.text_area(
            "COMPARISON_ROW_JSON",
            value=json.dumps(comparison_row, sort_keys=True),
            height=160,
            disabled=True,
        )
