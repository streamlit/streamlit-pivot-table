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

from __future__ import annotations

import os
from dataclasses import dataclass
from math import prod

import numpy as np
import pandas as pd
import streamlit as st

from streamlit_pivot import st_pivot_table


@dataclass(frozen=True)
class Profile:
    cardinalities: dict[str, int]
    rows: tuple[str, ...]
    columns: tuple[str, ...]
    values: tuple[str, ...]


PROFILES: dict[str, Profile] = {
    "balanced": Profile(
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
        rows=("Region", "Country"),
        columns=("Year", "Quarter"),
        values=("Revenue", "Profit"),
    ),
    "high_row_cardinality": Profile(
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
        rows=("Country", "City"),
        columns=("Year",),
        values=("Revenue",),
    ),
    "wide_columns": Profile(
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
        rows=("Region",),
        columns=("Year", "Month", "Channel"),
        values=("Revenue",),
    ),
}


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


def generate_dataset(n_rows: int, profile_name: str, seed: int = 42) -> pd.DataFrame:
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
    n_rows: int, selected_fields: tuple[str, ...], cardinalities: dict[str, int]
) -> int:
    if not selected_fields:
        return 1
    return min(n_rows, prod(cardinalities[field] for field in selected_fields))


def main() -> None:
    row_count = int(os.getenv("PERF_ROWS", "100000"))
    profile_name = os.getenv("PERF_PROFILE", "balanced")
    execution_mode = os.getenv("LOAD_TEST_EXECUTION_MODE", "auto")
    if profile_name not in PROFILES:
        raise ValueError(f"Unknown PERF_PROFILE: {profile_name}")

    profile = PROFILES[profile_name]
    df = generate_dataset(row_count, profile_name)
    row_groups = estimate_groups(row_count, profile.rows, profile.cardinalities)
    col_groups = estimate_groups(row_count, profile.columns, profile.cardinalities)
    visible_cells = row_groups * min(col_groups, 200) * len(profile.values)
    memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)

    st.set_page_config(page_title="Pivot Table Perf App", layout="wide")
    st.title("Pivot Table Perf App")
    metric_cols = st.columns(4)
    metric_cols[0].metric("Rows", f"{row_count:,}")
    metric_cols[1].metric("Approx. row groups", f"{row_groups:,}")
    metric_cols[2].metric("Approx. column groups", f"{col_groups:,}")
    metric_cols[3].metric("DataFrame memory", f"{memory_mb:.1f} MB")
    st.caption(
        f"profile={profile_name} execution_mode={execution_mode} visible_cells={visible_cells:,}"
    )
    st.text(f"VISIBLE_CELLS={visible_cells}")

    st_pivot_table(
        df,
        key="load_playground_pivot",
        rows=list(profile.rows),
        columns=list(profile.columns),
        values=list(profile.values),
        aggregation="sum",
        interactive=True,
        show_totals=True,
        enable_drilldown=True,
        execution_mode=execution_mode,
    )


if __name__ == "__main__":
    main()
