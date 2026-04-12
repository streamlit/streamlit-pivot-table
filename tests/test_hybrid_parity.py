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

"""Oracle-backed parity: threshold_hybrid pre-aggregation matches pandas on raw rows."""

from __future__ import annotations

import pandas as pd
import pytest


def _sort_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.reset_index(drop=True)
    cols = [c for c in df.columns]
    return df.sort_values(cols).reset_index(drop=True)


def _pandas_client_oracle(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Expected pre-aggregated frame from grouping raw rows (same as client-side aggregation)."""
    group_fields = [*cfg.get("rows", []), *cfg.get("columns", [])]
    value_fields = list(cfg.get("values", []))
    aggregation = dict(cfg.get("aggregation", {}))
    if not value_fields:
        if not group_fields:
            return df.iloc[0:0].copy()
        return pd.DataFrame(columns=group_fields)

    if not group_fields:
        row: dict = {}
        for vf in value_fields:
            agg = aggregation.get(vf, "sum")
            ser = df[vf]
            if agg == "avg":
                cnt = int(ser.count())
                row[vf] = float(ser.sum() / cnt) if cnt else float("nan")
            elif agg == "count_distinct":
                row[vf] = ser.nunique()
            elif agg == "median":
                row[vf] = ser.median()
            elif agg == "percentile_90":
                row[vf] = ser.quantile(0.9)
            elif agg == "first":
                numeric = pd.to_numeric(ser, errors="coerce").dropna()
                row[vf] = numeric.iloc[0] if len(numeric) > 0 else float("nan")
            elif agg == "last":
                numeric = pd.to_numeric(ser, errors="coerce").dropna()
                row[vf] = numeric.iloc[-1] if len(numeric) > 0 else float("nan")
            else:
                row[vf] = ser.agg(agg)
        return pd.DataFrame([row])

    parts = []
    for vf in value_fields:
        agg = aggregation.get(vf, "sum")
        g = df.groupby(group_fields, dropna=False, observed=True)[vf]
        if agg == "avg":
            s = g.mean()
        elif agg == "count":
            s = g.count()
        elif agg == "count_distinct":
            s = g.nunique()
        elif agg == "median":
            s = g.median()
        elif agg == "percentile_90":
            s = g.quantile(0.9)
        elif agg == "first":
            s = g.first()
        elif agg == "last":
            s = g.last()
        else:
            s = getattr(g, agg)()
        parts.append(s.rename(vf))
    out = pd.concat(parts, axis=1).reset_index()
    return out


def _assert_hybrid_and_client_match_pandas(
    pivot_module, df: pd.DataFrame, cfg: dict
) -> None:
    expected = _pandas_client_oracle(df, cfg)
    hybrid = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    client_oracle = _pandas_client_oracle(df, cfg)

    pd.testing.assert_frame_equal(_sort_frame(hybrid), _sort_frame(expected))
    pd.testing.assert_frame_equal(_sort_frame(client_oracle), _sort_frame(expected))
    use_hybrid, _ = pivot_module._should_use_threshold_hybrid(
        df, cfg, "threshold_hybrid"
    )
    assert use_hybrid is True
    use_client, _ = pivot_module._should_use_threshold_hybrid(df, cfg, "client_only")
    assert use_client is False


@pytest.mark.parametrize(
    "agg",
    ["sum", "count", "min", "max", "avg", "median", "count_distinct", "first", "last"],
)
def test_hybrid_and_client_oracle_match_pandas_per_hybrid_aggregator(
    pivot_module, agg: str
):
    df = pd.DataFrame(
        {
            "r": ["a", "a", "b", "b", "b"],
            "c": [1, 1, 1, 2, 2],
            "v": [10.0, 30.0, 5.0, 15.0, 25.0],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["r"],
        "columns": ["c"],
        "values": ["v"],
        "aggregation": {"v": agg},
        "synthetic_measures": [],
    }
    _assert_hybrid_and_client_match_pandas(pivot_module, df, cfg)


def test_avg_uses_weighted_mean_not_mean_of_subgroup_means(pivot_module):
    # One pivot cell (c=1) contains 101 rows: one 0 and one hundred 100s.
    # Weighted mean = 10000/101; mean of (0,) and (100,...,100) group means would be 50.
    df = pd.DataFrame(
        {
            "batch": ["A"] + ["B"] * 100,
            "c": [1] * 101,
            "v": [0.0] + [100.0] * 100,
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": [],
        "columns": ["c"],
        "values": ["v"],
        "aggregation": {"v": "avg"},
        "synthetic_measures": [],
    }
    weighted = (0.0 + 100.0 * 100) / 101
    mean_of_means = (0.0 + 100.0) / 2
    assert weighted != mean_of_means

    expected = _pandas_client_oracle(df, cfg)
    assert len(expected) == 1
    assert abs(float(expected["v"].iloc[0]) - weighted) < 1e-9

    hybrid = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    assert abs(float(hybrid["v"].iloc[0]) - weighted) < 1e-9
    pd.testing.assert_frame_equal(
        _sort_frame(hybrid), _sort_frame(expected), check_dtype=False
    )
