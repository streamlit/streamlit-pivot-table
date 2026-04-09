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

"""Unit tests for threshold_hybrid helpers (aggregation, thresholds, pre-aggregation)."""

import numpy as np
import pandas as pd
import pytest


def test_can_use_threshold_hybrid_accepts_avg(pivot_module):
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["a"],
        "columns": ["b"],
        "values": ["v"],
        "aggregation": {"v": "avg"},
        "synthetic_measures": [],
    }
    ok, _msg = pivot_module._can_use_threshold_hybrid(cfg)
    assert ok is True


def test_prepare_threshold_hybrid_frame_avg_matches_pandas_groupby(pivot_module):
    df = pd.DataFrame(
        {
            "r": ["x", "x", "y", "y", "y"],
            "c": [1, 1, 1, 1, 1],
            "v": [10.0, 20.0, 1.0, 3.0, 5.0],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["r"],
        "columns": ["c"],
        "values": ["v"],
        "aggregation": {"v": "avg"},
        "synthetic_measures": [],
    }
    got = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    expected = (
        df.groupby(["r", "c"], dropna=False, observed=True)["v"].mean().reset_index()
    )
    pd.testing.assert_frame_equal(
        got.sort_values(["r", "c"]).reset_index(drop=True),
        expected.sort_values(["r", "c"]).reset_index(drop=True),
    )


def test_should_use_threshold_hybrid_forced_bypasses_row_threshold(pivot_module):
    tiny = pd.DataFrame({"a": [1], "b": [2], "v": [3.0]})
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["a"],
        "columns": ["b"],
        "values": ["v"],
        "aggregation": {"v": "sum"},
        "synthetic_measures": [],
    }
    use, reason = pivot_module._should_use_threshold_hybrid(
        tiny, cfg, "threshold_hybrid"
    )
    assert use is True
    assert "threshold_hybrid" in reason
    assert "not applied" in reason


def test_should_use_threshold_hybrid_auto_100k_high_cardinality(pivot_module):
    rng = np.random.default_rng(0)
    n = 100_000
    df = pd.DataFrame(
        {
            "r": rng.integers(0, 100, size=n),
            "c": rng.integers(0, 101, size=n),
            "v": rng.standard_normal(n),
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["r"],
        "columns": ["c"],
        "values": ["v"],
        "aggregation": {"v": "sum"},
        "synthetic_measures": [],
    }
    use, reason = pivot_module._should_use_threshold_hybrid(df, cfg, "auto")
    assert use is True
    assert "100,000" in reason or "100000" in reason


@pytest.mark.parametrize(
    "rows,cols,expected_max_rows",
    [
        (["r"], ["c"], 100 * 101),
    ],
)
def test_prepare_threshold_hybrid_frame_shape_preaggregated(
    pivot_module, rows, cols, expected_max_rows
):
    rng = np.random.default_rng(1)
    n = 50_000
    df = pd.DataFrame(
        {
            "r": rng.integers(0, 100, size=n),
            "c": rng.integers(0, 101, size=n),
            "v": rng.standard_normal(n),
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": rows,
        "columns": cols,
        "values": ["v"],
        "aggregation": {"v": "avg"},
        "synthetic_measures": [],
    }
    got = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    assert len(got) <= expected_max_rows
    assert list(got.columns) == ["r", "c", "v"]
