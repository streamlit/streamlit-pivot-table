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

import pandas as pd  # type: ignore[import-untyped]
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


def test_auto_date_hierarchy_matches_month_bucket_oracle(pivot_module):
    df = pd.DataFrame(
        {
            "region": ["US", "US", "US", "EU", "EU"],
            "order_date": pd.to_datetime(
                ["2024-01-03", "2024-01-20", "2024-02-10", "2024-01-04", "2024-02-12"]
            ),
            "revenue": [100, 30, 150, 80, 95],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["region"],
        "columns": ["order_date"],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
        "synthetic_measures": [],
        "auto_date_hierarchy": True,
    }

    expected_df = df.copy()
    expected_df["order_date"] = expected_df["order_date"].map(
        lambda value: pivot_module._bucket_temporal_key(value, "date", "month")
    )
    expected = _pandas_client_oracle(expected_df, cfg)
    hybrid = pivot_module._prepare_threshold_hybrid_frame(
        df,
        cfg,
        column_types={"order_date": "date"},
    )

    pd.testing.assert_frame_equal(_sort_frame(hybrid), _sort_frame(expected))


# ---------------------------------------------------------------------------
# Adaptive date grain tests
# ---------------------------------------------------------------------------


class TestComputeAdaptiveDateGrain:
    """Unit tests for _compute_adaptive_date_grain threshold selection."""

    def test_5year_span_returns_year(self, pivot_module):
        dates = pd.to_datetime(["2019-01-01", "2024-06-15"])
        assert pivot_module._compute_adaptive_date_grain(dates) == "year"

    def test_18month_span_returns_quarter(self, pivot_module):
        dates = pd.to_datetime(["2023-01-01", "2024-06-30"])
        assert pivot_module._compute_adaptive_date_grain(dates) == "quarter"

    def test_12month_span_returns_month(self, pivot_module):
        dates = pd.to_datetime(["2024-01-01", "2024-12-31"])
        assert pivot_module._compute_adaptive_date_grain(dates) == "month"

    def test_4month_span_returns_month(self, pivot_module):
        dates = pd.to_datetime(["2024-03-01", "2024-06-30"])
        assert pivot_module._compute_adaptive_date_grain(dates) == "month"

    def test_1month_span_returns_day(self, pivot_module):
        dates = pd.to_datetime(["2024-06-01", "2024-06-28"])
        assert pivot_module._compute_adaptive_date_grain(dates) == "day"

    def test_10day_span_returns_day(self, pivot_module):
        dates = pd.to_datetime(["2024-06-01", "2024-06-10"])
        assert pivot_module._compute_adaptive_date_grain(dates) == "day"

    def test_empty_series_returns_default(self, pivot_module):
        dates = pd.Series(dtype="datetime64[ns]")
        assert pivot_module._compute_adaptive_date_grain(dates) == "month"

    def test_single_value_returns_day(self, pivot_module):
        dates = pd.to_datetime(["2024-06-15"])
        assert pivot_module._compute_adaptive_date_grain(dates) == "day"

    def test_week_never_returned(self, pivot_module):
        """Adaptive selector only returns drill-path grains, never week."""
        start = pd.Timestamp("2024-01-01")
        for days in [7, 14, 21, 28, 45, 55]:
            dates = pd.to_datetime([start, start + pd.Timedelta(days=days)])
            grain = pivot_module._compute_adaptive_date_grain(dates)
            assert grain != "week", f"span={days}d returned week"


class TestComputeAdaptiveDateGrains:
    """Tests for _compute_adaptive_date_grains map building."""

    def test_maps_multiple_date_columns(self, pivot_module):
        df = pd.DataFrame(
            {
                "order_date": pd.to_datetime(["2019-01-01", "2024-06-15"]),
                "ship_date": pd.to_datetime(["2024-06-01", "2024-06-10"]),
                "region": ["US", "EU"],
            }
        )
        column_types = {"order_date": "date", "ship_date": "date", "region": "string"}
        result = pivot_module._compute_adaptive_date_grains(df, column_types)
        assert result["order_date"] == "year"
        assert result["ship_date"] == "day"
        assert "region" not in result

    def test_skips_non_temporal_columns(self, pivot_module):
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 40]})
        column_types = {"name": "string", "age": "number"}
        result = pivot_module._compute_adaptive_date_grains(df, column_types)
        assert result == {}


class TestGetEffectiveDateGrainWithAdaptive:
    """Verify adaptive_grains parameter precedence in _get_effective_date_grain."""

    def test_adaptive_overrides_default(self, pivot_module):
        grain = pivot_module._get_effective_date_grain(
            "d",
            ["d"],
            [],
            None,
            True,
            {"d": "date"},
            adaptive_grains={"d": "year"},
        )
        assert grain == "year"

    def test_explicit_beats_adaptive(self, pivot_module):
        grain = pivot_module._get_effective_date_grain(
            "d",
            ["d"],
            [],
            {"d": "quarter"},
            True,
            {"d": "date"},
            adaptive_grains={"d": "year"},
        )
        assert grain == "quarter"

    def test_null_optout_beats_adaptive(self, pivot_module):
        grain = pivot_module._get_effective_date_grain(
            "d",
            ["d"],
            [],
            {"d": None},
            True,
            {"d": "date"},
            adaptive_grains={"d": "year"},
        )
        assert grain is None

    def test_auto_hierarchy_off_ignores_adaptive(self, pivot_module):
        grain = pivot_module._get_effective_date_grain(
            "d",
            ["d"],
            [],
            None,
            False,
            {"d": "date"},
            adaptive_grains={"d": "year"},
        )
        assert grain is None

    def test_missing_field_in_adaptive_falls_back_to_default(self, pivot_module):
        grain = pivot_module._get_effective_date_grain(
            "d",
            ["d"],
            [],
            None,
            True,
            {"d": "date"},
            adaptive_grains={"other": "year"},
        )
        assert grain == "month"


class TestAdaptiveHybridParity:
    """Verify hybrid pre-aggregation uses adaptive grains correctly."""

    def test_5year_adaptive_year_bucket(self, pivot_module):
        dates = pd.date_range("2019-01-01", "2024-12-31", freq="MS")
        df = pd.DataFrame(
            {
                "order_date": dates,
                "revenue": range(len(dates)),
            }
        )
        column_types = {"order_date": "date"}
        adaptive = pivot_module._compute_adaptive_date_grains(df, column_types)
        assert adaptive["order_date"] == "year"

        cfg = {
            "version": pivot_module.CONFIG_SCHEMA_VERSION,
            "rows": ["order_date"],
            "columns": [],
            "values": ["revenue"],
            "aggregation": {"revenue": "sum"},
            "synthetic_measures": [],
            "auto_date_hierarchy": True,
        }
        hybrid = pivot_module._prepare_threshold_hybrid_frame(
            df,
            cfg,
            column_types=column_types,
            adaptive_grains=adaptive,
        )
        assert all(k.startswith("20") and len(k) == 4 for k in hybrid["order_date"])

    def test_10day_adaptive_day_bucket(self, pivot_module):
        dates = pd.date_range("2024-06-01", "2024-06-10", freq="D")
        df = pd.DataFrame(
            {
                "order_date": dates,
                "revenue": range(len(dates)),
            }
        )
        column_types = {"order_date": "date"}
        adaptive = pivot_module._compute_adaptive_date_grains(df, column_types)
        assert adaptive["order_date"] == "day"

        cfg = {
            "version": pivot_module.CONFIG_SCHEMA_VERSION,
            "rows": ["order_date"],
            "columns": [],
            "values": ["revenue"],
            "aggregation": {"revenue": "sum"},
            "synthetic_measures": [],
            "auto_date_hierarchy": True,
        }
        hybrid = pivot_module._prepare_threshold_hybrid_frame(
            df,
            cfg,
            column_types=column_types,
            adaptive_grains=adaptive,
        )
        assert len(hybrid) == 10


class TestSidecarFingerprintWithAdaptive:
    """Verify adaptive grains are included in the sidecar fingerprint."""

    def test_different_adaptive_grains_produce_different_fingerprints(
        self, pivot_module
    ):
        cfg = {
            "version": 1,
            "rows": ["d"],
            "columns": [],
            "values": ["v"],
            "aggregation": {"v": "sum"},
        }
        fp1 = pivot_module._build_sidecar_fingerprint(
            cfg,
            None,
            adaptive_date_grains={"d": "month"},
        )
        fp2 = pivot_module._build_sidecar_fingerprint(
            cfg,
            None,
            adaptive_date_grains={"d": "year"},
        )
        assert fp1 != fp2

    def test_same_adaptive_grains_produce_same_fingerprint(self, pivot_module):
        cfg = {
            "version": 1,
            "rows": ["d"],
            "columns": [],
            "values": ["v"],
            "aggregation": {"v": "sum"},
        }
        fp1 = pivot_module._build_sidecar_fingerprint(
            cfg,
            None,
            adaptive_date_grains={"d": "month"},
        )
        fp2 = pivot_module._build_sidecar_fingerprint(
            cfg,
            None,
            adaptive_date_grains={"d": "month"},
        )
        assert fp1 == fp2


# ---------------------------------------------------------------------------
# Fix 1 — Golden parity tests (vectorised _compute_hybrid_totals vs fixtures)
# ---------------------------------------------------------------------------


class TestSidecarGoldenParity:
    """Assert the vectorised sidecar implementation matches pre-vectorisation fixtures.

    Fixtures were generated with `python - <<'EOF'...EOF` script against the
    original Python-loop implementation and saved to tests/golden_data/sidecar_golden.json.
    """

    @pytest.fixture(scope="class")
    def golden(self):
        import json
        import pathlib

        p = pathlib.Path(__file__).parent / "golden_data" / "sidecar_golden.json"
        return json.loads(p.read_text())

    def _normalise(self, obj):
        """Sort list entries by JSON key so ordering differences don't fail equality."""
        import json
        import math

        if isinstance(obj, dict):
            return {k: self._normalise(v) for k, v in obj.items()}
        if isinstance(obj, list):
            normalised = [self._normalise(v) for v in obj]
            try:
                return sorted(
                    normalised, key=lambda x: json.dumps(x, sort_keys=True, default=str)
                )
            except Exception:
                return normalised
        if isinstance(obj, float) and math.isnan(obj):
            return None
        return obj

    def _run(self, pm, df, cfg, null_handling=None, column_types=None):
        result = pm._compute_hybrid_totals(
            df, cfg, null_handling, column_types=column_types
        )
        if result is None:
            return None
        return {k: v for k, v in result.items() if k != "sidecar_fingerprint"}

    def _assert_parity(self, name, actual, golden):
        assert name in golden, f"No fixture for {name!r}"
        expected = golden[name]
        assert self._normalise(actual) == self._normalise(
            expected
        ), f"Parity failure for {name!r}"

    def test_rows_only_avg_separate(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU", None],
                "revenue": [100.0, 200.0, 50.0, 150.0, 75.0],
            }
        )
        cfg = {
            "rows": ["region"],
            "columns": [],
            "values": ["revenue"],
            "aggregation": {"revenue": "avg"},
            "synthetic_measures": [],
        }
        self._assert_parity(
            "rows_only_avg_separate",
            self._run(pivot_module, df, cfg, "separate"),
            golden,
        )

    def test_rows_only_count_distinct_zero(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU", None],
                "revenue": [100.0, 200.0, 50.0, 150.0, 75.0],
            }
        )
        cfg = {
            "rows": ["region"],
            "columns": [],
            "values": ["revenue"],
            "aggregation": {"revenue": "count_distinct"},
            "synthetic_measures": [],
        }
        self._assert_parity(
            "rows_only_count_distinct_zero",
            self._run(pivot_module, df, cfg, "zero"),
            golden,
        )

    def test_rows_only_median_exclude(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU", None],
                "revenue": [100.0, 200.0, 50.0, 150.0, 75.0],
            }
        )
        cfg = {
            "rows": ["region"],
            "columns": [],
            "values": ["revenue"],
            "aggregation": {"revenue": "median"},
            "synthetic_measures": [],
        }
        self._assert_parity(
            "rows_only_median_exclude",
            self._run(pivot_module, df, cfg, "exclude"),
            golden,
        )

    def test_rows_only_first_separate(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU", None],
                "revenue": [100.0, 200.0, 50.0, 150.0, 75.0],
            }
        )
        cfg = {
            "rows": ["region"],
            "columns": [],
            "values": ["revenue"],
            "aggregation": {"revenue": "first"},
            "synthetic_measures": [],
        }
        self._assert_parity(
            "rows_only_first_separate",
            self._run(pivot_module, df, cfg, "separate"),
            golden,
        )

    def test_rows_only_last_separate(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU", None],
                "revenue": [100.0, 200.0, 50.0, 150.0, 75.0],
            }
        )
        cfg = {
            "rows": ["region"],
            "columns": [],
            "values": ["revenue"],
            "aggregation": {"revenue": "last"},
            "synthetic_measures": [],
        }
        self._assert_parity(
            "rows_only_last_separate",
            self._run(pivot_module, df, cfg, "separate"),
            golden,
        )

    def test_rows_cols_avg_separate(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU"],
                "year": ["2023", "2024", "2023", "2024"],
                "revenue": [100.0, 150.0, 200.0, 250.0],
            }
        )
        cfg = {
            "rows": ["region"],
            "columns": ["year"],
            "values": ["revenue"],
            "aggregation": {"revenue": "avg"},
            "synthetic_measures": [],
        }
        self._assert_parity(
            "rows_cols_avg_separate",
            self._run(pivot_module, df, cfg, "separate"),
            golden,
        )

    def test_rows_cols_count_distinct_zero(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU"],
                "year": ["2023", "2024", "2023", "2024"],
                "revenue": [100.0, 150.0, 200.0, 250.0],
            }
        )
        cfg = {
            "rows": ["region"],
            "columns": ["year"],
            "values": ["revenue"],
            "aggregation": {"revenue": "count_distinct"},
            "synthetic_measures": [],
        }
        self._assert_parity(
            "rows_cols_count_distinct_zero",
            self._run(pivot_module, df, cfg, "zero"),
            golden,
        )

    def test_col_prefix_sidecar(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU"],
                "year": ["2023", "2023", "2024", "2024"],
                "quarter": ["Q1", "Q2", "Q1", "Q2"],
                "revenue": [100.0, 150.0, 200.0, 250.0],
            }
        )
        cfg = {
            "rows": ["region"],
            "columns": ["year", "quarter"],
            "values": ["revenue"],
            "aggregation": {"revenue": "avg"},
            "synthetic_measures": [],
        }
        self._assert_parity(
            "col_prefix_avg_separate",
            self._run(pivot_module, df, cfg, "separate"),
            golden,
        )

    def test_subtotals_3row_avg_separate(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US"] * 4 + ["EU"] * 4,
                "country": ["NY", "NY", "LA", "LA"] * 2,
                "city": ["NYC", "BKN"] * 4,
                "revenue": [100.0, 80.0, 120.0, 60.0, 200.0, 150.0, 90.0, 110.0],
            }
        )
        cfg = {
            "rows": ["region", "country", "city"],
            "columns": [],
            "values": ["revenue"],
            "aggregation": {"revenue": "avg"},
            "show_subtotals": True,
            "synthetic_measures": [],
        }
        self._assert_parity(
            "subtotals_3row_avg_separate",
            self._run(pivot_module, df, cfg, "separate"),
            golden,
        )

    def test_cross_subtotals_count_distinct_zero(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU"] * 3,
                "year": ["2023"] * 4 + ["2024"] * 4 + ["2023"] * 4,
                "cat": list("AABB") * 3,
                "revenue": [float(i) for i in range(12)],
            }
        )
        cfg = {
            "rows": ["region", "year"],
            "columns": ["cat"],
            "values": ["revenue"],
            "aggregation": {"revenue": "count_distinct"},
            "show_subtotals": True,
            "synthetic_measures": [],
        }
        self._assert_parity(
            "cross_subtotals_countdistinct_zero",
            self._run(pivot_module, df, cfg, "zero"),
            golden,
        )

    def test_special_column_names_avg_separate(self, pivot_module, golden):
        df = pd.DataFrame(
            {
                "my region": ["US", "US", "EU"],
                "my.value": [100.0, 200.0, 150.0],
            }
        )
        cfg = {
            "rows": ["my region"],
            "columns": [],
            "values": ["my.value"],
            "aggregation": {"my.value": "avg"},
            "synthetic_measures": [],
        }
        self._assert_parity(
            "special_col_names_avg_separate",
            self._run(pivot_module, df, cfg, "separate"),
            golden,
        )
