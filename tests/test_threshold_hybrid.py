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


def test_non_dimension_filter_causes_fallback(pivot_module):
    """Filter on a field not in rows/columns makes hybrid incompatible."""
    cfg = {
        "rows": ["region"],
        "columns": ["year"],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
        "filters": {"category": {"include": ["A"]}},
    }
    ok, msg = pivot_module._can_use_threshold_hybrid(cfg)
    assert ok is False
    assert "category" in msg


def test_dimension_filter_allows_hybrid(pivot_module):
    """Filter on a row/column dimension is compatible with hybrid."""
    cfg = {
        "rows": ["region"],
        "columns": ["year"],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
        "filters": {"region": {"include": ["US"]}},
    }
    ok, _msg = pivot_module._can_use_threshold_hybrid(cfg)
    assert ok is True


def test_no_filters_allows_hybrid(pivot_module):
    """No filters at all is compatible with hybrid."""
    cfg = {
        "rows": ["region"],
        "columns": ["year"],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
    }
    ok, _msg = pivot_module._can_use_threshold_hybrid(cfg)
    assert ok is True


def test_apply_source_filters_none_is_noop(pivot_module):
    df = pd.DataFrame({"region": ["US", "EU"], "revenue": [100, 200]})
    result = pivot_module._apply_source_filters(df, None)
    pd.testing.assert_frame_equal(result, df)


def test_apply_source_filters_empty_dict_is_noop(pivot_module):
    df = pd.DataFrame({"region": ["US", "EU"], "revenue": [100, 200]})
    result = pivot_module._apply_source_filters(df, {})
    pd.testing.assert_frame_equal(result, df)


def test_apply_source_filters_missing_column_raises_clear_error(pivot_module):
    df = pd.DataFrame({"region": ["US", "EU"], "revenue": [100, 200]})
    with pytest.raises(
        ValueError, match="source_filters contains column not in DataFrame"
    ):
        pivot_module._apply_source_filters(df, {"missing": {"include": ["x"]}})


def test_apply_source_filters_include_and_exclude(pivot_module):
    df = pd.DataFrame(
        {
            "region": ["US", "US", "EU", "EU"],
            "category": ["A", "B", "A", "B"],
        }
    )
    include_only = pivot_module._apply_source_filters(
        df, {"category": {"include": ["A"]}}
    )
    exclude_only = pivot_module._apply_source_filters(
        df, {"category": {"exclude": ["B"]}}
    )
    assert list(include_only["category"]) == ["A", "A"]
    assert list(exclude_only["category"]) == ["A", "A"]


def test_apply_source_filters_none_matches_null_but_not_empty_string(pivot_module):
    df = pd.DataFrame(
        {
            "category": ["A", "", None, np.nan],
            "revenue": [1, 2, 3, 4],
        }
    )
    null_rows = pivot_module._apply_source_filters(
        df, {"category": {"include": [None]}}
    )
    empty_rows = pivot_module._apply_source_filters(df, {"category": {"include": [""]}})
    assert sorted(null_rows["revenue"].tolist()) == [3, 4]
    assert empty_rows["revenue"].tolist() == [2]


def test_apply_source_filters_exclude_none_removes_null_rows(pivot_module):
    df = pd.DataFrame(
        {
            "category": ["A", "", None, np.nan],
            "revenue": [1, 2, 3, 4],
        }
    )
    result = pivot_module._apply_source_filters(df, {"category": {"exclude": [None]}})
    assert result["revenue"].tolist() == [1, 2]


def test_apply_source_filters_include_takes_precedence_over_exclude(pivot_module):
    df = pd.DataFrame({"region": ["US", "EU"], "revenue": [100, 200]})
    result = pivot_module._apply_source_filters(
        df,
        {"region": {"include": ["US"], "exclude": ["US"]}},
    )
    assert result["region"].tolist() == ["US"]


def test_apply_source_filters_do_not_coerce_types(pivot_module):
    df = pd.DataFrame({"year": ["2023", "2024"], "revenue": [100, 200]})
    result = pivot_module._apply_source_filters(df, {"year": {"include": [2023]}})
    assert result.empty


def test_apply_source_filters_and_config_filters_intersect(pivot_module):
    df = pd.DataFrame({"region": ["US", "EU"], "revenue": [100, 200]})
    prefiltered = pivot_module._apply_source_filters(
        df, {"region": {"include": ["US"]}}
    )
    narrowed = pivot_module._resolve_and_filter(
        prefiltered,
        {"region": {"exclude": ["US"]}},
        null_handling=None,
    )
    assert narrowed.empty


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


def test_prepare_threshold_hybrid_frame_groups_dates_by_month(pivot_module):
    df = pd.DataFrame(
        {
            "order_date": pd.to_datetime(
                ["2024-01-03", "2024-01-18", "2024-02-05", "2024-02-20"]
            ),
            "region": ["US", "EU", "US", "EU"],
            "revenue": [100, 80, 150, 95],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["region"],
        "columns": ["order_date"],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
        "date_grains": {"order_date": "month"},
        "synthetic_measures": [],
    }
    got = pivot_module._prepare_threshold_hybrid_frame(
        df,
        cfg,
        column_types={"order_date": "date"},
    )
    expected = pd.DataFrame(
        {
            "region": ["EU", "EU", "US", "US"],
            "order_date": ["2024-01", "2024-02", "2024-01", "2024-02"],
            "revenue": [80, 95, 100, 150],
        }
    )
    pd.testing.assert_frame_equal(
        got.sort_values(["region", "order_date"]).reset_index(drop=True),
        expected.sort_values(["region", "order_date"]).reset_index(drop=True),
    )


def test_compute_hybrid_drilldown_filters_grouped_date_bucket(pivot_module):
    df = pd.DataFrame(
        {
            "order_date": pd.to_datetime(
                ["2024-01-03", "2024-01-18", "2024-02-05", "2024-02-20"]
            ),
            "region": ["US", "EU", "US", "EU"],
            "revenue": [100, 80, 150, 95],
        }
    )
    records, columns, total, page = pivot_module._compute_hybrid_drilldown(
        df,
        {"filters": {"order_date": "2024-01"}, "page": 0},
        dims=["region", "order_date"],
        column_types={"order_date": "date"},
        date_grains={"order_date": "month"},
    )
    assert total == 2
    assert page == 0
    assert columns == ["order_date", "region", "revenue"]
    assert {record["region"] for record in records} == {"US", "EU"}
    assert {str(record["order_date"])[:10] for record in records} == {
        "2024-01-03",
        "2024-01-18",
    }


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


# ---- Hybrid drill-down tests ----


class TestComputeHybridDrilldown:
    """Tests for _compute_hybrid_drilldown (server-side drill-down filtering)."""

    @pytest.fixture
    def df(self):
        return pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU", "US"],
                "year": ["2023", "2024", "2023", "2024", "2023"],
                "revenue": [100, 150, 200, 250, 50],
                "profit": [40, 60, 80, 100, 20],
            }
        )

    def test_single_filter_returns_matching_rows(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"region": "US"}}
        )
        assert total == 3
        assert len(records) == 3
        assert page == 0
        assert all(r["region"] == "US" for r in records)

    def test_multi_filter_returns_intersection(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"region": "US", "year": "2023"}}
        )
        assert total == 2
        assert len(records) == 2
        revenues = sorted(r["revenue"] for r in records)
        assert revenues == [50, 100]

    def test_columns_include_all_df_columns(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"region": "EU"}}
        )
        assert columns == ["region", "year", "revenue", "profit"]

    def test_empty_filters_returns_all_rows(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {}}
        )
        assert total == 5
        assert len(records) == 5

    def test_no_match_returns_empty(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"region": "APAC"}}
        )
        assert total == 0
        assert len(records) == 0

    def test_page_size_caps_returned_records(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"region": "US"}}, page_size=2
        )
        assert total == 3
        assert len(records) == 2
        assert page == 0

    def test_page_1_returns_remaining_records(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"region": "US"}, "page": 1}, page_size=2
        )
        assert total == 3
        assert len(records) == 1
        assert page == 1

    def test_page_beyond_end_returns_empty(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"region": "US"}, "page": 99}, page_size=2
        )
        assert total == 3
        assert len(records) == 0
        assert page == 99

    def test_negative_page_clamps_to_zero(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"region": "US"}, "page": -5}, page_size=2
        )
        assert page == 0
        assert len(records) == 2

    def test_null_sentinel_matches_nan_rows_separate_mode(self, pivot_module):
        """With null_handling='separate', '(null)' filter matches NaN rows."""
        df = pd.DataFrame(
            {
                "region": ["US", None, "EU", None],
                "revenue": [100, 200, 300, 400],
            }
        )
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"region": "(null)"}},
            null_handling="separate",
            dims=["region"],
        )
        assert total == 2
        assert len(records) == 2
        revenues = sorted(r["revenue"] for r in records)
        assert revenues == [200, 400]

    def test_null_filter_empty_string_matches_nan_rows_exclude_mode(self, pivot_module):
        """With null_handling='exclude' (default), '' filter matches NaN rows."""
        df = pd.DataFrame(
            {
                "region": ["US", None, "EU", None],
                "revenue": [100, 200, 300, 400],
            }
        )
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"region": ""}},
            null_handling="exclude",
            dims=["region"],
        )
        assert total == 2
        assert len(records) == 2

    def test_separate_mode_empty_string_also_resolves_to_null_sentinel(
        self, pivot_module
    ):
        """Empty strings resolve to '(null)' in separate mode, matching NaN."""
        df = pd.DataFrame(
            {
                "region": ["US", "", "EU", None],
                "revenue": [100, 200, 300, 400],
            }
        )
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"region": "(null)"}},
            null_handling="separate",
            dims=["region"],
        )
        assert total == 2
        assert len(records) == 2
        revenues = sorted(r["revenue"] for r in records)
        assert revenues == [200, 400]

    def test_numeric_dimension_filter_matches_via_string_coercion(self, pivot_module):
        df = pd.DataFrame(
            {
                "year": [2023, 2023, 2024],
                "revenue": [100, 50, 200],
            }
        )
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"year": "2023"}}, dims=["year"]
        )
        assert total == 2
        assert len(records) == 2

    def test_unknown_filter_column_is_ignored(self, pivot_module, df):
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df, {"filters": {"nonexistent": "value", "region": "US"}}
        )
        assert total == 3

    def test_non_dim_column_uses_per_field_null_mode(self, pivot_module):
        """All fields use per-field _get_null_mode, matching frontend _resolveDimValue."""
        df = pd.DataFrame(
            {
                "region": ["US", "EU"],
                "revenue": [100, None],
            }
        )
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"revenue": "(null)"}},
            null_handling="separate",
            dims=["region"],
        )
        assert total == 1
        assert records[0]["region"] == "EU"

    def test_non_dim_column_exclude_mode_uses_empty_string(self, pivot_module):
        """With null_handling='exclude', null non-dim values resolve to ''."""
        df = pd.DataFrame(
            {
                "region": ["US", "EU"],
                "revenue": [100, None],
            }
        )
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"revenue": ""}},
            null_handling="exclude",
            dims=["region"],
        )
        assert total == 1
        assert records[0]["region"] == "EU"

    def test_config_filters_intersect_cell_filters(self, pivot_module, df):
        """Config filter 'region: include US' + cell click 'year: 2023' = only US+2023 rows."""
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"year": "2023"}},
            null_handling=None,
            dims=["region", "year"],
            config_filters={"region": {"include": ["US"]}},
        )
        assert total == 2
        assert all(r["region"] == "US" for r in records)
        assert all(r["year"] == "2023" for r in records)

    def test_config_exclude_filter_in_drilldown(self, pivot_module, df):
        """Config filter 'region: exclude EU' removes EU from drilldown."""
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {}},
            config_filters={"region": {"exclude": ["EU"]}},
        )
        assert total == 3
        assert all(r["region"] == "US" for r in records)

    def test_config_filter_separate_mode_null_in_drilldown(self, pivot_module):
        """Config filter + null_handling='separate' correctly filters '(null)'."""
        df = pd.DataFrame(
            {
                "region": ["US", None, "EU", None],
                "revenue": [100, 200, 300, 400],
            }
        )
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {}},
            null_handling="separate",
            dims=["region"],
            config_filters={"region": {"include": ["(null)"]}},
        )
        assert total == 2
        revenues = sorted(r["revenue"] for r in records)
        assert revenues == [200, 400]

    def test_config_filters_none_is_noop(self, pivot_module, df):
        """config_filters=None should not affect results."""
        records_with, _, total_with, _ = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"region": "US"}},
            config_filters=None,
        )
        records_without, _, total_without, _ = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"region": "US"}},
        )
        assert total_with == total_without


# ---- New aggregation support tests ----


@pytest.mark.parametrize(
    "agg", ["median", "percentile_90", "count_distinct", "first", "last"]
)
def test_prepare_threshold_hybrid_frame_new_aggs(pivot_module, agg):
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
        "aggregation": {"v": agg},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    assert len(result) > 0
    assert "v" in result.columns

    expected = df.groupby(["r", "c"], dropna=False, observed=True)["v"]
    if agg == "median":
        oracle = expected.median().reset_index()
    elif agg == "percentile_90":
        oracle = expected.quantile(0.9).reset_index()
    elif agg == "count_distinct":
        oracle = expected.nunique().reset_index()
    elif agg == "first":
        oracle = expected.first().reset_index()
    elif agg == "last":
        oracle = expected.last().reset_index()
    else:
        oracle = expected.agg(agg).reset_index()

    for _, row in result.iterrows():
        expected_val = oracle[(oracle["r"] == row["r"]) & (oracle["c"] == row["c"])][
            "v"
        ].values[0]
        assert (
            abs(float(row["v"]) - float(expected_val)) < 1e-9
        ), f"Mismatch for r={row['r']}, c={row['c']}"


def test_prepare_threshold_hybrid_frame_uses_sort_false(pivot_module):
    """sort=False preserves original order for first/last."""
    df = pd.DataFrame(
        {
            "r": ["b", "a", "b", "a"],
            "v": [10.0, 20.0, 30.0, 40.0],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["r"],
        "columns": [],
        "values": ["v"],
        "aggregation": {"v": "first"},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    b_val = result[result["r"] == "b"]["v"].values[0]
    assert b_val == 10.0


def test_build_hybrid_agg_remap(pivot_module):
    remap = pivot_module._build_hybrid_agg_remap(
        {"v1": "count", "v2": "sum", "v3": "count_distinct"}
    )
    assert remap == {"v1": "sum", "v3": "sum"}
    assert "v2" not in remap


def test_build_hybrid_agg_remap_empty_for_decomposable(pivot_module):
    remap = pivot_module._build_hybrid_agg_remap({"v": "sum", "w": "min", "x": "max"})
    assert remap == {}


# ---- Numeric coercion parity tests ----


@pytest.mark.parametrize(
    "agg", ["sum", "avg", "min", "max", "median", "percentile_90", "first", "last"]
)
def test_prepare_hybrid_frame_coerces_non_numeric_strings(pivot_module, agg):
    """Numeric aggs must drop non-numeric strings, matching frontend toNumber()."""
    df = pd.DataFrame(
        {
            "r": ["x", "x", "x"],
            "v": ["10", "abc", "30"],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["r"],
        "columns": [],
        "values": ["v"],
        "aggregation": {"v": agg},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    val = result["v"].values[0]
    if agg == "sum":
        assert float(val) == 40.0
    elif agg == "avg":
        assert float(val) == 20.0
    elif agg == "min":
        assert float(val) == 10.0
    elif agg == "max":
        assert float(val) == 30.0
    elif agg == "median":
        assert float(val) == 20.0
    elif agg == "percentile_90":
        assert float(val) >= 28.0
    elif agg == "first":
        assert float(val) == 10.0
    elif agg == "last":
        assert float(val) == 30.0


def test_prepare_hybrid_frame_count_does_not_coerce(pivot_module):
    """count should count all non-null values, even non-numeric strings."""
    df = pd.DataFrame(
        {
            "r": ["x", "x", "x"],
            "v": ["abc", "def", None],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["r"],
        "columns": [],
        "values": ["v"],
        "aggregation": {"v": "count"},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    assert int(result["v"].values[0]) == 2


def test_prepare_hybrid_frame_count_distinct_does_not_coerce(pivot_module):
    """count_distinct should count distinct non-null values, including strings."""
    df = pd.DataFrame(
        {
            "r": ["x", "x", "x", "x"],
            "v": ["abc", "abc", "def", None],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["r"],
        "columns": [],
        "values": ["v"],
        "aggregation": {"v": "count_distinct"},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    assert int(result["v"].values[0]) == 2


def test_prepare_hybrid_frame_no_groups_coerces_non_numeric(pivot_module):
    """No-group path should also coerce non-numeric strings for numeric aggs."""
    df = pd.DataFrame(
        {
            "v": ["10", "abc", "30"],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": [],
        "columns": [],
        "values": ["v"],
        "aggregation": {"v": "sum"},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    assert float(result["v"].values[0]) == 40.0


def test_sidecar_agg_func_coerces_non_numeric_strings(pivot_module):
    """_sidecar_agg_func must coerce non-numeric strings for numeric aggs."""
    series = pd.Series(["10", "abc", "30"])
    assert float(pivot_module._sidecar_agg_func("avg", series)) == 20.0
    assert float(pivot_module._sidecar_agg_func("median", series)) == 20.0


def test_sidecar_agg_func_count_distinct_does_not_coerce(pivot_module):
    """count_distinct in sidecar should count distinct non-null values of any type."""
    series = pd.Series(["abc", "abc", "def", None])
    assert int(pivot_module._sidecar_agg_func("count_distinct", series)) == 2


# ---- Pre-aggregation config.filters tests ----


def test_prepare_hybrid_frame_applies_dimension_include_filter(pivot_module):
    """Include filter removes excluded groups from pre-aggregated output."""
    df = pd.DataFrame(
        {
            "region": ["US", "US", "EU", "EU"],
            "year": ["2023", "2024", "2023", "2024"],
            "revenue": [100.0, 150.0, 200.0, 250.0],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["region"],
        "columns": ["year"],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
        "filters": {"region": {"include": ["US"]}},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    assert set(result["region"].unique()) == {"US"}
    assert len(result) == 2


def test_prepare_hybrid_frame_applies_dimension_exclude_filter(pivot_module):
    """Exclude filter removes those groups from pre-aggregated output."""
    df = pd.DataFrame(
        {
            "region": ["US", "US", "EU", "EU"],
            "year": ["2023", "2024", "2023", "2024"],
            "revenue": [100.0, 150.0, 200.0, 250.0],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["region"],
        "columns": ["year"],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
        "filters": {"region": {"exclude": ["EU"]}},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    assert set(result["region"].unique()) == {"US"}
    assert len(result) == 2


def test_prepare_hybrid_frame_separate_mode_null_filter(pivot_module):
    """null_handling='separate' + filter on '(null)' correctly pre-filters."""
    df = pd.DataFrame(
        {
            "region": ["US", None, "EU", None],
            "revenue": [100.0, 200.0, 300.0, 400.0],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["region"],
        "columns": [],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
        "filters": {"region": {"include": ["(null)"]}},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(
        df, cfg, null_handling="separate"
    )
    assert len(result) == 1
    assert float(result["revenue"].values[0]) == 600.0


def test_prepare_hybrid_frame_no_filters_unchanged(pivot_module):
    """No filters produces same output as before."""
    df = pd.DataFrame(
        {
            "region": ["US", "EU"],
            "revenue": [100.0, 200.0],
        }
    )
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["region"],
        "columns": [],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(df, cfg)
    assert len(result) == 2


def test_prepare_hybrid_frame_source_filtered_data_supports_hidden_filters(
    pivot_module,
):
    """Hidden report-level filters can be applied before hybrid pre-aggregation."""
    df = pd.DataFrame(
        {
            "region": ["US", "US", "EU", "EU"],
            "year": ["2023", "2024", "2023", "2024"],
            "category": ["A", "B", "A", "B"],
            "revenue": [100.0, 150.0, 200.0, 250.0],
        }
    )
    filtered = pivot_module._apply_source_filters(df, {"category": {"include": ["A"]}})
    cfg = {
        "version": pivot_module.CONFIG_SCHEMA_VERSION,
        "rows": ["region"],
        "columns": ["year"],
        "values": ["revenue"],
        "aggregation": {"revenue": "sum"},
        "synthetic_measures": [],
    }
    result = pivot_module._prepare_threshold_hybrid_frame(filtered, cfg)
    assert sorted(result["revenue"].tolist()) == [100.0, 200.0]


# ---- Sidecar computation tests ----


class TestComputeHybridTotals:
    @pytest.fixture
    def df(self):
        return pd.DataFrame(
            {
                "region": ["US", "US", "EU", "EU", "US"],
                "year": ["2023", "2024", "2023", "2024", "2023"],
                "revenue": [100.0, 150.0, 200.0, 250.0, 50.0],
            }
        )

    def _cfg(self, agg="median", rows=None, cols=None, **kwargs):
        return {
            "rows": rows or ["region"],
            "columns": cols or ["year"],
            "values": ["revenue"],
            "aggregation": {"revenue": agg},
            **kwargs,
        }

    def test_grand_total(self, pivot_module, df):
        sidecar = pivot_module._compute_hybrid_totals(df, self._cfg("median"), None)
        assert sidecar is not None
        assert sidecar["grand"]["revenue"] == df["revenue"].median()

    def test_row_totals(self, pivot_module, df):
        sidecar = pivot_module._compute_hybrid_totals(df, self._cfg("median"), None)
        row_vals = {tuple(e["key"]): e["values"]["revenue"] for e in sidecar["row"]}
        us_rev = df[df["region"] == "US"]["revenue"]
        eu_rev = df[df["region"] == "EU"]["revenue"]
        assert row_vals[("US",)] == us_rev.median()
        assert row_vals[("EU",)] == eu_rev.median()

    def test_col_totals(self, pivot_module, df):
        sidecar = pivot_module._compute_hybrid_totals(df, self._cfg("median"), None)
        col_vals = {tuple(e["key"]): e["values"]["revenue"] for e in sidecar["col"]}
        assert col_vals[("2023",)] == df[df["year"] == "2023"]["revenue"].median()

    def test_returns_none_for_fully_decomposable(self, pivot_module, df):
        cfg = self._cfg("sum")
        sidecar = pivot_module._compute_hybrid_totals(df, cfg, None)
        assert sidecar is None

    def test_includes_avg(self, pivot_module, df):
        sidecar = pivot_module._compute_hybrid_totals(df, self._cfg("avg"), None)
        assert sidecar is not None
        assert abs(sidecar["grand"]["revenue"] - df["revenue"].mean()) < 1e-9

    def test_respects_filters(self, pivot_module, df):
        cfg = self._cfg("median", filters={"region": {"include": ["US"]}})
        sidecar = pivot_module._compute_hybrid_totals(df, cfg, None)
        us_only = df[df["region"] == "US"]
        assert sidecar["grand"]["revenue"] == us_only["revenue"].median()

    def test_null_handling_separate(self, pivot_module):
        df = pd.DataFrame(
            {
                "region": ["US", None, "EU"],
                "year": ["2023", "2023", "2023"],
                "revenue": [100.0, 200.0, 300.0],
            }
        )
        sidecar = pivot_module._compute_hybrid_totals(
            df, self._cfg("median"), "separate"
        )
        row_keys = [tuple(e["key"]) for e in sidecar["row"]]
        assert ("(null)",) in row_keys

    def test_null_handling_exclude(self, pivot_module):
        df = pd.DataFrame(
            {
                "region": ["US", None, "EU"],
                "year": ["2023", "2023", "2023"],
                "revenue": [100.0, 200.0, 300.0],
            }
        )
        sidecar = pivot_module._compute_hybrid_totals(
            df, self._cfg("median"), "exclude"
        )
        row_keys = [tuple(e["key"]) for e in sidecar["row"]]
        assert ("",) in row_keys
        assert ("(null)",) not in row_keys

    def test_filter_null_separate(self, pivot_module):
        """With null_handling='separate', filtering include=['(null)'] should match null rows."""
        df = pd.DataFrame(
            {
                "region": ["US", None, "EU"],
                "year": ["2023", "2023", "2023"],
                "revenue": [100.0, 200.0, 300.0],
            }
        )
        cfg = self._cfg(
            "median",
            filters={"region": {"include": ["(null)"]}},
        )
        sidecar = pivot_module._compute_hybrid_totals(df, cfg, "separate")
        assert sidecar["grand"]["revenue"] == 200.0

    def test_serialization_no_numpy(self, pivot_module, df):
        import numpy as np

        sidecar = pivot_module._compute_hybrid_totals(df, self._cfg("median"), None)

        def check_values(obj):
            if isinstance(obj, dict):
                for v in obj.values():
                    check_values(v)
            elif isinstance(obj, list):
                for v in obj:
                    check_values(v)
            else:
                assert not isinstance(
                    obj, (np.integer, np.floating)
                ), f"numpy scalar: {type(obj)}"

        check_values(sidecar)

    def test_fingerprint_present(self, pivot_module, df):
        sidecar = pivot_module._compute_hybrid_totals(df, self._cfg("median"), None)
        assert isinstance(sidecar["sidecar_fingerprint"], str)
        assert len(sidecar["sidecar_fingerprint"]) > 0

    def test_fingerprint_changes_with_layout(self, pivot_module, df):
        fp1 = pivot_module._build_sidecar_fingerprint(
            self._cfg("median", rows=["region"]), None
        )
        fp2 = pivot_module._build_sidecar_fingerprint(
            self._cfg("median", rows=["year"]), None
        )
        assert fp1 != fp2

    def test_fingerprint_changes_with_agg(self, pivot_module, df):
        fp1 = pivot_module._build_sidecar_fingerprint(self._cfg("median"), None)
        fp2 = pivot_module._build_sidecar_fingerprint(self._cfg("avg"), None)
        assert fp1 != fp2

    def test_fingerprint_deterministic(self, pivot_module):
        cfg1 = {
            "rows": ["a"],
            "columns": ["b"],
            "values": ["v"],
            "aggregation": {"v": "median"},
        }
        cfg2 = {
            "aggregation": {"v": "median"},
            "values": ["v"],
            "columns": ["b"],
            "rows": ["a"],
        }
        fp1 = pivot_module._build_sidecar_fingerprint(cfg1, None)
        fp2 = pivot_module._build_sidecar_fingerprint(cfg2, None)
        assert fp1 == fp2

    def test_fingerprint_sorted_keys(self, pivot_module):
        import json

        cfg = {
            "rows": ["a"],
            "columns": ["b"],
            "values": ["v"],
            "aggregation": {"v": "avg"},
        }
        fp = pivot_module._build_sidecar_fingerprint(cfg, None)
        parsed = json.loads(fp)
        keys = list(parsed.keys())
        assert keys == sorted(keys)

    def test_fingerprint_null_handling_dict_order(self, pivot_module):
        cfg = {
            "rows": ["a", "b"],
            "columns": [],
            "values": ["v"],
            "aggregation": {"v": "median"},
        }
        fp1 = pivot_module._build_sidecar_fingerprint(
            cfg, {"a": "separate", "b": "exclude"}
        )
        fp2 = pivot_module._build_sidecar_fingerprint(
            cfg, {"b": "exclude", "a": "separate"}
        )
        assert fp1 == fp2

    def test_fingerprint_show_subtotals_array_order(self, pivot_module):
        cfg1 = {
            "rows": ["a", "b"],
            "columns": [],
            "values": ["v"],
            "aggregation": {"v": "median"},
            "show_subtotals": ["a", "b"],
        }
        cfg2 = {
            "rows": ["a", "b"],
            "columns": [],
            "values": ["v"],
            "aggregation": {"v": "median"},
            "show_subtotals": ["b", "a"],
        }
        fp1 = pivot_module._build_sidecar_fingerprint(cfg1, None)
        fp2 = pivot_module._build_sidecar_fingerprint(cfg2, None)
        assert fp1 == fp2

    def test_first_last_ordering(self, pivot_module):
        df = pd.DataFrame(
            {
                "region": ["A", "A", "A"],
                "year": ["2023", "2023", "2023"],
                "revenue": [10.0, 20.0, 30.0],
            }
        )
        cfg_first = self._cfg("first", rows=["region"], cols=["year"])
        cfg_last = self._cfg("last", rows=["region"], cols=["year"])
        sc_first = pivot_module._compute_hybrid_totals(df, cfg_first, None)
        sc_last = pivot_module._compute_hybrid_totals(df, cfg_last, None)
        assert sc_first["grand"]["revenue"] == 10.0
        assert sc_last["grand"]["revenue"] == 30.0


class TestAdaptiveGrainFingerprint:
    """Changing adaptive grains alone invalidates the sidecar fingerprint."""

    def test_different_adaptive_grains_different_fingerprint(self, pivot_module):
        cfg = {
            "version": pivot_module.CONFIG_SCHEMA_VERSION,
            "rows": ["d"],
            "columns": [],
            "values": ["v"],
            "aggregation": {"v": "avg"},
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
