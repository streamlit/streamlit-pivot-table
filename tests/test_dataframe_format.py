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

"""Tests for DataFrame format respect: column type detection, canonical temporal keys,
column_config translation, Styler extraction, and precedence."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd


# ---------------------------------------------------------------------------
# _classify_temporal_value
# ---------------------------------------------------------------------------


class TestClassifyTemporalValue:
    def test_python_date_instance(self, pivot_module):
        assert pivot_module._classify_temporal_value(date(2024, 1, 15)) == "date"

    def test_python_datetime_instance(self, pivot_module):
        assert (
            pivot_module._classify_temporal_value(datetime(2024, 1, 15, 12, 30))
            == "datetime"
        )

    def test_pandas_timestamp(self, pivot_module):
        assert (
            pivot_module._classify_temporal_value(pd.Timestamp("2024-01-15"))
            == "datetime"
        )

    def test_iso_date_only_string(self, pivot_module):
        assert pivot_module._classify_temporal_value("2024-01-15") == "date"

    def test_iso_datetime_string(self, pivot_module):
        assert (
            pivot_module._classify_temporal_value("2024-06-01T14:30:00") == "datetime"
        )

    def test_iso_datetime_with_timezone(self, pivot_module):
        assert (
            pivot_module._classify_temporal_value("2024-06-01T14:30:00Z") == "datetime"
        )

    def test_non_temporal_string(self, pivot_module):
        assert pivot_module._classify_temporal_value("hello") is None

    def test_integer(self, pivot_module):
        assert pivot_module._classify_temporal_value(42) is None

    def test_none(self, pivot_module):
        assert pivot_module._classify_temporal_value(None) is None


# ---------------------------------------------------------------------------
# _build_original_column_types
# ---------------------------------------------------------------------------


class TestBuildOriginalColumnTypes:
    def test_datetime64_column(self, pivot_module):
        df = pd.DataFrame({"ts": pd.to_datetime(["2024-01-15", "2024-06-01"])})
        result = pivot_module._build_original_column_types(df)
        assert result["ts"] == "datetime"

    def test_integer_column(self, pivot_module):
        df = pd.DataFrame({"x": [1, 2, 3]})
        result = pivot_module._build_original_column_types(df)
        assert result["x"] == "integer"

    def test_float_column(self, pivot_module):
        df = pd.DataFrame({"x": [1.0, 2.5, 3.7]})
        result = pivot_module._build_original_column_types(df)
        assert result["x"] == "float"

    def test_boolean_column(self, pivot_module):
        df = pd.DataFrame({"x": [True, False, True]})
        result = pivot_module._build_original_column_types(df)
        assert result["x"] == "boolean"

    def test_string_column(self, pivot_module):
        df = pd.DataFrame({"x": ["a", "b", "c"]})
        result = pivot_module._build_original_column_types(df)
        assert result["x"] == "string"

    def test_object_date_only_strings(self, pivot_module):
        """Object column with only 'YYYY-MM-DD' strings -> 'date'."""
        df = pd.DataFrame(
            {"d": pd.array(["2024-01-15", "2024-06-01", "2025-03-10"], dtype=object)}
        )
        result = pivot_module._build_original_column_types(df)
        assert result["d"] == "date"

    def test_object_datetime_strings(self, pivot_module):
        """Object column with ISO datetime strings -> 'datetime'."""
        df = pd.DataFrame(
            {
                "d": pd.array(
                    ["2024-06-01T14:30:00", "2024-01-15T08:00:00"], dtype=object
                )
            }
        )
        result = pivot_module._build_original_column_types(df)
        assert result["d"] == "datetime"

    def test_object_mixed_date_datetime_promotes_to_datetime(self, pivot_module):
        """Mixed date-only and datetime strings -> 'datetime'."""
        df = pd.DataFrame(
            {"d": pd.array(["2024-01-15", "2024-06-01T14:30:00"], dtype=object)}
        )
        result = pivot_module._build_original_column_types(df)
        assert result["d"] == "datetime"

    def test_object_python_date_instances(self, pivot_module):
        """Object column with datetime.date instances -> 'date'."""
        df = pd.DataFrame({"d": [date(2024, 1, 15), date(2024, 6, 1)]})
        result = pivot_module._build_original_column_types(df)
        assert result["d"] == "date"

    def test_object_python_datetime_instances(self, pivot_module):
        """Object column with datetime.datetime instances -> 'datetime'."""
        df = pd.DataFrame(
            {"d": [datetime(2024, 1, 15, 12, 30), datetime(2024, 6, 1, 8, 0)]}
        )
        result = pivot_module._build_original_column_types(df)
        assert result["d"] == "datetime"

    def test_object_non_temporal_strings(self, pivot_module):
        """Object column with non-date strings -> 'string'."""
        df = pd.DataFrame({"d": ["hello", "world"]})
        result = pivot_module._build_original_column_types(df)
        assert result["d"] == "string"

    def test_empty_object_column(self, pivot_module):
        """Empty object column -> 'string'."""
        df = pd.DataFrame({"d": pd.array([None, None], dtype=object)})
        result = pivot_module._build_original_column_types(df)
        assert result["d"] == "string"


# ---------------------------------------------------------------------------
# _canonical_temporal_key (Python side)
# ---------------------------------------------------------------------------


class TestCanonicalTemporalKey:
    def test_datetime_naive_midnight(self, pivot_module):
        """Naive midnight -> ISO UTC with milliseconds."""
        key = pivot_module._canonical_temporal_key(
            pd.Timestamp("2024-01-15"), "datetime"
        )
        assert key == "2024-01-15T00:00:00.000Z"

    def test_datetime_naive_midday(self, pivot_module):
        """Naive midday -> ISO UTC with milliseconds."""
        key = pivot_module._canonical_temporal_key(
            pd.Timestamp("2024-01-15 12:30:00"), "datetime"
        )
        assert key == "2024-01-15T12:30:00.000Z"

    def test_datetime_tz_aware_utc(self, pivot_module):
        ts = pd.Timestamp("2024-01-15 12:30:00", tz="UTC")
        key = pivot_module._canonical_temporal_key(ts, "datetime")
        assert key == "2024-01-15T12:30:00.000Z"

    def test_datetime_tz_aware_eastern(self, pivot_module):
        """Eastern time -> must convert to UTC."""
        ts = pd.Timestamp("2024-01-15 12:30:00", tz="US/Eastern")
        key = pivot_module._canonical_temporal_key(ts, "datetime")
        assert key == "2024-01-15T17:30:00.000Z"

    def test_datetime_sub_second_precision(self, pivot_module):
        ts = pd.Timestamp("2024-01-15 12:30:00.123456")
        key = pivot_module._canonical_temporal_key(ts, "datetime")
        assert key == "2024-01-15T12:30:00.123Z"

    def test_date_column(self, pivot_module):
        key = pivot_module._canonical_temporal_key(pd.Timestamp("2024-01-15"), "date")
        assert key == "2024-01-15"

    def test_date_from_python_date(self, pivot_module):
        key = pivot_module._canonical_temporal_key(date(2024, 1, 15), "date")
        assert key == "2024-01-15"

    def test_nan_returns_empty(self, pivot_module):
        key = pivot_module._canonical_temporal_key(float("nan"), "datetime")
        assert key == ""

    def test_none_returns_empty(self, pivot_module):
        key = pivot_module._canonical_temporal_key(None, "datetime")
        assert key == ""

    def test_naive_datetime_string(self, pivot_module):
        """Naive datetime string '2024-01-15T12:30:00' -> UTC key."""
        key = pivot_module._canonical_temporal_key("2024-01-15T12:30:00", "datetime")
        assert key == "2024-01-15T12:30:00.000Z"

    def test_date_only_string(self, pivot_module):
        key = pivot_module._canonical_temporal_key("2024-01-15", "date")
        assert key == "2024-01-15"


# ---------------------------------------------------------------------------
# _translate_column_config
# ---------------------------------------------------------------------------


class TestTranslateColumnConfig:
    def test_number_column_format(self, pivot_module):
        """NumberColumn with format string -> number_format."""
        config = {
            "Revenue": {"format": ",.2f"},
        }
        df = pd.DataFrame({"Revenue": [100.0]})
        nf, df_fmt = pivot_module._translate_column_config(config, df)
        assert nf.get("Revenue") == ",.2f"

    def test_date_column_format(self, pivot_module):
        """DateColumn -> dimension_format."""
        config = {
            "order_date": {"format": "YYYY-MM-DD", "type": "date"},
        }
        df = pd.DataFrame({"order_date": pd.to_datetime(["2024-01-15"])})
        nf, df_fmt = pivot_module._translate_column_config(config, df)
        assert df_fmt.get("order_date") == "YYYY-MM-DD"

    def test_empty_config(self, pivot_module):
        df = pd.DataFrame({"x": [1]})
        nf, df_fmt = pivot_module._translate_column_config({}, df)
        assert nf == {}
        assert df_fmt == {}

    def test_printf_style_translated(self, pivot_module):
        """Streamlit printf-style '%,.2f' -> d3-style ',.2f'."""
        config = {"Revenue": {"format": "%,.2f"}}
        df = pd.DataFrame({"Revenue": [100.0]})
        nf, _ = pivot_module._translate_column_config(config, df)
        assert nf["Revenue"] == ",.2f"


# ---------------------------------------------------------------------------
# _extract_styler_formats and _guess_format_pattern
# ---------------------------------------------------------------------------


class TestStylerExtraction:
    def test_guess_format_pattern_currency(self, pivot_module):
        """$1,234.57 -> '$,.2f'."""
        pattern = pivot_module._guess_format_pattern("$1,234.57", 1234.5678)
        assert pattern == "$,.2f"

    def test_guess_format_pattern_plain_integer(self, pivot_module):
        pattern = pivot_module._guess_format_pattern("1235", 1234.5678)
        assert pattern == ".0f"

    def test_guess_format_pattern_grouped_2dec(self, pivot_module):
        pattern = pivot_module._guess_format_pattern("1,234.57", 1234.5678)
        assert pattern == ",.2f"

    def test_guess_format_pattern_percent(self, pivot_module):
        pattern = pivot_module._guess_format_pattern("123,456.8%", 1234.5678)
        assert pattern == ".1%"

    def test_guess_format_pattern_empty(self, pivot_module):
        pattern = pivot_module._guess_format_pattern("", 1234.5678)
        assert pattern is None

    def test_extract_styler_formats_currency(self, pivot_module):
        """Styler with '$' format -> number_format."""
        df = pd.DataFrame({"Revenue": [100.0, 200.0]})
        styler = df.style.format({"Revenue": "${:,.2f}"})
        nf, df_fmt = pivot_module._extract_styler_formats(styler)
        assert "Revenue" in nf
        assert nf["Revenue"] == "$,.2f"

    def test_extract_styler_formats_empty_display_funcs(self, pivot_module):
        """Styler without custom format -> empty dicts."""
        df = pd.DataFrame({"Revenue": [100.0]})
        styler = df.style
        nf, df_fmt = pivot_module._extract_styler_formats(styler)
        assert nf == {}
        assert df_fmt == {}


# ---------------------------------------------------------------------------
# Precedence: explicit > column_config > Styler > auto-default
# ---------------------------------------------------------------------------


class TestFormatPrecedence:
    def test_explicit_overrides_column_config(
        self, pivot_module, mount_recorder, sample_df
    ):
        """Explicit number_format overrides column_config."""
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_prec",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            number_format={"Revenue": ",.0f"},
            column_config={"Revenue": {"format": ",.4f"}},
        )
        assert len(calls) == 1
        config = calls[0]["default"]["config"]
        assert config["number_format"]["Revenue"] == ",.0f"

    def test_column_config_fills_gap(self, pivot_module, mount_recorder, sample_df):
        """column_config fills missing number_format entries."""
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_cc",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue", "Profit"],
            number_format={"Revenue": ",.0f"},
            column_config={"Profit": {"format": ",.2f"}},
        )
        assert len(calls) == 1
        config = calls[0]["default"]["config"]
        assert config["number_format"]["Revenue"] == ",.0f"
        assert config["number_format"]["Profit"] == ",.2f"


# ---------------------------------------------------------------------------
# Hybrid drilldown with temporal dimensions
# ---------------------------------------------------------------------------


class TestHybridDrilldownTemporal:
    def test_datetime_drilldown_uses_canonical_key(self, pivot_module):
        """Drilldown filter with canonical ISO UTC key matches correctly."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2024-01-15", "2024-01-15", "2024-06-01"]),
                "revenue": [100.0, 50.0, 200.0],
            }
        )
        column_types = pivot_module._build_original_column_types(df)
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"date": "2024-01-15T00:00:00.000Z"}},
            column_types=column_types,
            dims=["date"],
        )
        assert total == 2
        revenues = sorted(r["revenue"] for r in records)
        assert revenues == [50.0, 100.0]

    def test_date_only_drilldown_uses_canonical_key(self, pivot_module):
        """Date-only column drilldown with YYYY-MM-DD key."""
        df = pd.DataFrame(
            {
                "d": [date(2024, 1, 15), date(2024, 6, 1)],
                "v": [100.0, 200.0],
            }
        )
        column_types = pivot_module._build_original_column_types(df)
        records, columns, total, page = pivot_module._compute_hybrid_drilldown(
            df,
            {"filters": {"d": "2024-01-15"}},
            column_types=column_types,
            dims=["d"],
        )
        assert total == 1
        assert records[0]["v"] == 100.0


# ---------------------------------------------------------------------------
# _resolve_dim_value_series (canonical key for various types)
# ---------------------------------------------------------------------------


class TestResolveDimValueSeries:
    def test_datetime_series(self, pivot_module):
        """datetime64 series -> ISO UTC keys."""
        s = pd.Series(pd.to_datetime(["2024-01-15 12:30:00", "2024-06-01 00:00:00"]))
        result = pivot_module._resolve_dim_value_series(s, "datetime", "exclude")
        assert result.iloc[0] == "2024-01-15T12:30:00.000Z"
        assert result.iloc[1] == "2024-06-01T00:00:00.000Z"

    def test_date_series(self, pivot_module):
        """date column -> YYYY-MM-DD keys."""
        s = pd.Series([date(2024, 1, 15), date(2024, 6, 1)])
        result = pivot_module._resolve_dim_value_series(s, "date", "exclude")
        assert result.iloc[0] == "2024-01-15"
        assert result.iloc[1] == "2024-06-01"

    def test_null_handling_separate(self, pivot_module):
        s = pd.Series(["US", None, "EU"])
        result = pivot_module._resolve_dim_value_series(s, "string", "separate")
        assert result.iloc[1] == "(null)"

    def test_null_handling_exclude(self, pivot_module):
        s = pd.Series(["US", None, "EU"])
        result = pivot_module._resolve_dim_value_series(s, "string", "exclude")
        assert result.iloc[1] == ""

    def test_string_passthrough(self, pivot_module):
        s = pd.Series(["US", "EU"])
        result = pivot_module._resolve_dim_value_series(s, None, "exclude")
        assert list(result) == ["US", "EU"]


# ---------------------------------------------------------------------------
# Cross-language parity: Python canonical key must match frontend
# ---------------------------------------------------------------------------


class TestCrossLanguageParity:
    """Verify Python _canonical_temporal_key produces values matching
    the frontend _canonicalTemporalKey for the same logical values."""

    def test_naive_midnight(self, pivot_module):
        """Both sides: naive midnight -> '2024-01-15T00:00:00.000Z'."""
        key = pivot_module._canonical_temporal_key(
            pd.Timestamp("2024-01-15"), "datetime"
        )
        assert key == "2024-01-15T00:00:00.000Z"

    def test_naive_midday(self, pivot_module):
        """Both sides: naive midday -> '2024-01-15T12:30:00.000Z'."""
        key = pivot_module._canonical_temporal_key(
            pd.Timestamp("2024-01-15 12:30:00"), "datetime"
        )
        assert key == "2024-01-15T12:30:00.000Z"

    def test_date_only(self, pivot_module):
        """Both sides: date-only -> '2024-01-15'."""
        key = pivot_module._canonical_temporal_key(pd.Timestamp("2024-01-15"), "date")
        assert key == "2024-01-15"

    def test_non_temporal_backward_compat(self, pivot_module):
        """Non-temporal values produce the same key as String(raw)."""
        s = pd.Series(["US", "EU"])
        result = pivot_module._resolve_dim_value_series(s, "string", "exclude")
        assert list(result) == ["US", "EU"]

    def test_integer_backward_compat(self, pivot_module):
        """Integer dimensions produce the same key as String(raw)."""
        s = pd.Series([2023, 2024])
        result = pivot_module._resolve_dim_value_series(s, None, "exclude")
        assert list(result) == ["2023", "2024"]
