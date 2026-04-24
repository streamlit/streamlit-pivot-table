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
        result = pivot_module._translate_column_config(config, df)
        assert result["number_format"].get("Revenue") == ",.2f"

    def test_date_column_format(self, pivot_module):
        """DateColumn -> dimension_format."""
        config = {
            "order_date": {"format": "YYYY-MM-DD", "type": "date"},
        }
        df = pd.DataFrame({"order_date": pd.to_datetime(["2024-01-15"])})
        result = pivot_module._translate_column_config(config, df)
        assert result["dimension_format"].get("order_date") == "YYYY-MM-DD"

    def test_empty_config(self, pivot_module):
        df = pd.DataFrame({"x": [1]})
        result = pivot_module._translate_column_config({}, df)
        assert result == {}

    def test_printf_style_translated(self, pivot_module):
        """Streamlit printf-style '%,.2f' -> d3-style ',.2f'."""
        config = {"Revenue": {"format": "%,.2f"}}
        df = pd.DataFrame({"Revenue": [100.0]})
        result = pivot_module._translate_column_config(config, df)
        assert result["number_format"]["Revenue"] == ",.2f"

    def test_label_emitted(self, pivot_module):
        config = {"Revenue": {"label": "Total Revenue"}}
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config(config, df)
        assert result["field_labels"] == {"Revenue": "Total Revenue"}

    def test_help_emitted(self, pivot_module):
        config = {"Revenue": {"help": "USD, pre-tax"}}
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config(config, df)
        assert result["field_help"] == {"Revenue": "USD, pre-tax"}

    def test_help_blank_string_ignored(self, pivot_module):
        config = {"Revenue": {"help": ""}}
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config(config, df)
        assert "field_help" not in result

    def test_width_presets(self, pivot_module):
        df = pd.DataFrame({"A": [1], "B": [1], "C": [1]})
        result = pivot_module._translate_column_config(
            {
                "A": {"width": "small"},
                "B": {"width": "medium"},
                "C": {"width": "large"},
            },
            df,
        )
        assert result["field_widths"] == {
            "A": "small",
            "B": "medium",
            "C": "large",
        }

    def test_width_pixel_int_accepted(self, pivot_module):
        df = pd.DataFrame({"A": [1]})
        result = pivot_module._translate_column_config({"A": {"width": 150}}, df)
        assert result["field_widths"] == {"A": 150}

    def test_width_bounds_20_and_2000_accepted(self, pivot_module):
        df = pd.DataFrame({"A": [1], "B": [1]})
        result = pivot_module._translate_column_config(
            {"A": {"width": 20}, "B": {"width": 2000}}, df
        )
        assert result["field_widths"] == {"A": 20, "B": 2000}

    def test_width_rejects_below_min(self, pivot_module, recwarn):
        df = pd.DataFrame({"A": [1]})
        result = pivot_module._translate_column_config({"A": {"width": 5}}, df)
        assert "field_widths" not in result
        assert any("invalid width" in str(w.message).lower() for w in recwarn)

    def test_width_rejects_above_max(self, pivot_module, recwarn):
        df = pd.DataFrame({"A": [1]})
        result = pivot_module._translate_column_config({"A": {"width": 99999}}, df)
        assert "field_widths" not in result
        assert any("invalid width" in str(w.message).lower() for w in recwarn)

    def test_width_rejects_zero_and_negative(self, pivot_module, recwarn):
        df = pd.DataFrame({"A": [1], "B": [1]})
        result = pivot_module._translate_column_config(
            {"A": {"width": 0}, "B": {"width": -50}}, df
        )
        assert "field_widths" not in result
        assert sum(1 for w in recwarn if "invalid width" in str(w.message).lower()) == 2

    def test_width_rejects_float(self, pivot_module, recwarn):
        df = pd.DataFrame({"A": [1]})
        result = pivot_module._translate_column_config({"A": {"width": 120.0}}, df)
        assert "field_widths" not in result
        assert any("invalid width" in str(w.message).lower() for w in recwarn)

    def test_width_rejects_bool(self, pivot_module, recwarn):
        df = pd.DataFrame({"A": [1]})
        # bool is int in Python; must be explicitly excluded
        result = pivot_module._translate_column_config({"A": {"width": True}}, df)
        assert "field_widths" not in result
        assert any("invalid width" in str(w.message).lower() for w in recwarn)

    def test_width_rejects_unknown_preset(self, pivot_module, recwarn):
        df = pd.DataFrame({"A": [1]})
        result = pivot_module._translate_column_config({"A": {"width": "huge"}}, df)
        assert "field_widths" not in result
        assert any("invalid width" in str(w.message).lower() for w in recwarn)

    def test_width_one_shot_warning_per_field_key(self, pivot_module, recwarn):
        df = pd.DataFrame({"A": [1], "B": [1]})
        pivot_module._translate_column_config(
            {"A": {"width": -1}, "B": {"width": -2}}, df
        )
        width_warnings = [
            w for w in recwarn if "invalid width" in str(w.message).lower()
        ]
        # Distinct fields each get their own warning.
        assert len(width_warnings) == 2

    def test_pinned_left_emitted(self, pivot_module):
        config = {"Region": {"pinned": "left"}, "Revenue": {"pinned": True}}
        df = pd.DataFrame({"Region": ["E"], "Revenue": [1.0]})
        result = pivot_module._translate_column_config(config, df)
        assert sorted(result["pinned_fields"]) == ["Region", "Revenue"]

    def test_pinned_false_and_none_ignored(self, pivot_module):
        config = {
            "A": {"pinned": False},
            "B": {"pinned": None},
        }
        df = pd.DataFrame({"A": [1], "B": [1]})
        result = pivot_module._translate_column_config(config, df)
        assert "pinned_fields" not in result

    def test_pinned_right_warns_and_is_ignored(self, pivot_module, recwarn):
        config = {"A": {"pinned": "right"}}
        df = pd.DataFrame({"A": [1]})
        result = pivot_module._translate_column_config(config, df)
        assert "pinned_fields" not in result
        assert any("right" in str(w.message).lower() for w in recwarn)

    def test_dict_literal_unknown_key_warns(self, pivot_module, recwarn):
        """Unknown top-level key in a dict-literal triggers a warning."""
        config = {"Revenue": {"format": ",.2f", "nonsense": "x"}}
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config(config, df)
        assert result["number_format"]["Revenue"] == ",.2f"
        assert any(
            "nonsense" in str(w.message) and "unrecognized" in str(w.message)
            for w in recwarn
        )

    def test_dict_literal_with_type_config_still_warns_on_typos(
        self, pivot_module, recwarn
    ):
        """A dict-literal that happens to include `type_config` must still
        surface typos on sibling keys. The object-style bypass must only kick
        in when the spec was an actual st.column_config.* object."""
        config = {
            "Revenue": {
                "type_config": {"format": "$,.0f"},
                "lable": "Total Revenue",
            }
        }
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config(config, df)
        assert result["number_format"]["Revenue"] == "$,.0f"
        assert any(
            "lable" in str(w.message) and "unrecognized" in str(w.message)
            for w in recwarn
        )

    def test_object_style_silent_on_streamlit_defaults(self, pivot_module, recwarn):
        """st.column_config.NumberColumn(...) expands to a dict with many
        internal defaults. We must not warn on those."""
        import streamlit as st

        spec = dict(st.column_config.NumberColumn(format="$,.2f"))
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config({"Revenue": spec}, df)
        assert result["number_format"]["Revenue"] == "$,.2f"
        assert [w for w in recwarn if "unrecognized" in str(w.message)] == []

    def test_object_style_reads_nested_format(self, pivot_module):
        """Format nested inside type_config (Streamlit object shape) is read."""
        import streamlit as st

        spec = dict(st.column_config.NumberColumn(format="$,.0f"))
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config({"Revenue": spec}, df)
        assert result["number_format"]["Revenue"] == "$,.0f"

    def test_object_style_label_and_help(self, pivot_module):
        import streamlit as st

        spec = dict(
            st.column_config.NumberColumn(
                label="Total Revenue", help="USD, pre-tax", pinned=True
            )
        )
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config({"Revenue": spec}, df)
        assert result["field_labels"] == {"Revenue": "Total Revenue"}
        assert result["field_help"] == {"Revenue": "USD, pre-tax"}
        assert result["pinned_fields"] == ["Revenue"]

    def test_unsupported_type_warns_once(self, pivot_module, recwarn):
        import streamlit as st

        spec = dict(st.column_config.LineChartColumn())
        df = pd.DataFrame({"Sales": [1.0]})
        pivot_module._translate_column_config({"Sales": spec}, df)
        lc_warnings = [w for w in recwarn if "line_chart" in str(w.message)]
        assert len(lc_warnings) == 1

    def test_selectbox_warns_as_unsupported(self, pivot_module, recwarn):
        import streamlit as st

        spec = dict(st.column_config.SelectboxColumn(options=["a", "b"]))
        df = pd.DataFrame({"Choice": ["a"]})
        pivot_module._translate_column_config({"Choice": spec}, df)
        assert any("selectbox" in str(w.message).lower() for w in recwarn)

    def test_width_frontend_bound_via_mount(
        self, pivot_module, mount_recorder, sample_df
    ):
        """field_widths round-trips into PivotConfig for the frontend."""
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_fw",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            column_config={
                "Revenue": {"width": "large"},
                "Region": {"width": 150},
            },
        )
        config = calls[0]["default"]["config"]
        assert config["field_widths"] == {"Revenue": "large", "Region": 150}

    def test_label_and_help_round_trip_to_config(
        self, pivot_module, mount_recorder, sample_df
    ):
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_lh",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            column_config={
                "Revenue": {"label": "Rev.", "help": "USD"},
            },
        )
        config = calls[0]["default"]["config"]
        assert config["field_labels"] == {"Revenue": "Rev."}
        assert config["field_help"] == {"Revenue": "USD"}
        # Canonical field ids preserved in rows/columns/values:
        assert config["values"] == ["Revenue"]
        assert config["rows"] == ["Region"]

    def test_pinned_unions_with_frozen_columns(
        self, pivot_module, mount_recorder, sample_df
    ):
        """column_config pinned + explicit frozen_columns must union."""
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_pin",
            rows=["Region", "Category"],
            columns=["Year"],
            values=["Revenue"],
            frozen_columns=["Region"],
            column_config={"Category": {"pinned": True}},
        )
        data = calls[0]["data"]
        assert sorted(data["hidden_from_drag_drop"]) == sorted(["Region", "Category"])

    def test_pinned_alone_populates_hidden_from_drag_drop(
        self, pivot_module, mount_recorder, sample_df
    ):
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_pin_alone",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            column_config={"Region": {"pinned": True}},
        )
        data = calls[0]["data"]
        assert data["hidden_from_drag_drop"] == ["Region"]


# ---------------------------------------------------------------------------
# column_config.alignment ↔ column_alignment reconciliation
# ---------------------------------------------------------------------------


class TestAlignmentReconciliation:
    """column_config.alignment is a supported key that unions with the
    `column_alignment` kwarg. Precedence: explicit kwarg > column_config >
    default. Invalid values from column_config warn-and-skip (mirrors width
    policy); invalid values via the kwarg still raise (unchanged behavior)."""

    def test_dict_literal_alignment_populates_translation(self, pivot_module):
        config = {"Revenue": {"alignment": "right"}, "Region": {"alignment": "center"}}
        df = pd.DataFrame({"Revenue": [1.0], "Region": ["A"]})
        result = pivot_module._translate_column_config(config, df)
        assert result["column_alignment"] == {"Revenue": "right", "Region": "center"}

    def test_object_style_alignment_populates_translation(self, pivot_module):
        """A column_config spec dict with an 'alignment' key flows through.

        Streamlit ≥ 1.55 removed the alignment= parameter from NumberColumn,
        so we build the spec dict directly rather than through the live API —
        the test is validating _translate_column_config, not Streamlit's API.
        """
        # Mimic the shape that older Streamlit would have produced via
        # dict(st.column_config.NumberColumn(alignment="center"))
        spec = {"label": None, "width": None, "alignment": "center"}
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config({"Revenue": spec}, df)
        assert result["column_alignment"] == {"Revenue": "center"}

    def test_object_style_alignment_none_is_silently_ignored(
        self, pivot_module, recwarn
    ):
        """st.column_config.Column() with no alignment set must not warn or
        contribute an entry (alignment defaults to None in the object)."""
        import streamlit as st

        spec = dict(st.column_config.NumberColumn(format="$,.0f"))
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config({"Revenue": spec}, df)
        assert "column_alignment" not in result
        assert [w for w in recwarn if "alignment" in str(w.message).lower()] == []

    def test_invalid_alignment_warns_and_skips(self, pivot_module, recwarn):
        config = {"Revenue": {"alignment": "middle"}}
        df = pd.DataFrame({"Revenue": [1.0]})
        result = pivot_module._translate_column_config(config, df)
        assert "column_alignment" not in result
        assert any(
            "alignment" in str(w.message).lower() and "middle" in str(w.message)
            for w in recwarn
        )

    def test_invalid_alignment_warns_once_per_field(self, pivot_module, recwarn):
        config = {"Revenue": {"alignment": "bogus"}}
        df = pd.DataFrame({"Revenue": [1.0]})
        pivot_module._translate_column_config(config, df)
        pivot_module._translate_column_config(config, df)
        align_warnings = [w for w in recwarn if "alignment" in str(w.message).lower()]
        # Each call creates its own `warned` set; one-shot cadence is per-call.
        assert len(align_warnings) == 2

    def test_alignment_allowlist_no_unknown_key_warning(self, pivot_module, recwarn):
        """`alignment` must be in the user-facing allowlist, so a dict-literal
        with only alignment does not trip the unknown-key warning."""
        config = {"Revenue": {"alignment": "right"}}
        df = pd.DataFrame({"Revenue": [1.0]})
        pivot_module._translate_column_config(config, df)
        assert [w for w in recwarn if "unrecognized" in str(w.message).lower()] == []

    def test_column_config_alignment_reaches_mounted_config(
        self, pivot_module, mount_recorder, sample_df
    ):
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_align_cc",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            column_config={"Revenue": {"alignment": "center"}},
        )
        config = calls[0]["default"]["config"]
        assert config["column_alignment"] == {"Revenue": "center"}

    def test_explicit_kwarg_overrides_column_config(
        self, pivot_module, mount_recorder, sample_df
    ):
        """Explicit column_alignment kwarg wins for fields set in both."""
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_align_override",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            column_alignment={"Revenue": "left"},
            column_config={"Revenue": {"alignment": "right"}},
        )
        config = calls[0]["default"]["config"]
        assert config["column_alignment"]["Revenue"] == "left"

    def test_column_config_fills_gap_when_kwarg_misses_field(
        self, pivot_module, mount_recorder, sample_df
    ):
        """Fields only mentioned in column_config still contribute."""
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_align_gap",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue", "Profit"],
            column_alignment={"Revenue": "left"},
            column_config={"Profit": {"alignment": "center"}},
        )
        config = calls[0]["default"]["config"]
        assert config["column_alignment"]["Revenue"] == "left"
        assert config["column_alignment"]["Profit"] == "center"

    def test_explicit_kwarg_invalid_still_raises(self, pivot_module, sample_df):
        """The existing kwarg validator is unchanged; bad explicit values
        still raise ValueError even when column_config is also set."""
        import pytest

        with pytest.raises(ValueError, match="column_alignment"):
            pivot_module.st_pivot_table(
                sample_df,
                key="pivot_align_bad",
                rows=["Region"],
                columns=["Year"],
                values=["Revenue"],
                column_alignment={"Revenue": "middle"},
                column_config={"Revenue": {"alignment": "right"}},
            )


# ---------------------------------------------------------------------------
# Tier 2 column_config cell renderers
# ---------------------------------------------------------------------------


class TestCellRenderers:
    """column_config types that produce per-field cell renderers
    (LinkColumn / ImageColumn / CheckboxColumn / TextColumn.max_chars).

    Emitted as ``field_renderers[field]`` with a tagged shape consumed by the
    frontend dispatcher at ``renderers/cellRenderer.tsx``.
    """

    def test_link_dict_literal_flat(self, pivot_module):
        config = {
            "Website": {"type": "link", "display_text": "Visit {}"},
        }
        df = pd.DataFrame({"Website": ["https://example.com"]})
        result = pivot_module._translate_column_config(config, df)
        assert result["field_renderers"] == {
            "Website": {"type": "link", "display_text": "Visit {}"},
        }

    def test_link_dict_literal_nested_type_config(self, pivot_module):
        """Users may nest renderer options under type_config in dict literals."""
        config = {
            "Website": {
                "type": "link",
                "type_config": {"display_text": "Click"},
            },
        }
        df = pd.DataFrame({"Website": ["https://example.com"]})
        result = pivot_module._translate_column_config(config, df)
        assert result["field_renderers"] == {
            "Website": {"type": "link", "display_text": "Click"},
        }

    def test_link_without_display_text(self, pivot_module):
        config = {"Website": {"type": "link"}}
        df = pd.DataFrame({"Website": ["https://example.com"]})
        result = pivot_module._translate_column_config(config, df)
        assert result["field_renderers"] == {"Website": {"type": "link"}}

    def test_link_object_style(self, pivot_module):
        """st.column_config.LinkColumn(display_text=...) flows through."""
        import streamlit as st

        spec = dict(st.column_config.LinkColumn(display_text="Visit {}"))
        df = pd.DataFrame({"Website": ["https://example.com"]})
        result = pivot_module._translate_column_config({"Website": spec}, df)
        assert result["field_renderers"]["Website"]["type"] == "link"
        assert result["field_renderers"]["Website"]["display_text"] == "Visit {}"

    def test_image_renderer(self, pivot_module):
        config = {"Logo": {"type": "image"}}
        df = pd.DataFrame({"Logo": ["/tmp/a.png"]})
        result = pivot_module._translate_column_config(config, df)
        assert result["field_renderers"] == {"Logo": {"type": "image"}}

    def test_checkbox_renderer(self, pivot_module):
        config = {"Active": {"type": "checkbox"}}
        df = pd.DataFrame({"Active": [True, False]})
        result = pivot_module._translate_column_config(config, df)
        assert result["field_renderers"] == {"Active": {"type": "checkbox"}}

    def test_text_max_chars_emits_renderer(self, pivot_module):
        config = {"Notes": {"type": "text", "max_chars": 40}}
        df = pd.DataFrame({"Notes": ["long text"]})
        result = pivot_module._translate_column_config(config, df)
        assert result["field_renderers"] == {
            "Notes": {"type": "text", "max_chars": 40},
        }

    def test_text_without_max_chars_no_renderer(self, pivot_module):
        """TextColumn without max_chars is equivalent to default text
        rendering, so no renderer entry is emitted."""
        config = {"Notes": {"type": "text"}}
        df = pd.DataFrame({"Notes": ["abc"]})
        result = pivot_module._translate_column_config(config, df)
        assert "field_renderers" not in result

    def test_text_invalid_max_chars_warns_and_skips(self, pivot_module, recwarn):
        config = {"Notes": {"type": "text", "max_chars": 0}}
        df = pd.DataFrame({"Notes": ["abc"]})
        result = pivot_module._translate_column_config(config, df)
        assert "field_renderers" not in result
        assert any("max_chars" in str(w.message).lower() for w in recwarn), [
            str(w.message) for w in recwarn
        ]

    def test_text_negative_max_chars_warns_and_skips(self, pivot_module, recwarn):
        config = {"Notes": {"type": "text", "max_chars": -5}}
        df = pd.DataFrame({"Notes": ["abc"]})
        result = pivot_module._translate_column_config(config, df)
        assert "field_renderers" not in result
        assert any("max_chars" in str(w.message).lower() for w in recwarn)

    def test_text_bool_max_chars_rejected(self, pivot_module, recwarn):
        """bool is a subclass of int in Python; explicit exclusion."""
        config = {"Notes": {"type": "text", "max_chars": True}}
        df = pd.DataFrame({"Notes": ["abc"]})
        result = pivot_module._translate_column_config(config, df)
        assert "field_renderers" not in result
        assert any("max_chars" in str(w.message).lower() for w in recwarn)

    def test_allowlist_accepts_renderer_specific_keys(self, pivot_module, recwarn):
        """display_text / max_chars / min_value / max_value must not trigger
        unknown-key warnings on dict-literal specs."""
        config = {
            "Website": {"type": "link", "display_text": "x"},
            "Notes": {"type": "text", "max_chars": 10},
            "Pct": {
                "type": "progress",
                "min_value": 0,
                "max_value": 100,
            },
        }
        df = pd.DataFrame(
            {
                "Website": ["https://x"],
                "Notes": ["x"],
                "Pct": [50],
            }
        )
        pivot_module._translate_column_config(config, df)
        unknown = [w for w in recwarn if "unrecognized" in str(w.message).lower()]
        assert unknown == [], [str(w.message) for w in unknown]

    def test_multiple_renderer_types_emitted_together(self, pivot_module):
        config = {
            "URL": {"type": "link"},
            "IMG": {"type": "image"},
            "OK": {"type": "checkbox"},
            "Notes": {"type": "text", "max_chars": 25},
        }
        df = pd.DataFrame(
            {
                "URL": ["https://x"],
                "IMG": ["/tmp/a.png"],
                "OK": [True],
                "Notes": ["x"],
            }
        )
        result = pivot_module._translate_column_config(config, df)
        assert result["field_renderers"] == {
            "URL": {"type": "link"},
            "IMG": {"type": "image"},
            "OK": {"type": "checkbox"},
            "Notes": {"type": "text", "max_chars": 25},
        }

    def test_unsupported_renderer_type_does_not_emit(self, pivot_module):
        """Tier 3 column types (line_chart etc.) must not leak into
        field_renderers — they emit an unsupported-type warning but produce
        no renderer entry."""
        config = {"X": {"type": "line_chart"}}
        df = pd.DataFrame({"X": [1]})
        result = pivot_module._translate_column_config(config, df)
        assert "field_renderers" not in result

    def test_progress_type_warns_until_wired(self, pivot_module, recwarn):
        """``ProgressColumn`` is planned for a later Tier 2 increment but is
        not yet implemented. Users who configure it today should see an
        unsupported-type warning rather than silent no-op, even though
        ``min_value`` / ``max_value`` are allowlisted as known keys."""
        config = {
            "Pct": {
                "type": "progress",
                "min_value": 0,
                "max_value": 100,
            },
        }
        df = pd.DataFrame({"Pct": [50]})
        result = pivot_module._translate_column_config(config, df)
        assert "field_renderers" not in result
        unsupported = [
            w
            for w in recwarn
            if "not supported" in str(w.message).lower()
            and "progress" in str(w.message).lower()
        ]
        assert len(unsupported) == 1, [str(w.message) for w in recwarn]
        unknown = [w for w in recwarn if "unrecognized" in str(w.message).lower()]
        assert unknown == [], [str(w.message) for w in unknown]

    def test_renderer_reaches_mounted_config(
        self, pivot_module, mount_recorder, sample_df
    ):
        calls = mount_recorder()
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot_renderer_mount",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            column_config={
                "Region": {"type": "text", "max_chars": 10},
            },
        )
        config = calls[0]["default"]["config"]
        assert config["field_renderers"] == {
            "Region": {"type": "text", "max_chars": 10},
        }


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
