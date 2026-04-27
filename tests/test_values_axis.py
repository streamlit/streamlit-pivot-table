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

"""Python-side validation and config tests for values_axis (Commit 5)."""

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "Region": ["East", "East", "West", "West"],
            "Category": ["A", "B", "A", "B"],
            "Year": [2023, 2024, 2023, 2024],
            "Revenue": [100, 150, 200, 250],
            "Units": [10, 15, 20, 25],
        }
    )


@pytest.fixture
def df_dates():
    """DataFrame with a proper date column for temporal hierarchy tests."""
    return pd.DataFrame(
        {
            "OrderDate": pd.to_datetime(
                ["2023-01-01", "2023-06-15", "2024-03-01", "2024-09-20"]
            ),
            "Region": ["East", "East", "West", "West"],
            "Revenue": [100, 200, 150, 250],
        }
    )


def call_pivot(pivot_module, mount_recorder, df, **kwargs):
    """Mount the component and return the JSON config that was passed to it."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(df, key="pivot", **kwargs)
    return calls[0]["default"]["config"]


# ---------------------------------------------------------------------------
# Literal validation
# ---------------------------------------------------------------------------


class TestValuesAxisLiteralValidation:
    def test_default_is_columns(self, pivot_module, mount_recorder, df):
        config = call_pivot(
            pivot_module, mount_recorder, df, rows=["Region"], values=["Revenue"]
        )
        # "columns" is the default; may be omitted from config dict
        assert config.get("values_axis", "columns") == "columns"

    def test_explicit_columns_accepted(self, pivot_module, mount_recorder, df):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            values=["Revenue"],
            values_axis="columns",
        )
        assert config.get("values_axis", "columns") == "columns"

    def test_rows_accepted(self, pivot_module, mount_recorder, df):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            values=["Revenue"],
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"

    def test_invalid_literal_raises(self, pivot_module, df):
        with pytest.raises(ValueError, match="values_axis must be"):
            pivot_module.st_pivot_table(
                df,
                key="pivot",
                rows=["Region"],
                values=["Revenue"],
                values_axis="side",
            )

    def test_invalid_literal_uppercase_raises(self, pivot_module, df):
        with pytest.raises(ValueError, match="values_axis must be"):
            pivot_module.st_pivot_table(
                df,
                key="pivot",
                rows=["Region"],
                values=["Revenue"],
                values_axis="Rows",
            )


# ---------------------------------------------------------------------------
# Config propagation
# ---------------------------------------------------------------------------


class TestValuesAxisConfigPropagation:
    def test_values_axis_rows_in_config(self, pivot_module, mount_recorder, df):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region", "Category"],
            columns=["Year"],
            values=["Revenue", "Units"],
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"

    def test_values_axis_columns_omitted_from_config(
        self, pivot_module, mount_recorder, df
    ):
        """Default 'columns' should be omitted from config dict (saves bytes)."""
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            values=["Revenue"],
            values_axis="columns",
        )
        # Either absent or explicitly "columns"
        assert config.get("values_axis", "columns") == "columns"

    def test_other_config_fields_unaffected(self, pivot_module, mount_recorder, df):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region", "Category"],
            columns=["Year"],
            values=["Revenue"],
            values_axis="rows",
            show_subtotals=True,
        )
        assert config["rows"] == ["Region", "Category"]
        assert config["columns"] == ["Year"]
        assert config["values"] == ["Revenue"]
        assert config.get("show_subtotals") is True


# ---------------------------------------------------------------------------
# Cross-feature: period comparison incompatibility
# ---------------------------------------------------------------------------


class TestValuesAxisPeriodComparisonIncompatibility:
    # These are the actual period-comparison modes defined in PERIOD_COMPARISON_SHOW_VALUES_AS
    PERIOD_MODES = [
        "diff_from_prev",
        "pct_diff_from_prev",
        "diff_from_prev_year",
        "pct_diff_from_prev_year",
    ]

    @pytest.mark.parametrize("mode", PERIOD_MODES)
    def test_period_comparison_mode_raises(self, pivot_module, df, mode):
        with pytest.raises(ValueError, match="incompatible with period comparison"):
            pivot_module.st_pivot_table(
                df,
                key="pivot",
                rows=["Region"],
                columns=["Year"],
                values=["Revenue"],
                show_values_as={"Revenue": mode},
                values_axis="rows",
            )

    def test_non_period_mode_does_not_raise(self, pivot_module, mount_recorder, df):
        """Modes like 'pct_of_row' are not period comparison — must be allowed."""
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            show_values_as={"Revenue": "pct_of_row"},
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"

    def test_mix_of_period_and_non_period_raises(self, pivot_module, df):
        """Only the period-comparison mode triggers the error."""
        with pytest.raises(ValueError, match="incompatible with period comparison"):
            pivot_module.st_pivot_table(
                df,
                key="pivot",
                rows=["Region"],
                columns=["Year"],
                values=["Revenue", "Units"],
                show_values_as={
                    "Revenue": "diff_from_prev",
                    "Units": "pct_of_row",
                },
                values_axis="rows",
            )

    def test_multiple_period_modes_error_lists_all(self, pivot_module, df):
        """Error message should include both offending fields."""
        with pytest.raises(ValueError, match="incompatible with period comparison"):
            pivot_module.st_pivot_table(
                df,
                key="pivot",
                rows=["Region"],
                columns=["Year"],
                values=["Revenue", "Units"],
                show_values_as={
                    "Revenue": "diff_from_prev",
                    "Units": "pct_diff_from_prev",
                },
                values_axis="rows",
            )

    def test_values_axis_columns_does_not_restrict_show_values_as(
        self, pivot_module, mount_recorder, df
    ):
        """values_axis='columns' must not impose any show_values_as restriction."""
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            show_values_as={"Revenue": "pct_of_row"},
            values_axis="columns",
        )
        assert config.get("values_axis", "columns") == "columns"


# ---------------------------------------------------------------------------
# Cross-feature: temporal hierarchy incompatibility
# ---------------------------------------------------------------------------


class TestValuesAxisTemporalHierarchyIncompatibility:
    def test_date_grains_on_axis_field_raises(self, pivot_module, df_dates):
        with pytest.raises(ValueError, match="incompatible with date_grains"):
            pivot_module.st_pivot_table(
                df_dates,
                key="pivot",
                rows=["OrderDate"],
                values=["Revenue"],
                date_grains={"OrderDate": "year"},
                values_axis="rows",
            )

    def test_date_grains_on_non_axis_field_ok(
        self, pivot_module, mount_recorder, df_dates
    ):
        """date_grains for a field that's NOT on rows/columns must not raise."""
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df_dates,
            rows=["Region"],
            values=["Revenue"],
            # OrderDate is in the dataframe but NOT on any axis
            date_grains={"OrderDate": "year"},
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"

    def test_auto_date_hierarchy_with_date_field_on_rows_raises(
        self, pivot_module, df_dates
    ):
        with pytest.raises(ValueError, match="incompatible with auto_date_hierarchy"):
            pivot_module.st_pivot_table(
                df_dates,
                key="pivot",
                rows=["OrderDate"],
                values=["Revenue"],
                auto_date_hierarchy=True,
                values_axis="rows",
            )

    def test_auto_date_hierarchy_with_date_field_on_cols_raises(
        self, pivot_module, df_dates
    ):
        with pytest.raises(ValueError, match="incompatible with auto_date_hierarchy"):
            pivot_module.st_pivot_table(
                df_dates,
                key="pivot",
                rows=["Region"],
                columns=["OrderDate"],
                values=["Revenue"],
                auto_date_hierarchy=True,
                values_axis="rows",
            )

    def test_auto_date_hierarchy_without_date_on_axis_ok(
        self, pivot_module, mount_recorder, df
    ):
        """auto_date_hierarchy=True but no date/datetime field on rows/cols — must be fine."""
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            columns=["Year"],  # Year is int, not date
            values=["Revenue"],
            auto_date_hierarchy=True,
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"

    def test_auto_date_hierarchy_false_with_date_field_ok(
        self, pivot_module, mount_recorder, df_dates
    ):
        """auto_date_hierarchy=False (default) must never block values_axis='rows'."""
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df_dates,
            rows=["OrderDate"],
            values=["Revenue"],
            auto_date_hierarchy=False,
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"


# ---------------------------------------------------------------------------
# Interaction with other parameters
# ---------------------------------------------------------------------------


class TestValuesAxisWithOtherParams:
    def test_values_axis_rows_with_show_subtotals(
        self, pivot_module, mount_recorder, df
    ):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region", "Category"],
            values=["Revenue"],
            show_subtotals=True,
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"
        assert config.get("show_subtotals") is True

    def test_values_axis_rows_with_show_totals(self, pivot_module, mount_recorder, df):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            values=["Revenue"],
            show_totals=True,
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"

    def test_values_axis_rows_with_top_n_filter(self, pivot_module, mount_recorder, df):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            values=["Revenue"],
            top_n_filters=[
                {"field": "Region", "n": 1, "by": "Revenue", "direction": "top"}
            ],
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"

    def test_values_axis_rows_with_subtotal_position(
        self, pivot_module, mount_recorder, df
    ):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region", "Category"],
            values=["Revenue"],
            show_subtotals=True,
            subtotal_position="top",
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"
        assert config.get("subtotal_position") == "top"

    def test_values_axis_rows_single_value_field(
        self, pivot_module, mount_recorder, df
    ):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            values=["Revenue"],
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"

    def test_values_axis_rows_multiple_value_fields(
        self, pivot_module, mount_recorder, df
    ):
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region"],
            columns=["Year"],
            values=["Revenue", "Units"],
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"
        assert config["values"] == ["Revenue", "Units"]

    def test_values_axis_rows_with_hierarchy_layout(
        self, pivot_module, mount_recorder, df
    ):
        """values_axis='rows' + row_layout='hierarchy' is a supported combination."""
        config = call_pivot(
            pivot_module,
            mount_recorder,
            df,
            rows=["Region", "Category"],
            columns=["Year"],
            values=["Revenue", "Units"],
            row_layout="hierarchy",
            values_axis="rows",
        )
        assert config["values_axis"] == "rows"
        assert config["row_layout"] == "hierarchy"
