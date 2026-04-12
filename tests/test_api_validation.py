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

"""Python-side validation tests for the public API."""

import pytest


@pytest.mark.parametrize(
    ("aggregation", "error"),
    [
        ("bogus", "aggregation must be one of"),
        ({"Revenue": "bogus"}, r"aggregation\['Revenue'\] must be one of"),
    ],
)
def test_invalid_aggregation_raises(pivot_module, sample_df, aggregation, error):
    with pytest.raises(ValueError, match=error):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            aggregation=aggregation,
        )


def test_missing_value_column_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="values contains columns not in DataFrame"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            values=["DoesNotExist"],
        )


def test_invalid_null_handling_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="null_handling must be one of"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            null_handling="invalid",
        )


def test_invalid_show_values_as_raises(pivot_module, sample_df):
    with pytest.raises(
        ValueError, match="show_values_as\\['Revenue'\\] must be one of"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            show_values_as={"Revenue": "invalid"},
        )


def test_period_comparison_requires_grouped_temporal_axis(pivot_module, sample_df):
    with pytest.raises(
        ValueError,
        match="period comparison show_values_as modes require at least one date_grains entry",
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            show_values_as={"Revenue": "diff_from_prev"},
        )


def test_invalid_number_format_type_raises(pivot_module, sample_df):
    with pytest.raises(TypeError, match="number_format must be str, dict, or None"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            number_format=123,
        )


def test_invalid_column_alignment_raises(pivot_module, sample_df):
    with pytest.raises(
        ValueError, match="column_alignment\\['Revenue'\\] must be one of"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            column_alignment={"Revenue": "diagonal"},
        )


def test_invalid_source_filters_type_raises(pivot_module, sample_df):
    with pytest.raises(TypeError, match="source_filters must be a dict or None"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            source_filters=["not-a-dict"],
        )


def test_source_filters_unknown_column_raises(pivot_module, sample_df):
    with pytest.raises(
        ValueError, match="source_filters contains column not in DataFrame"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            source_filters={"Missing": {"include": ["x"]}},
        )


def test_source_filters_non_list_operand_raises(pivot_module, sample_df):
    with pytest.raises(
        TypeError, match=r"source_filters\['Region'\]\['include'\] must be a list"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            source_filters={"Region": {"include": "East"}},
        )


def test_source_filters_non_scalar_value_raises(pivot_module, sample_df):
    with pytest.raises(
        TypeError,
        match=r"source_filters\['Region'\]\['include'\]\[0\] must be a scalar value",
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            source_filters={"Region": {"include": [["East"]]}},
        )


def test_duplicate_synthetic_measure_ids_raise(pivot_module, sample_df):
    with pytest.raises(ValueError, match="duplicate synthetic_measures id"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            synthetic_measures=[
                {
                    "id": "margin",
                    "label": "Margin 1",
                    "operation": "difference",
                    "numerator": "Revenue",
                    "denominator": "Profit",
                },
                {
                    "id": "margin",
                    "label": "Margin 2",
                    "operation": "difference",
                    "numerator": "Revenue",
                    "denominator": "Profit",
                },
            ],
        )


@pytest.mark.parametrize("menu_limit", [0, True])
def test_invalid_menu_limit_raises(pivot_module, sample_df, menu_limit):
    with pytest.raises(ValueError, match="menu_limit must be a positive integer"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            menu_limit=menu_limit,
        )
