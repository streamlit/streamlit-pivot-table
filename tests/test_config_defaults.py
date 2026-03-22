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

"""Python-side defaulting and auto-detection tests."""

import pandas as pd


def test_auto_detects_dimensions_and_measures(pivot_module, mount_recorder):
    calls = mount_recorder()
    df = pd.DataFrame(
        {
            "Region": ["East"] * 12 + ["West"] * 13,
            "Year": [2023, 2024] * 12 + [2023],
            "Revenue": list(range(25)),
            "Profit": list(range(100, 125)),
        }
    )

    result = pivot_module.st_pivot_table(df, key="pivot")
    config = result["config"]

    assert config["rows"] == ["Region"]
    assert config["columns"] == ["Year"]
    assert config["values"] == ["Revenue", "Profit"]
    assert config["aggregation"] == {"Revenue": "sum", "Profit": "sum"}
    assert calls[0]["data"]["config"]["values"] == ["Revenue", "Profit"]


def test_number_format_string_normalizes_to_all(
    sample_df, pivot_module, mount_recorder
):
    mount_recorder()

    result = pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        number_format="$,.0f",
    )

    assert result["config"]["number_format"] == {"__all__": "$,.0f"}


def test_partial_totals_lists_are_filtered_and_normalized(
    sample_df, pivot_module, mount_recorder
):
    mount_recorder()

    result = pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        show_row_totals=["Revenue", "Missing"],
        show_column_totals=["Revenue", "Profit"],
    )

    assert result["config"]["show_row_totals"] == ["Revenue"]
    assert result["config"]["show_column_totals"] is True


def test_scalar_aggregation_normalizes_for_multiple_values(
    sample_df, pivot_module, mount_recorder
):
    mount_recorder()

    result = pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        aggregation="avg",
    )

    assert result["config"]["aggregation"] == {"Revenue": "avg", "Profit": "avg"}
