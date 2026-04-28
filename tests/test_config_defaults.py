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

import json

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


# ---------------------------------------------------------------------------
# style= serialization and default-wiring tests
# ---------------------------------------------------------------------------


def test_style_none_not_in_config(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    result = pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        style=None,
    )
    assert "style" not in result["config"]


def test_style_preset_lookup(sample_df, pivot_module, mount_recorder):
    mount_recorder()
    result = pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        style="striped",
    )
    assert "style" in result["config"]
    assert "stripe_color" in result["config"]["style"]


def test_style_roundtrips_through_json(pivot_module):
    full_style = pivot_module.PivotStyle(
        density="compact",
        font_size="13px",
        background_color="var(--st-background-color)",
        text_color="var(--st-text-color)",
        stripe_color="color-mix(in srgb, var(--st-text-color) 4%, transparent)",
        row_hover_color=None,
        borders="rows",
        border_color="red",
        column_header=pivot_module.RegionStyle(
            background_color="blue",
            text_color="white",
            font_weight="bold",
            vertical_align="top",
        ),
        data_cell_by_measure={
            "Revenue": pivot_module.RegionStyle(background_color="green")
        },
    )
    roundtripped = json.loads(json.dumps(full_style))
    assert roundtripped["density"] == "compact"
    assert roundtripped["borders"] == "rows"
    assert roundtripped["row_hover_color"] is None
    assert roundtripped["column_header"]["font_weight"] == "bold"
    assert roundtripped["column_header"]["vertical_align"] == "top"
    assert (
        roundtripped["data_cell_by_measure"]["Revenue"]["background_color"] == "green"
    )


def test_style_default_preset_equals_none_in_config(
    sample_df, pivot_module, mount_recorder
):
    """style='default' resolves to an empty dict which becomes None — same as style=None."""
    mount_recorder()
    result = pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue"],
        style="default",
    )
    assert "style" not in result["config"]


def test_style_none_disables_stripe_and_hover_serialized_as_null(pivot_module):
    """stripe_color=None and row_hover_color=None must serialize as JSON null."""
    style = pivot_module.PivotStyle(stripe_color=None, row_hover_color=None)
    serialized = json.dumps(style)
    parsed = json.loads(serialized)
    assert parsed["stripe_color"] is None
    assert parsed["row_hover_color"] is None
