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

import json
from pathlib import Path

import numpy as np
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


def test_accepts_numpy_arrays_for_rows_columns_and_values(
    pivot_module, sample_df, mount_recorder
):
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=np.array(["Region"]),
        columns=np.array(["Year"]),
        values=np.array(["Revenue"]),
    )

    sent_config = calls[0]["data"]["config"]
    assert sent_config["rows"] == ["Region"]
    assert sent_config["columns"] == ["Year"]
    assert sent_config["values"] == ["Revenue"]


def test_numpy_value_arrays_still_reject_non_string_entries(pivot_module, sample_df):
    with pytest.raises(TypeError, match="values must be a list of strings"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=np.array([1]),
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
        match="period comparison show_values_as modes require either auto_date_hierarchy=True",
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            auto_date_hierarchy=False,
            show_values_as={"Revenue": "diff_from_prev"},
        )


# ---------------------------------------------------------------------------
# 0.5.0 — analytical show_values_as modes passthrough
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode",
    ["running_total", "pct_running_total", "rank", "pct_of_parent", "index"],
)
def test_analytical_show_values_as_mode_accepted(
    pivot_module, sample_df, mount_recorder, mode
):
    """Each 0.5.0 analytical mode passes Python validation and is forwarded verbatim."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        show_values_as={"Revenue": mode},
    )
    cfg = calls[0]["data"]["config"]
    assert cfg["show_values_as"]["Revenue"] == mode


def test_analytical_show_values_as_multi_measure_accepted(
    pivot_module, sample_df, mount_recorder
):
    """Multiple analytical modes can be mixed across measures in a single call."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        values=["Revenue", "Profit"],
        show_values_as={"Revenue": "running_total", "Profit": "rank"},
    )
    cfg = calls[0]["data"]["config"]
    assert cfg["show_values_as"]["Revenue"] == "running_total"
    assert cfg["show_values_as"]["Profit"] == "rank"


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


def test_formula_syntax_error_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="is invalid"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            synthetic_measures=[
                {
                    "id": "bad",
                    "label": "Bad",
                    "operation": "formula",
                    "formula": '"Revenue" + * "Profit"',
                },
            ],
        )


def test_formula_unknown_column_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="references unknown columns"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            synthetic_measures=[
                {
                    "id": "typo",
                    "label": "Typo",
                    "operation": "formula",
                    "formula": '"Revenue" - "Cosst"',
                },
            ],
        )


def test_formula_wrong_arity_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="expects exactly 2 argument"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            synthetic_measures=[
                {
                    "id": "bad",
                    "label": "Bad",
                    "operation": "formula",
                    "formula": 'min("Revenue", "Profit", "Year")',
                },
            ],
        )


def test_formula_bare_identifier_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="Unknown identifier"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            synthetic_measures=[
                {
                    "id": "bad",
                    "label": "Bad",
                    "operation": "formula",
                    "formula": 'Revenue + "Profit"',
                },
            ],
        )


def test_formula_valid_accepted(pivot_module, sample_df):
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        synthetic_measures=[
            {
                "id": "margin",
                "label": "Margin",
                "operation": "formula",
                "formula": '"Revenue" - "Profit"',
            },
        ],
    )


def test_formula_if_valid(pivot_module, sample_df):
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        synthetic_measures=[
            {
                "id": "ratio",
                "label": "Ratio",
                "operation": "formula",
                "formula": 'if("Profit" > 0, "Revenue" / "Profit", 0)',
            },
        ],
    )


def test_formula_leading_and_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="is invalid"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            synthetic_measures=[
                {
                    "id": "bad",
                    "label": "Bad",
                    "operation": "formula",
                    "formula": 'and "Revenue"',
                },
            ],
        )


def test_formula_dangling_and_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="is invalid"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            synthetic_measures=[
                {
                    "id": "bad",
                    "label": "Bad",
                    "operation": "formula",
                    "formula": '"Revenue" + and "Profit"',
                },
            ],
        )


def test_formula_caret_exponentiation_valid(pivot_module, sample_df):
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        synthetic_measures=[
            {
                "id": "sq",
                "label": "Squared",
                "operation": "formula",
                "formula": '"Revenue" ^ 2 + "Profit" ^ 2',
            },
        ],
    )


def test_formula_double_star_rejected(pivot_module, sample_df):
    with pytest.raises(ValueError, match="is invalid"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            synthetic_measures=[
                {
                    "id": "sq",
                    "label": "Squared",
                    "operation": "formula",
                    "formula": '"Revenue" ** 2',
                },
            ],
        )


def test_formula_trailing_operator_rejected(pivot_module, sample_df):
    with pytest.raises(ValueError, match="is invalid"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            synthetic_measures=[
                {
                    "id": "bad",
                    "label": "Bad",
                    "operation": "formula",
                    "formula": '"Revenue" +',
                },
            ],
        )


def test_formula_not_valid(pivot_module, sample_df):
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        synthetic_measures=[
            {
                "id": "inv",
                "label": "Inverted",
                "operation": "formula",
                "formula": 'if(not "Revenue", 1, 0)',
            },
        ],
    )


_GRAMMAR_CASES = json.loads(
    (Path(__file__).parent / "formula_grammar_cases.json").read_text()
)


@pytest.mark.parametrize(
    "case",
    _GRAMMAR_CASES,
    ids=[c["note"] for c in _GRAMMAR_CASES],
)
def test_formula_grammar_parity(pivot_module, case):
    """Shared grammar corpus — must agree with TS compileFormula tests."""
    validate = pivot_module._validate_formula
    if case["valid"]:
        refs = validate(case["formula"])
        assert isinstance(refs, list)
        assert len(refs) > 0
    else:
        with pytest.raises(ValueError):
            validate(case["formula"])


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


# ---------------------------------------------------------------------------
# conditional_formatting color_scale — mid_value validation
# ---------------------------------------------------------------------------


def test_color_scale_accepts_mid_value_with_mid_color(
    pivot_module, sample_df, mount_recorder
):
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        conditional_formatting=[
            {
                "type": "color_scale",
                "apply_to": ["Revenue"],
                "min_color": "#ff0000",
                "mid_color": "#ffffff",
                "max_color": "#0000ff",
                "mid_value": 0,
            },
        ],
    )

    sent_config = calls[0]["data"]["config"]
    rule = sent_config["conditional_formatting"][0]
    assert rule["mid_value"] == 0
    assert rule["mid_color"] == "#ffffff"


def test_color_scale_mid_value_requires_mid_color(pivot_module, sample_df):
    with pytest.raises(ValueError, match="'mid_value' requires 'mid_color'"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            conditional_formatting=[
                {
                    "type": "color_scale",
                    "apply_to": ["Revenue"],
                    "min_color": "#ff0000",
                    "max_color": "#0000ff",
                    "mid_value": 0,
                },
            ],
        )


@pytest.mark.parametrize(
    "bad_mid_value",
    [
        True,  # bool (isinstance(True, int) is True in Python)
        False,
        "0",  # string
        float("nan"),
        float("inf"),
        float("-inf"),
        [0],  # list
    ],
)
def test_color_scale_mid_value_rejects_non_finite_number(
    pivot_module, sample_df, bad_mid_value
):
    with pytest.raises(TypeError, match=r"\['mid_value'\] must be a finite number"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            conditional_formatting=[
                {
                    "type": "color_scale",
                    "apply_to": ["Revenue"],
                    "min_color": "#ff0000",
                    "mid_color": "#ffffff",
                    "max_color": "#0000ff",
                    "mid_value": bad_mid_value,
                },
            ],
        )


def test_color_scale_mid_color_must_be_string(pivot_module, sample_df):
    with pytest.raises(TypeError, match=r"\['mid_color'\] must be a string"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            conditional_formatting=[
                {
                    "type": "color_scale",
                    "apply_to": ["Revenue"],
                    "min_color": "#ff0000",
                    "mid_color": 123,
                    "max_color": "#0000ff",
                },
            ],
        )


@pytest.mark.parametrize("valid_scope", ["global", "per_column"])
def test_conditional_formatting_scope_accepts_valid_values(
    pivot_module, sample_df, mount_recorder, valid_scope
):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        conditional_formatting=[
            {
                "type": "data_bars",
                "apply_to": ["Revenue"],
                "fill": "gradient",
                "scope": valid_scope,
            },
        ],
    )
    rule = calls[0]["data"]["config"]["conditional_formatting"][0]
    assert rule["scope"] == valid_scope


def test_conditional_formatting_scope_rejects_invalid_value(pivot_module, sample_df):
    with pytest.raises(ValueError, match=r"\['scope'\] must be"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            conditional_formatting=[
                {
                    "type": "color_scale",
                    "apply_to": ["Revenue"],
                    "min_color": "#ffffff",
                    "max_color": "#ff0000",
                    "scope": "by_row",  # invalid
                },
            ],
        )


def test_color_scale_mid_value_none_is_ignored(pivot_module, sample_df, mount_recorder):
    """Passing mid_value=None alongside mid_color is treated as omitted."""
    calls = mount_recorder()

    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        conditional_formatting=[
            {
                "type": "color_scale",
                "apply_to": ["Revenue"],
                "min_color": "#ff0000",
                "mid_color": "#ffffff",
                "max_color": "#0000ff",
                "mid_value": None,
            },
        ],
    )

    sent_config = calls[0]["data"]["config"]
    rule = sent_config["conditional_formatting"][0]
    assert rule.get("mid_value") is None


# ---------------------------------------------------------------------------
# style= validation tests
# ---------------------------------------------------------------------------


def test_style_invalid_preset_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="unknown preset"):
        pivot_module._resolve_style("nonexistent_preset")


def test_style_invalid_density_raises(pivot_module):
    with pytest.raises(ValueError, match="density"):
        pivot_module._resolve_style({"density": "ultracompact"})


def test_style_invalid_borders_raises(pivot_module):
    with pytest.raises(ValueError, match="borders"):
        pivot_module._resolve_style({"borders": "All"})  # case-sensitive


def test_style_invalid_font_weight_raises(pivot_module):
    with pytest.raises(ValueError, match="font_weight"):
        pivot_module._resolve_style({"column_header": {"font_weight": "700"}})


def test_style_invalid_font_size_raises(pivot_module):
    with pytest.raises(ValueError, match="font_size"):
        pivot_module._resolve_style({"font_size": "13"})  # missing unit


def test_style_font_size_accepts_css_functions(pivot_module):
    """calc(), clamp(), min(), max() are valid CSS size values and must not raise."""
    for value in [
        "calc(1rem + 2px)",
        "clamp(12px, 2vw, 16px)",
        "min(14px, 1.2em)",
        "max(12px, 0.8rem)",
    ]:
        result = pivot_module._resolve_style({"font_size": value})
        assert result is not None
        assert result["font_size"] == value


def test_style_invalid_color_empty_raises(pivot_module):
    with pytest.raises(ValueError, match="non-empty string"):
        pivot_module._resolve_style({"background_color": ""})


def test_style_composition_merges_regions(pivot_module):
    result = pivot_module._resolve_style(
        [
            {"column_header": {"font_weight": "bold"}},
            {"column_header": {"background_color": "#000"}},
        ]
    )
    assert result is not None
    assert result["column_header"]["font_weight"] == "bold"
    assert result["column_header"]["background_color"] == "#000"


def test_style_composition_merges_data_cell_by_measure(pivot_module):
    result = pivot_module._resolve_style(
        [
            {"data_cell_by_measure": {"Revenue": {"background_color": "red"}}},
            {
                "data_cell_by_measure": {
                    "Revenue": {"font_weight": "bold"},
                    "Profit": {"text_color": "green"},
                }
            },
        ]
    )
    assert result is not None
    dcbm = result["data_cell_by_measure"]
    assert dcbm["Revenue"]["background_color"] == "red"
    assert dcbm["Revenue"]["font_weight"] == "bold"
    assert dcbm["Profit"]["text_color"] == "green"


def test_style_data_cell_by_measure_unknown_field_warns(
    pivot_module, sample_df, mount_recorder
):
    mount_recorder()
    import warnings

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            style={
                "data_cell_by_measure": {"NonExistent": {"background_color": "red"}}
            },
        )
    messages = [str(warning.message) for warning in w]
    assert any("NonExistent" in m for m in messages)


def test_style_data_cell_by_measure_invalid_value_raises(pivot_module):
    with pytest.raises(TypeError):
        pivot_module._resolve_style({"data_cell_by_measure": {"Revenue": "not_a_dict"}})


def test_style_unknown_top_level_key_warns(pivot_module):
    import warnings

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        pivot_module._resolve_style({"unknown_key_xyz": "value"})
    messages = [str(warning.message) for warning in w]
    assert any("unknown_key_xyz" in m for m in messages)


def test_style_unknown_region_key_warns(pivot_module):
    import warnings

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        pivot_module._resolve_style({"column_header": {"unknown_region_key": "val"}})
    messages = [str(warning.message) for warning in w]
    assert any("unknown_region_key" in m for m in messages)


def test_style_composition_none_overridden_by_later_value(pivot_module):
    """Later item (string value) wins over earlier None sentinel."""
    result = pivot_module._resolve_style(["minimal", {"row_hover_color": "blue"}])
    assert result is not None
    assert result.get("row_hover_color") == "blue"


def test_style_composition_value_overridden_by_later_none(pivot_module):
    """Later None sentinel wins over earlier color string."""
    result = pivot_module._resolve_style(
        [{"stripe_color": "red"}, {"stripe_color": None}]
    )
    assert result is not None
    assert result.get("stripe_color") is None


# ---------------------------------------------------------------------------
# 0.5.0 — top_n_filters and value_filters validation
# ---------------------------------------------------------------------------


def test_top_n_filters_valid_passthrough(pivot_module, sample_df, mount_recorder):
    """Valid top_n_filters are accepted and passed to the config."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue"],
        top_n_filters=[
            {"field": "Region", "n": 2, "by": "Revenue", "direction": "top"}
        ],
    )
    cfg = calls[0]["data"]["config"]
    assert cfg["top_n_filters"] == [
        {"field": "Region", "n": 2, "by": "Revenue", "direction": "top"}
    ]


def test_top_n_filters_with_axis_column(pivot_module, sample_df, mount_recorder):
    """top_n_filters with axis='columns' is accepted for column dimensions."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue"],
        top_n_filters=[
            {
                "field": "Category",
                "n": 1,
                "by": "Revenue",
                "direction": "bottom",
                "axis": "columns",
            }
        ],
    )
    cfg = calls[0]["data"]["config"]
    assert cfg["top_n_filters"][0]["axis"] == "columns"


def test_top_n_filters_invalid_direction_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="direction.*must be one of"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Category"],
            values=["Revenue"],
            top_n_filters=[
                {"field": "Region", "n": 3, "by": "Revenue", "direction": "middle"}
            ],
        )


def test_top_n_filters_invalid_n_raises(pivot_module, sample_df):
    with pytest.raises(TypeError, match=r"top_n_filters\[0\]\['n'\].*positive integer"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Category"],
            values=["Revenue"],
            top_n_filters=[
                {"field": "Region", "n": 0, "by": "Revenue", "direction": "top"}
            ],
        )


def test_top_n_filters_field_not_in_rows_raises(pivot_module, sample_df):
    with pytest.raises(
        ValueError, match=r"top_n_filters\[0\]\['field'\].*not in the rows"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Category"],
            values=["Revenue"],
            top_n_filters=[
                {"field": "Category", "n": 2, "by": "Revenue", "direction": "top"}
            ],
        )


def test_top_n_filters_by_not_in_values_raises(pivot_module, sample_df):
    with pytest.raises(
        ValueError, match=r"top_n_filters\[0\]\['by'\].*not in the available"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Category"],
            values=["Revenue"],
            top_n_filters=[
                {"field": "Region", "n": 2, "by": "DoesNotExist", "direction": "top"}
            ],
        )


def test_value_filters_valid_passthrough(pivot_module, sample_df, mount_recorder):
    """Valid value_filters are accepted and passed to the config."""
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue"],
        value_filters=[
            {"field": "Region", "by": "Revenue", "operator": "gt", "value": 100}
        ],
    )
    cfg = calls[0]["data"]["config"]
    assert cfg["value_filters"][0]["operator"] == "gt"


def test_value_filters_between_requires_value2(pivot_module, sample_df):
    with pytest.raises(ValueError, match="'between' requires 'value2'"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Category"],
            values=["Revenue"],
            value_filters=[
                {
                    "field": "Region",
                    "by": "Revenue",
                    "operator": "between",
                    "value": 100,
                    # value2 intentionally missing
                }
            ],
        )


def test_value_filters_invalid_operator_raises(pivot_module, sample_df):
    with pytest.raises(ValueError, match="operator.*must be one of"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Category"],
            values=["Revenue"],
            value_filters=[
                {
                    "field": "Region",
                    "by": "Revenue",
                    "operator": "not_an_op",
                    "value": 0,
                }
            ],
        )


def test_value_filters_field_not_in_rows_raises(pivot_module, sample_df):
    with pytest.raises(
        ValueError, match=r"value_filters\[0\]\['field'\].*not in the rows"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Category"],
            values=["Revenue"],
            value_filters=[
                {"field": "Category", "by": "Revenue", "operator": "gt", "value": 0}
            ],
        )


def test_value_filters_by_not_in_values_raises(pivot_module, sample_df):
    with pytest.raises(
        ValueError, match=r"value_filters\[0\]\['by'\].*not in the available"
    ):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Category"],
            values=["Revenue"],
            value_filters=[
                {"field": "Region", "by": "NoSuchMeasure", "operator": "gt", "value": 0}
            ],
        )


def test_filters_do_not_alter_sidecar_fingerprint(pivot_module, sample_df):
    """top_n_filters and value_filters must NOT change the hybrid sidecar fingerprint."""
    base_fp = pivot_module._build_sidecar_fingerprint(
        {
            "rows": ["Region"],
            "columns": ["Category"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "sum"},
        },
        None,
    )
    # Adding top_n_filters to a config dict does NOT affect the fingerprint since
    # _build_sidecar_fingerprint uses explicit key inclusion and does not list these keys.
    filtered_fp = pivot_module._build_sidecar_fingerprint(
        {
            "rows": ["Region"],
            "columns": ["Category"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "sum"},
            "top_n_filters": [
                {"field": "Region", "n": 2, "by": "Revenue", "direction": "top"}
            ],
            "value_filters": [
                {"field": "Region", "by": "Revenue", "operator": "gt", "value": 100}
            ],
        },
        None,
    )
    assert (
        base_fp == filtered_fp
    ), "top_n_filters / value_filters must not alter the sidecar fingerprint"


def test_style_data_cell_by_measure_with_null_values_no_crash(
    pivot_module, mount_recorder
):
    """data_cell_by_measure key warning must not crash when values=None.

    Regression test for the set(resolved_values) bug: if the user provides
    rows+columns but not values (so auto-detect is bypassed and resolved_values
    stays None), and also provides data_cell_by_measure, the warning code path
    must use set(resolved_values or []) and not raise TypeError.
    """
    import pandas as pd

    df = pd.DataFrame(
        {
            "Region": ["East", "West"],
            "Category": ["A", "B"],
            "Revenue": [100, 200],
        }
    )
    mount_recorder()
    import warnings

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        # Provide rows+columns but NOT values — resolved_values stays None
        pivot_module.st_pivot_table(
            df,
            key="pivot",
            rows=["Region"],
            columns=["Category"],
            # values intentionally omitted
            style={"data_cell_by_measure": {"Revenue": {"background_color": "red"}}},
        )
