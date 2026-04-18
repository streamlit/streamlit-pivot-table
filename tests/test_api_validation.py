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
