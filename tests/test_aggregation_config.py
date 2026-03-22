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

"""Python-side tests for aggregation normalization and config hydration."""

from importlib import import_module
from unittest.mock import patch


with patch("streamlit.components.v2.component", return_value=lambda **_kwargs: None):
    pivot_module = import_module("streamlit_pivot_table")

_default_config = pivot_module._default_config
_normalize_aggregation_config = pivot_module._normalize_aggregation_config
_normalize_config_aggregation = pivot_module._normalize_config_aggregation
_resolve_config_to_send = pivot_module._resolve_config_to_send


def test_normalize_aggregation_config_defaults_missing_values_to_sum():
    assert _normalize_aggregation_config(None, ["Revenue", "Profit"]) == {
        "Revenue": "sum",
        "Profit": "sum",
    }


def test_normalize_aggregation_config_fills_missing_map_entries():
    assert _normalize_aggregation_config(
        {"Revenue": "count"}, ["Revenue", "Profit"]
    ) == {
        "Revenue": "count",
        "Profit": "sum",
    }


def test_normalize_config_aggregation_normalizes_persisted_scalar_shape():
    normalized = _normalize_config_aggregation(
        {
            "values": ["Revenue", "Profit"],
            "aggregation": "avg",
        }
    )

    assert normalized["aggregation"] == {
        "Revenue": "avg",
        "Profit": "avg",
    }


def test_resolve_config_to_send_preserves_persisted_config_when_python_unchanged():
    session_state: dict[str, object] = {}
    initial_config = _default_config(
        values=["Revenue", "Profit"],
        aggregation="sum",
    )

    first = _resolve_config_to_send(session_state, "pivot", initial_config)
    assert first["aggregation"] == {"Revenue": "sum", "Profit": "sum"}

    session_state["pivot"] = {
        "config": {
            **initial_config,
            "aggregation": "min",
        }
    }

    resolved = _resolve_config_to_send(session_state, "pivot", initial_config)
    assert resolved["aggregation"] == {"Revenue": "min", "Profit": "min"}


def test_resolve_config_to_send_prefers_python_when_python_config_changes():
    initial_sum = _default_config(
        values=["Revenue", "Profit"],
        aggregation="sum",
    )
    changed_avg = _default_config(
        values=["Revenue", "Profit"],
        aggregation="avg",
    )
    session_state: dict[str, object] = {
        "pivot": {
            "config": {
                **initial_sum,
                "aggregation": {"Revenue": "sum", "Profit": "count"},
            }
        }
    }

    _resolve_config_to_send(session_state, "pivot", initial_sum)
    resolved = _resolve_config_to_send(session_state, "pivot", changed_avg)

    assert resolved["aggregation"] == {"Revenue": "avg", "Profit": "avg"}
