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

"""Shared fixtures for Python-side unit tests."""

from importlib import import_module
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def pivot_module():
    with patch(
        "streamlit.components.v2.component", return_value=lambda **_kwargs: None
    ):
        return import_module("streamlit_pivot")


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "Region": ["East", "East", "West", "West"],
            "Category": ["A", "B", "A", "B"],
            "Year": [2023, 2024, 2023, 2024],
            "Revenue": [100, 150, 200, 250],
            "Profit": [10, 20, 30, 40],
        }
    )


@pytest.fixture
def mount_recorder(pivot_module, monkeypatch):
    def _mount(session_state=None):
        calls = []

        def fake_component(**kwargs):
            calls.append(kwargs)
            return {"config": kwargs["default"]["config"]}

        monkeypatch.setattr(pivot_module, "_component", fake_component)
        monkeypatch.setattr(
            pivot_module,
            "convert_anything_to_pandas_df",
            lambda data: data if isinstance(data, pd.DataFrame) else pd.DataFrame(data),
        )
        monkeypatch.setattr(
            pivot_module,
            "st",
            SimpleNamespace(session_state=session_state or {}),
        )
        return calls

    return _mount
