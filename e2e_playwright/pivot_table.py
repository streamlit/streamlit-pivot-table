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

"""Manual E2E test app that can render one suite at a time."""

from __future__ import annotations

import streamlit as st

from pivot_table_app_support import init_page, load_data
from pivot_table_data_app import render_app as render_data_app
from pivot_table_interactions_app import render_app as render_interactions_app
from pivot_table_toolbar_app import render_app as render_toolbar_app

init_page()
data = load_data()

suite = st.radio(
    "E2E suite",
    options=["toolbar", "interactions", "data"],
    horizontal=True,
)

if suite == "toolbar":
    render_toolbar_app(data)
elif suite == "interactions":
    render_interactions_app(data)
else:
    render_data_app(data)
