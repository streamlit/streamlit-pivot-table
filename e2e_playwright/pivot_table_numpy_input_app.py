#!/usr/bin/env python
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

"""Streamlit app covering numpy-backed config inputs for Playwright tests."""

from __future__ import annotations

from typing import Any, cast

import numpy as np
import streamlit as st

from streamlit_pivot import st_pivot_table

from pivot_table_app_support import init_page, load_data


def main() -> None:
    init_page()
    df = load_data()["df"]
    raw_rows = cast(Any, np.array(["Region"]))
    raw_columns = cast(Any, np.array(["Year"]))
    raw_values = cast(Any, np.array(["Revenue"]))

    st.subheader("Raw numpy.ndarray config")
    st_pivot_table(
        df,
        key="numpy_array_pivot",
        rows=raw_rows,
        columns=raw_columns,
        values=raw_values,
        aggregation="sum",
        interactive=True,
    )

    st.subheader("list(np.array(...)) config")
    st_pivot_table(
        df,
        key="numpy_str_list_pivot",
        rows=list(np.array(["Region"])),
        columns=list(np.array(["Year"])),
        values=list(np.array(["Revenue"])),
        aggregation="sum",
        interactive=True,
    )


if __name__ == "__main__":
    main()
