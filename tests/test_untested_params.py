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

"""Unit tests for st_pivot_table params that should round-trip in the mount payload."""

import pytest


def test_export_filename_my_report_passes_through(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        export_filename="my_report",
    )
    assert calls[0]["data"]["export_filename"] == "my_report"


def test_menu_limit_50_passes_through(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        menu_limit=50,
    )
    assert calls[0]["data"]["menu_limit"] == 50


@pytest.mark.parametrize("bad_limit", [0, -1, True])
def test_menu_limit_invalid_raises(sample_df, pivot_module, mount_recorder, bad_limit):
    mount_recorder()
    with pytest.raises(ValueError, match="menu_limit must be a positive integer"):
        pivot_module.st_pivot_table(
            sample_df,
            key="pivot",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            menu_limit=bad_limit,
        )


def test_sorters_year_order_passes_through(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        sorters={"Year": ["2024", "2023", "2022"]},
    )
    assert calls[0]["data"]["sorters"] == {"Year": ["2024", "2023", "2022"]}


def test_menu_limit_omitted_not_in_payload(sample_df, pivot_module, mount_recorder):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
    )
    assert "menu_limit" not in calls[0]["data"]


def test_export_filename_omitted_not_in_payload(
    sample_df, pivot_module, mount_recorder
):
    calls = mount_recorder()
    pivot_module.st_pivot_table(
        sample_df,
        key="pivot",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
    )
    assert "export_filename" not in calls[0]["data"]
