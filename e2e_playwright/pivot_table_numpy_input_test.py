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

"""Browser regression tests for numpy-backed config inputs."""

from playwright.sync_api import Page, expect

from e2e_utils import get_pivot


def test_numpy_backed_configs_render_without_frontend_validation_errors(
    page_at_app: Page,
):
    page = page_at_app

    raw_array = get_pivot(page, "numpy_array_pivot")
    expect(raw_array.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    expect(raw_array.get_by_test_id("pivot-data-cell").first).to_be_visible(
        timeout=5000
    )
    expect(raw_array.get_by_test_id("toolbar-rows-chips")).to_contain_text("Region")
    expect(raw_array.get_by_test_id("toolbar-columns-chips")).to_contain_text("Year")
    expect(raw_array.get_by_test_id("toolbar-values-chips")).to_contain_text("Revenue")

    numpy_str_list = get_pivot(page, "numpy_str_list_pivot")
    expect(numpy_str_list.get_by_test_id("pivot-table")).to_be_visible(timeout=15000)
    expect(numpy_str_list.get_by_test_id("pivot-data-cell").first).to_be_visible(
        timeout=5000
    )
    expect(numpy_str_list.get_by_test_id("toolbar-rows-chips")).to_contain_text(
        "Region"
    )
    expect(numpy_str_list.get_by_test_id("toolbar-columns-chips")).to_contain_text(
        "Year"
    )
    expect(numpy_str_list.get_by_test_id("toolbar-values-chips")).to_contain_text(
        "Revenue"
    )

    expect(page.locator("text=must contain only strings")).to_have_count(0)
