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

"""Shared Playwright fixtures and pytest hooks for E2E tests."""

from typing import Any, Dict, Generator

import pytest
from playwright.sync_api import Browser, BrowserContext, Page


@pytest.fixture(scope="function")
def context(browser: Browser) -> Generator[BrowserContext, None, None]:
    """Create a new browser context with custom settings."""
    context_options: Dict[str, Any] = {
        "accept_downloads": True,
        "service_workers": "block"
        if browser.browser_type.name == "chromium"
        else "allow",
        "permissions": ["clipboard-read", "clipboard-write"],
    }

    context = browser.new_context(**context_options)
    context.set_default_timeout(30000)

    yield context
    context.close()


@pytest.fixture(scope="function")
def page(context: BrowserContext) -> Generator[Page, None, None]:
    """Create a new page with console/error logging for debugging."""
    page = context.new_page()

    def handle_console(msg):
        if msg.type == "error":
            print(f"[CONSOLE ERROR] {msg.text}")
            if msg.location:
                print(f"  at {msg.location}")
        elif msg.type == "warning":
            print(f"[CONSOLE WARNING] {msg.text}")

    page.on("console", handle_console)
    page.on("pageerror", lambda err: print(f"[PAGE ERROR] {err}"))

    yield page
    page.close()


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers",
        "chromium_only: marks tests that require Chromium clipboard API support",
    )


def pytest_collection_modifyitems(config, items):
    """Auto-skip chromium_only tests when running in non-Chromium browsers."""
    for item in items:
        if "chromium_only" in item.keywords:
            if hasattr(item, "callspec") and "browser" in item.callspec.params:
                browser_name = item.callspec.params["browser"]
            else:
                browser_name = config.getoption("--browser", default=["chromium"])
                if isinstance(browser_name, list):
                    browser_name = browser_name[0]
            if browser_name != "chromium":
                item.add_marker(
                    pytest.mark.skip(reason="Clipboard API only works in Chromium")
                )


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Add browser context to test reports on failure."""
    outcome = yield
    rep = outcome.get_result()

    if hasattr(item, "funcargs") and "page" in item.funcargs:
        page = item.funcargs["page"]
        browser_name = page.context.browser.browser_type.name
        rep.browser = browser_name

        if rep.failed and call.when == "call":
            print(f"\n[FAILED in {browser_name}] Test: {item.name}")
            try:
                print(f"Page title: {page.title()}")
                print(f"Page URL: {page.url}")
                print(f"Number of frames: {len(page.frames)}")
            except Exception as e:
                print(f"Could not get page info: {e}")
