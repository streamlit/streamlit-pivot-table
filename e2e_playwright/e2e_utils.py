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

"""Utilities for running Streamlit apps in E2E tests.

Provides StreamlitRunner, a context manager that starts a Streamlit server
in a subprocess, waits for it to become healthy, and tears it down on exit.
Also provides shared Playwright locator helpers used across E2E test modules.
"""

import contextlib
import logging
import os
import re
import shlex
import socket
import subprocess
import sys
import time
import typing
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryFile

import requests
from playwright.sync_api import Locator, Page, expect

LOGGER = logging.getLogger(__file__)

SCRIPT = Path(__file__).parent / "pivot_table.py"
TOOLBAR_SCRIPT = Path(__file__).parent / "pivot_table_toolbar_app.py"
INTERACTIONS_SCRIPT = Path(__file__).parent / "pivot_table_interactions_app.py"
DATA_SCRIPT = Path(__file__).parent / "pivot_table_data_app.py"
GOLDEN_SCRIPT = Path(__file__).parent / "pivot_table_golden_app.py"
COLUMN_CONFIG_SCRIPT = Path(__file__).parent / "pivot_table_column_config_app.py"
STYLING_SCRIPT = Path(__file__).parent / "pivot_table_styling_app.py"
FILTERS_SCRIPT = Path(__file__).parent / "pivot_table_filters_app.py"
NEW_FEATURES_SCRIPT = Path(__file__).parent / "pivot_table_new_features_app.py"

PIVOT_KEYS = [
    "test_pivot",
    "test_pivot_subtotals",
    "test_pivot_locked",
    "test_pivot_locked_groups",
    "test_pivot_cond_fmt",
    "test_pivot_readonly",
    "test_pivot_number_fmt",
    "test_pivot_drilldown",
    "test_pivot_empty",
    "test_pivot_single_row",
    "test_pivot_no_cols",
    "test_pivot_count_distinct",
    "test_pivot_median",
    "test_pivot_auto",
    "test_pivot_threshold",
    "test_pivot_col_groups",
    "test_pivot_alignment",
    "test_pivot_tall",
    "test_pivot_null_separate",
    "test_pivot_null_zero",
    "test_pivot_dim_toggle",
    "test_pivot_no_drilldown",
    "test_pivot_per_dim_subtotals",
    "test_pivot_per_measure_row_totals",
    "test_pivot_per_measure_col_totals",
    "test_pivot_sparse_drilldown",
    "test_pivot_synthetic",
    "test_pivot_scalar_roundtrip",
]

TOOLBAR_PIVOT_KEYS = [
    "test_pivot",
    "test_pivot_subtotals",
    "test_pivot_cond_fmt",
    "test_pivot_scalar_roundtrip",
    "test_pivot_numpy_list",
]

# Must match the exact order of st_pivot_table(key=...) calls in
# pivot_table_interactions_app.py so get_pivot()'s nth() fallback matches
# the right component when Streamlit does not emit a stable .st-key-* class.
INTERACTIONS_PIVOT_KEYS = [
    "test_pivot",
    "test_pivot_subtotals",
    "test_pivot_cond_fmt",
    "test_pivot_locked",
    "test_pivot_locked_groups",
    "test_pivot_readonly",
    "test_pivot_drilldown",
    "test_pivot_no_drilldown",
    "test_pivot_dim_toggle",
    "test_pivot_per_dim_subtotals",
    "test_pivot_per_measure_row_totals",
    "test_pivot_per_measure_col_totals",
    "test_pivot_col_groups",
    "test_pivot_date_hierarchy",
    "test_pivot_date_hierarchy_multidim",
    "test_pivot_date_hierarchy_rows",
    "test_pivot_date_hierarchy_rows_mixed",
    "test_pivot_adaptive_year",
    "test_pivot_adaptive_month",
    "test_pivot_hierarchy",
    "test_pivot_hierarchy_totals",
    "test_pivot_hierarchy_locked",
    "test_pivot_drilldown_pagination",
    "test_pivot_drilldown_pagination_hybrid",
    "test_pivot_formula",
    "test_pivot_subtotal_bottom",
    "test_pivot_subtotal_top",
]

DATA_PIVOT_KEYS = [
    "test_pivot",
    "test_pivot_cond_fmt",
    "test_pivot_cond_fmt_mid_value",
    "test_pivot_number_fmt",
    "test_pivot_empty",
    "test_pivot_single_row",
    "test_pivot_no_cols",
    "test_pivot_count_distinct",
    "test_pivot_median",
    "test_pivot_auto",
    "test_pivot_threshold",
    "test_pivot_tall",
    "test_pivot_alignment",
    "test_pivot_null_separate",
    "test_pivot_null_zero",
    "test_pivot_sparse_drilldown",
    "test_pivot_hybrid_median",
    "test_pivot_hybrid_count_distinct",
    "test_pivot_synthetic",
]

GOLDEN_PIVOT_KEYS = [
    "golden_a",
    "golden_c",
    "golden_e",
    "golden_f",
    "golden_f2",
    "golden_h",
    "golden_f3",
    "golden_no_totals",
    "golden_export",
    "golden_va",
]

COLUMN_CONFIG_PIVOT_KEYS = [
    "test_pivot_cc_label",
    "test_pivot_cc_help",
    "test_pivot_cc_width_px",
    "test_pivot_cc_width_preset",
    "test_pivot_cc_pinned",
    "test_pivot_cc_link",
    "test_pivot_cc_image",
    "test_pivot_cc_checkbox",
    "test_pivot_cc_text_max",
    "test_pivot_cc_renderer_totals",
    "test_pivot_cc_help_col_dim_single",
    "test_pivot_cc_help_col_dim_label",
    "test_pivot_cc_help_temporal",
]

# Must match the exact order of st_pivot_table(key=...) calls in
# pivot_table_styling_app.py.
STYLING_PIVOT_KEYS = [
    "style_none",
    "style_custom_bg",
    "style_density_compact",
    "style_borders_rows",
    "style_stripes_off",
    "style_hover_off",
    "style_column_header_bg",
    "style_row_total_mapping",
    "style_column_total_mapping",
    "style_per_measure",
    "style_cf_precedence",
    "style_composition",
]

APP_CONFIGS = {
    "default": {"script": SCRIPT, "pivot_keys": PIVOT_KEYS},
    "pivot_table_test.py": {"script": TOOLBAR_SCRIPT, "pivot_keys": TOOLBAR_PIVOT_KEYS},
    "pivot_table_interactions_test.py": {
        "script": INTERACTIONS_SCRIPT,
        "pivot_keys": INTERACTIONS_PIVOT_KEYS,
    },
    "pivot_table_data_test.py": {"script": DATA_SCRIPT, "pivot_keys": DATA_PIVOT_KEYS},
    "pivot_table_golden_test.py": {
        "script": GOLDEN_SCRIPT,
        "pivot_keys": GOLDEN_PIVOT_KEYS,
    },
    "pivot_table_column_config_test.py": {
        "script": COLUMN_CONFIG_SCRIPT,
        "pivot_keys": COLUMN_CONFIG_PIVOT_KEYS,
    },
    "pivot_table_styling_test.py": {
        "script": STYLING_SCRIPT,
        "pivot_keys": STYLING_PIVOT_KEYS,
    },
    "pivot_table_filters_test.py": {
        "script": FILTERS_SCRIPT,
        "pivot_keys": [
            "test_pivot_top_n",
            "test_pivot_bottom_n",
            "test_pivot_value_filter",
            "test_pivot_top_n_interactive",
            "test_pivot_value_filter_interactive",
        ],
    },
    "pivot_table_new_features_test.py": {
        "script": NEW_FEATURES_SCRIPT,
        "pivot_keys": [
            "test_pivot_running_total",
            "test_pivot_rank",
            "test_pivot_multi_sort_asc",
            "test_pivot_multi_sort_desc",
        ],
    },
}


def get_pivot(page: Page, key: str) -> Locator:
    """Return a Locator scoped to the pivot-container for *key*."""
    class_name = re.sub(r"[^a-zA-Z0-9_-]", "-", key.strip())
    keyed_container = page.locator(f".st-key-{class_name}")
    if keyed_container.count():
        container = keyed_container.get_by_test_id("pivot-container").first
    else:
        pivot_keys = getattr(page, "_pivot_keys", PIVOT_KEYS)
        idx = pivot_keys.index(key)
        container = page.get_by_test_id("pivot-container").nth(idx)
    expect(container).to_have_count(1, timeout=15000)
    container.evaluate("el => el.scrollIntoView({ block: 'center' })")
    return container


def open_settings_panel(page: Page, container: Locator) -> Locator:
    """Open the settings panel in the toolbar.

    Waits for any exit animation to complete before re-opening.
    """
    panel = page.get_by_test_id("settings-panel")

    # If the panel is animating out, wait for it to fully disappear first
    if panel.count():
        try:
            panel.wait_for(state="hidden", timeout=1500)
        except Exception:
            try:
                if panel.is_visible():
                    return panel
            except Exception:
                pass

    button = container.get_by_test_id("toolbar-settings")
    button.scroll_into_view_if_needed()
    button.evaluate("el => el.click()")
    expect(panel).to_be_visible(timeout=5000)
    # Wait for at least one available-field chip to confirm that allColumns has
    # been populated from the Arrow dataframe (not just the outer panel div).
    # On a loaded CI runner, React 18's concurrent renderer can commit the panel
    # wrapper before a subsequent data-delivery render cycle completes, leaving
    # allColumns=[] momentarily.  Locked panels have no field chips; swallow the
    # timeout so callers that open locked-panel views aren't affected.
    try:
        panel.locator("[data-testid^='settings-available-']").first.wait_for(
            state="visible", timeout=10000
        )
        # Allow the one-shot ResizeObserver (available-fields container height)
        # to fire before callers read bounding boxes, e.g. for DnD drag targets.
        page.wait_for_timeout(200)
    except Exception:
        page.wait_for_timeout(100)
    return panel


def open_settings_popover(page: Page, container: Locator) -> Locator:
    """Deprecated alias for open_settings_panel."""
    return open_settings_panel(page, container)


def _find_free_port() -> int:
    """Find and return a free port on the local machine."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(s.getsockname()[1])


class AsyncSubprocess:
    """Wraps subprocess.Popen to capture output safely via a temp file.

    Using a temp file instead of subprocess.PIPE avoids deadlocks when the
    child process produces large amounts of output.
    """

    def __init__(
        self,
        args: typing.List[str],
        cwd: typing.Optional[str] = None,
        env: typing.Optional[typing.Dict[str, str]] = None,
    ):
        self.args = args
        self.cwd = cwd
        self.env = env
        self._proc: typing.Optional[subprocess.Popen] = None
        self._stdout_file: typing.Optional[typing.IO] = None

    def start(self):
        self._stdout_file = TemporaryFile("w+")
        LOGGER.info("Running command: %s", shlex.join(self.args))
        self._proc = subprocess.Popen(
            self.args,
            cwd=self.cwd,
            stdout=self._stdout_file,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ.copy(), **self.env} if self.env else None,
        )

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        if self._stdout_file is not None:
            self._stdout_file.close()
            self._stdout_file = None

    def terminate(self) -> typing.Optional[str]:
        """Terminate the process and return its stdout/stderr as a string."""
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None

        stdout = None
        if self._stdout_file is not None:
            self._stdout_file.seek(0)
            stdout = self._stdout_file.read()
            self._stdout_file.close()
            self._stdout_file = None

        return stdout

    def __enter__(self) -> "AsyncSubprocess":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class StreamlitRunner:
    """Context manager that runs a Streamlit script on a free port.

    Usage::

        with StreamlitRunner(Path("my_app.py")) as runner:
            page.goto(runner.server_url)
            ...
    """

    def __init__(
        self,
        script_path: os.PathLike,
        server_port: typing.Optional[int] = None,
    ):
        self._process: typing.Optional[AsyncSubprocess] = None
        self.server_port = server_port
        self.script_path = script_path

    def __enter__(self) -> "StreamlitRunner":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        self.server_port = self.server_port or _find_free_port()
        self._process = AsyncSubprocess(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(self.script_path),
                f"--server.port={self.server_port}",
                "--server.headless=true",
                "--browser.gatherUsageStats=false",
                "--global.developmentMode=false",
            ]
        )
        self._process.start()
        if not self.is_server_running():
            self._process.stop()
            raise RuntimeError("Streamlit app failed to start")

    def stop(self):
        if self._process is not None:
            self._process.stop()

    def is_server_running(self, timeout: int = 30) -> bool:
        """Poll the Streamlit health endpoint until it responds 'ok'."""
        with requests.Session() as http_session:
            start_time = time.time()
            while True:
                with contextlib.suppress(requests.RequestException):
                    response = http_session.get(self.server_url + "/_stcore/health")
                    if response.text == "ok":
                        return True
                time.sleep(3)
                if time.time() - start_time > timeout:
                    return False

    @property
    def server_url(self) -> str:
        if not self.server_port:
            raise RuntimeError("Unknown server port")
        return f"http://localhost:{self.server_port}"
