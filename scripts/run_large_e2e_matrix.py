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

"""Run isolated large-load Playwright suites with a consistent interface."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

SUITES = {
    "data": ["e2e_playwright/pivot_table_data_test.py"],
    "interactions": ["e2e_playwright/pivot_table_interactions_test.py"],
    "toolbar": ["e2e_playwright/pivot_table_test.py"],
    "all-isolated": [
        "e2e_playwright/pivot_table_data_test.py",
        "e2e_playwright/pivot_table_interactions_test.py",
        "e2e_playwright/pivot_table_test.py",
    ],
}


@dataclass
class RunResult:
    rows: int
    suite: str
    elapsed_s: float
    return_code: int


def run_suite(
    rows: int, suite: str, browser: str, extra_pytest_args: list[str]
) -> RunResult:
    if suite not in SUITES:
        raise ValueError(f"Unknown suite: {suite}")

    env = os.environ.copy()
    env["E2E_MAIN_DATASET_ROWS"] = str(rows)
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        *SUITES[suite],
        "--browser",
        browser,
        *extra_pytest_args,
    ]

    print(f"\n=== rows={rows:,} suite={suite} browser={browser} ===")
    print(" ".join(cmd))
    start = time.perf_counter()
    completed = subprocess.run(cmd, cwd=ROOT, env=env, check=False)
    elapsed_s = time.perf_counter() - start
    print(
        f"--- completed rows={rows:,} suite={suite} "
        f"exit={completed.returncode} elapsed={elapsed_s:.2f}s ---"
    )
    return RunResult(
        rows=rows, suite=suite, elapsed_s=elapsed_s, return_code=completed.returncode
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run isolated large-load Playwright suites at selected row counts."
    )
    parser.add_argument(
        "--rows",
        type=int,
        action="append",
        required=True,
        help="Dataset row count to use. Repeat for multiple sizes.",
    )
    parser.add_argument(
        "--suite",
        choices=sorted(SUITES),
        action="append",
        required=True,
        help="Suite to run. Repeat for multiple suites.",
    )
    parser.add_argument(
        "--browser",
        default="chromium",
        help="Browser passed to pytest-playwright. Defaults to chromium.",
    )
    parser.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        help="Extra argument forwarded directly to pytest. Repeat as needed.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results: list[RunResult] = []

    for rows in args.rows:
        for suite in args.suite:
            results.append(run_suite(rows, suite, args.browser, args.pytest_arg))

    print("\n=== summary ===")
    failures = 0
    for result in results:
        status = "PASS" if result.return_code == 0 else "FAIL"
        if result.return_code != 0:
            failures += 1
        print(
            f"{status} rows={result.rows:,} suite={result.suite} "
            f"elapsed={result.elapsed_s:.2f}s"
        )

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
