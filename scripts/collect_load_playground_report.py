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

"""Collect repeatable load-playground metrics with Playwright."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from playwright.sync_api import Page, expect, sync_playwright

ROOT = Path(__file__).resolve().parent.parent
APP_SCRIPT = ROOT / "e2e_playwright" / "pivot_table_perf_app.py"
E2E_DIR = ROOT / "e2e_playwright"
PIVOT_KEY_CLASS = ".st-key-load_playground_pivot"
PROFILE_PRIMARY_FIELD = {
    "balanced": "Region",
    "high_row_cardinality": "Country",
    "wide_columns": "Region",
}

if str(E2E_DIR) not in sys.path:
    sys.path.insert(0, str(E2E_DIR))

from e2e_utils import StreamlitRunner  # noqa: E402


@contextmanager
def temporary_env(overrides: dict[str, str]):
    previous = {key: os.environ.get(key) for key in overrides}
    try:
        os.environ.update(overrides)
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def get_pivot(page: Page):
    container = page.locator(PIVOT_KEY_CLASS).get_by_test_id("pivot-container").first
    expect(container).to_be_visible(timeout=30000)
    return container


def read_static_metrics(page: Page) -> dict[str, Any]:
    metric_values = page.locator("[data-testid='stMetricValue']")
    expect(metric_values.nth(3)).to_be_visible(timeout=30000)
    body_text = page.locator("body").inner_text()
    visible_cells = None
    for line in body_text.splitlines():
        if line.startswith("VISIBLE_CELLS="):
            visible_cells = int(line.split("=", 1)[1])
            break
    return {
        "rows": metric_values.nth(0).text_content(),
        "row_groups": metric_values.nth(1).text_content(),
        "col_groups": metric_values.nth(2).text_content(),
        "memory_payload": metric_values.nth(3).text_content(),
        "visible_cells": visible_cells,
    }


def read_warning_state(page: Page) -> dict[str, Any]:
    banner = page.get_by_test_id("pivot-warning-banner")
    warning_lines: list[str] = []
    if banner.count():
        warning_lines = [
            line.strip()
            for line in banner.first.inner_text().splitlines()
            if line.strip()
        ]
    return {
        "virtualized": any(
            "Virtualization" in line or "Virtualization enabled" in line
            for line in warning_lines
        ),
        "truncated": any(
            "column values exceed cardinality cap" in line for line in warning_lines
        ),
        "warnings": len(warning_lines),
        "warning_lines": warning_lines,
    }


def current_table_snapshot(container) -> str:
    row_headers = [
        text.strip()
        for text in container.get_by_test_id("pivot-row-header").all_text_contents()
        if text.strip()
    ]
    if row_headers:
        return "|".join(row_headers[:500])
    return container.get_by_test_id("pivot-table").inner_text()[:2000]


def click_menu_item(locator) -> None:
    locator.evaluate(
        "el => { el.scrollIntoView({ block: 'center', inline: 'nearest' }); el.click(); }"
    )


def wait_for_row(page: Page, predicate, timeout_s: float = 30.0) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    static_metrics = read_static_metrics(page)
    last_row: dict[str, Any] | None = None
    while time.time() < deadline:
        try:
            row = {**static_metrics, **read_warning_state(page)}
            last_row = row
            if predicate(row):
                return row
        except Exception:
            pass
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for comparison row update: {last_row}")


def collect_initial(page: Page) -> dict[str, Any]:
    container = get_pivot(page)
    expect(container.get_by_test_id("pivot-table")).to_be_visible(timeout=30000)
    return {**read_static_metrics(page), **read_warning_state(page)}


def exercise_sort(page: Page, field: str) -> tuple[float, dict[str, Any]]:
    container = get_pivot(page)
    before_snapshot = current_table_snapshot(container)
    start = time.perf_counter()
    trigger = container.get_by_test_id(f"header-menu-trigger-{field}")
    trigger.evaluate("el => el.click()")
    menu = page.get_by_test_id(f"header-menu-{field}")
    expect(menu).to_be_visible(timeout=5000)
    option = menu.get_by_test_id("header-sort-key-desc")
    if option.get_attribute("aria-pressed") == "true":
        click_menu_item(option)
        trigger.evaluate("el => el.click()")
        expect(menu).to_be_visible(timeout=5000)
        option = menu.get_by_test_id("header-sort-key-desc")
    click_menu_item(option)
    page.keyboard.press("Escape")
    wait_for_table_change(container, before_snapshot)
    return (time.perf_counter() - start) * 1000, {
        **read_static_metrics(page),
        **read_warning_state(page),
    }


def wait_for_table_change(
    container, before_snapshot: str, timeout_s: float = 15.0
) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        after_snapshot = current_table_snapshot(container)
        if after_snapshot and after_snapshot != before_snapshot:
            return
        time.sleep(0.2)
    raise TimeoutError("Rendered pivot table did not change")


def exercise_filter(page: Page, field: str) -> tuple[float, dict[str, Any]]:
    container = get_pivot(page)
    before_snapshot = current_table_snapshot(container)
    start = time.perf_counter()
    trigger = container.get_by_test_id(f"header-menu-trigger-{field}")
    trigger.evaluate("el => el.click()")
    menu = page.get_by_test_id(f"header-menu-{field}")
    expect(menu).to_be_visible(timeout=5000)
    first_checkbox = (
        menu.get_by_test_id("header-menu-filter")
        .locator("label")
        .first.locator("input")
    )
    expect(first_checkbox).to_be_visible(timeout=5000)
    click_menu_item(first_checkbox)
    page.keyboard.press("Escape")
    wait_for_table_change(container, before_snapshot)
    return (time.perf_counter() - start) * 1000, {
        **read_static_metrics(page),
        **read_warning_state(page),
    }


def exercise_drilldown(page: Page) -> tuple[float, dict[str, Any]]:
    container = get_pivot(page)
    start = time.perf_counter()
    cell = container.get_by_test_id("pivot-data-cell").first
    cell.evaluate("el => el.click()")
    expect(page.get_by_test_id("drilldown-panel")).to_be_visible(timeout=10000)
    return (time.perf_counter() - start) * 1000, {
        **read_static_metrics(page),
        **read_warning_state(page),
    }


def run_scenario(
    page: Page,
    rows: str,
    profile: str,
    execution_mode: str,
    *,
    initial_only: bool,
    skip_drilldown: bool,
) -> list[dict[str, Any]]:
    with temporary_env(
        {
            "PERF_ROWS": str(parse_rows(rows)),
            "PERF_PROFILE": profile,
            "LOAD_TEST_EXECUTION_MODE": execution_mode,
        }
    ):
        with StreamlitRunner(APP_SCRIPT) as runner:
            start = time.perf_counter()
            page.goto(runner.server_url)
            page.wait_for_selector("text=Pivot Table Perf App", timeout=30000)
            primary_field = PROFILE_PRIMARY_FIELD[profile]
            records = []
            initial = collect_initial(page)
            records.append(
                {
                    "rows_label": rows,
                    "profile": profile,
                    "execution_mode": execution_mode,
                    "stage": "initial",
                    "first_render_ms": round((time.perf_counter() - start) * 1000, 2),
                    "sort_ms": None,
                    "filter_ms": None,
                    "drilldown_ms": None,
                    **initial,
                }
            )
            if initial_only:
                return records
            collectors: list[tuple[str, Any]] = [
                (
                    "sort",
                    lambda current_page: exercise_sort(current_page, primary_field),
                ),
                (
                    "filter",
                    lambda current_page: exercise_filter(current_page, primary_field),
                ),
            ]
            if not skip_drilldown:
                collectors.append(("drilldown", exercise_drilldown))
            for stage, collector in collectors:
                elapsed_ms, row = collector(page)
                records.append(
                    {
                        "rows_label": rows,
                        "profile": profile,
                        "execution_mode": execution_mode,
                        "stage": stage,
                        "first_render_ms": records[0]["first_render_ms"],
                        "sort_ms": round(elapsed_ms, 2) if stage == "sort" else None,
                        "filter_ms": round(elapsed_ms, 2)
                        if stage == "filter"
                        else None,
                        "drilldown_ms": round(elapsed_ms, 2)
                        if stage == "drilldown"
                        else None,
                        **row,
                    }
                )
            return records


def parse_rows(value: str) -> int:
    normalized = value.strip().lower().replace(",", "")
    if normalized.endswith("k"):
        return int(float(normalized[:-1]) * 1_000)
    if normalized.endswith("m"):
        return int(float(normalized[:-1]) * 1_000_000)
    return int(normalized)


def write_markdown(path: Path, rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Load Playground Report",
        "",
        "| Rows | Profile | Mode | Stage | First render (ms) | Sort (ms) | Filter (ms) | Drilldown (ms) | Virtualized | Truncated | Warnings |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- | ---: |",
    ]
    for row in rows:
        lines.append(
            "| {rows_label} | {profile} | {execution_mode} | {stage} | {first_render_ms} | {sort_ms} | {filter_ms} | {drilldown_ms} | {virtualized} | {truncated} | {warnings} |".format(
                rows_label=row.get("rows_label"),
                profile=row.get("profile"),
                execution_mode=row.get("execution_mode"),
                stage=row.get("stage"),
                first_render_ms=row.get("first_render_ms"),
                sort_ms=row.get("sort_ms"),
                filter_ms=row.get("filter_ms"),
                drilldown_ms=row.get("drilldown_ms"),
                virtualized=row.get("virtualized"),
                truncated=row.get("truncated"),
                warnings=row.get("warnings"),
            )
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows", action="append", required=True, help="Row label such as 50k or 100k."
    )
    parser.add_argument(
        "--profile",
        action="append",
        required=True,
        help="Profile name such as balanced or wide_columns.",
    )
    parser.add_argument(
        "--execution-mode",
        action="append",
        required=True,
        choices=["auto", "client_only", "threshold_hybrid"],
        help="Execution mode to run.",
    )
    parser.add_argument("--json-out", type=Path, help="Optional JSON output path.")
    parser.add_argument(
        "--markdown-out", type=Path, help="Optional Markdown output path."
    )
    parser.add_argument(
        "--initial-only",
        action="store_true",
        help="Collect only the initial load stage and skip sort/filter/drilldown actions.",
    )
    parser.add_argument(
        "--skip-drilldown",
        action="store_true",
        help="Skip the drilldown interaction stage.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows: list[dict[str, Any]] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            for rows_label in args.rows:
                for profile in args.profile:
                    for execution_mode in args.execution_mode:
                        rows.extend(
                            run_scenario(
                                page,
                                rows_label,
                                profile,
                                execution_mode,
                                initial_only=args.initial_only,
                                skip_drilldown=args.skip_drilldown,
                            )
                        )
        finally:
            browser.close()

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    if args.markdown_out:
        write_markdown(args.markdown_out, rows)

    print(json.dumps(rows, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
