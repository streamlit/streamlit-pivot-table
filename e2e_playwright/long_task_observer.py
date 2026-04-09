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

"""Long Task observer for Playwright E2E performance measurement.

Injects a PerformanceObserver('longtask') into the page, collects entries
during pivot computation, and provides helpers to write results to JSON.

Usage in Playwright tests:
    from long_task_observer import inject_long_task_observer, collect_long_tasks

    inject_long_task_observer(page)
    # ... trigger pivot computation ...
    tasks = collect_long_tasks(page)
    # tasks is a list of dicts with startTime, duration, name
"""

import json
from pathlib import Path
from typing import Any

from playwright.sync_api import Page


def inject_long_task_observer(page: Page) -> None:
    """Inject a PerformanceObserver that collects Long Task entries."""
    page.evaluate("""() => {
        window.__longTasks = [];
        if (typeof PerformanceObserver !== 'undefined') {
            const observer = new PerformanceObserver((list) => {
                for (const entry of list.getEntries()) {
                    window.__longTasks.push({
                        name: entry.name,
                        startTime: entry.startTime,
                        duration: entry.duration,
                    });
                }
            });
            try {
                observer.observe({ type: 'longtask', buffered: true });
                window.__longTaskObserver = observer;
            } catch (e) {
                // longtask not supported in this browser
                console.warn('Long Task API not supported:', e.message);
            }
        }
    }""")


def collect_long_tasks(page: Page) -> list[dict[str, Any]]:
    """Collect all Long Task entries captured since injection."""
    return page.evaluate("() => window.__longTasks || []")


def disconnect_long_task_observer(page: Page) -> None:
    """Disconnect the observer to stop collecting."""
    page.evaluate("""() => {
        if (window.__longTaskObserver) {
            window.__longTaskObserver.disconnect();
            window.__longTaskObserver = null;
        }
    }""")


def write_long_tasks_report(
    tasks: list[dict[str, Any]],
    output_path: str | Path,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Write collected Long Task entries to a JSON report file."""
    report = {
        "timestamp": __import__("datetime").datetime.now().isoformat(),
        "taskCount": len(tasks),
        "totalBlockingMs": round(sum(t.get("duration", 0) for t in tasks), 2),
        "tasks": tasks,
    }
    if metadata:
        report["metadata"] = metadata
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
