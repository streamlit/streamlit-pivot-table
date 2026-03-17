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

"""Performance benchmark harness for the pivot table engine.

Loads golden datasets, runs pivot computation (Python-side groupby as a proxy
for the client-side PivotData computation), and measures timing.

Usage:
    python tests/perf_benchmark.py
    python tests/perf_benchmark.py --update-baseline

Results are compared against perf_baseline.json.  A >20% regression from
baseline fails the benchmark (regression-based threshold, median-of-N runs).
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

import pandas as pd

GOLDEN_DIR = Path(__file__).parent / "golden_data"
BASELINE_PATH = Path(__file__).parent / "perf_baseline.json"

DATASETS = {
    "small": GOLDEN_DIR / "small.csv",
    "medium": GOLDEN_DIR / "medium.csv",
    "large": GOLDEN_DIR / "large.csv",
}

PIVOT_CONFIGS = {
    "small": {
        "rows": ["Region"],
        "columns": ["Year"],
        "values": ["Revenue", "Profit"],
        "agg": "sum",
    },
    "medium": {
        "rows": ["Region", "Country"],
        "columns": ["Year", "Quarter"],
        "values": ["Revenue", "Profit", "Units"],
        "agg": "sum",
    },
    "large": {
        "rows": ["Region", "Country", "City"],
        "columns": ["Year", "Quarter"],
        "values": ["Revenue", "Profit"],
        "agg": "sum",
    },
}

N_RUNS = 5
REGRESSION_THRESHOLD = 0.20  # 20%


def run_pivot(df: pd.DataFrame, config: dict) -> float:
    """Simulate pivot computation and return elapsed time in ms."""
    start = time.perf_counter()

    rows = config["rows"]
    cols = config["columns"]
    values = config["values"]
    group_keys = rows + cols

    if group_keys and values:
        df.groupby(group_keys, observed=True)[values].agg(config["agg"])

    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms


def benchmark_dataset(name: str) -> dict:
    """Run N_RUNS benchmarks and return median timing + metadata."""
    path = DATASETS[name]
    if not path.exists():
        return {"skipped": True, "reason": f"{path} not found"}

    df = pd.read_csv(path)
    config = PIVOT_CONFIGS[name]

    timings = []
    for _ in range(N_RUNS):
        ms = run_pivot(df, config)
        timings.append(ms)

    median_ms = statistics.median(timings)
    return {
        "rows": len(df),
        "cols": len(df.columns),
        "pivot_rows": config["rows"],
        "pivot_cols": config["columns"],
        "n_runs": N_RUNS,
        "median_ms": round(median_ms, 2),
        "min_ms": round(min(timings), 2),
        "max_ms": round(max(timings), 2),
    }


def load_baseline() -> dict | None:
    if BASELINE_PATH.exists():
        return json.loads(BASELINE_PATH.read_text())
    return None


def save_baseline(results: dict) -> None:
    BASELINE_PATH.write_text(json.dumps(results, indent=2) + "\n")


def check_regression(results: dict, baseline: dict) -> list[str]:
    """Compare results against baseline, return list of failures."""
    failures = []
    for name in results:
        if "skipped" in results[name]:
            continue
        if name not in baseline or "skipped" in baseline[name]:
            continue

        current = results[name]["median_ms"]
        base = baseline[name]["median_ms"]

        if base > 0:
            regression_pct = (current - base) / base
            if regression_pct > REGRESSION_THRESHOLD:
                failures.append(
                    f"{name}: {current:.1f}ms vs baseline {base:.1f}ms "
                    f"({regression_pct:+.0%} regression, threshold {REGRESSION_THRESHOLD:.0%})"
                )

    return failures


def main():
    parser = argparse.ArgumentParser(description="Performance benchmark harness")
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Save current results as the new baseline",
    )
    args = parser.parse_args()

    print("Running performance benchmarks...")
    print(f"  N_RUNS={N_RUNS}, REGRESSION_THRESHOLD={REGRESSION_THRESHOLD:.0%}\n")

    results = {}
    for name in DATASETS:
        print(f"  {name}...", end=" ", flush=True)
        result = benchmark_dataset(name)
        results[name] = result
        if "skipped" in result:
            print(f"SKIPPED ({result['reason']})")
        else:
            print(
                f"{result['median_ms']:.1f}ms (min={result['min_ms']:.1f}, max={result['max_ms']:.1f})"
            )

    if args.update_baseline:
        save_baseline(results)
        print(f"\nBaseline updated: {BASELINE_PATH}")
        return

    baseline = load_baseline()
    if baseline is None:
        print("\nNo baseline found. Run with --update-baseline to create one.")
        save_baseline(results)
        print(f"Created initial baseline: {BASELINE_PATH}")
        return

    failures = check_regression(results, baseline)
    if failures:
        print(f"\nREGRESSION DETECTED ({len(failures)} failures):")
        for f in failures:
            print(f"  FAIL: {f}")
        raise SystemExit(1)
    else:
        print("\nAll benchmarks within threshold. No regressions detected.")


if __name__ == "__main__":
    main()
