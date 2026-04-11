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
import os
import statistics
import time
from pathlib import Path

import random

import pandas as pd

DEFAULT_BASELINE_PATH = Path(__file__).parent / "perf_baseline.json"

BENCHMARK_CONFIGS = {
    "small": {
        "n_rows": 1_000,
        "rows": ["Region"],
        "columns": ["Year"],
        "values": ["Revenue", "Profit"],
        "agg": "sum",
    },
    "medium": {
        "n_rows": 50_000,
        "rows": ["Region", "Country"],
        "columns": ["Year", "Quarter"],
        "values": ["Revenue", "Profit", "Units"],
        "agg": "sum",
    },
    "large": {
        "n_rows": 200_000,
        "rows": ["Region", "Country", "City"],
        "columns": ["Year", "Quarter"],
        "values": ["Revenue", "Profit"],
        "agg": "sum",
    },
}


def _generate_dataframe(config: dict) -> pd.DataFrame:
    """Generate a synthetic DataFrame at the specified size."""
    n = config["n_rows"]
    rng = random.Random(42)
    data: dict = {}

    dim_pools = {
        "Region": [f"Region_{i}" for i in range(10)],
        "Country": [f"Country_{i}" for i in range(50)],
        "City": [f"City_{i}" for i in range(200)],
        "Year": [str(y) for y in range(2000, 2020)],
        "Quarter": ["Q1", "Q2", "Q3", "Q4"],
    }
    for col in config["rows"] + config["columns"]:
        pool = dim_pools[col]
        data[col] = [rng.choice(pool) for _ in range(n)]

    for val_col in config["values"]:
        data[val_col] = [round(rng.uniform(0, 10_000), 2) for _ in range(n)]

    return pd.DataFrame(data)


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
    config = BENCHMARK_CONFIGS[name]
    df = _generate_dataframe(config)

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


def load_baseline(path: Path) -> dict | None:
    if path.exists():
        return json.loads(path.read_text())
    return None


def save_baseline(results: dict, path: Path) -> None:
    path.write_text(json.dumps(results, indent=2) + "\n")


def check_regression(results: dict, baseline: dict) -> tuple[list[str], list[dict]]:
    """Compare results against baseline, return (failures, row_details)."""
    failures = []
    rows = []
    for name in results:
        if "skipped" in results[name]:
            continue
        if name not in baseline or "skipped" in baseline[name]:
            continue

        current = results[name]["median_ms"]
        base = baseline[name]["median_ms"]

        if base > 0:
            pct = (current - base) / base
            if pct > REGRESSION_THRESHOLD:
                status = "FAIL"
                failures.append(
                    f"{name}: {current:.1f}ms vs baseline {base:.1f}ms "
                    f"({pct:+.0%} regression, threshold {REGRESSION_THRESHOLD:.0%})"
                )
            elif pct < -REGRESSION_THRESHOLD:
                status = "FASTER"
            else:
                status = "OK"
            rows.append(
                {
                    "name": name,
                    "base": base,
                    "current": current,
                    "pct": pct,
                    "status": status,
                }
            )

    return failures, rows


def main():
    parser = argparse.ArgumentParser(description="Performance benchmark harness")
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Save current results as the new baseline",
    )
    parser.add_argument(
        "--baseline-path",
        type=Path,
        default=DEFAULT_BASELINE_PATH,
        help="Path to the baseline JSON file (default: tests/perf_baseline.json)",
    )
    args = parser.parse_args()
    baseline_path: Path = args.baseline_path

    print("Running performance benchmarks...")
    print(f"  N_RUNS={N_RUNS}, REGRESSION_THRESHOLD={REGRESSION_THRESHOLD:.0%}\n")

    results = {}
    for name in BENCHMARK_CONFIGS:
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
        save_baseline(results, baseline_path)
        print(f"\nBaseline updated: {baseline_path}")
        return

    baseline = load_baseline(baseline_path)
    if baseline is None:
        print("\nNo baseline found. Run with --update-baseline to create one.")
        save_baseline(results, baseline_path)
        print(f"Created initial baseline: {baseline_path}")
        return

    failures, rows = check_regression(results, baseline)

    for r in rows:
        symbol = {"FAIL": "✗", "FASTER": "↑", "OK": "✓"}[r["status"]]
        pct_str = f"{r['pct']:+.1%}"
        print(
            f"  {symbol} {r['name']}: {r['base']:.1f}ms → {r['current']:.1f}ms ({pct_str}) [{r['status']}]"
        )

    _write_step_summary(rows, failures)

    if failures:
        print(f"\nREGRESSION DETECTED ({len(failures)} failures):")
        for f in failures:
            print(f"  FAIL: {f}")
        raise SystemExit(1)
    else:
        print("\nAll benchmarks within threshold. No regressions detected.")


def _write_step_summary(rows: list[dict], failures: list[str]) -> None:
    """Append a markdown table to $GITHUB_STEP_SUMMARY when running in CI."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path or not rows:
        return
    lines = [
        "### Python Benchmark Results\n",
        "| Dataset | Baseline | Current | Change | Status |",
        "|---------|----------|---------|--------|--------|",
    ]
    symbols = {"FAIL": "✗", "FASTER": "↑", "OK": "✓"}
    for r in rows:
        pct = f"{r['pct']:+.1%}"
        lines.append(
            f"| {r['name']} | {r['base']:.2f} ms | {r['current']:.2f} ms | {pct} | {symbols[r['status']]} {r['status']} |"
        )
    lines.append(
        f"\n> Threshold: {REGRESSION_THRESHOLD:.0%} · {len(rows)} benchmarks checked · {len(failures)} regression(s)\n"
    )
    with open(summary_path, "a") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    main()
