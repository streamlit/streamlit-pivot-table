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
import sys
import time
from importlib import import_module
from pathlib import Path
from unittest.mock import patch

import random

import pandas as pd

# ---------------------------------------------------------------------------
# Ensure the workspace root (parent of tests/) is first in sys.path so we
# always import the source version of streamlit_pivot, not any older version
# that may be installed in site-packages.
# ---------------------------------------------------------------------------
_workspace_root = str(Path(__file__).resolve().parent.parent)
if sys.path[0] != _workspace_root:
    sys.path.insert(0, _workspace_root)

# ---------------------------------------------------------------------------
# Import streamlit_pivot with component registration patched out.
# This must happen before any reference to pivot internals.
# ---------------------------------------------------------------------------
with patch("streamlit.components.v2.component", return_value=lambda **_kwargs: None):
    _pivot_module = import_module("streamlit_pivot")

DEFAULT_BASELINE_PATH = Path(__file__).parent / "perf_baseline.json"

# ---------------------------------------------------------------------------
# Benchmark configurations
# ---------------------------------------------------------------------------

BENCHMARK_CONFIGS = {
    # ---- Existing raw-groupby scenarios (unchanged) ----
    "small": {
        "n_rows": 1_000,
        "rows": ["Region"],
        "columns": ["Year"],
        "values": ["Revenue", "Profit"],
        "agg": "sum",
        "run": "pivot",
    },
    "medium": {
        "n_rows": 50_000,
        "rows": ["Region", "Country"],
        "columns": ["Year", "Quarter"],
        "values": ["Revenue", "Profit", "Units"],
        "agg": "sum",
        "run": "pivot",
    },
    "large": {
        "n_rows": 200_000,
        "rows": ["Region", "Country", "City"],
        "columns": ["Year", "Quarter"],
        "values": ["Revenue", "Profit"],
        "agg": "sum",
        "run": "pivot",
    },
    # ---- Sidecar vectorization scenarios (Fix 1) ----
    "sidecar_medium": {
        "n_rows": 50_000,
        "rows": ["Region", "Country"],
        "columns": ["Year", "Quarter"],
        "values": ["Revenue", "Units"],
        "aggregation": {"Revenue": "avg", "Units": "count_distinct"},
        "run": "sidecar",
    },
    "sidecar_large": {
        "n_rows": 200_000,
        "rows": ["Region", "Country"],
        "columns": ["Year", "Quarter"],
        "values": ["Revenue", "Units"],
        "aggregation": {"Revenue": "avg", "Units": "count_distinct"},
        "run": "sidecar",
    },
    "sidecar_first_last": {
        "n_rows": 50_000,
        "rows": ["Region", "Country"],
        "columns": ["Year"],
        "values": ["Revenue", "Units"],
        "aggregation": {"Revenue": "first", "Units": "last"},
        "run": "sidecar",
    },
    # ---- Threshold + nunique cache scenarios (Fix 2 + Fix 3) ----
    "threshold_check_large": {
        "n_rows": 600_000,
        "n_distinct": 5,  # low-cardinality: the correctness bug scenario
        "rows": ["Region"],
        "columns": ["Category"],
        "values": ["Revenue"],
        "aggregation": {"Revenue": "sum"},
        "run": "threshold",
    },
    "nunique_cache_warmup": {
        "n_rows": 50_000,
        "n_distinct": 50,
        "rows": ["Region"],
        "columns": ["Category"],
        "values": ["Revenue"],
        "aggregation": {"Revenue": "avg"},
        "run": "cache_warmup",
    },
}

# ---------------------------------------------------------------------------
# DataFrame generators
# ---------------------------------------------------------------------------

_DIM_POOLS: dict[str, list] = {
    "Region": [f"Region_{i}" for i in range(10)],
    "Country": [f"Country_{i}" for i in range(50)],
    "City": [f"City_{i}" for i in range(200)],
    "Year": [str(y) for y in range(2000, 2020)],
    "Quarter": ["Q1", "Q2", "Q3", "Q4"],
}


def _generate_dataframe(config: dict) -> pd.DataFrame:
    """Generate a synthetic DataFrame at the specified size."""
    n = config["n_rows"]
    rng = random.Random(42)
    data: dict = {}

    dim_cols = config.get("rows", []) + config.get("columns", [])
    n_distinct = config.get("n_distinct", None)

    for col in dim_cols:
        if n_distinct is not None:
            pool = [f"{col}_{i}" for i in range(n_distinct)]
        else:
            pool = _DIM_POOLS.get(col, [f"{col}_{i}" for i in range(10)])
        data[col] = [rng.choice(pool) for _ in range(n)]

    for val_col in config.get("values", []):
        data[val_col] = [round(rng.uniform(0, 10_000), 2) for _ in range(n)]

    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# Run functions
# ---------------------------------------------------------------------------

N_RUNS = 5
REGRESSION_THRESHOLD = 0.20  # 20% — applied only when base >= MIN_ABSOLUTE_MS
# Sub-millisecond baselines are too noisy for percentage-based gating: a 0.01ms
# jitter on a 0.05ms baseline reads as +20% and would spuriously fail.  Skip
# the percentage check for any scenario whose baseline is below this floor.
MIN_ABSOLUTE_MS = 1.0


def run_pivot(df: pd.DataFrame, config: dict) -> float:
    """Simulate pivot computation and return elapsed time in ms (raw groupby)."""
    start = time.perf_counter()
    rows = config["rows"]
    cols = config["columns"]
    values = config["values"]
    group_keys = rows + cols
    if group_keys and values:
        df.groupby(group_keys, observed=True)[values].agg(config.get("agg", "sum"))
    return (time.perf_counter() - start) * 1000


def _make_sidecar_config(config: dict) -> dict:
    return {
        "rows": config["rows"],
        "columns": config["columns"],
        "values": config["values"],
        "aggregation": config["aggregation"],
        "synthetic_measures": [],
    }


def run_sidecar(df: pd.DataFrame, config: dict) -> float:
    """Time _compute_hybrid_totals (sidecar path)."""
    cfg = _make_sidecar_config(config)
    start = time.perf_counter()
    _pivot_module._compute_hybrid_totals(df, cfg, null_handling=None)
    return (time.perf_counter() - start) * 1000


def run_threshold(df: pd.DataFrame, config: dict) -> tuple[float, str, int]:
    """Time _should_use_threshold_hybrid 10×; return (median_ms, mode_selected, est_payload_bytes)."""
    cfg = _make_sidecar_config(config)
    timings = []
    mode = None
    for _ in range(10):
        t0 = time.perf_counter()
        use, reason = _pivot_module._should_use_threshold_hybrid(df, cfg, "auto")
        timings.append((time.perf_counter() - t0) * 1000)
        mode = "threshold_hybrid" if use else "client_only"
    # Rough Arrow payload estimate: full df, numeric cols 8 bytes × n_rows
    est_bytes = int(df.memory_usage(deep=False).sum())
    return statistics.median(timings), mode, est_bytes


def run_cache_warmup(df: pd.DataFrame, config: dict) -> tuple[float, float]:
    """Time cold vs warm _should_use_threshold_hybrid call with shared cache dict."""
    cfg = _make_sidecar_config(config)
    cache: dict = {}
    t0 = time.perf_counter()
    _pivot_module._should_use_threshold_hybrid(df, cfg, "auto", _nunique_cache=cache)
    cold_ms = (time.perf_counter() - t0) * 1000
    t1 = time.perf_counter()
    _pivot_module._should_use_threshold_hybrid(df, cfg, "auto", _nunique_cache=cache)
    warm_ms = (time.perf_counter() - t1) * 1000
    return cold_ms, warm_ms


# ---------------------------------------------------------------------------
# Benchmark runner
# ---------------------------------------------------------------------------


def benchmark_dataset(name: str) -> dict:
    """Run N_RUNS benchmarks and return median timing + metadata."""
    config = BENCHMARK_CONFIGS[name]
    run_type = config.get("run", "pivot")
    df = _generate_dataframe(config)

    if run_type == "pivot":
        timings = [run_pivot(df, config) for _ in range(N_RUNS)]
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

    elif run_type == "sidecar":
        timings = [run_sidecar(df, config) for _ in range(N_RUNS)]
        median_ms = statistics.median(timings)
        return {
            "rows": len(df),
            "cols": len(df.columns),
            "pivot_rows": config["rows"],
            "pivot_cols": config["columns"],
            "aggregation": config.get("aggregation", {}),
            "n_runs": N_RUNS,
            "median_ms": round(median_ms, 2),
            "min_ms": round(min(timings), 2),
            "max_ms": round(max(timings), 2),
        }

    elif run_type == "threshold":
        median_ms, mode_selected, est_bytes = run_threshold(df, config)
        return {
            "rows": len(df),
            "n_distinct": config.get("n_distinct"),
            "mode_selected": mode_selected,
            "est_payload_bytes": est_bytes,
            "median_ms": round(median_ms, 4),
        }

    elif run_type == "cache_warmup":
        # cache_warmup requires _should_use_threshold_hybrid to accept _nunique_cache;
        # gracefully skip if the parameter doesn't exist yet (pre-Fix-3)
        try:
            cold_ms, warm_ms = run_cache_warmup(df, config)
            return {
                "rows": len(df),
                "cold_ms": round(cold_ms, 4),
                "warm_ms": round(warm_ms, 4),
                "median_ms": round(cold_ms, 4),  # baseline uses cold time
            }
        except TypeError:
            return {
                "rows": len(df),
                "skipped": True,
                "reason": "_should_use_threshold_hybrid does not yet accept _nunique_cache (pre-Fix-3)",
                "median_ms": 0,
            }

    return {"skipped": True, "reason": f"unknown run type: {run_type}"}


# ---------------------------------------------------------------------------
# Baseline I/O and regression checking
# ---------------------------------------------------------------------------


def load_baseline(path: Path) -> dict | None:
    if path.exists():
        return json.loads(path.read_text())
    return None


def save_baseline(results: dict, path: Path) -> None:
    path.write_text(json.dumps(results, indent=2) + "\n")


def check_regression(results: dict, baseline: dict) -> tuple[list[str], list[dict]]:
    """Compare results against baseline, return (failures, row_details).

    Percentage-based regression is only applied when the baseline is at or
    above MIN_ABSOLUTE_MS.  Sub-millisecond operations are too noisy for a
    percentage gate — a 0.01ms OS scheduling jitter on a 0.05ms baseline would
    read as a +20% regression and spuriously fail.  Such scenarios are shown in
    the summary table with status "SKIP-NOISE" and are never counted as failures.
    """
    failures = []
    rows = []
    for name in results:
        if results[name].get("skipped"):
            continue
        if name not in baseline or baseline[name].get("skipped"):
            continue

        current = results[name]["median_ms"]
        base = baseline[name]["median_ms"]

        if base <= 0:
            continue

        pct = (current - base) / base

        if base < MIN_ABSOLUTE_MS:
            # Baseline is below the noise floor; record for visibility but
            # never gate the exit code on it.
            status = "SKIP-NOISE"
        elif pct > REGRESSION_THRESHOLD:
            status = "FAIL"
            failures.append(
                f"{name}: {current:.2f}ms vs baseline {base:.2f}ms "
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


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
        if result.get("skipped"):
            print(f"SKIPPED ({result.get('reason', '')})")
        else:
            extra = ""
            if "mode_selected" in result:
                extra = f" mode={result['mode_selected']} payload≈{result['est_payload_bytes']//1024}KB"
            elif "cold_ms" in result:
                extra = (
                    f" cold={result['cold_ms']:.2f}ms warm={result['warm_ms']:.2f}ms"
                )
            print(
                f"{result['median_ms']:.2f}ms (min={result.get('min_ms', result['median_ms']):.2f}, "
                f"max={result.get('max_ms', result['median_ms']):.2f}){extra}"
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
        symbol = {"FAIL": "✗", "FASTER": "↑", "OK": "✓", "SKIP-NOISE": "~"}[r["status"]]
        pct_str = f"{r['pct']:+.1%}"
        noise_note = (
            " (below noise floor, not gated)" if r["status"] == "SKIP-NOISE" else ""
        )
        print(
            f"  {symbol} {r['name']}: {r['base']:.2f}ms → {r['current']:.2f}ms ({pct_str}) [{r['status']}]{noise_note}"
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
    symbols = {"FAIL": "✗", "FASTER": "↑", "OK": "✓", "SKIP-NOISE": "~"}
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
