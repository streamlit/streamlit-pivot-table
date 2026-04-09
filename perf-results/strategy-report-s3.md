<!--
Copyright 2025 Snowflake Inc.
SPDX-License-Identifier: Apache-2.0
-->

# Strategy 3: Threshold hybrid hardening — report

## Summary

Server-side threshold hybrid pre-aggregation now supports **`avg` (mean)** in addition to `sum`, `count`, `min`, and `max`. Auto-selection uses a **lower row threshold (100k)** when estimated pivot cardinality is high. Forced **`execution_mode="threshold_hybrid"`** returns an explicit reason that automatic thresholds are skipped. **`server_mode_reason`** includes a drill-down explanation, and the **frontend shows that text** in the existing warning banner when `execution_mode === "threshold_hybrid"`.

**Standard deviation (`std`)** was **not** added: it is not part of the public `VALID_AGGREGATIONS` / frontend aggregation union, and correct roll-up of subgroup variances would require extra state (or would be wrong if re-aggregated like means).

## Python changes (`streamlit_pivot/__init__.py`)

- **`SUPPORTED_THRESHOLD_HYBRID_AGGREGATIONS`**: added `"avg"`.
- **`_prepare_threshold_hybrid_frame`**: for `avg`, uses `pandas.NamedAgg` with per-group `sum` and `count`, then **`mean = sum / count`** (same as `groupby(...).mean()` for numeric data). Handles the no–group-by case as a single aggregate row. Empty `values` returns an empty frame with group columns only when grouping keys exist.
- **`_should_use_threshold_hybrid`**:
  - **`threshold_hybrid`**: if compatible, always enables hybrid and explains that **row-count heuristics are not applied** (clarified vs. older wording).
  - **`auto`**: `estimated_pivot_groups = row_groups * col_groups`; if `> 10_000`, **`row_threshold = 100_000`**, else **`250_000`**. Shape gate unchanged (`visible_cells > 5000` or `col_groups > 200` or `row_groups > 5000`).
- **`st_pivot_table`**: when hybrid is active, **`server_mode_reason`** appends a short **drill-down unavailable** sentence (unless already present).
- **`import pandas as pd`** for `NamedAgg` / frame helpers (pandas is already required via Streamlit).

## Aggregation coverage in hybrid

| Supported in hybrid | Notes |
|---------------------|--------|
| `sum`, `count`, `min`, `max` | Unchanged |
| `avg` | Pre-aggregated as true group mean (sum/count) |

Still **not** supported in hybrid (unchanged): `count_distinct`, `median`, `percentile_90`, `first`, `last`, synthetic measures.

**Coverage vs. “common” configs:** The default toolbar-style set is typically **sum, avg, count, min, max** — hybrid now supports **all five**, up from four previously (~80% of that set by count; previously 4/5 = 80% of this slice). Against **all** `VALID_AGGREGATIONS` entries, hybrid covers **5 / 10** named types (50%); the remaining five are specialized.

## Threshold tuning

| Condition | Row threshold | Rationale |
|-----------|---------------|-----------|
| `row_groups * col_groups > 10_000` | **100,000** | High cardinality benefits earlier server reduction |
| Otherwise | **250,000** | Preserves previous behavior for moderate shapes |
| `execution_mode="threshold_hybrid"` | N/A (always on if compatible) | Explicit force path; no size checks |

## Frontend changes

- **`index.tsx`**: passes **`server_mode_reason`** into `PivotRoot`.
- **`PivotRoot.tsx`**: when **`execution_mode === "threshold_hybrid"`**, appends **`server_mode_reason`** (or a short fallback) to **`allWarnings`** so the **WarningBanner** explains hybrid + drill-down limits.

No change to `PivotData` / worker paths for `avg`: pre-aggregated means are shipped as ordinary numeric cells at the final granularity.

## Tests

### Python (`python -m pytest tests/ -v`)

- **32 passed** (0 failed). Includes new **`tests/test_threshold_hybrid.py`** and updated mount tests (`median` for incompatible hybrid; `server_mode_reason` / drill-down assertion for hybrid mount).

### Frontend (`npm test`)

- **528 passed** (15 files).

## Benchmarks (frontend)

`npm run bench:ci` — representative lines from latest run:

- small dataset (1K): ~2.35k hz
- medium (50K, 100×20): ~44.5 hz
- stress (200K, 500×20): ~9.9 hz
- parseArrow 50K / 200K: ~112.7 hz / ~25.3 hz

Output also written to `streamlit_pivot/frontend/bench-results.json`.

**Interpretation:** Numbers are **unchanged in spirit** from a server-only change; the client still runs the same pivot code on (usually) fewer rows in hybrid mode.

## Memory profile (frontend)

`npm run bench:memory`:

- PivotData 50K (100×20): heap delta **~4.94 MB**
- PivotData 200K (500×20): heap delta **~20.25 MB**

Written to `streamlit_pivot/frontend/perf-results/memory-profile.json`.

**Python RSS:** Not instrumented in this pass; hybrid reduces rows transferred to the browser, which typically lowers browser memory for large raw datasets.

## Risks

1. **Mean of means:** If the client **re-aggregates** pre-aggregated means across groups (e.g. subtotals / roll-ups), the result is not the global mean of underlying raw rows. Same class of issue as any pre-aggregated measure; **documented** in code comments / this report.
2. **Advanced aggregations** still force **client_only** for large data or require a different server strategy.
3. **100k + high-cardinality auto path** may increase server CPU and shift load earlier; tune `10_000` / thresholds if needed in production.

## Preliminary verdict

**Ship:** `avg` in hybrid, clearer forced-mode messaging, improved auto thresholds for high-cardinality layouts, and visible **hybrid + drill-down** guidance in the UI. **Defer `std`** until API, frontend, and roll-up semantics are defined.
