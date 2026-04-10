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

"""Generate golden expected values using pandas as an independent oracle.

Every expected value in the output JSON is computed **entirely by pandas** —
no PivotData output is consulted.  The pandas code is the oracle; the TS
engine is the system under test.

Usage:
    python tests/golden_data/generate_golden_expected.py

Output:
    tests/golden_data/golden_expected.json
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).parent
SMALL_CSV = DATA_DIR / "small.csv"
MEDIUM_CSV = DATA_DIR / "medium.csv"
LARGE_CSV = DATA_DIR / "large.csv"
OUTPUT = DATA_DIR / "golden_expected.json"


def _to_serializable(obj):
    """Recursively convert numpy types to native Python types."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return round(float(obj), 6)
    if isinstance(obj, np.ndarray):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, dict):
        return {str(k): _to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, float):
        return round(obj, 6)
    return obj


def _pivot_cells(
    df: pd.DataFrame, rows: list[str], cols: list[str], values: str, aggfunc: str
) -> dict:
    """Build a pivot table and return cells as {row_key: {col_key: value}}."""
    pt = pd.pivot_table(
        df, index=rows, columns=cols, values=values, aggfunc=aggfunc, observed=True
    )
    result = {}
    for row_idx in pt.index:
        row_key = row_idx if isinstance(row_idx, tuple) else (row_idx,)
        row_key_str = "|".join(str(k) for k in row_key)
        result[row_key_str] = {}
        for col_idx in pt.columns:
            col_key = col_idx if isinstance(col_idx, tuple) else (col_idx,)
            col_key_str = "|".join(str(k) for k in col_key)
            val = pt.loc[row_idx, col_idx]
            if pd.notna(val):
                result[row_key_str][col_key_str] = round(float(val), 2)
    return result


def _row_totals(df: pd.DataFrame, rows: list[str], values: str, aggfunc: str) -> dict:
    """Compute row totals (aggregate across all column dimensions)."""
    grouped = df.groupby(rows, observed=True)[values].agg(aggfunc)
    result = {}
    for idx, val in grouped.items():
        key = idx if isinstance(idx, tuple) else (idx,)
        key_str = "|".join(str(k) for k in key)
        result[key_str] = round(float(val), 2)
    return result


def _col_totals(df: pd.DataFrame, cols: list[str], values: str, aggfunc: str) -> dict:
    """Compute column totals (aggregate across all row dimensions)."""
    grouped = df.groupby(cols, observed=True)[values].agg(aggfunc)
    result = {}
    for idx, val in grouped.items():
        key = idx if isinstance(idx, tuple) else (idx,)
        key_str = "|".join(str(k) for k in key)
        result[key_str] = round(float(val), 2)
    return result


def _grand_total(df: pd.DataFrame, values: str, aggfunc: str) -> float:
    """Compute grand total."""
    if aggfunc == "sum":
        return round(float(df[values].sum()), 2)
    elif aggfunc == "mean":
        return round(float(df[values].mean()), 2)
    elif aggfunc == "count":
        return int(df[values].count())
    elif aggfunc == "min":
        return round(float(df[values].min()), 2)
    elif aggfunc == "max":
        return round(float(df[values].max()), 2)
    raise ValueError(f"Unknown aggfunc: {aggfunc}")


def config_a(df: pd.DataFrame) -> dict:
    """A — Basic sum: Region × Year, Revenue sum."""
    return {
        "description": "Basic sum: Region × Year, Revenue sum",
        "config": {
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "sum"},
        },
        "cells": _pivot_cells(df, ["Region"], ["Year"], "Revenue", "sum"),
        "row_totals": _row_totals(df, ["Region"], "Revenue", "sum"),
        "col_totals": _col_totals(df, ["Year"], "Revenue", "sum"),
        "grand_total": _grand_total(df, "Revenue", "sum"),
    }


def config_b(df: pd.DataFrame) -> dict:
    """B — Multi-measure: Region × Year, [Revenue, Profit] sum."""
    result = {
        "description": "Multi-measure: Region × Year, [Revenue, Profit] sum",
        "config": {
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue", "Profit"],
            "aggregation": {"Revenue": "sum", "Profit": "sum"},
        },
        "measures": {},
    }
    for measure in ["Revenue", "Profit"]:
        result["measures"][measure] = {
            "cells": _pivot_cells(df, ["Region"], ["Year"], measure, "sum"),
            "row_totals": _row_totals(df, ["Region"], measure, "sum"),
            "col_totals": _col_totals(df, ["Year"], measure, "sum"),
            "grand_total": _grand_total(df, measure, "sum"),
        }
    return result


def config_c(df: pd.DataFrame) -> dict:
    """C — Per-measure agg: Region × Year, Revenue=sum, Units=avg."""
    result = {
        "description": "Per-measure agg: Region × Year, Revenue=sum, Units=avg",
        "config": {
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue", "Units"],
            "aggregation": {"Revenue": "sum", "Units": "avg"},
        },
        "measures": {},
    }
    result["measures"]["Revenue"] = {
        "cells": _pivot_cells(df, ["Region"], ["Year"], "Revenue", "sum"),
        "row_totals": _row_totals(df, ["Region"], "Revenue", "sum"),
        "col_totals": _col_totals(df, ["Year"], "Revenue", "sum"),
        "grand_total": _grand_total(df, "Revenue", "sum"),
    }
    result["measures"]["Units"] = {
        "cells": _pivot_cells(df, ["Region"], ["Year"], "Units", "mean"),
        "row_totals": _row_totals(df, ["Region"], "Units", "mean"),
        "col_totals": _col_totals(df, ["Year"], "Units", "mean"),
        "grand_total": _grand_total(df, "Units", "mean"),
    }
    return result


def config_d(df: pd.DataFrame) -> dict:
    """D — Count: Region × Year, Revenue count."""
    return {
        "description": "Count: Region × Year, Revenue count",
        "config": {
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "count"},
        },
        "cells": _pivot_cells(df, ["Region"], ["Year"], "Revenue", "count"),
        "row_totals": _row_totals(df, ["Region"], "Revenue", "count"),
        "col_totals": _col_totals(df, ["Year"], "Revenue", "count"),
        "grand_total": _grand_total(df, "Revenue", "count"),
    }


def config_e(df: pd.DataFrame) -> dict:
    """E — Subtotals: [Region, Category] × Year, Revenue sum, subtotals."""
    cells = _pivot_cells(df, ["Region", "Category"], ["Year"], "Revenue", "sum")
    row_totals = _row_totals(df, ["Region", "Category"], "Revenue", "sum")
    col_totals = _col_totals(df, ["Year"], "Revenue", "sum")
    grand = _grand_total(df, "Revenue", "sum")

    # Subtotals: groupby Region only (parent level)
    subtotal_cells = _pivot_cells(df, ["Region"], ["Year"], "Revenue", "sum")
    subtotal_row_totals = _row_totals(df, ["Region"], "Revenue", "sum")

    return {
        "description": "Subtotals: [Region, Category] × Year, Revenue sum",
        "config": {
            "rows": ["Region", "Category"],
            "columns": ["Year"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "sum"},
            "show_subtotals": True,
        },
        "cells": cells,
        "row_totals": row_totals,
        "col_totals": col_totals,
        "grand_total": grand,
        "subtotals": {
            "by_region": {
                "cells": subtotal_cells,
                "row_totals": subtotal_row_totals,
            },
        },
    }


def config_f(df: pd.DataFrame) -> dict:
    """F — Pct of total: Region × Year, Revenue pct_of_total."""
    cells_raw = _pivot_cells(df, ["Region"], ["Year"], "Revenue", "sum")
    grand = _grand_total(df, "Revenue", "sum")

    pct_cells = {}
    for rk, col_vals in cells_raw.items():
        pct_cells[rk] = {}
        for ck, val in col_vals.items():
            pct_cells[rk][ck] = round(val / grand * 100, 2)

    row_totals_raw = _row_totals(df, ["Region"], "Revenue", "sum")
    pct_row_totals = {rk: round(v / grand * 100, 2) for rk, v in row_totals_raw.items()}

    col_totals_raw = _col_totals(df, ["Year"], "Revenue", "sum")
    pct_col_totals = {ck: round(v / grand * 100, 2) for ck, v in col_totals_raw.items()}

    return {
        "description": "Pct of total: Region × Year, Revenue pct_of_total",
        "config": {
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "sum"},
            "show_values_as": {"Revenue": "pct_of_total"},
        },
        "cells_raw": cells_raw,
        "grand_total": grand,
        "pct_cells": pct_cells,
        "pct_row_totals": pct_row_totals,
        "pct_col_totals": pct_col_totals,
    }


def config_f2(df: pd.DataFrame) -> dict:
    """F2 — Pct of row: Region × Year, Revenue pct_of_row."""
    cells_raw = _pivot_cells(df, ["Region"], ["Year"], "Revenue", "sum")
    row_totals_raw = _row_totals(df, ["Region"], "Revenue", "sum")

    pct_cells = {}
    for rk, col_vals in cells_raw.items():
        row_total = row_totals_raw[rk]
        pct_cells[rk] = {}
        for ck, val in col_vals.items():
            pct_cells[rk][ck] = round(val / row_total * 100, 2)

    return {
        "description": "Pct of row: Region × Year, Revenue pct_of_row",
        "config": {
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "sum"},
            "show_values_as": {"Revenue": "pct_of_row"},
        },
        "cells_raw": cells_raw,
        "row_totals_raw": row_totals_raw,
        "pct_cells": pct_cells,
    }


def config_f3(df: pd.DataFrame) -> dict:
    """F3 — Pct of col: Region × Year, Revenue pct_of_col."""
    cells_raw = _pivot_cells(df, ["Region"], ["Year"], "Revenue", "sum")
    col_totals_raw = _col_totals(df, ["Year"], "Revenue", "sum")

    pct_cells = {}
    for rk, col_vals in cells_raw.items():
        pct_cells[rk] = {}
        for ck, val in col_vals.items():
            col_total = col_totals_raw[ck]
            pct_cells[rk][ck] = round(val / col_total * 100, 2)

    return {
        "description": "Pct of col: Region × Year, Revenue pct_of_col",
        "config": {
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "sum"},
            "show_values_as": {"Revenue": "pct_of_col"},
        },
        "cells_raw": cells_raw,
        "col_totals_raw": col_totals_raw,
        "pct_cells": pct_cells,
    }


def config_g(df: pd.DataFrame) -> dict:
    """G — Filtering: Region × Year, Revenue sum, filter North+South."""
    filtered = df[df["Region"].isin(["North", "South"])]
    return {
        "description": "Filtering: Region × Year, Revenue sum, filter North+South",
        "config": {
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "sum"},
            "filters": {"Region": {"include": ["North", "South"]}},
        },
        "cells": _pivot_cells(filtered, ["Region"], ["Year"], "Revenue", "sum"),
        "row_totals": _row_totals(filtered, ["Region"], "Revenue", "sum"),
        "col_totals": _col_totals(filtered, ["Year"], "Revenue", "sum"),
        "grand_total": _grand_total(filtered, "Revenue", "sum"),
    }


def config_h(df: pd.DataFrame) -> dict:
    """H — Synthetic sum_over_sum: Region, [Revenue, Units], sum_over_sum."""
    rev_by_region = df.groupby("Region", observed=True)["Revenue"].sum()
    units_by_region = df.groupby("Region", observed=True)["Units"].sum()
    ratios = (rev_by_region / units_by_region).round(6)

    grand_rev = float(df["Revenue"].sum())
    grand_units = float(df["Units"].sum())
    grand_ratio = round(grand_rev / grand_units, 6)

    return {
        "description": "Synthetic sum_over_sum: Region, Revenue/Units ratio",
        "config": {
            "rows": ["Region"],
            "columns": [],
            "values": ["Revenue", "Units"],
            "aggregation": {"Revenue": "sum", "Units": "sum"},
            "synthetic_measures": [
                {
                    "id": "rev_per_unit",
                    "label": "Rev/Unit",
                    "operation": "sum_over_sum",
                    "numerator": "Revenue",
                    "denominator": "Units",
                }
            ],
        },
        "raw_revenue": {k: round(float(v), 2) for k, v in rev_by_region.items()},
        "raw_units": {k: int(v) for k, v in units_by_region.items()},
        "synthetic_ratios": {k: round(float(v), 6) for k, v in ratios.items()},
        "grand_total_revenue": round(grand_rev, 2),
        "grand_total_units": int(grand_units),
        "grand_total_ratio": grand_ratio,
    }


def config_h2(df: pd.DataFrame) -> dict:
    """H2 — Synthetic difference: Region, [Revenue, Profit], difference."""
    rev_by_region = df.groupby("Region", observed=True)["Revenue"].sum()
    profit_by_region = df.groupby("Region", observed=True)["Profit"].sum()
    diffs = (rev_by_region - profit_by_region).round(2)

    grand_rev = float(df["Revenue"].sum())
    grand_profit = float(df["Profit"].sum())
    grand_diff = round(grand_rev - grand_profit, 2)

    return {
        "description": "Synthetic difference: Region, Revenue - Profit",
        "config": {
            "rows": ["Region"],
            "columns": [],
            "values": ["Revenue", "Profit"],
            "aggregation": {"Revenue": "sum", "Profit": "sum"},
            "synthetic_measures": [
                {
                    "id": "rev_minus_profit",
                    "label": "Rev-Profit",
                    "operation": "difference",
                    "numerator": "Revenue",
                    "denominator": "Profit",
                }
            ],
        },
        "raw_revenue": {k: round(float(v), 2) for k, v in rev_by_region.items()},
        "raw_profit": {k: round(float(v), 2) for k, v in profit_by_region.items()},
        "synthetic_diffs": {k: round(float(v), 2) for k, v in diffs.items()},
        "grand_total_revenue": round(grand_rev, 2),
        "grand_total_profit": round(grand_profit, 2),
        "grand_total_diff": grand_diff,
    }


def config_i(df: pd.DataFrame) -> dict:
    """I — Value sort: Region × Year, Revenue sum, sort desc by value."""
    rev_by_region = df.groupby("Region", observed=True)["Revenue"].sum()
    sorted_regions = rev_by_region.sort_values(ascending=False).index.tolist()

    return {
        "description": "Value sort: Region × Year, Revenue sum, sort desc by value",
        "config": {
            "rows": ["Region"],
            "columns": ["Year"],
            "values": ["Revenue"],
            "aggregation": {"Revenue": "sum"},
            "row_sort": {
                "by": "value",
                "direction": "desc",
                "value_field": "Revenue",
            },
        },
        "cells": _pivot_cells(df, ["Region"], ["Year"], "Revenue", "sum"),
        "row_totals": _row_totals(df, ["Region"], "Revenue", "sum"),
        "col_totals": _col_totals(df, ["Year"], "Revenue", "sum"),
        "grand_total": _grand_total(df, "Revenue", "sum"),
        "expected_row_order": sorted_regions,
    }


def generate_scaled_configs():
    """Generate scaled golden values for medium and large datasets."""
    scaled = {}

    if MEDIUM_CSV.exists():
        df_med = pd.read_csv(MEDIUM_CSV)
        scaled["A_medium"] = {
            "description": "Basic sum at 10K: Region × Year, Revenue sum",
            "cells": _pivot_cells(df_med, ["Region"], ["Year"], "Revenue", "sum"),
            "row_totals": _row_totals(df_med, ["Region"], "Revenue", "sum"),
            "col_totals": _col_totals(df_med, ["Year"], "Revenue", "sum"),
            "grand_total": _grand_total(df_med, "Revenue", "sum"),
        }
        scaled["C_medium"] = {
            "description": "Per-measure agg at 10K: Region × Year, Revenue=sum, Units=avg",
            "measures": {
                "Revenue": {
                    "cells": _pivot_cells(
                        df_med, ["Region"], ["Year"], "Revenue", "sum"
                    ),
                    "row_totals": _row_totals(df_med, ["Region"], "Revenue", "sum"),
                    "grand_total": _grand_total(df_med, "Revenue", "sum"),
                },
                "Units": {
                    "cells": _pivot_cells(
                        df_med, ["Region"], ["Year"], "Units", "mean"
                    ),
                    "row_totals": _row_totals(df_med, ["Region"], "Units", "mean"),
                    "grand_total": _grand_total(df_med, "Units", "mean"),
                },
            },
        }
        scaled["E_medium"] = {
            "description": "2-level subtotals at 10K: [Region,Category] × Year, Revenue sum",
            "cells": _pivot_cells(
                df_med, ["Region", "Category"], ["Year"], "Revenue", "sum"
            ),
            "row_totals": _row_totals(df_med, ["Region", "Category"], "Revenue", "sum"),
            "col_totals": _col_totals(df_med, ["Year"], "Revenue", "sum"),
            "grand_total": _grand_total(df_med, "Revenue", "sum"),
            "subtotals": {
                "by_region": {
                    "cells": _pivot_cells(
                        df_med, ["Region"], ["Year"], "Revenue", "sum"
                    ),
                    "row_totals": _row_totals(df_med, ["Region"], "Revenue", "sum"),
                },
            },
        }

    if LARGE_CSV.exists():
        df_lg = pd.read_csv(LARGE_CSV)
        scaled["A_large"] = {
            "description": "Basic sum at 200K: Region × Year, Revenue sum",
            "cells": _pivot_cells(df_lg, ["Region"], ["Year"], "Revenue", "sum"),
            "row_totals": _row_totals(df_lg, ["Region"], "Revenue", "sum"),
            "col_totals": _col_totals(df_lg, ["Year"], "Revenue", "sum"),
            "grand_total": _grand_total(df_lg, "Revenue", "sum"),
        }
        scaled["E_large"] = {
            "description": "2-level subtotals at 200K: [Region,Category] × Year, Revenue sum",
            "cells": _pivot_cells(
                df_lg, ["Region", "Category"], ["Year"], "Revenue", "sum"
            ),
            "row_totals": _row_totals(df_lg, ["Region", "Category"], "Revenue", "sum"),
            "col_totals": _col_totals(df_lg, ["Year"], "Revenue", "sum"),
            "grand_total": _grand_total(df_lg, "Revenue", "sum"),
            "subtotals": {
                "by_region": {
                    "cells": _pivot_cells(
                        df_lg, ["Region"], ["Year"], "Revenue", "sum"
                    ),
                    "row_totals": _row_totals(df_lg, ["Region"], "Revenue", "sum"),
                },
            },
        }

    return scaled


def main():
    df = pd.read_csv(SMALL_CSV)
    print(f"Loaded {SMALL_CSV}: {len(df)} rows, {len(df.columns)} cols")

    golden = {
        "A": config_a(df),
        "B": config_b(df),
        "C": config_c(df),
        "D": config_d(df),
        "E": config_e(df),
        "F": config_f(df),
        "F2": config_f2(df),
        "F3": config_f3(df),
        "G": config_g(df),
        "H": config_h(df),
        "H2": config_h2(df),
        "I": config_i(df),
    }

    scaled = generate_scaled_configs()
    if scaled:
        golden["scaled"] = scaled

    output = _to_serializable(golden)
    OUTPUT.write_text(json.dumps(output, indent=2, ensure_ascii=False) + "\n")
    print(f"Wrote {OUTPUT} ({OUTPUT.stat().st_size:,} bytes)")
    print(f"Configs: {', '.join(k for k in golden if k != 'scaled')}")
    if scaled:
        print(f"Scaled configs: {', '.join(scaled.keys())}")


if __name__ == "__main__":
    main()
