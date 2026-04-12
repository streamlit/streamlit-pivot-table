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

"""Streamlit Pivot Table -- BI-focused pivot table component for Streamlit."""

from __future__ import annotations

import json
import warnings
from math import prod
from typing import TYPE_CHECKING, Any, TypedDict, cast

import pandas as pd

if TYPE_CHECKING:
    from collections.abc import Callable

try:
    import streamlit as st
    from streamlit.dataframe_util import convert_anything_to_pandas_df
except ImportError as e:
    raise ImportError(
        "streamlit_pivot requires Streamlit >= 1.51. "
        "Install it with: pip install 'streamlit>=1.51'"
    ) from e


# ---------------------------------------------------------------------------
# Config schema v1
# ---------------------------------------------------------------------------

CONFIG_SCHEMA_VERSION = 1


class SortConfig(TypedDict, total=False):
    """Sort configuration for a pivot axis (rows or columns).

    When ``by="key"`` (default), rows/columns are sorted alphabetically by
    their dimension labels.  When ``by="value"``, they are sorted by an
    aggregated measure (optionally restricted to a specific column via
    ``col_key`` for row sorting).
    """

    by: str  # "key" | "value"
    direction: str  # "asc" | "desc"
    value_field: str  # required when by="value"
    col_key: list[str]  # for row sort: sort within this specific column
    dimension: str  # optional: scope sort to this dimension level and below


class PivotConfig(TypedDict, total=False):
    """Versioned configuration schema for the pivot table (v1).

    Persisted via setStateValue("config", ...) and round-tripped through
    Python session state.  Every field has an explicit default so partial
    configs are safe.
    """

    version: int  # always 1 for this schema
    rows: list[str]
    columns: list[str]
    values: list[str]
    synthetic_measures: list[dict[str, Any]]
    aggregation: dict[str, str]
    show_totals: bool
    show_row_totals: bool | list[str]
    show_column_totals: bool | list[str]
    empty_cell_value: str
    interactive: bool
    row_sort: SortConfig
    col_sort: SortConfig
    show_subtotals: bool | list[str]
    repeat_row_labels: bool
    collapsed_groups: list[str]
    collapsed_col_groups: list[str]
    sticky_headers: bool
    show_values_as: dict[str, str]
    conditional_formatting: list[dict[str, Any]]
    number_format: dict[str, str]
    column_alignment: dict[str, str]


VALID_AGGREGATIONS = frozenset(
    (
        "sum",
        "avg",
        "count",
        "min",
        "max",
        "count_distinct",
        "median",
        "percentile_90",
        "first",
        "last",
    )
)
VALID_SHOW_VALUES_AS = frozenset(("raw", "pct_of_total", "pct_of_row", "pct_of_col"))
VALID_ALIGNMENTS = frozenset(("left", "center", "right"))
VALID_COND_FMT_TYPES = frozenset(("color_scale", "data_bars", "threshold"))
VALID_NULL_MODES = frozenset(("exclude", "zero", "separate"))
_HYBRID_PANDAS_AGGFUNC: dict[str, str | None] = {
    "sum": "sum",
    "count": "count",
    "min": "min",
    "max": "max",
    "avg": None,
    "count_distinct": "nunique",
    "median": "median",
    "percentile_90": "percentile_90",
    "first": "first",
    "last": "last",
}

_SIDECAR_REQUIRED_AGGS = frozenset(
    ("avg", "count_distinct", "median", "percentile_90", "first", "last")
)

_FULLY_DECOMPOSABLE_AGGS = frozenset(("sum", "min", "max"))

_NUMERIC_COERCE_AGGS = frozenset(
    ("sum", "avg", "min", "max", "median", "percentile_90", "first", "last")
)


_warned_keys: set[str] = set()
_PYTHON_CONFIG_STATE_PREFIX = "__streamlit_pivot_python_config__:"
_DRILLDOWN_PAGE_SIZE = 500


def _estimate_group_count(df: Any, fields: list[str]) -> int:
    if not fields:
        return 1
    distinct_counts = [int(df[field].nunique(dropna=False)) for field in fields]
    return min(len(df), int(prod(distinct_counts)))


def _match_source_filter_values(series: Any, values: list[Any]) -> Any:
    """Match raw source-filter values with explicit null handling.

    ``None`` in the filter list matches null-like pandas values via ``isna()``.
    Other values use raw ``isin()`` comparison with no type coercion.
    """
    has_null = any(v is None for v in values)
    non_null = [v for v in values if v is not None]
    mask = pd.Series(False, index=series.index)
    if non_null:
        mask |= series.isin(non_null)
    if has_null:
        mask |= series.isna()
    return mask


def _apply_source_filters(
    df: Any,
    source_filters: dict[str, dict[str, list[Any]]] | None,
) -> Any:
    """Apply server-only raw-value filters to the source DataFrame.

    Semantics intentionally differ from ``_resolve_and_filter``:
    - raw Python values, not resolved frontend keys
    - ``None`` matches null-like pandas values
    - ``""`` matches only literal empty strings
    - no type coercion is performed
    - include takes precedence over exclude
    """
    if not source_filters:
        return df
    mask = pd.Series(True, index=df.index)
    for field, filt in source_filters.items():
        if field not in df.columns:
            raise ValueError(
                f"source_filters contains column not in DataFrame: {field!r}. "
                f"Available columns: {sorted(df.columns.tolist())}"
            )
        col = df[field]
        include = filt.get("include")
        exclude = filt.get("exclude")
        if include:
            mask &= _match_source_filter_values(col, include)
        elif exclude:
            mask &= ~_match_source_filter_values(col, exclude)
    return df[mask]


def _can_use_threshold_hybrid(config: PivotConfig) -> tuple[bool, str]:
    if config.get("synthetic_measures"):
        return False, "threshold_hybrid currently skips synthetic measures"
    filters = config.get("filters", {})
    if filters:
        dim_set = set(config.get("rows", []) + config.get("columns", []))
        non_dim = [f for f in filters if f not in dim_set]
        if non_dim:
            return False, (
                f"threshold_hybrid requires filters on row/column dimensions only; "
                f"filter on {non_dim} is not in the current layout"
            )
    return True, "config is compatible with threshold_hybrid"


def _should_use_threshold_hybrid(
    df: Any,
    config: PivotConfig,
    execution_mode: str,
) -> tuple[bool, str]:
    compatible, reason = _can_use_threshold_hybrid(config)
    if execution_mode == "client_only":
        return False, "execution_mode forced client_only"
    if execution_mode == "threshold_hybrid":
        if not compatible:
            return False, reason
        return True, (
            "Server pre-aggregation is enabled because execution_mode is "
            "'threshold_hybrid' (automatic row-count thresholds are not applied)."
        )
    if not compatible:
        return False, reason

    row_groups = _estimate_group_count(df, config.get("rows", []))
    col_groups = _estimate_group_count(df, config.get("columns", []))
    rendered_values = max(1, len(config.get("values", [])))
    visible_cells = row_groups * min(col_groups, 200) * rendered_values
    estimated_pivot_groups = row_groups * col_groups
    high_cardinality = estimated_pivot_groups > 10_000
    row_threshold = 100_000 if high_cardinality else 250_000
    if len(df) >= row_threshold and (
        visible_cells > 5_000 or col_groups > 200 or row_groups > 5_000
    ):
        card_note = (
            "high estimated pivot cardinality"
            if high_cardinality
            else "moderate estimated pivot cardinality"
        )
        return True, (
            f"auto-selected threshold_hybrid: dataset has at least {row_threshold:,} "
            f"rows with {card_note}, and the estimated pivot shape exceeds the "
            "client-side comfort budget."
        )
    return False, "auto-selected client_only because the dataset stays within budget"


def _prepare_threshold_hybrid_frame(
    df: Any, config: PivotConfig, null_handling: Any = None
) -> Any:
    group_fields = [*config.get("rows", []), *config.get("columns", [])]
    aggregation = dict(config.get("aggregation", {}))
    value_fields = list(config.get("values", []))

    if not value_fields:
        if not group_fields:
            return df.iloc[0:0].copy()
        return pd.DataFrame(columns=group_fields)

    named: dict[str, pd.NamedAgg] = {}
    avg_fields: list[str] = []
    numeric_coerce_fields: list[str] = []

    for vf in value_fields:
        agg = aggregation.get(vf, "sum")
        if agg in _NUMERIC_COERCE_AGGS:
            numeric_coerce_fields.append(vf)
        if agg == "avg":
            avg_fields.append(vf)
            named[f"{vf}__sum"] = pd.NamedAgg(column=vf, aggfunc="sum")
            named[f"{vf}__cnt"] = pd.NamedAgg(column=vf, aggfunc="count")
        elif agg == "percentile_90":
            named[vf] = pd.NamedAgg(column=vf, aggfunc=lambda x: x.quantile(0.9))
        elif agg in ("first", "last"):
            named[vf] = pd.NamedAgg(column=vf, aggfunc=agg)
        elif agg == "count_distinct":
            named[vf] = pd.NamedAgg(column=vf, aggfunc="nunique")
        else:
            named[vf] = pd.NamedAgg(column=vf, aggfunc=agg)

    filtered_df = _resolve_and_filter(df, config.get("filters", {}), null_handling)

    if not group_fields:
        row: dict[str, Any] = {}
        for vf in value_fields:
            agg = aggregation.get(vf, "sum")
            ser = filtered_df[vf]
            if agg in _NUMERIC_COERCE_AGGS:
                ser = pd.to_numeric(ser, errors="coerce")
            if agg == "avg":
                cnt = int(ser.count())
                row[vf] = float(ser.sum() / cnt) if cnt else float("nan")
            elif agg == "percentile_90":
                row[vf] = ser.quantile(0.9)
            elif agg == "count_distinct":
                row[vf] = ser.nunique()
            elif agg in ("first", "last"):
                numeric_ser = ser.dropna()
                row[vf] = (
                    numeric_ser.iloc[0]
                    if agg == "first" and len(numeric_ser) > 0
                    else (
                        numeric_ser.iloc[-1]
                        if agg == "last" and len(numeric_ser) > 0
                        else float("nan")
                    )
                )
            else:
                row[vf] = ser.agg(agg)
        return pd.DataFrame([row])

    working = filtered_df.copy()
    for vf in numeric_coerce_fields:
        working[vf] = pd.to_numeric(working[vf], errors="coerce")

    out = (
        working.groupby(group_fields, dropna=False, observed=True, sort=False)
        .agg(**named)
        .reset_index()
    )

    for vf in avg_fields:
        sum_col = f"{vf}__sum"
        cnt_col = f"{vf}__cnt"
        cnt = out[cnt_col].astype("float64")
        sm = out[sum_col].astype("float64")
        out[vf] = sm.div(cnt).where(cnt > 0)
        out = out.drop(columns=[sum_col, cnt_col])

    return out


def _build_hybrid_agg_remap(aggregation: dict[str, str]) -> dict[str, str]:
    """Map fields whose client-side aggregation must change for correct leaf cells."""
    remap: dict[str, str] = {}
    for field, agg in aggregation.items():
        if agg in ("count", "count_distinct"):
            remap[field] = "sum"
    return remap


def _get_null_mode(field: str, null_handling: Any) -> str:
    """Resolve per-field null handling mode (mirrors frontend getNullMode)."""
    if null_handling is None:
        return "exclude"
    if isinstance(null_handling, str):
        return null_handling
    if isinstance(null_handling, dict):
        return null_handling.get(field, "exclude")
    return "exclude"


def _normalize_dim_values(df: Any, dims: list[str], null_handling: Any) -> Any:
    """Rewrite null/empty dimension values to match frontend _resolveDimValue."""
    df = df.copy()
    for dim in dims:
        if dim not in df.columns:
            continue
        mode = _get_null_mode(dim, null_handling)
        if mode == "separate":
            df[dim] = df[dim].fillna("(null)").replace("", "(null)").astype(str)
        else:
            df[dim] = df[dim].fillna("").astype(str)
    return df


def _resolve_and_filter(
    df: Any,
    filters: dict[str, dict] | None,
    null_handling: Any,
) -> Any:
    """Apply dimension filters to a raw DataFrame using resolved-value semantics.

    Mirrors PivotData._shouldIncludeRow + _resolveDimValue: for every filter
    field, resolve null/empty values via per-field _get_null_mode, then compare.
    """
    if not filters:
        return df
    mask = pd.Series(True, index=df.index)
    for field, filt in filters.items():
        if field not in df.columns:
            continue
        mode = _get_null_mode(field, null_handling)
        if mode == "separate":
            resolved = df[field].fillna("(null)").replace("", "(null)").astype(str)
        else:
            resolved = df[field].fillna("").astype(str)
        inc = filt.get("include")
        exc = filt.get("exclude")
        if inc:
            mask &= resolved.isin(inc)
        elif exc:
            mask &= ~resolved.isin(exc)
    return df[mask]


def _normalize_sidecar_value(v: Any) -> int | float | None:
    """Coerce numpy scalars/NaN to JSON-compatible Python types."""
    import math
    import numpy as np

    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        val = float(v)
        return None if math.isnan(val) else val
    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return None
        return v
    return v


def _normalize_null_handling(nh: Any) -> Any:
    """Canonicalize null_handling: sort keys when it's a per-field dict."""
    if isinstance(nh, dict):
        return dict(sorted(nh.items()))
    return nh


def _normalize_show_subtotals(st: Any) -> Any:
    """Canonicalize show_subtotals: sort when it's a string list (set semantics)."""
    if isinstance(st, list):
        return sorted(st)
    return st


def _build_sidecar_fingerprint(config: PivotConfig, null_handling: Any) -> str:
    """Deterministic canonical JSON string for staleness detection."""
    agg = config.get("aggregation", {})
    filters = config.get("filters", {})
    obj = {
        "aggregation": dict(sorted(agg.items())) if agg else {},
        "columns": config.get("columns", []),
        "filters": {
            k: {
                "exclude": sorted(v.get("exclude", [])),
                "include": sorted(v.get("include", [])),
            }
            for k, v in sorted(filters.items())
        }
        if filters
        else {},
        "null_handling": _normalize_null_handling(null_handling),
        "rows": config.get("rows", []),
        "show_subtotals": _normalize_show_subtotals(
            config.get("show_subtotals", False)
        ),
        "values": config.get("values", []),
    }
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _sidecar_agg_func(agg: str, series: Any) -> Any:
    """Compute a single aggregate on a pandas Series for sidecar totals.

    Applies pd.to_numeric coercion for numeric aggs to match the frontend's
    toNumber() semantics (non-numeric strings are dropped).
    """
    if agg == "count_distinct":
        return series.nunique()
    numeric = pd.to_numeric(series, errors="coerce")
    if agg == "avg":
        return numeric.mean()
    if agg == "median":
        return numeric.median()
    if agg == "percentile_90":
        return numeric.quantile(0.9)
    if agg == "first":
        clean = numeric.dropna()
        return clean.iloc[0] if len(clean) > 0 else None
    if agg == "last":
        clean = numeric.dropna()
        return clean.iloc[-1] if len(clean) > 0 else None
    return None


def _sidecar_groupby_agg(
    df: Any,
    group_cols: list[str],
    sidecar_fields: dict[str, str],
) -> list[dict[str, Any]]:
    """GroupBy + aggregate for sidecar total entries."""
    if not group_cols or not sidecar_fields:
        return []
    entries: list[dict[str, Any]] = []
    grouped = df.groupby(group_cols, dropna=False, observed=True, sort=False)
    for key_tuple, group_df in grouped:
        key = list(key_tuple) if isinstance(key_tuple, tuple) else [key_tuple]
        key = [str(k) for k in key]
        values: dict[str, int | float | None] = {}
        for field, agg in sidecar_fields.items():
            val = _sidecar_agg_func(agg, group_df[field])
            values[field] = _normalize_sidecar_value(val)
        entries.append({"key": key, "values": values})
    return entries


def _compute_hybrid_totals(
    df: Any,
    config: PivotConfig,
    null_handling: Any,
) -> dict[str, Any] | None:
    """Build the hybrid_totals sidecar dict with pre-computed totals."""
    aggregation = config.get("aggregation", {})
    rows = config.get("rows", [])
    columns = config.get("columns", [])
    values = config.get("values", [])
    show_subtotals = config.get("show_subtotals", False)

    sidecar_fields = {
        vf: agg
        for vf in values
        if (agg := aggregation.get(vf, "sum")) in _SIDECAR_REQUIRED_AGGS
    }
    remap_only_fields = {
        vf: agg
        for vf in values
        if (agg := aggregation.get(vf, "sum")) in ("count", "count_distinct")
        and vf not in sidecar_fields
    }

    if not sidecar_fields and not remap_only_fields:
        return None

    if not sidecar_fields:
        return {
            "sidecar_fingerprint": _build_sidecar_fingerprint(config, null_handling),
            "grand": {},
            "row": [],
            "col": [],
        }

    all_dims = rows + columns
    working = _normalize_dim_values(df, all_dims, null_handling)
    working = _resolve_and_filter(working, config.get("filters", {}), null_handling)

    fingerprint = _build_sidecar_fingerprint(config, null_handling)

    grand: dict[str, int | float | None] = {}
    for field, agg in sidecar_fields.items():
        val = _sidecar_agg_func(agg, working[field])
        grand[field] = _normalize_sidecar_value(val)

    row_entries = _sidecar_groupby_agg(working, rows, sidecar_fields) if rows else []
    col_entries = (
        _sidecar_groupby_agg(working, columns, sidecar_fields) if columns else []
    )

    result: dict[str, Any] = {
        "sidecar_fingerprint": fingerprint,
        "grand": grand,
        "row": row_entries,
        "col": col_entries,
    }

    if len(columns) >= 2:
        col_prefix = columns[:1]
        col_prefix_entries: list[dict[str, Any]] = []
        col_prefix_grand_entries: list[dict[str, Any]] = []

        if rows:
            grouped = working.groupby(
                rows + col_prefix, dropna=False, observed=True, sort=False
            )
            for key_tuple, group_df in grouped:
                parts = list(key_tuple) if isinstance(key_tuple, tuple) else [key_tuple]
                parts = [str(p) for p in parts]
                row_key = parts[: len(rows)]
                cp_key = parts[len(rows) :]
                vals: dict[str, int | float | None] = {}
                for field, agg in sidecar_fields.items():
                    v = _sidecar_agg_func(agg, group_df[field])
                    vals[field] = _normalize_sidecar_value(v)
                col_prefix_entries.append(
                    {"key": cp_key, "row": row_key, "values": vals}
                )

        grouped_grand = working.groupby(
            col_prefix, dropna=False, observed=True, sort=False
        )
        for key_val, group_df in grouped_grand:
            cp_key = (
                [str(key_val)]
                if not isinstance(key_val, tuple)
                else [str(k) for k in key_val]
            )
            vals_grand: dict[str, int | float | None] = {}
            for field, agg in sidecar_fields.items():
                v = _sidecar_agg_func(agg, group_df[field])
                vals_grand[field] = _normalize_sidecar_value(v)
            col_prefix_grand_entries.append({"key": cp_key, "values": vals_grand})

        result["col_prefix"] = col_prefix_entries
        result["col_prefix_grand"] = col_prefix_grand_entries

    if show_subtotals and len(rows) >= 2:
        subtotal_entries: list[dict[str, Any]] = []
        cross_subtotal_entries: list[dict[str, Any]] = []

        for depth in range(1, len(rows)):
            row_prefix = rows[:depth]

            if columns:
                grouped_sub = working.groupby(
                    row_prefix + columns, dropna=False, observed=True, sort=False
                )
                for key_tuple, group_df in grouped_sub:
                    parts = (
                        list(key_tuple) if isinstance(key_tuple, tuple) else [key_tuple]
                    )
                    parts = [str(p) for p in parts]
                    rp = parts[:depth]
                    ck = parts[depth:]
                    vals_sub: dict[str, int | float | None] = {}
                    for field, agg in sidecar_fields.items():
                        v = _sidecar_agg_func(agg, group_df[field])
                        vals_sub[field] = _normalize_sidecar_value(v)
                    subtotal_entries.append({"key": rp, "col": ck, "values": vals_sub})

            grouped_row_total = working.groupby(
                row_prefix, dropna=False, observed=True, sort=False
            )
            for key_tuple, group_df in grouped_row_total:
                parts = list(key_tuple) if isinstance(key_tuple, tuple) else [key_tuple]
                parts = [str(p) for p in parts]
                vals_rt: dict[str, int | float | None] = {}
                for field, agg in sidecar_fields.items():
                    v = _sidecar_agg_func(agg, group_df[field])
                    vals_rt[field] = _normalize_sidecar_value(v)
                subtotal_entries.append({"key": parts, "col": [], "values": vals_rt})

            if len(columns) >= 2:
                col_prefix = columns[:1]
                grouped_cross = working.groupby(
                    row_prefix + col_prefix, dropna=False, observed=True, sort=False
                )
                for key_tuple, group_df in grouped_cross:
                    parts_c = (
                        list(key_tuple) if isinstance(key_tuple, tuple) else [key_tuple]
                    )
                    parts_c = [str(p) for p in parts_c]
                    rp_c = parts_c[:depth]
                    cp_c = parts_c[depth:]
                    vals_cross: dict[str, int | float | None] = {}
                    for field, agg in sidecar_fields.items():
                        v = _sidecar_agg_func(agg, group_df[field])
                        vals_cross[field] = _normalize_sidecar_value(v)
                    cross_subtotal_entries.append(
                        {"key": rp_c, "col_prefix": cp_c, "values": vals_cross}
                    )

        result["subtotals"] = subtotal_entries
        result["cross_subtotals"] = cross_subtotal_entries

    return result


def _compute_hybrid_drilldown(
    df: Any,
    drilldown_request: dict[str, Any],
    null_handling: Any = None,
    dims: list[str] | None = None,
    config_filters: dict[str, dict] | None = None,
    page_size: int = _DRILLDOWN_PAGE_SIZE,
) -> tuple[list[dict[str, Any]], list[str], int, int]:
    """Filter the original DataFrame for a hybrid-mode drill-down request.

    Uses resolved-dimension semantics (matching _resolveDimValue on the
    frontend) so that filter values like "(null)" align correctly with
    null_handling modes.  Applies config-level dimension filters first
    (matching _shouldIncludeRow), then cell-click filters.

    Returns (records_list, column_names, total_matching_count, page).
    """
    working = _resolve_and_filter(df, config_filters or {}, null_handling)

    filters: dict[str, str] = drilldown_request.get("filters", {})
    page: int = max(0, int(drilldown_request.get("page", 0)))

    mask = pd.Series(True, index=working.index)
    for col, val in filters.items():
        if col not in working.columns:
            continue
        mode = _get_null_mode(col, null_handling)
        if mode == "separate":
            resolved = working[col].fillna("(null)").replace("", "(null)").astype(str)
        else:
            resolved = working[col].fillna("").astype(str)
        mask &= resolved == str(val)
    filtered = working[mask]
    total_count = len(filtered)
    offset = page * page_size
    page_slice = filtered.iloc[offset : offset + page_size]
    records = json.loads(page_slice.to_json(orient="records", date_format="iso"))
    return records, list(page_slice.columns), total_count, page


def _normalize_aggregation_config(
    aggregation: str | dict[str, str] | None,
    values: list[str],
) -> dict[str, str]:
    """Normalize aggregation input to a canonical per-value map."""
    if not values:
        return {}
    if aggregation is None:
        return {value: "sum" for value in values}
    if isinstance(aggregation, str):
        if aggregation not in VALID_AGGREGATIONS:
            raise ValueError(
                f"aggregation must be one of {sorted(VALID_AGGREGATIONS)}, got {aggregation!r}"
            )
        return {value: aggregation for value in values}
    if not isinstance(aggregation, dict):
        raise TypeError(
            f"aggregation must be a str, dict[str, str], or None, got {type(aggregation).__name__}"
        )
    normalized: dict[str, str] = {}
    for value in values:
        agg = aggregation.get(value, "sum")
        if not isinstance(agg, str):
            raise TypeError(
                f"aggregation[{value!r}] must be a string, got {type(agg).__name__}"
            )
        if agg not in VALID_AGGREGATIONS:
            raise ValueError(
                f"aggregation[{value!r}] must be one of {sorted(VALID_AGGREGATIONS)}, got {agg!r}"
            )
        normalized[value] = agg
    return normalized


def _normalize_config_aggregation(config: Any) -> Any:
    """Normalize a config object's aggregation field in-place-compatible form."""
    if not isinstance(config, dict):
        return config
    values = config.get("values")
    value_list = (
        [v for v in values if isinstance(v, str)] if isinstance(values, list) else []
    )
    normalized = dict(config)
    normalized["aggregation"] = _normalize_aggregation_config(
        normalized.get("aggregation"),
        value_list,
    )
    return normalized


def _stable_config_json(config: Any) -> str:
    """Serialize a config deterministically after aggregation normalization."""
    return json.dumps(
        _normalize_config_aggregation(config),
        sort_keys=True,
        separators=(",", ":"),
    )


def _resolve_config_to_send(
    session_state: Any,
    key: str,
    initial_config: PivotConfig,
) -> PivotConfig:
    """Resolve Python config vs persisted frontend config precedence.

    Preserve persisted user config across normal reruns, but when Python sends a
    new config for the same component key, prefer that new Python config.
    """
    tracker_key = f"{_PYTHON_CONFIG_STATE_PREFIX}{key}"
    initial_json = _stable_config_json(initial_config)

    previous_python_json = None
    try:
        previous_python_json = session_state.get(tracker_key)
    except (AttributeError, TypeError):
        previous_python_json = None

    persisted_config = None
    try:
        persisted_config = session_state.get(key, {}).get("config")
    except (AttributeError, TypeError):
        persisted_config = None

    normalized_persisted = (
        _normalize_config_aggregation(persisted_config)
        if persisted_config is not None
        else None
    )
    python_config_changed = (
        previous_python_json is not None and previous_python_json != initial_json
    )

    try:
        session_state[tracker_key] = initial_json
    except Exception:
        pass

    if normalized_persisted is None or python_config_changed:
        return initial_config
    return normalized_persisted


def _validate_list_field(
    items: list[str],
    valid: list[str],
    param_name: str,
    valid_label: str,
) -> bool | list[str]:
    """Filter list to valid members, warn once per unknown entry, normalize."""
    filtered: list[str] = []
    for item in items:
        if item in valid:
            filtered.append(item)
        else:
            warn_key = f"{param_name}:{item}"
            if warn_key not in _warned_keys:
                _warned_keys.add(warn_key)
                warnings.warn(
                    f"{param_name}: ignoring unknown entry {item!r} "
                    f"— valid {valid_label} are {valid}",
                    stacklevel=4,
                )
    if len(filtered) == 0:
        return False
    if len(filtered) == len(valid):
        return True
    return filtered


def _default_config(
    rows: list[str] | None = None,
    columns: list[str] | None = None,
    values: list[str] | None = None,
    synthetic_measures: list[dict[str, Any]] | None = None,
    aggregation: str | dict[str, str] = "sum",
    show_totals: bool = True,
    show_row_totals: bool | list[str] | None = None,
    show_column_totals: bool | list[str] | None = None,
    empty_cell_value: str = "-",
    interactive: bool = True,
    row_sort: SortConfig | None = None,
    col_sort: SortConfig | None = None,
    sticky_headers: bool = True,
    show_subtotals: bool | list[str] = False,
    repeat_row_labels: bool = False,
    show_values_as: dict[str, str] | None = None,
    conditional_formatting: list[dict[str, Any]] | None = None,
    number_format: str | dict[str, str] | None = None,
    column_alignment: dict[str, str] | None = None,
) -> PivotConfig:
    _rows = rows or []
    _values = values or []
    cfg = PivotConfig(
        version=CONFIG_SCHEMA_VERSION,
        rows=_rows,
        columns=columns or [],
        values=_values,
        synthetic_measures=synthetic_measures or [],
        aggregation=_normalize_aggregation_config(aggregation, _values),
        show_totals=show_totals,
        show_row_totals=show_row_totals if show_row_totals is not None else show_totals,
        show_column_totals=show_column_totals
        if show_column_totals is not None
        else show_totals,
        empty_cell_value=empty_cell_value,
        interactive=interactive,
    )
    if isinstance(cfg["show_row_totals"], list):
        cfg["show_row_totals"] = _validate_list_field(
            cfg["show_row_totals"],
            _values,
            "show_row_totals",
            "values",
        )
    if isinstance(cfg["show_column_totals"], list):
        cfg["show_column_totals"] = _validate_list_field(
            cfg["show_column_totals"],
            _values,
            "show_column_totals",
            "values",
        )
    if row_sort is not None:
        cfg["row_sort"] = row_sort
    if col_sort is not None:
        cfg["col_sort"] = col_sort
    if not sticky_headers:
        cfg["sticky_headers"] = False
    if show_subtotals:
        validated: bool | list[str] = show_subtotals
        if isinstance(validated, list):
            validated = _validate_list_field(
                validated, _rows[:-1], "show_subtotals", "rows"
            )
        cfg["show_subtotals"] = validated
    if repeat_row_labels:
        cfg["repeat_row_labels"] = True
    if show_values_as is not None:
        cfg["show_values_as"] = show_values_as
    if conditional_formatting is not None:
        cfg["conditional_formatting"] = conditional_formatting
    if number_format is not None:
        nf = (
            {"__all__": number_format}
            if isinstance(number_format, str)
            else number_format
        )
        cfg["number_format"] = nf
    if column_alignment is not None:
        cfg["column_alignment"] = column_alignment
    return cfg


# ---------------------------------------------------------------------------
# Event payload schemas
# ---------------------------------------------------------------------------


class CellClickPayload(TypedDict):
    """Payload fired by setTriggerValue("cell_click", ...).

    Canonical schema -- both frontend and Python must agree on this shape.
    """

    rowKey: list[str]
    colKey: list[str]
    value: float | None
    filters: dict[str, str]
    valueField: str


class PerfActionMeasurement(TypedDict, total=False):
    """Payload fired by setStateValue("perf_metrics", ...)."""

    kind: str
    elapsedMs: float
    axis: str
    field: str
    totalCount: int


class PivotPerfMetrics(TypedDict, total=False):
    """Performance metrics published by the frontend for the current pivot view."""

    parseMs: float
    pivotComputeMs: float
    renderMs: float
    firstMountMs: float
    sourceRows: int
    sourceCols: int
    totalRows: int
    totalCols: int
    totalCells: int
    executionMode: str
    needsVirtualization: bool
    columnsTruncated: bool
    truncatedColumnCount: int
    warnings: list[str]
    lastAction: PerfActionMeasurement


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------


class PivotTableResult(TypedDict, total=False):
    """Value returned by st_pivot_table() to the caller."""

    config: PivotConfig
    perf_metrics: PivotPerfMetrics


# ---------------------------------------------------------------------------
# CCv2 component registration
# ---------------------------------------------------------------------------

# Registration key follows the CCv2 packaged-component convention:
#   "<project.name>.<component.name>"
# where both segments come from the in-package manifest at
# streamlit_pivot/pyproject.toml:
#   [project] name = "streamlit-pivot"                 -> project.name
#   [[tool.streamlit.component.components]] name = ... -> component.name
# See component_manifest_handler.py line 65 for the join logic.
_component = st.components.v2.component(
    "streamlit-pivot.streamlit_pivot",
    js="index-*.js",
    css="index-*.css",
    html='<div class="react-root"></div>',
)


def _noop_callback(*_args: Any, **_kwargs: Any) -> None:
    """No-op callback supplied at mount when user omits on_config_change."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def st_pivot_table(
    data: Any,
    *,
    key: str,
    rows: list[str] | None = None,
    columns: list[str] | None = None,
    values: list[str] | None = None,
    synthetic_measures: list[dict[str, Any]] | None = None,
    aggregation: str | dict[str, str] = "sum",
    show_totals: bool = True,
    show_row_totals: bool | list[str] | None = None,
    show_column_totals: bool | list[str] | None = None,
    empty_cell_value: str = "-",
    interactive: bool = True,
    height: int | None = None,
    max_height: int = 500,
    on_cell_click: Callable[[], None] | None = None,
    on_config_change: Callable[[], None] | None = None,
    # Phase 2 parameters
    null_handling: str | dict[str, str] | None = None,
    hidden_attributes: list[str] | None = None,
    hidden_from_aggregators: list[str] | None = None,
    frozen_columns: list[str] | None = None,
    hidden_from_drag_drop: list[str]
    | None = None,  # deprecated alias for frozen_columns
    sorters: dict[str, list[str]] | None = None,
    locked: bool = False,
    menu_limit: int | None = None,
    row_sort: SortConfig | None = None,
    col_sort: SortConfig | None = None,
    # Phase 3 parameters
    sticky_headers: bool = True,
    show_subtotals: bool | list[str] = False,
    repeat_row_labels: bool = False,
    show_values_as: dict[str, str] | None = None,
    conditional_formatting: list[dict[str, Any]] | None = None,
    number_format: str | dict[str, str] | None = None,
    column_alignment: dict[str, str] | None = None,
    # Phase 4 parameters
    enable_drilldown: bool = True,
    export_filename: str | None = None,
    execution_mode: str = "auto",
    # Report-level filtering
    source_filters: dict[str, dict[str, list[Any]]] | None = None,
) -> PivotTableResult:
    """Create a pivot table component.

    Parameters
    ----------
    data : DataFrame-like
        Source data for the pivot table. Accepts the same data types as
        ``st.dataframe``: Pandas DataFrame/Series, Polars DataFrame/Series,
        NumPy arrays, dicts, lists of dicts, pyarrow Tables, and any object
        supporting the DataFrame Interchange Protocol or ``to_pandas()``.
    key : str
        **Required.** A unique string that identifies this component
        instance.  Used for state persistence: user config changes made
        via the frontend are hydrated from ``st.session_state[key]`` on
        rerun.  Each pivot table on a page must have a distinct key.
    rows : list[str] or None
        Column names from *data* to use as row dimensions.
    columns : list[str] or None
        Column names from *data* to use as column dimensions.
    values : list[str] or None
        Column names from *data* to aggregate as measures.
    aggregation : str or dict[str, str]
        Aggregation setting for raw value fields. A single string applies to all
        measures, while a dict maps each value field to its aggregation.
    show_totals : bool
        Whether to display grand total rows and columns. Acts as default
        for ``show_row_totals`` and ``show_column_totals`` when unset.
    show_row_totals : bool, list[str], or None
        Show row totals column. ``True`` = all measures, ``False`` = none,
        ``["Revenue"]`` = only listed measures (others show ``–``).
        Defaults to ``show_totals`` when None.
    show_column_totals : bool, list[str], or None
        Show column totals row. Same semantics as ``show_row_totals``.
        Defaults to ``show_totals`` when None.
    empty_cell_value : str
        Display string for cells with no data.
    interactive : bool
        If True, the user can reconfigure the pivot via toolbar controls and
        header-menu actions. If False, the toolbar is hidden and header-menu
        sort/filter/show-values-as actions are disabled.
    height : int or None
        .. deprecated:: Use ``max_height`` instead.
        If provided, treated as ``max_height``.  Kept for backwards
        compatibility.
    max_height : int
        Maximum height in pixels.  The table auto-sizes up to this limit
        and becomes scrollable with sticky headers once content exceeds it.
        Default 500.
    on_cell_click : callable or None
        Called (with no arguments) when a user clicks a data cell. Read the
        payload from ``st.session_state[key]`` after the callback fires.
        Mapped internally to ``on_cell_click_change`` at mount time.
    on_config_change : callable or None
        Called (with no arguments) when the user changes the pivot config
        via the toolbar. Read the updated config from
        ``st.session_state[key]`` after the callback fires.
        If None, a no-op is supplied at mount to satisfy the CCv2 contract
        (every ``default={}`` key needs a matching ``on_<key>_change``).
    source_filters : dict[str, dict[str, list[Any]]] or None
        Server-only report-level filters applied to the source DataFrame
        before any pivot processing. Unlike interactive ``config.filters``,
        these filters are not sent to the frontend and are not tied to the
        current row/column layout. ``include`` takes precedence over
        ``exclude``. ``None`` matches null-like values, while ``""`` matches
        only literal empty strings. No type coercion is performed.
    null_handling : str or dict[str, str] or None
        How to treat null/NaN values. Global mode ("exclude", "zero",
        "separate") or per-field dict mapping column names to modes.
        Defaults to None ("exclude").
    hidden_attributes : list[str] or None
        Column names to hide entirely from the UI.
    hidden_from_aggregators : list[str] or None
        Column names hidden from the values/aggregators dropdown only.
    frozen_columns : list[str] or None
        Column names that cannot be removed from their toolbar zone and
        cannot be reordered or moved between zones via drag-and-drop.
        Frozen chips render without a drag handle.
    hidden_from_drag_drop : list[str] or None
        Deprecated alias for ``frozen_columns``. Use ``frozen_columns``
        instead.
    sorters : dict[str, list[str]] or None
        Custom sort orderings per dimension. Maps column name to a list
        of values in the desired order.
    locked : bool
        If True, toolbar config controls are disabled. The settings gear stays
        visible so users can inspect current view status and expand/collapse
        groups. Data export plus header-menu sorting, filtering, and show-values-as
        remain available. Defaults to False.
    menu_limit : int or None
        Max items to show in the header-menu filter checklist. Defaults
        to 50 when None.
    row_sort : dict or None
        Initial sort configuration for rows. Dict with keys ``by``
        ("key" or "value"), ``direction`` ("asc" or "desc"), and
        optionally ``value_field`` (str), ``col_key`` (list[str]), and
        ``dimension`` (str).  When ``dimension`` is set and subtotals
        are enabled, only the targeted level and below sort — parent
        groups keep their existing order (scoped sorting).
    col_sort : dict or None
        Initial sort configuration for columns. Same shape as row_sort
        (without ``col_key``).
    sticky_headers : bool
        If True (default), column headers stick to the top of the
        scrolling container so they remain visible when scrolling
        down through large tables.
    show_subtotals : bool or list[str]
        ``True`` = subtotal rows at every parent level, ``False`` = none,
        ``["Region"]`` = only Region-level subtotals. Requires 2+ row
        dimensions. Defaults to False.
    repeat_row_labels : bool
        If True, row dimension labels are repeated on every row
        instead of being merged/spanned. Defaults to False.
    show_values_as : dict[str, str] or None
        Per-field display mode. Maps value field names to one of
        ``"raw"``, ``"pct_of_total"``, ``"pct_of_row"``, or
        ``"pct_of_col"``. Omitted fields default to ``"raw"``.
    conditional_formatting : list[dict] or None
        List of conditional formatting rules applied to value cells.
        Each rule is a dict with ``"type"`` (``"color_scale"``,
        ``"data_bars"``, or ``"threshold"``), ``"apply_to"`` (list of
        field names, empty = all), and type-specific keys.
    number_format : str or dict[str, str] or None
        Number format pattern(s). A single string applies to all
        value fields; a dict maps field names to patterns. Use
        ``"__all__"`` as a key for a default pattern. Patterns
        follow a lightweight d3-style syntax, e.g. ``"$,.0f"``
        for currency integers, ``",.2f"`` for grouped 2-decimal.
    column_alignment : dict[str, str] or None
        Per-field text alignment override. Maps value field names
        to ``"left"``, ``"center"``, or ``"right"``.
    enable_drilldown : bool
        If True (default), clicking a data cell opens an inline
        drill-down panel below the pivot table showing the
        contributing source records. Set to False to disable.
    export_filename : str or None
        Base filename (without extension) used when exporting data
        (.xlsx, .csv, .tsv). The date and file extension are appended
        automatically. Defaults to ``"pivot-table"`` when not set.
    execution_mode : str
        Performance execution mode. ``"auto"`` (default) keeps the client-side
        path unless the dataset is large enough to trigger the threshold_hybrid
        pre-aggregation path. ``"client_only"`` always ships raw rows to the
        frontend. ``"threshold_hybrid"`` forces server-side pre-aggregation when
        the current config is compatible with the prototype.

    Returns
    -------
    PivotTableResult
        A dict containing the current ``config`` state.
    """
    if not isinstance(key, str) or not key:
        raise TypeError(
            "key is required: pass a unique string that identifies this "
            "pivot table instance (e.g. key='my_pivot')"
        )

    # --- Convert data to pandas DataFrame ---
    try:
        data = convert_anything_to_pandas_df(data)
    except (ValueError, TypeError) as exc:
        raise TypeError(
            f"data must be a DataFrame-like object (Pandas/Polars DataFrame, "
            f"dict, list, NumPy array, etc.), got {type(data).__name__}: {exc}"
        ) from exc
    if not isinstance(show_totals, bool):
        raise TypeError(f"show_totals must be a bool, got {type(show_totals).__name__}")
    if execution_mode not in {"auto", "client_only", "threshold_hybrid"}:
        raise ValueError(
            "execution_mode must be one of: 'auto', 'client_only', 'threshold_hybrid'"
        )
    if show_row_totals is not None and not isinstance(show_row_totals, (bool, list)):
        raise TypeError(
            f"show_row_totals must be bool, list[str], or None, got {type(show_row_totals).__name__}"
        )
    if isinstance(show_row_totals, list) and not all(
        isinstance(s, str) for s in show_row_totals
    ):
        raise TypeError("show_row_totals list items must be strings")
    if show_column_totals is not None and not isinstance(
        show_column_totals, (bool, list)
    ):
        raise TypeError(
            f"show_column_totals must be bool, list[str], or None, got {type(show_column_totals).__name__}"
        )
    if isinstance(show_column_totals, list) and not all(
        isinstance(s, str) for s in show_column_totals
    ):
        raise TypeError("show_column_totals list items must be strings")
    if not isinstance(empty_cell_value, str):
        raise TypeError(
            f"empty_cell_value must be a str, got {type(empty_cell_value).__name__}"
        )
    if not isinstance(interactive, bool):
        raise TypeError(f"interactive must be a bool, got {type(interactive).__name__}")

    # --- Phase 2 type validation ---
    if null_handling is not None:
        if isinstance(null_handling, str):
            if null_handling not in VALID_NULL_MODES:
                raise ValueError(
                    f"null_handling must be one of {sorted(VALID_NULL_MODES)}, got {null_handling!r}"
                )
        elif isinstance(null_handling, dict):
            for k, v in null_handling.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise TypeError(
                        "null_handling dict keys and values must be strings"
                    )
                if v not in VALID_NULL_MODES:
                    raise ValueError(
                        f"null_handling[{k!r}] must be one of {sorted(VALID_NULL_MODES)}, got {v!r}"
                    )
        else:
            raise TypeError(
                f"null_handling must be str, dict, or None, got {type(null_handling).__name__}"
            )

    for param_name, hidden_list in [
        ("hidden_attributes", hidden_attributes),
        ("hidden_from_aggregators", hidden_from_aggregators),
        ("frozen_columns", frozen_columns or hidden_from_drag_drop),
    ]:
        if hidden_list is not None:
            if not isinstance(hidden_list, list) or not all(
                isinstance(c, str) for c in hidden_list
            ):
                raise TypeError(f"{param_name} must be a list of strings")

    if sorters is not None:
        if not isinstance(sorters, dict):
            raise TypeError(f"sorters must be a dict, got {type(sorters).__name__}")
        for sorter_key, sorter_values in sorters.items():
            if not isinstance(sorter_key, str):
                raise TypeError(
                    f"sorters keys must be strings, got {type(sorter_key).__name__}"
                )
            if not isinstance(sorter_values, list) or not all(
                isinstance(s, str) for s in sorter_values
            ):
                raise TypeError(f"sorters[{sorter_key!r}] must be a list of strings")

    if not isinstance(locked, bool):
        raise TypeError(f"locked must be a bool, got {type(locked).__name__}")

    # --- Sort config validation ---
    _VALID_SORT_BY = frozenset(("key", "value"))
    _VALID_SORT_DIR = frozenset(("asc", "desc"))
    for param_name, sort_cfg in [("row_sort", row_sort), ("col_sort", col_sort)]:
        if sort_cfg is not None:
            if not isinstance(sort_cfg, dict):
                raise TypeError(
                    f"{param_name} must be a dict or None, got {type(sort_cfg).__name__}"
                )
            if sort_cfg.get("by") not in _VALID_SORT_BY:
                raise ValueError(f"{param_name}['by'] must be 'key' or 'value'")
            if sort_cfg.get("direction") not in _VALID_SORT_DIR:
                raise ValueError(f"{param_name}['direction'] must be 'asc' or 'desc'")
            if sort_cfg.get("by") == "value":
                vf = sort_cfg.get("value_field")
                if vf is not None and not isinstance(vf, str):
                    raise TypeError(f"{param_name}['value_field'] must be a string")
            ck = sort_cfg.get("col_key")
            if ck is not None:
                if not isinstance(ck, list) or not all(isinstance(s, str) for s in ck):
                    raise TypeError(
                        f"{param_name}['col_key'] must be a list of strings"
                    )

    # --- Column list type + membership validation ---
    df_cols = set(data.columns)

    if source_filters is not None:
        if not isinstance(source_filters, dict):
            raise TypeError(
                f"source_filters must be a dict or None, got {type(source_filters).__name__}"
            )
        if not source_filters:
            source_filters = None
        else:
            for field, filt in source_filters.items():
                if not isinstance(field, str):
                    raise TypeError("source_filters keys must be strings")
                if field not in df_cols:
                    raise ValueError(
                        f"source_filters contains column not in DataFrame: {field!r}. "
                        f"Available columns: {sorted(df_cols)}"
                    )
                if not isinstance(filt, dict):
                    raise TypeError(f"source_filters[{field!r}] must be a dict")
                extra_keys = [k for k in filt if k not in {"include", "exclude"}]
                if extra_keys:
                    raise ValueError(
                        f"source_filters[{field!r}] contains unsupported keys: {extra_keys}. "
                        "Only 'include' and 'exclude' are allowed."
                    )
                for op_name in ("include", "exclude"):
                    vals = filt.get(op_name)
                    if vals is None:
                        continue
                    if not isinstance(vals, list):
                        raise TypeError(
                            f"source_filters[{field!r}]['{op_name}'] must be a list"
                        )
                    for idx, value in enumerate(vals):
                        if not pd.api.types.is_scalar(value):
                            raise TypeError(
                                f"source_filters[{field!r}]['{op_name}'][{idx}] must be a scalar value"
                            )

    for param_name, col_list in [
        ("rows", rows),
        ("columns", columns),
        ("values", values),
    ]:
        if col_list is not None:
            if not isinstance(col_list, list) or not all(
                isinstance(c, str) for c in col_list
            ):
                raise TypeError(
                    f"{param_name} must be a list of strings, got {type(col_list).__name__}"
                )
            missing = [c for c in col_list if c not in df_cols]
            if missing:
                raise ValueError(
                    f"{param_name} contains columns not in DataFrame: {missing}. "
                    f"Available columns: {sorted(df_cols)}"
                )

    filtered_data = _apply_source_filters(data, source_filters)

    # --- Auto-detect dimensions/measures when not specified ---
    resolved_rows = rows
    resolved_columns = columns
    resolved_values = values

    if resolved_rows is None and resolved_columns is None and resolved_values is None:
        numeric_cols = filtered_data.select_dtypes(include="number").columns.tolist()
        categorical_cols = [c for c in filtered_data.columns if c not in numeric_cols]
        # Heuristic: numeric columns with few unique values (<=20) likely
        # represent dimensions (e.g. Year) rather than measures.
        likely_measures = [c for c in numeric_cols if filtered_data[c].nunique() > 20]
        likely_numeric_dims = [
            c for c in numeric_cols if filtered_data[c].nunique() <= 20
        ]
        # Treat low-cardinality numerics as dimensions alongside categoricals
        all_dims = categorical_cols + likely_numeric_dims
        resolved_rows = all_dims[:1] if all_dims else []
        resolved_columns = all_dims[1:2] if len(all_dims) > 1 else []
        resolved_values = likely_measures[:2] if likely_measures else numeric_cols[:1]

    normalized_synthetic_measures: list[dict[str, Any]] = []
    if synthetic_measures is not None:
        if not isinstance(synthetic_measures, list):
            raise TypeError("synthetic_measures must be a list of dicts")
        seen_ids: set[str] = set()
        seen_labels: set[str] = set()
        valid_ops = {"sum_over_sum", "difference"}
        for i, item in enumerate(synthetic_measures):
            if not isinstance(item, dict):
                raise TypeError(f"synthetic_measures[{i}] must be a dict")
            sid = item.get("id")
            label = item.get("label")
            op = item.get("operation")
            numerator = item.get("numerator")
            denominator = item.get("denominator")
            if not isinstance(sid, str) or sid == "":
                raise ValueError(
                    f"synthetic_measures[{i}]['id'] must be a non-empty string"
                )
            if not isinstance(label, str) or label == "":
                raise ValueError(
                    f"synthetic_measures[{i}]['label'] must be a non-empty string"
                )
            if sid in seen_ids:
                raise ValueError(f"duplicate synthetic_measures id: {sid!r}")
            if label in seen_labels:
                raise ValueError(f"duplicate synthetic_measures label: {label!r}")
            seen_ids.add(sid)
            seen_labels.add(label)
            if op not in valid_ops:
                raise ValueError(
                    f"synthetic_measures[{i}]['operation'] must be one of {sorted(valid_ops)}"
                )
            if not isinstance(numerator, str) or numerator not in df_cols:
                raise ValueError(
                    f"synthetic_measures[{i}]['numerator'] must be a DataFrame column name"
                )
            if not isinstance(denominator, str) or denominator not in df_cols:
                raise ValueError(
                    f"synthetic_measures[{i}]['denominator'] must be a DataFrame column name"
                )
            normalized_synthetic_measures.append(
                {
                    "id": sid,
                    "label": label,
                    "operation": op,
                    "numerator": numerator,
                    "denominator": denominator,
                    "format": item.get("format"),
                }
            )

    normalized_aggregation = _normalize_aggregation_config(
        aggregation, resolved_values or []
    )

    # --- Phase 3 validation ---
    if not isinstance(show_subtotals, (bool, list)):
        raise TypeError(
            f"show_subtotals must be bool or list[str], got {type(show_subtotals).__name__}"
        )
    if isinstance(show_subtotals, list) and not all(
        isinstance(s, str) for s in show_subtotals
    ):
        raise TypeError("show_subtotals list items must be strings")
    if not isinstance(repeat_row_labels, bool):
        raise TypeError(
            f"repeat_row_labels must be a bool, got {type(repeat_row_labels).__name__}"
        )

    if show_values_as is not None:
        if not isinstance(show_values_as, dict):
            raise TypeError(
                f"show_values_as must be a dict or None, got {type(show_values_as).__name__}"
            )
        for k, v in show_values_as.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise TypeError("show_values_as keys and values must be strings")
            if v not in VALID_SHOW_VALUES_AS:
                raise ValueError(
                    f"show_values_as[{k!r}] must be one of {sorted(VALID_SHOW_VALUES_AS)}, got {v!r}"
                )

    if conditional_formatting is not None:
        if not isinstance(conditional_formatting, list):
            raise TypeError(
                f"conditional_formatting must be a list or None, got {type(conditional_formatting).__name__}"
            )
        for i, rule in enumerate(conditional_formatting):
            if not isinstance(rule, dict):
                raise TypeError(f"conditional_formatting[{i}] must be a dict")
            rtype = rule.get("type")
            if rtype not in VALID_COND_FMT_TYPES:
                raise ValueError(
                    f"conditional_formatting[{i}]['type'] must be one of "
                    f"{sorted(VALID_COND_FMT_TYPES)}, got {rtype!r}"
                )
            apply_to = rule.get("apply_to", [])
            if not isinstance(apply_to, list) or not all(
                isinstance(a, str) for a in apply_to
            ):
                raise TypeError(
                    f"conditional_formatting[{i}]['apply_to'] must be a list of strings"
                )
            if rtype == "color_scale":
                for color_key in ("min_color", "max_color"):
                    if not isinstance(rule.get(color_key, ""), str):
                        raise TypeError(
                            f"conditional_formatting[{i}][{color_key!r}] must be a string"
                        )
                if not rule.get("min_color") or not rule.get("max_color"):
                    raise ValueError(
                        f"conditional_formatting[{i}]: color_scale requires 'min_color' and 'max_color'"
                    )
            elif rtype == "threshold":
                conditions = rule.get("conditions")
                if not isinstance(conditions, list) or len(conditions) == 0:
                    raise ValueError(
                        f"conditional_formatting[{i}]: threshold requires non-empty 'conditions' list"
                    )
                valid_ops = {"gt", "gte", "lt", "lte", "eq", "between"}
                for j, cond in enumerate(conditions):
                    if not isinstance(cond, dict):
                        raise TypeError(
                            f"conditional_formatting[{i}]['conditions'][{j}] must be a dict"
                        )
                    op = cond.get("operator")
                    if op not in valid_ops:
                        raise ValueError(
                            f"conditional_formatting[{i}]['conditions'][{j}]['operator'] "
                            f"must be one of {sorted(valid_ops)}, got {op!r}"
                        )
                    if "value" not in cond:
                        raise ValueError(
                            f"conditional_formatting[{i}]['conditions'][{j}] requires 'value'"
                        )

    if number_format is not None:
        if isinstance(number_format, str):
            pass  # global format string
        elif isinstance(number_format, dict):
            for k, v in number_format.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise TypeError("number_format keys and values must be strings")
        else:
            raise TypeError(
                f"number_format must be str, dict, or None, got {type(number_format).__name__}"
            )

    if column_alignment is not None:
        if not isinstance(column_alignment, dict):
            raise TypeError(
                f"column_alignment must be a dict or None, got {type(column_alignment).__name__}"
            )
        for k, v in column_alignment.items():
            if not isinstance(k, str) or v not in VALID_ALIGNMENTS:
                raise ValueError(
                    f"column_alignment[{k!r}] must be one of {sorted(VALID_ALIGNMENTS)}, got {v!r}"
                )

    initial_config = _default_config(
        rows=resolved_rows,
        columns=resolved_columns,
        values=resolved_values,
        synthetic_measures=normalized_synthetic_measures,
        aggregation=normalized_aggregation,
        show_totals=show_totals,
        show_row_totals=show_row_totals,
        show_column_totals=show_column_totals,
        empty_cell_value=empty_cell_value,
        interactive=interactive,
        row_sort=row_sort,
        col_sort=col_sort,
        sticky_headers=sticky_headers,
        show_subtotals=show_subtotals,
        repeat_row_labels=repeat_row_labels,
        show_values_as=show_values_as,
        conditional_formatting=conditional_formatting,
        number_format=number_format,
        column_alignment=column_alignment,
    )

    # Controlled-state hydration: preserve persisted user config across normal
    # reruns, but let explicit Python config changes take precedence.
    config_to_send = _resolve_config_to_send(st.session_state, key, initial_config)
    use_threshold_hybrid, threshold_reason = _should_use_threshold_hybrid(
        filtered_data, config_to_send, execution_mode
    )
    if use_threshold_hybrid:
        drill_note = (
            " Drill-down uses a server round-trip to fetch matching rows "
            "from the original dataset."
        )
        if drill_note not in threshold_reason:
            threshold_reason = f"{threshold_reason}{drill_note}"
    materialized_data = (
        _prepare_threshold_hybrid_frame(filtered_data, config_to_send, null_handling)
        if use_threshold_hybrid
        else filtered_data
    )
    effective_execution_mode = (
        "threshold_hybrid" if use_threshold_hybrid else "client_only"
    )

    effective_max_height = height if height is not None else max_height
    data_payload: dict[str, Any] = {
        "dataframe": materialized_data,
        "height": None,
        "max_height": effective_max_height,
        "config": config_to_send,
        "execution_mode": effective_execution_mode,
        "server_mode_reason": threshold_reason,
    }

    if use_threshold_hybrid:
        data_payload["source_row_count"] = len(filtered_data)
        agg_dict = config_to_send.get("aggregation", {})
        agg_remap = _build_hybrid_agg_remap(agg_dict)
        if agg_remap:
            data_payload["hybrid_agg_remap"] = agg_remap
        totals_sidecar = _compute_hybrid_totals(
            filtered_data, config_to_send, null_handling
        )
        if totals_sidecar:
            data_payload["hybrid_totals"] = totals_sidecar

    if null_handling is not None:
        data_payload["null_handling"] = null_handling
    if hidden_attributes is not None:
        data_payload["hidden_attributes"] = hidden_attributes
    if hidden_from_aggregators is not None:
        data_payload["hidden_from_aggregators"] = hidden_from_aggregators
    _frozen = frozen_columns or hidden_from_drag_drop
    if _frozen is not None:
        data_payload["hidden_from_drag_drop"] = _frozen
    if sorters is not None:
        data_payload["sorters"] = sorters
    if locked:
        data_payload["locked"] = True
    if menu_limit is not None:
        if (
            isinstance(menu_limit, bool)
            or not isinstance(menu_limit, int)
            or menu_limit < 1
        ):
            raise ValueError(
                f"menu_limit must be a positive integer, got {menu_limit!r}"
            )
        data_payload["menu_limit"] = menu_limit
    if not enable_drilldown:
        data_payload["enable_drilldown"] = False
    if export_filename is not None:
        data_payload["export_filename"] = export_filename

    # Server-side drill-down for hybrid mode: read the pending request from
    # session state, filter the *original* (un-aggregated) DataFrame, and
    # ship the matching rows back as JSON records.
    if use_threshold_hybrid and enable_drilldown:
        drilldown_request: dict[str, Any] | None = None
        try:
            state = st.session_state.get(key, {})
            drilldown_request = (
                state.get("drilldown_request") if isinstance(state, dict) else None
            )
        except (AttributeError, TypeError):
            drilldown_request = None

        if isinstance(drilldown_request, dict) and "filters" in drilldown_request:
            all_dims = list(config_to_send.get("rows", [])) + list(
                config_to_send.get("columns", [])
            )
            records, columns, total, page = _compute_hybrid_drilldown(
                filtered_data,
                drilldown_request,
                null_handling=null_handling,
                dims=all_dims,
                config_filters=config_to_send.get("filters"),
            )
            data_payload["drilldown_records"] = records
            data_payload["drilldown_columns"] = columns
            data_payload["drilldown_total_count"] = total
            data_payload["drilldown_page"] = page
            data_payload["drilldown_page_size"] = _DRILLDOWN_PAGE_SIZE

    mount_kwargs: dict[str, Any] = {
        "key": key,
        "default": {
            "config": config_to_send,
            "perf_metrics": None,
            "drilldown_request": None,
        },
        "data": data_payload,
        "on_config_change": on_config_change or _noop_callback,
        "on_perf_metrics_change": _noop_callback,
        "on_drilldown_request_change": _noop_callback,
    }

    if on_cell_click is not None:
        mount_kwargs["on_cell_click_change"] = on_cell_click

    return cast(PivotTableResult, _component(**mount_kwargs))
