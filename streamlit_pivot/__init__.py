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
import re
import warnings
from datetime import date, datetime
from math import prod
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast

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
    auto_date_hierarchy: bool
    date_grains: dict[str, str | None]
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
    collapsed_temporal_groups: dict[str, list[str]]
    collapsed_temporal_row_groups: dict[str, list[str]]
    sticky_headers: bool
    show_values_as: dict[str, str]
    conditional_formatting: list[dict[str, Any]]
    number_format: dict[str, str]
    dimension_format: dict[str, str]
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
VALID_SHOW_VALUES_AS = frozenset(
    (
        "raw",
        "pct_of_total",
        "pct_of_row",
        "pct_of_col",
        "diff_from_prev",
        "pct_diff_from_prev",
        "diff_from_prev_year",
        "pct_diff_from_prev_year",
    )
)
PERIOD_COMPARISON_SHOW_VALUES_AS = frozenset(
    (
        "diff_from_prev",
        "pct_diff_from_prev",
        "diff_from_prev_year",
        "pct_diff_from_prev_year",
    )
)
VALID_DATE_GRAINS = frozenset(("year", "quarter", "month", "week", "day"))
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
_DEFAULT_AUTO_DATE_GRAIN = "month"


def _get_temporal_hierarchy_levels(grain: str) -> list[str]:
    """Return the hierarchy levels (outermost to leaf) for a given grain."""
    if grain == "year":
        return ["year"]
    if grain == "quarter":
        return ["year", "quarter"]
    if grain == "month":
        return ["year", "quarter", "month"]
    if grain == "day":
        return ["year", "month", "day"]
    if grain == "week":
        return ["year", "week"]
    return [grain]


def _rebucket_temporal_string(leaf_bucket: str, parent_grain: str) -> str:
    """Derive a parent-grain bucket from an already-bucketed leaf string.

    Works on the canonical bucket strings produced by ``_bucket_temporal_key``
    (e.g. "2024-03", "2024-Q2", "2024-03-15", "2024-W12") rather than raw
    datetime values, so it is safe to call after dimension stringification.
    Returns "" for null/empty sentinels.
    """
    if not leaf_bucket or leaf_bucket in ("(null)", ""):
        return ""
    if parent_grain == "year":
        return leaf_bucket[:4]
    if parent_grain == "quarter":
        # Leaf could be month ("2024-03") or day ("2024-03-15")
        month_str = leaf_bucket[5:7]
        try:
            month = int(month_str)
        except (ValueError, IndexError):
            return leaf_bucket[:4]
        q = (month - 1) // 3 + 1
        return f"{leaf_bucket[:4]}-Q{q}"
    if parent_grain == "month":
        # Leaf is day ("2024-03-15") → "2024-03"
        return leaf_bucket[:7]
    return leaf_bucket


def _compute_adaptive_date_grain(series: "pd.Series[Any]") -> str:
    """Pick the best default grain for a temporal series based on its date span.

    Only returns grains on the standard drill path (year/quarter/month/day).
    Week is available via the header menu but is never auto-selected, consistent
    with Excel, Power BI, Tableau, and Sigma.
    """
    clean = series.dropna()
    if clean.empty:
        return _DEFAULT_AUTO_DATE_GRAIN
    span = clean.max() - clean.min()
    if span > pd.Timedelta(days=730):
        return "year"
    if span > pd.Timedelta(days=365):
        return "quarter"
    if span > pd.Timedelta(days=60):
        return "month"
    return "day"


def _compute_adaptive_date_grains(
    df: Any,
    column_types: dict[str, str],
) -> dict[str, str]:
    """Build a map of field -> adaptive grain for all temporal columns."""
    result: dict[str, str] = {}
    for field, ctype in column_types.items():
        if ctype not in {"date", "datetime"} or field not in df.columns:
            continue
        series = pd.to_datetime(df[field], errors="coerce")
        result[field] = _compute_adaptive_date_grain(series)
    return result


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
    df: Any,
    config: PivotConfig,
    null_handling: Any = None,
    column_types: dict[str, str] | None = None,
    adaptive_grains: dict[str, str] | None = None,
) -> Any:
    group_fields = [*config.get("rows", []), *config.get("columns", [])]
    date_grains = config.get("date_grains", {})
    auto_date_hierarchy = config.get("auto_date_hierarchy", True)
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

    filtered_df = _resolve_and_filter(
        df,
        config.get("filters", {}),
        null_handling,
        column_types,
        rows=config.get("rows", []),
        columns=config.get("columns", []),
        auto_date_hierarchy=auto_date_hierarchy,
        date_grains=date_grains,
        adaptive_grains=adaptive_grains,
    )

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
    for dim in group_fields:
        grain = _get_effective_date_grain(
            dim,
            config.get("rows", []),
            config.get("columns", []),
            date_grains,
            auto_date_hierarchy,
            column_types,
            adaptive_grains=adaptive_grains,
        )
        if not grain or dim not in working.columns:
            continue
        mode = _get_null_mode(dim, null_handling)
        col_type = column_types.get(dim) if column_types else None
        working[dim] = _resolve_dim_value_series(working[dim], col_type, mode, grain)
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


# ---------------------------------------------------------------------------
# Temporal key canonicalization + column type detection
# ---------------------------------------------------------------------------

_ISO_DATE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?)?$"
)
_ISO_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _classify_temporal_value(v: Any) -> str | None:
    """Classify a single value as 'date', 'datetime', or None (not temporal)."""
    if isinstance(v, date) and not isinstance(v, datetime):
        return "date"
    if isinstance(v, (pd.Timestamp, datetime)):
        return "datetime"
    if isinstance(v, str):
        if _ISO_DATE_ONLY_RE.match(v):
            return "date"
        if _ISO_DATE_RE.match(v):
            return "datetime"
    return None


def _build_original_column_types(df: Any) -> dict[str, str]:
    """Build a column name -> semantic type map from the DataFrame.

    Uses pandas.api.types checks plus sample-based detection for object
    columns. Distinguishes 'date' from 'datetime' for both Python temporal
    instances and ISO-format date strings.
    """
    result: dict[str, str] = {}
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_datetime64_any_dtype(dtype):
            result[col] = "datetime"
        elif pd.api.types.is_integer_dtype(dtype):
            result[col] = "integer"
        elif pd.api.types.is_float_dtype(dtype):
            result[col] = "float"
        elif pd.api.types.is_bool_dtype(dtype):
            result[col] = "boolean"
        elif pd.api.types.is_object_dtype(dtype) or pd.api.types.is_string_dtype(dtype):
            sample = df[col].dropna().head(20)
            if len(sample) > 0:
                classifications = [_classify_temporal_value(v) for v in sample]
                if all(c is not None for c in classifications):
                    if "datetime" in classifications:
                        result[col] = "datetime"
                    else:
                        result[col] = "date"
                else:
                    result[col] = "string"
            else:
                result[col] = "string"
        else:
            result[col] = "string"
    return result


def _validate_period_comparison_config(
    show_values_as: dict[str, str] | None,
    rows: list[str] | None,
    columns: list[str] | None,
    date_grains: dict[str, str | None] | None,
    auto_date_hierarchy: bool,
    column_types: dict[str, str],
    adaptive_grains: dict[str, str] | None = None,
) -> None:
    """Require a grain-enabled temporal axis before allowing period comparisons."""
    if not show_values_as:
        return
    has_period_mode = any(
        mode in PERIOD_COMPARISON_SHOW_VALUES_AS for mode in show_values_as.values()
    )
    if not has_period_mode:
        return
    grouped_dims = set(rows or []) | set(columns or [])
    for field in grouped_dims:
        grain = _get_effective_date_grain(
            field,
            rows or [],
            columns or [],
            date_grains,
            auto_date_hierarchy,
            column_types,
            adaptive_grains=adaptive_grains,
        )
        if grain and column_types.get(field) in {"date", "datetime"}:
            return
    raise ValueError(
        "period comparison show_values_as modes require either auto_date_hierarchy=True "
        "or an explicit date_grains override on a date/datetime field used in rows or columns"
    )


def _get_effective_date_grain(
    field: str,
    rows: list[str] | None,
    columns: list[str] | None,
    date_grains: dict[str, str | None] | None,
    auto_date_hierarchy: bool,
    column_types: dict[str, str] | None,
    adaptive_grains: dict[str, str] | None = None,
) -> str | None:
    if date_grains and field in date_grains:
        grain = date_grains[field]
        return grain if grain in VALID_DATE_GRAINS else None
    if not auto_date_hierarchy:
        return None
    if field not in set(rows or []) | set(columns or []):
        return None
    if (column_types or {}).get(field) not in {"date", "datetime"}:
        return None
    if adaptive_grains:
        return adaptive_grains.get(field, _DEFAULT_AUTO_DATE_GRAIN)
    return _DEFAULT_AUTO_DATE_GRAIN


def _canonical_temporal_key(value: Any, col_type: str) -> str:
    """Convert a temporal value to the canonical key matching the frontend."""
    if pd.isna(value):
        return ""
    ts = pd.Timestamp(value)
    if col_type == "datetime":
        if ts.tzinfo is not None:
            ts = ts.tz_convert("UTC")
        else:
            ts = ts.tz_localize("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"
    if col_type == "date":
        return ts.strftime("%Y-%m-%d")
    return str(value)


def _bucket_temporal_key(value: Any, col_type: str, grain: str | None) -> str:
    """Convert a temporal value to a grouped canonical key matching the frontend."""
    if not grain:
        return _canonical_temporal_key(value, col_type)
    if pd.isna(value):
        return ""
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC")
    else:
        ts = ts.tz_localize("UTC")
    if grain == "year":
        return f"{ts.year}"
    if grain == "quarter":
        return f"{ts.year}-Q{((ts.month - 1) // 3) + 1}"
    if grain == "month":
        return f"{ts.year}-{ts.month:02d}"
    if grain == "week":
        iso = ts.isocalendar()
        return f"{int(iso.year)}-W{int(iso.week):02d}"
    return ts.strftime("%Y-%m-%d")


def _resolve_dim_value_series(
    series: Any,
    col_type: str | None,
    null_handling_mode: str,
    date_grain: str | None = None,
) -> Any:
    """Resolve a dimension column to canonical string keys matching the frontend."""
    if col_type in ("datetime", "date"):
        return series.apply(
            lambda v: _bucket_temporal_key(v, col_type, date_grain)
            if pd.notna(v)
            else ("(null)" if null_handling_mode == "separate" else "")
        )
    if null_handling_mode == "separate":
        return series.fillna("(null)").replace("", "(null)").astype(str)
    return series.fillna("").astype(str)


def _extract_styler_formats(
    styler: Any,
) -> tuple[dict[str, str], dict[str, str]]:
    """Extract format patterns from a pandas Styler's _display_funcs.

    Probes each column's formatter with representative values to reverse-
    engineer the format pattern. Returns (number_format, dimension_format)
    dicts. Unrecognizable formatters are silently skipped.
    """
    number_fmts: dict[str, str] = {}
    dimension_fmts: dict[str, str] = {}

    display_funcs = getattr(styler, "_display_funcs", {})
    if not display_funcs:
        return number_fmts, dimension_fmts

    probe_value = 1234.5678

    for (row_idx, col_idx), func in display_funcs.items():
        if row_idx != 0:
            continue
        try:
            col_name = styler.data.columns[col_idx]
        except (IndexError, AttributeError):
            continue
        try:
            result = func(probe_value)
        except Exception:
            continue
        if not isinstance(result, str):
            continue

        # Try to reverse-engineer the format pattern
        pattern = _guess_format_pattern(result, probe_value)
        if pattern:
            number_fmts[col_name] = pattern

    return number_fmts, dimension_fmts


def _guess_format_pattern(formatted: str, probe: float) -> str | None:
    """Reverse-engineer a d3-style format pattern from a formatted probe value."""
    stripped = formatted.strip()
    if not stripped:
        return None

    # Detect currency prefix
    prefix = ""
    for symbol in ("$", "€", "£", "¥"):
        if stripped.startswith(symbol):
            prefix = symbol
            stripped = stripped[len(symbol) :].strip()
            break

    # Detect percent suffix
    if stripped.endswith("%"):
        # The probe was already a fraction; detect decimal places
        stripped = stripped[:-1].strip()
        try:
            float(stripped.replace(",", ""))
            # Determine decimal places from the formatted string
            if "." in stripped:
                decimals = len(stripped.split(".")[-1])
            else:
                decimals = 0
            return f".{decimals}%"
        except ValueError:
            return None

    # Detect grouping (commas)
    has_grouping = (
        "," in stripped
        and stripped.replace(",", "").replace(".", "").replace("-", "").isdigit()
    )

    # Detect decimal places
    if "." in stripped:
        decimal_part = stripped.split(".")[-1]
        decimals = len(decimal_part)
    else:
        decimals = 0

    grouping = "," if has_grouping else ""
    result = f"{prefix}{grouping}.{decimals}f"
    return result


def _translate_column_config(
    column_config: dict[str, Any],
    df: Any,
) -> tuple[dict[str, str], dict[str, str]]:
    """Translate Streamlit column_config into number_format and dimension_format dicts.

    Returns (number_format_additions, dimension_format_additions).
    """
    number_additions: dict[str, str] = {}
    dimension_additions: dict[str, str] = {}

    for col_name, col_spec in column_config.items():
        if not isinstance(col_spec, dict):
            # Could be a column type string like "number" — try to extract
            # the format from the object's attributes if it has them
            if hasattr(col_spec, "to_dict"):
                col_spec = col_spec.to_dict()
            elif hasattr(col_spec, "__dict__"):
                col_spec = col_spec.__dict__
            else:
                continue

        fmt = col_spec.get("format")
        if not isinstance(fmt, str) or not fmt:
            continue

        type_name = (
            col_spec.get("type_config", {}).get("type", "")
            if isinstance(col_spec.get("type_config"), dict)
            else ""
        )
        col_type = col_spec.get("type", type_name)

        if col_type in ("date", "datetime", "time"):
            dimension_additions[col_name] = fmt
        else:
            # Translate Streamlit printf-style "%,.2f" to d3-style ",.2f"
            translated = fmt
            if translated.startswith("%"):
                translated = translated[1:]
            number_additions[col_name] = translated

    return number_additions, dimension_additions


def _get_null_mode(field: str, null_handling: Any) -> str:
    """Resolve per-field null handling mode (mirrors frontend getNullMode)."""
    if null_handling is None:
        return "exclude"
    if isinstance(null_handling, str):
        return null_handling
    if isinstance(null_handling, dict):
        return null_handling.get(field, "exclude")
    return "exclude"


def _normalize_dim_values(
    df: Any,
    dims: list[str],
    null_handling: Any,
    column_types: dict[str, str] | None = None,
    rows: list[str] | None = None,
    columns: list[str] | None = None,
    auto_date_hierarchy: bool = True,
    date_grains: dict[str, str | None] | None = None,
    adaptive_grains: dict[str, str] | None = None,
) -> Any:
    """Rewrite null/empty dimension values to match frontend _resolveDimKey."""
    df = df.copy()
    for dim in dims:
        if dim not in df.columns:
            continue
        mode = _get_null_mode(dim, null_handling)
        col_type = column_types.get(dim) if column_types else None
        grain = _get_effective_date_grain(
            dim,
            rows,
            columns,
            date_grains,
            auto_date_hierarchy,
            column_types,
            adaptive_grains=adaptive_grains,
        )
        df[dim] = _resolve_dim_value_series(df[dim], col_type, mode, grain)
    return df


def _resolve_and_filter(
    df: Any,
    filters: dict[str, dict] | None,
    null_handling: Any,
    column_types: dict[str, str] | None = None,
    rows: list[str] | None = None,
    columns: list[str] | None = None,
    auto_date_hierarchy: bool = True,
    date_grains: dict[str, str | None] | None = None,
    adaptive_grains: dict[str, str] | None = None,
) -> Any:
    """Apply dimension filters to a raw DataFrame using resolved-value semantics.

    Mirrors PivotData._shouldIncludeRow + _resolveDimKey: for every filter
    field, resolve null/empty values via per-field _get_null_mode, then compare.
    """
    if not filters:
        return df
    mask = pd.Series(True, index=df.index)
    for field, filt in filters.items():
        if field not in df.columns:
            continue
        mode = _get_null_mode(field, null_handling)
        col_type = column_types.get(field) if column_types else None
        grain = _get_effective_date_grain(
            field,
            rows,
            columns,
            date_grains,
            auto_date_hierarchy,
            column_types,
            adaptive_grains=adaptive_grains,
        )
        resolved = _resolve_dim_value_series(df[field], col_type, mode, grain)
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


def _build_sidecar_fingerprint(
    config: PivotConfig,
    null_handling: Any,
    adaptive_date_grains: dict[str, str] | None = None,
) -> str:
    """Deterministic canonical JSON string for staleness detection."""
    agg = config.get("aggregation", {})
    filters = config.get("filters", {})
    obj: dict[str, Any] = {
        "adaptive_date_grains": dict(sorted(adaptive_date_grains.items()))
        if adaptive_date_grains
        else {},
        "aggregation": dict(sorted(agg.items())) if agg else {},
        "auto_date_hierarchy": config.get("auto_date_hierarchy", True),
        "columns": config.get("columns", []),
        "date_grains": dict(sorted(config.get("date_grains", {}).items()))
        if config.get("date_grains")
        else {},
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
    column_types: dict[str, str] | None = None,
    adaptive_grains: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """Build the hybrid_totals sidecar dict with pre-computed totals."""
    aggregation = config.get("aggregation", {})
    rows = config.get("rows", [])
    columns = config.get("columns", [])
    values = config.get("values", [])
    show_subtotals = config.get("show_subtotals", False)
    date_grains = config.get("date_grains", {})
    auto_date_hierarchy = config.get("auto_date_hierarchy", True)

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
            "sidecar_fingerprint": _build_sidecar_fingerprint(
                config,
                null_handling,
                adaptive_date_grains=adaptive_grains,
            ),
            "grand": {},
            "row": [],
            "col": [],
        }

    all_dims = rows + columns
    working = _normalize_dim_values(
        df,
        all_dims,
        null_handling,
        column_types,
        rows=rows,
        columns=columns,
        auto_date_hierarchy=auto_date_hierarchy,
        date_grains=date_grains,
        adaptive_grains=adaptive_grains,
    )
    working = _resolve_and_filter(
        working,
        config.get("filters", {}),
        null_handling,
        column_types,
        rows=rows,
        columns=columns,
        auto_date_hierarchy=auto_date_hierarchy,
        date_grains=date_grains,
        adaptive_grains=adaptive_grains,
    )

    fingerprint = _build_sidecar_fingerprint(
        config,
        null_handling,
        adaptive_date_grains=adaptive_grains,
    )

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

    # Temporal parent sidecar entries for collapsed hierarchy aggregation.
    # For each temporal column field with hierarchy (grain finer than year),
    # compute parent-bucket aggregates preserving sibling column dimensions.
    if columns:
        temporal_parent_entries: list[dict[str, Any]] = []
        temporal_parent_grand_entries: list[dict[str, Any]] = []
        for temporal_idx, col_field in enumerate(columns):
            col_type = (column_types or {}).get(col_field)
            if col_type not in ("date", "datetime"):
                continue
            effective_grain = _get_effective_date_grain(
                col_field,
                rows,
                columns,
                date_grains,
                auto_date_hierarchy,
                column_types,
                adaptive_grains,
            )
            if not effective_grain or effective_grain == "year":
                continue
            hierarchy_levels = _get_temporal_hierarchy_levels(effective_grain)
            for parent_grain in hierarchy_levels[:-1]:
                groupby_cols = list(columns)
                parent_series = working[col_field].apply(
                    lambda v, _pg=parent_grain: _rebucket_temporal_string(
                        str(v) if not pd.isna(v) else "",
                        _pg,
                    )
                )
                tp_col_name = f"__tp_{col_field}_{parent_grain}__"
                working[tp_col_name] = parent_series
                groupby_cols[temporal_idx] = tp_col_name

                if rows:
                    grouped_tp = working.groupby(
                        rows + groupby_cols, dropna=False, observed=True, sort=False
                    )
                    for key_tuple, group_df in grouped_tp:
                        parts = (
                            list(key_tuple)
                            if isinstance(key_tuple, tuple)
                            else [key_tuple]
                        )
                        parts = [str(p) for p in parts]
                        row_key = parts[: len(rows)]
                        col_key = parts[len(rows) :]
                        col_key[temporal_idx] = (
                            f"tp:{col_field}:{col_key[temporal_idx]}"
                        )
                        vals_tp: dict[str, int | float | None] = {}
                        for field, agg in sidecar_fields.items():
                            v = _sidecar_agg_func(agg, group_df[field])
                            vals_tp[field] = _normalize_sidecar_value(v)
                        temporal_parent_entries.append(
                            {
                                "row": row_key,
                                "col": col_key,
                                "field": col_field,
                                "grain": parent_grain,
                                "values": vals_tp,
                            }
                        )

                grouped_tp_grand = working.groupby(
                    groupby_cols, dropna=False, observed=True, sort=False
                )
                for key_tuple, group_df in grouped_tp_grand:
                    parts = (
                        list(key_tuple) if isinstance(key_tuple, tuple) else [key_tuple]
                    )
                    parts = [str(p) for p in parts]
                    col_key_g = list(parts)
                    col_key_g[temporal_idx] = (
                        f"tp:{col_field}:{col_key_g[temporal_idx]}"
                    )
                    vals_tp_g: dict[str, int | float | None] = {}
                    for field, agg in sidecar_fields.items():
                        v = _sidecar_agg_func(agg, group_df[field])
                        vals_tp_g[field] = _normalize_sidecar_value(v)
                    temporal_parent_grand_entries.append(
                        {
                            "col": col_key_g,
                            "field": col_field,
                            "grain": parent_grain,
                            "values": vals_tp_g,
                        }
                    )

                working.drop(columns=[tp_col_name], inplace=True)

        if temporal_parent_entries:
            result["temporal_parent"] = temporal_parent_entries
        if temporal_parent_grand_entries:
            result["temporal_parent_grand"] = temporal_parent_grand_entries

    if rows:
        temporal_row_parent_entries: list[dict[str, Any]] = []
        temporal_row_parent_grand_entries: list[dict[str, Any]] = []
        for temporal_idx, row_field in enumerate(rows):
            col_type = (column_types or {}).get(row_field)
            if col_type not in ("date", "datetime"):
                continue
            effective_grain = _get_effective_date_grain(
                row_field,
                rows,
                columns,
                date_grains,
                auto_date_hierarchy,
                column_types,
                adaptive_grains,
            )
            if not effective_grain or effective_grain == "year":
                continue
            hierarchy_levels = _get_temporal_hierarchy_levels(effective_grain)
            for parent_grain in hierarchy_levels[:-1]:
                groupby_rows = list(rows)
                parent_series = working[row_field].apply(
                    lambda v, _pg=parent_grain: _rebucket_temporal_string(
                        str(v) if not pd.isna(v) else "",
                        _pg,
                    )
                )
                tp_col_name = f"__trp_{row_field}_{parent_grain}__"
                working[tp_col_name] = parent_series
                groupby_rows[temporal_idx] = tp_col_name

                def _append_temporal_row_parent_entries(
                    grouped_obj: Any,
                    col_key_builder: Any,
                ) -> None:
                    for key_tuple, group_df in grouped_obj:
                        parts = (
                            list(key_tuple)
                            if isinstance(key_tuple, tuple)
                            else [key_tuple]
                        )
                        parts = [str(p) for p in parts]
                        row_key = parts[: len(rows)]
                        row_key[temporal_idx] = (
                            f"tp:{row_field}:{row_key[temporal_idx]}"
                        )
                        col_key = col_key_builder(parts[len(rows) :])
                        vals_trp: dict[str, int | float | None] = {}
                        for field, agg in sidecar_fields.items():
                            v = _sidecar_agg_func(agg, group_df[field])
                            vals_trp[field] = _normalize_sidecar_value(v)
                        temporal_row_parent_entries.append(
                            {
                                "row": row_key,
                                "col": col_key,
                                "field": row_field,
                                "grain": parent_grain,
                                "values": vals_trp,
                            }
                        )

                grouped_trp = working.groupby(
                    groupby_rows + columns, dropna=False, observed=True, sort=False
                )
                _append_temporal_row_parent_entries(
                    grouped_trp, lambda col_parts: col_parts
                )

                if len(columns) >= 2:
                    for depth in range(1, len(columns)):
                        grouped_trp_prefix = working.groupby(
                            groupby_rows + columns[:depth],
                            dropna=False,
                            observed=True,
                            sort=False,
                        )
                        _append_temporal_row_parent_entries(
                            grouped_trp_prefix, lambda col_parts: col_parts
                        )

                for col_temporal_idx, col_field in enumerate(columns):
                    col_type = (column_types or {}).get(col_field)
                    if col_type not in ("date", "datetime"):
                        continue
                    col_effective_grain = _get_effective_date_grain(
                        col_field,
                        rows,
                        columns,
                        date_grains,
                        auto_date_hierarchy,
                        column_types,
                        adaptive_grains,
                    )
                    if not col_effective_grain or col_effective_grain == "year":
                        continue
                    col_hierarchy_levels = _get_temporal_hierarchy_levels(
                        col_effective_grain
                    )
                    for col_parent_grain in col_hierarchy_levels[:-1]:
                        groupby_cols = list(columns)
                        parent_col_series = working[col_field].apply(
                            lambda v, _pg=col_parent_grain: _rebucket_temporal_string(
                                str(v) if not pd.isna(v) else "",
                                _pg,
                            )
                        )
                        col_tp_name = f"__trp_col_{col_field}_{col_parent_grain}__"
                        working[col_tp_name] = parent_col_series
                        groupby_cols[col_temporal_idx] = col_tp_name
                        grouped_trp_temporal_col = working.groupby(
                            groupby_rows + groupby_cols,
                            dropna=False,
                            observed=True,
                            sort=False,
                        )
                        _append_temporal_row_parent_entries(
                            grouped_trp_temporal_col,
                            lambda col_parts, _idx=col_temporal_idx, _field=col_field: [
                                *col_parts[:_idx],
                                f"tp:{_field}:{col_parts[_idx]}",
                                *col_parts[_idx + 1 :],
                            ],
                        )
                        working.drop(columns=[col_tp_name], inplace=True)

                grouped_trp_total = working.groupby(
                    groupby_rows, dropna=False, observed=True, sort=False
                )
                for key_tuple, group_df in grouped_trp_total:
                    parts = (
                        list(key_tuple) if isinstance(key_tuple, tuple) else [key_tuple]
                    )
                    parts = [str(p) for p in parts]
                    row_key_g = list(parts)
                    row_key_g[temporal_idx] = (
                        f"tp:{row_field}:{row_key_g[temporal_idx]}"
                    )
                    vals_trp_g: dict[str, int | float | None] = {}
                    for field, agg in sidecar_fields.items():
                        v = _sidecar_agg_func(agg, group_df[field])
                        vals_trp_g[field] = _normalize_sidecar_value(v)
                    temporal_row_parent_grand_entries.append(
                        {
                            "row": row_key_g,
                            "field": row_field,
                            "grain": parent_grain,
                            "values": vals_trp_g,
                        }
                    )

                working.drop(columns=[tp_col_name], inplace=True)

        if temporal_row_parent_entries:
            result["temporal_row_parent"] = temporal_row_parent_entries
        if temporal_row_parent_grand_entries:
            result["temporal_row_parent_grand"] = temporal_row_parent_grand_entries

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
    drilldown_request: DrilldownRequest,
    null_handling: Any = None,
    dims: list[str] | None = None,
    config_filters: dict[str, dict] | None = None,
    page_size: int = _DRILLDOWN_PAGE_SIZE,
    column_types: dict[str, str] | None = None,
    rows: list[str] | None = None,
    columns: list[str] | None = None,
    auto_date_hierarchy: bool = True,
    date_grains: dict[str, str | None] | None = None,
    adaptive_grains: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], list[str], int, int]:
    """Filter the original DataFrame for a hybrid-mode drill-down request.

    Uses resolved-dimension semantics (matching _resolveDimKey on the
    frontend) so that filter values like "(null)" align correctly with
    null_handling modes.  Applies config-level dimension filters first
    (matching _shouldIncludeRow), then cell-click filters.

    Returns (records_list, column_names, total_matching_count, page).
    """
    working = _resolve_and_filter(
        df,
        config_filters or {},
        null_handling,
        column_types=column_types,
        rows=rows,
        columns=columns,
        auto_date_hierarchy=auto_date_hierarchy,
        date_grains=date_grains,
        adaptive_grains=adaptive_grains,
    )

    filters: dict[str, str] = drilldown_request.get("filters", {})
    page: int = max(0, int(drilldown_request.get("page", 0)))
    sort_column = drilldown_request.get("sortColumn")
    sort_direction = drilldown_request.get("sortDirection")

    mask = pd.Series(True, index=working.index)
    for col, val in filters.items():
        if col not in working.columns:
            continue
        mode = _get_null_mode(col, null_handling)
        col_type = column_types.get(col) if column_types else None
        grain = _get_effective_date_grain(
            col,
            rows,
            columns,
            date_grains,
            auto_date_hierarchy,
            column_types,
            adaptive_grains=adaptive_grains,
        )
        resolved = _resolve_dim_value_series(working[col], col_type, mode, grain)
        mask &= resolved == str(val)
    filtered = working[mask]
    total_count = len(filtered)
    if (
        sort_column
        and sort_direction in ("asc", "desc")
        and sort_column in filtered.columns
    ):
        sort_key = sort_column
        temp_sort_column: str | None = None
        col_type = column_types.get(sort_column) if column_types else None
        if col_type in ("date", "datetime"):
            temp_sort_column = "__drilldown_sort_key__"
            while temp_sort_column in filtered.columns:
                temp_sort_column += "_"
            filtered = filtered.assign(
                **{
                    temp_sort_column: pd.to_datetime(
                        filtered[sort_column], errors="coerce"
                    )
                }
            )
            sort_key = temp_sort_column
        elif col_type in ("integer", "float"):
            temp_sort_column = "__drilldown_sort_key__"
            while temp_sort_column in filtered.columns:
                temp_sort_column += "_"
            filtered = filtered.assign(
                **{
                    temp_sort_column: pd.to_numeric(
                        filtered[sort_column], errors="coerce"
                    )
                }
            )
            sort_key = temp_sort_column

        filtered = filtered.sort_values(
            by=sort_key,
            ascending=sort_direction == "asc",
            kind="mergesort",
            na_position="last",
        )
        if temp_sort_column is not None:
            filtered = filtered.drop(columns=[temp_sort_column])
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
    auto_date_hierarchy: bool = True,
    date_grains: dict[str, str | None] | None = None,
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
    dimension_format: dict[str, str] | None = None,
) -> PivotConfig:
    _rows = rows or []
    _values = values or []
    cfg = PivotConfig(
        version=CONFIG_SCHEMA_VERSION,
        rows=_rows,
        columns=columns or [],
        values=_values,
        auto_date_hierarchy=auto_date_hierarchy,
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
    if date_grains is not None:
        cfg["date_grains"] = dict(sorted(date_grains.items()))
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
    if dimension_format is not None:
        cfg["dimension_format"] = dimension_format
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


class DrilldownRequest(CellClickPayload, total=False):
    """Frontend-owned drilldown request state mirrored through session_state."""

    page: int
    sortColumn: str
    sortDirection: Literal["asc", "desc"]
    requestId: str


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
    auto_date_hierarchy: bool = True,
    date_grains: dict[str, str | None] | None = None,
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
    # Format hints from Streamlit column_config / dimension_format
    column_config: dict[str, Any] | None = None,
    dimension_format: str | dict[str, str] | None = None,
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
    auto_date_hierarchy : bool
        When True (default), typed date/datetime fields placed on rows or
        columns automatically use a default month grain and become hierarchy-
        capable without explicit Python config.
    date_grains : dict[str, str | None] or None
        Optional per-field temporal grouping override. Maps date/datetime
        dimensions to one of ``"year"``, ``"quarter"``, ``"month"``,
        ``"week"``, or ``"day"``. Set a field to ``None`` to explicitly opt
        out and keep the raw/original temporal values for that field.
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
        If True, toolbar config controls are disabled. Data export plus
        header-menu sorting, filtering, and show-values-as remain available.
        Defaults to False.
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
        ``"raw"``, ``"pct_of_total"``, ``"pct_of_row"``, ``"pct_of_col"``,
        ``"diff_from_prev"``, ``"pct_diff_from_prev"``,
        ``"diff_from_prev_year"``, or ``"pct_diff_from_prev_year"``.
        Period-comparison modes require at least one grain-enabled temporal
        dimension on rows or columns. Omitted fields default to ``"raw"``.
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

    # --- Styler extraction (Phase 3): detect before conversion strips it ---
    styler_number_formats: dict[str, str] = {}
    styler_dimension_formats: dict[str, str] = {}
    try:
        from pandas.io.formats.style import Styler as _PandasStyler

        if isinstance(data, _PandasStyler):
            styler_number_formats, styler_dimension_formats = _extract_styler_formats(
                data
            )
            data = data.data
    except ImportError:
        pass

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
    if not isinstance(auto_date_hierarchy, bool):
        raise TypeError(
            "auto_date_hierarchy must be a bool, "
            f"got {type(auto_date_hierarchy).__name__}"
        )

    normalized_date_grains: dict[str, str | None] | None = None

    if date_grains is not None:
        if not isinstance(date_grains, dict):
            raise TypeError(
                f"date_grains must be a dict or None, got {type(date_grains).__name__}"
            )
        normalized_date_grains = {}
        for field, grain in sorted(date_grains.items()):
            if not isinstance(field, str) or field not in df_cols:
                raise ValueError(
                    f"date_grains contains column not in DataFrame: {field!r}. "
                    f"Available columns: {sorted(df_cols)}"
                )
            if grain is not None and grain not in VALID_DATE_GRAINS:
                raise ValueError(
                    f"date_grains[{field!r}] must be one of "
                    f"{sorted(VALID_DATE_GRAINS)} or None, got {grain!r}"
                )
            normalized_date_grains[field] = grain

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

    if dimension_format is not None:
        if isinstance(dimension_format, str):
            dimension_format = {"__all__": dimension_format}
        elif isinstance(dimension_format, dict):
            for k, v in dimension_format.items():
                if not isinstance(k, str) or not isinstance(v, str):
                    raise TypeError("dimension_format keys and values must be strings")
        else:
            raise TypeError(
                f"dimension_format must be str, dict, or None, got {type(dimension_format).__name__}"
            )

    # Merge Styler formats (lowest precedence): fill gaps only
    if styler_number_formats:
        if number_format is None:
            number_format = styler_number_formats
        elif isinstance(number_format, dict):
            merged_snf = dict(styler_number_formats)
            merged_snf.update(number_format)
            number_format = merged_snf
    if styler_dimension_formats:
        if dimension_format is None:
            dimension_format = styler_dimension_formats
        elif isinstance(dimension_format, dict):
            merged_sdf = dict(styler_dimension_formats)
            merged_sdf.update(dimension_format)
            dimension_format = merged_sdf

    # Merge column_config formats: column_config fills gaps, explicit params override
    if column_config is not None:
        cc_number, cc_dimension = _translate_column_config(column_config, filtered_data)
        if cc_number:
            if number_format is None:
                number_format = cc_number
            elif isinstance(number_format, dict):
                merged_nf = dict(cc_number)
                merged_nf.update(number_format)
                number_format = merged_nf
        if cc_dimension:
            if dimension_format is None:
                dimension_format = cc_dimension
            elif isinstance(dimension_format, dict):
                merged_df = dict(cc_dimension)
                merged_df.update(dimension_format)
                dimension_format = merged_df

    # Normalize number_format: str -> {"__all__": str}
    if isinstance(number_format, str):
        number_format = {"__all__": number_format}

    original_column_types = _build_original_column_types(filtered_data)
    adaptive_date_grains = _compute_adaptive_date_grains(
        filtered_data, original_column_types
    )
    _validate_period_comparison_config(
        show_values_as,
        resolved_rows,
        resolved_columns,
        normalized_date_grains,
        auto_date_hierarchy,
        original_column_types,
        adaptive_grains=adaptive_date_grains,
    )

    initial_config = _default_config(
        rows=resolved_rows,
        columns=resolved_columns,
        values=resolved_values,
        auto_date_hierarchy=auto_date_hierarchy,
        date_grains=normalized_date_grains,
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
        dimension_format=dimension_format
        if isinstance(dimension_format, dict)
        else None,
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
        _prepare_threshold_hybrid_frame(
            filtered_data,
            config_to_send,
            null_handling,
            original_column_types,
            adaptive_grains=adaptive_date_grains,
        )
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
        "original_column_types": original_column_types,
        "adaptive_date_grains": adaptive_date_grains,
    }

    if use_threshold_hybrid:
        data_payload["source_row_count"] = len(filtered_data)
        agg_dict = config_to_send.get("aggregation", {})
        agg_remap = _build_hybrid_agg_remap(agg_dict)
        if agg_remap:
            data_payload["hybrid_agg_remap"] = agg_remap
        totals_sidecar = _compute_hybrid_totals(
            filtered_data,
            config_to_send,
            null_handling,
            original_column_types,
            adaptive_grains=adaptive_date_grains,
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
                column_types=original_column_types,
                rows=config_to_send.get("rows"),
                columns=config_to_send.get("columns"),
                auto_date_hierarchy=config_to_send.get("auto_date_hierarchy", True),
                date_grains=config_to_send.get("date_grains"),
                adaptive_grains=adaptive_date_grains,
            )
            data_payload["drilldown_records"] = records
            data_payload["drilldown_columns"] = columns
            data_payload["drilldown_total_count"] = total
            data_payload["drilldown_page"] = page
            data_payload["drilldown_page_size"] = _DRILLDOWN_PAGE_SIZE
            data_payload["drilldown_request_id"] = drilldown_request.get("requestId")

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
