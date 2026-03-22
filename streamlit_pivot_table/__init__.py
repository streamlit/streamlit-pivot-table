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
from typing import TYPE_CHECKING, Any, TypedDict, cast

if TYPE_CHECKING:
    from collections.abc import Callable

try:
    import streamlit as st
    from streamlit.dataframe_util import convert_anything_to_pandas_df
except ImportError as e:
    raise ImportError(
        "streamlit_pivot_table requires Streamlit >= 1.51. "
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


_warned_keys: set[str] = set()
_PYTHON_CONFIG_STATE_PREFIX = "__streamlit_pivot_table_python_config__:"


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


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------


class PivotTableResult(TypedDict, total=False):
    """Value returned by st_pivot_table() to the caller."""

    config: PivotConfig


# ---------------------------------------------------------------------------
# CCv2 component registration
# ---------------------------------------------------------------------------

# Registration key follows the CCv2 packaged-component convention:
#   "<project.name>.<component.name>"
# where both segments come from the in-package manifest at
# streamlit_pivot_table/pyproject.toml:
#   [project] name = "streamlit-pivot-table"          -> project.name
#   [[tool.streamlit.component.components]] name = ... -> component.name
# See component_manifest_handler.py line 65 for the join logic.
_component = st.components.v2.component(
    "streamlit-pivot-table.streamlit_pivot_table",
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
        Fixed height in pixels. None means auto-size (capped by ``max_height``).
    max_height : int
        Maximum height in pixels when ``height`` is None. The table becomes
        scrollable with sticky headers once content exceeds this value.
        Ignored when ``height`` is explicitly set. Default 500.
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
    null_handling : str or dict[str, str] or None
        How to treat null/NaN values. Global mode ("exclude", "zero",
        "separate") or per-field dict mapping column names to modes.
        Defaults to None ("exclude").
    hidden_attributes : list[str] or None
        Column names to hide entirely from the UI.
    hidden_from_aggregators : list[str] or None
        Column names hidden from the values/aggregators dropdown only.
    frozen_columns : list[str] or None
        Column names that cannot be removed from their toolbar zone.
    hidden_from_drag_drop : list[str] or None
        Deprecated alias for ``frozen_columns``.
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
        Base filename (without extension) used when exporting data.
        The date and file extension are appended automatically.
        Defaults to ``"pivot-table"`` when not set.

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

    # --- Auto-detect dimensions/measures when not specified ---
    resolved_rows = rows
    resolved_columns = columns
    resolved_values = values

    if resolved_rows is None and resolved_columns is None and resolved_values is None:
        numeric_cols = data.select_dtypes(include="number").columns.tolist()
        categorical_cols = [c for c in data.columns if c not in numeric_cols]
        # Heuristic: numeric columns with few unique values (<=20) likely
        # represent dimensions (e.g. Year) rather than measures.
        likely_measures = [c for c in numeric_cols if data[c].nunique() > 20]
        likely_numeric_dims = [c for c in numeric_cols if data[c].nunique() <= 20]
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

    data_payload: dict[str, Any] = {
        "dataframe": data,
        "height": height,
        "max_height": max_height,
        "config": config_to_send,
    }
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

    mount_kwargs: dict[str, Any] = {
        "key": key,
        "default": {"config": config_to_send},
        "data": data_payload,
        "on_config_change": on_config_change or _noop_callback,
    }

    if on_cell_click is not None:
        mount_kwargs["on_cell_click_change"] = on_cell_click

    return cast(PivotTableResult, _component(**mount_kwargs))
