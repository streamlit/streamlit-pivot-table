# streamlit-pivot

Pivot table component for [Streamlit](https://streamlit.io). Built with Streamlit Components V2, React, and TypeScript.

Supports multi-dimensional pivoting, interactive sorting and filtering, subtotals with collapse/expand, conditional formatting, data export (Excel/CSV/TSV/clipboard), drill-down detail panels, drag-and-drop field configuration, synthetic (derived) measures with a formula engine, date/time hierarchies with period-over-period comparisons, hierarchical row layouts, column resize, fullscreen mode, and server-side pre-aggregation for large datasets.

## Installation

```sh
pip install streamlit-pivot
```

**Requirements:** Python >= 3.10, Streamlit >= 1.51

## Quick Start

```python
import pandas as pd
import streamlit as st
from streamlit_pivot import st_pivot_table

df = pd.read_csv("sales.csv")

result = st_pivot_table(
    df,
    key="my_pivot",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    aggregation={"Revenue": "sum"},
    show_totals=True,
)
```

The `data` parameter accepts the same input types as `st.dataframe` — Pandas DataFrames, Polars DataFrames, NumPy arrays, dicts, lists of records, PyArrow tables, and any object supporting the DataFrame Interchange Protocol or `to_pandas()`. Data is automatically converted to a Pandas DataFrame internally.

Passing a Pandas `Styler` is also supported: its embedded number formatters are auto-extracted and used as default `number_format` patterns (explicit parameters still win). See [Formats from `Styler` and `column_config`](#formats-from-styler-and-column_config).

If `rows`, `columns`, and `values` are all omitted, the component auto-detects dimensions (categorical + low-cardinality numeric columns) and measures (high-cardinality numeric columns) from the data.

---

## API Reference

### `st_pivot_table(data, *, ...)`

Creates a pivot table component. All parameters except `data` are keyword-only.

Returns a `PivotTableResult` dict containing the current `config` state and optional `perf_metrics` (frontend-reported timing and layout stats). See [Callbacks and State](#callbacks-and-state).

#### Core Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data` | DataFrame-like | *(required)* | Source data. Accepts the same types as `st.dataframe`: Pandas/Polars DataFrame or Series, NumPy array, dict, list of records, PyArrow Table, etc. |
| `key` | `str` | *(required)* | **Required.** Unique component key for state persistence across reruns. Each pivot table on a page must have a distinct key. |
| `rows` | `list[str] \| None` | `None` | Column names to use as row dimensions. |
| `columns` | `list[str] \| None` | `None` | Column names to use as column dimensions. |
| `values` | `list[str] \| None` | `None` | Column names to aggregate as measures. |
| `synthetic_measures` | `list[dict] \| None` | `None` | Derived measures computed from source-field sums (for example, ratio of sums). See [Synthetic Measures](#synthetic-measures-v1). |
| `aggregation` | `str \| dict[str, str]` | `"sum"` | Aggregation setting for raw value fields. A single string applies to every raw measure; a dict enables per-measure aggregation. See [Aggregation Functions](#aggregation-functions). |
| `auto_date_hierarchy` | `bool` | `True` | Auto-group typed date/datetime fields placed on rows or columns. Default grain is adaptive based on the source data's date range (year for >2 years, quarter for >1 year, month for >2 months, day for shorter ranges). |
| `date_grains` | `dict[str, str \| None] \| None` | `None` | Per-field temporal overrides. Use `"year"`, `"quarter"`, `"month"`, `"week"`, or `"day"`. Use `None` for an explicit `Original` opt-out. |
| `interactive` | `bool` | `True` | Enable end-user config controls. When `False`, the toolbar is hidden and header-menu sort/filter/show-values-as actions are disabled. |

#### Totals and Subtotals

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `show_totals` | `bool` | `True` | Show grand total rows and columns. Acts as default for `show_row_totals` and `show_column_totals`. |
| `show_row_totals` | `bool \| list[str] \| None` | `None` | Row totals visibility: `True` all measures, `False` none, `["Revenue"]` only listed measures. Defaults to `show_totals` when `None`. |
| `show_column_totals` | `bool \| list[str] \| None` | `None` | Column totals visibility with the same semantics as `show_row_totals`. |
| `show_subtotals` | `bool \| list[str]` | `False` | Subtotal visibility per row dimension: `True` all parent dimensions, `False` none, or a list of dimension names. |
| `repeat_row_labels` | `bool` | `False` | Repeat row dimension labels on every row instead of merging. |

#### Sorting

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `row_sort` | `dict \| None` | `None` | Initial sort for rows. See [Sort Configuration](#sort-configuration). |
| `col_sort` | `dict \| None` | `None` | Initial sort for columns. Same shape as `row_sort` (without `col_key`). |

#### Display and Formatting

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `number_format` | `str \| dict[str, str] \| None` | `None` | Number format pattern(s) applied to value-field cells. See [Number Format Patterns](#number-format-patterns). |
| `dimension_format` | `str \| dict[str, str] \| None` | `None` | Number format pattern(s) applied to row/column dimension labels (e.g. to render a numeric `Year` as `2024` instead of `2,024.00`). A single string applies to every numeric dimension; a dict maps field names to patterns. Use `"__all__"` as a key for a default. |
| `column_alignment` | `dict[str, str] \| None` | `None` | Per-field text alignment: `"left"`, `"center"`, or `"right"`. |
| `show_values_as` | `dict[str, str] \| None` | `None` | Per-field display mode. See [Show Values As](#show-values-as). |
| `conditional_formatting` | `list[dict] \| None` | `None` | Visual formatting rules. See [Conditional Formatting](#conditional-formatting). |
| `column_config` | `dict[str, Any] \| None` | `None` | Optional per-column format hints, using a subset of the Streamlit [`column_config`](https://docs.streamlit.io/develop/api-reference/data/st.column_config) shape. Each entry is a dict with a `format` key (d3-style or printf-style `"%,.2f"`) and optionally `type` (`"date"` / `"datetime"` / `"time"` map to `dimension_format`; everything else maps to `number_format`). Explicit `number_format` / `dimension_format` parameters always win. See [Formats from `Styler` and `column_config`](#formats-from-styler-and-column_config). |
| `empty_cell_value` | `str` | `"-"` | Display string for cells with no data. |

#### Layout

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `height` | `int \| None` | `None` | **Deprecated.** Kept for backwards compatibility — when provided, it is treated as `max_height`. Use `max_height` in new code. |
| `max_height` | `int` | `500` | Maximum auto-size height in pixels. Table becomes scrollable when content exceeds this. |
| `sticky_headers` | `bool` | `True` | Column headers stick to the top of the scroll container. |
| `row_layout` | `"table" \| "hierarchy"` | `"table"` | Controls how row dimensions are rendered. `"table"` uses separate row-header columns, while `"hierarchy"` renders a single indented tree column with breadcrumb-level controls. Passing `"hierarchy"` with no explicit `show_subtotals` automatically enables subtotals for all grouping levels. See [Row Layout Modes](#row-layout-modes). |

#### Interactivity and Callbacks

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `on_cell_click` | `Callable[[], None] \| None` | `None` | Called when a user clicks a data cell. Read the payload from `st.session_state[key]`. |
| `on_config_change` | `Callable[[], None] \| None` | `None` | Called when the user changes the pivot config interactively, including toolbar and header-menu actions. |
| `enable_drilldown` | `bool` | `True` | Show an inline drill-down panel with source records when a cell is clicked. |
| `locked` | `bool` | `False` | Viewer mode with exploration enabled. Toolbar config controls are read-only, viewer-safe actions like data export and group expand/collapse remain available, and header-menu sorting/filtering/`Show Values As` plus drill-down still work. |
| `export_filename` | `str \| None` | `None` | Base filename (without extension) for exported files (.xlsx, .csv, .tsv). Date and extension are appended automatically. Defaults to `"pivot-table"`. |

> **Frontend-only interactions:** Drag-and-drop field reordering/moving, column resize (drag header edges), and fullscreen mode (toolbar expand icon) are available automatically when `interactive=True`. No additional Python parameters are needed.

#### Data Control

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `null_handling` | `str \| dict[str, str] \| None` | `None` | How to treat null/NaN values. See [Null Handling](#null-handling). |
| `source_filters` | `dict[str, dict[str, list[Any]]] \| None` | `None` | Server-only report-level filters applied before any pivot processing. `include` takes precedence over `exclude`. `None` matches null-like values, `""` matches only literal empty strings, and no type coercion is performed. |
| `hidden_attributes` | `list[str] \| None` | `None` | Column names to hide entirely from the UI. |
| `hidden_from_aggregators` | `list[str] \| None` | `None` | Column names hidden from the values/aggregators dropdown only. |
| `frozen_columns` | `list[str] \| None` | `None` | Column names that cannot be removed from their toolbar zone and cannot be reordered or moved via drag-and-drop. Frozen chips render without a drag handle. |
| `hidden_from_drag_drop` | `list[str] \| None` | `None` | **Deprecated alias** for `frozen_columns`. Use `frozen_columns` in new code. |
| `sorters` | `dict[str, list[str]] \| None` | `None` | Custom sort orderings per dimension. Maps column name to ordered list of values. |
| `menu_limit` | `int \| None` | `None` | Max items in the header-menu filter checklist. Defaults to 50. |
| `execution_mode` | `str` | `"auto"` | Performance execution mode. See [Execution Mode](#execution-mode). |

---

## Feature Guide

### Aggregation Functions

| Function | Value | Description |
|----------|-------|-------------|
| Sum | `"sum"` | Sum of values |
| Average | `"avg"` | Arithmetic mean |
| Count | `"count"` | Number of records |
| Min | `"min"` | Minimum value |
| Max | `"max"` | Maximum value |
| Count Distinct | `"count_distinct"` | Number of unique values |
| Median | `"median"` | Median value |
| 90th Percentile | `"percentile_90"` | 90th percentile |
| First | `"first"` | First value encountered |
| Last | `"last"` | Last value encountered |

```python
st_pivot_table(
    df,
    key="aggregation_example",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Units", "Price"],
    aggregation={
        "Revenue": "sum",
        "Units": "count",
        "Price": "avg",
    },
)
```

In the interactive toolbar, aggregation is edited inside the **Settings Panel**. Open the panel, click the aggregation badge on a value chip to change it, then click **Apply**. Raw measure chips in the toolbar display the selected aggregation inline in a compact name-first format such as `Revenue (Sum)`.

### Synthetic Measures

Synthetic measures let you render derived metrics alongside regular value fields.

Supported operations:

- `sum_over_sum` -> `sum(numerator) / sum(denominator)` (returns empty cell value when denominator is 0)
- `difference` -> `sum(numerator) - sum(denominator)`
- `formula` -> arbitrary arithmetic expression referencing aggregated fields

Optional synthetic-measure fields:

- `format` -> number format pattern applied only to that synthetic measure (for example `.1%`, `$,.0f`, or `,.2f`)

**Legacy operations** (`sum_over_sum`, `difference`) use `numerator` / `denominator` fields and always operate on sums:

```python
st_pivot_table(
    df,
    key="synthetic_measures_example",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    synthetic_measures=[
        {
            "id": "revenue_per_unit",
            "label": "Revenue / Unit",
            "operation": "sum_over_sum",
            "numerator": "Revenue",
            "denominator": "Units",
            "format": ".1%",
        },
        {
            "id": "revenue_minus_cost",
            "label": "Revenue - Cost",
            "operation": "difference",
            "numerator": "Revenue",
            "denominator": "Cost",
            "format": "$,.0f",
        },
    ],
)
```

**Formula measures** use a `formula` field with an arbitrary expression. Field references are quoted strings. Each field uses its configured aggregation (default: sum).

```python
st_pivot_table(
    df,
    key="formula_example",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Cost", "Headcount"],
    synthetic_measures=[
        {
            "id": "margin",
            "label": "Margin",
            "operation": "formula",
            "formula": '"Revenue" - "Cost"',
        },
        {
            "id": "margin_pct",
            "label": "Margin %",
            "operation": "formula",
            "formula": 'if("Revenue" > 0, ("Revenue" - "Cost") / "Revenue", 0)',
            "format": ".1%",
        },
        {
            "id": "rev_per_head",
            "label": "Rev / Head",
            "operation": "formula",
            "formula": '"Revenue" / "Headcount"',
            "format": ",.1f",
        },
    ],
)
```

Formula supported operations:

- **Arithmetic:** `+` `-` `*` `/` `^` (exponent) `%` (modulo)
- **Comparison:** `>` `>=` `<` `<=` `==` `!=`
- **Logical:** `and`, `or`, `not`
- **Conditional:** `if(condition, then, else)`
- **Functions:** `abs()`, `min()`, `max()`, `round(x, decimals)`

Division by zero and missing fields produce null values. The `if()` function short-circuits, so `if("Cost" > 0, "Revenue" / "Cost", 0)` safely returns `0` when Cost is zero. Formula evaluation is CSP-safe (no `eval()` or `new Function()`).

Per-measure aggregation applies only to raw entries in `values`. Legacy synthetic measures (`sum_over_sum`, `difference`) always operate on sums. Formula measures use each field's configured aggregation.

`aggregation="sum_over_sum"` is no longer supported as a table-wide aggregation mode. Use `synthetic_measures` for ratio-of-sums behavior.
In the interactive builder, the **Format** input includes presets (Percent, Currency, Number) and validates custom patterns before save.

Synthetic measures render as `fx`-badged chips in the Values zone and can be interleaved with raw value chips via drag-and-drop; the resulting order is persisted on the config as `value_order`. See [Drag-and-Drop Field Configuration](#drag-and-drop-field-configuration). When `value_order` is omitted, the default order is all raw `values` followed by synthetic measures in declaration order.

### Sort Configuration

Sort rows or columns by label or by aggregated value.

```python
row_sort = {
    "by": "value",           # "key" (alphabetical) or "value" (by measure)
    "direction": "desc",     # "asc" or "desc"
    "value_field": "Revenue", # required when by="value"
    "col_key": ["2023"],     # optional: sort within a specific column
    "dimension": "Category", # optional: scope sort to this level and below
}

col_sort = {
    "by": "key",
    "direction": "asc",
}
```

**Scoped sorting:** When `dimension` is set and subtotals are enabled, only the
targeted level and its children reorder — parent groups maintain their existing
order.  For example, with `rows=["Region", "Category", "Product"]` and
`dimension="Category"`, Region groups stay in their default (ascending by
subtotal) order while Categories within each Region sort descending.  Omit
`dimension` for a global sort that applies to all levels.

Users can also sort interactively via the column header menu (click the **&#8942;** icon).
When sorting from a specific dimension header, `dimension` is set automatically.

### Show Values As

Display measures as percentages instead of raw numbers.

| Mode | Value | Description |
|------|-------|-------------|
| Raw | `"raw"` | Display the aggregated number (default) |
| % of Grand Total | `"pct_of_total"` | Cell / Grand Total |
| % of Row Total | `"pct_of_row"` | Cell / Row Total |
| % of Column Total | `"pct_of_col"` | Cell / Column Total |
| Diff vs Previous Period | `"diff_from_prev"` | Current bucket minus previous bucket on the active temporal hierarchy |
| % Diff vs Previous Period | `"pct_diff_from_prev"` | Percent change vs previous bucket |
| Diff vs Previous Year | `"diff_from_prev_year"` | Current bucket minus same bucket in the prior year |
| % Diff vs Previous Year | `"pct_diff_from_prev_year"` | Percent change vs same bucket in the prior year |

```python
st_pivot_table(
    df,
    key="show_values_as_example",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    show_values_as={"Revenue": "pct_of_total"},
)
```

Users can also change this interactively via the value header menu (**&#8942;** icon on a value label header).
Synthetic measures are always rendered as raw derived values (`show_values_as` does not apply to them).
Period-comparison modes appear only when there is an active grouped temporal axis, whether that grouping came from auto hierarchy or an explicit `date_grains` override.

### Date Hierarchy and Time Comparisons

Typed `date` and `datetime` fields are treated as hierarchy-capable dimensions when they are placed on `rows` or `columns`.

- **Adaptive default grain**: with `auto_date_hierarchy=True`, temporal axis fields auto-group based on the date range of the source data (after `source_filters`):
  - **>2 years** → `year`
  - **>1 year** → `quarter`
  - **>2 months** → `month`
  - **≤2 months** → `day`
- Default drill ladder: `Year -> Quarter -> Month -> Day`.
- Alternate grouping: `Week` is available from the header menu, but it is not part of the default drill path.
- Explicit override precedence: explicit `date_grains[field]` beats interactive state, which beats the adaptive auto default.
- Explicit opt-out: `date_grains[field] = None` preserves the raw/original date values for that field.
- Hierarchical parent groups now render on **both axes**:
  - on `columns`, parent headers such as `2024` or `Q1 2024` collapse/expand inline
  - on `rows`, collapsing a parent replaces its visible descendants with one synthetic summary row

```python
# Adaptive date hierarchy: grain chosen from the data's date range
st_pivot_table(
    df,
    key="date_auto",
    rows=["region"],
    columns=["order_date"],
    values=["Revenue"],
    show_values_as={"Revenue": "diff_from_prev"},
)

# Deterministic starting grain from Python
st_pivot_table(
    df,
    key="date_quarter",
    rows=["region"],
    columns=["order_date"],
    values=["Revenue"],
    date_grains={"order_date": "quarter"},
    show_values_as={"Revenue": "diff_from_prev_year"},
)

# Disable auto hierarchy globally
st_pivot_table(
    df,
    key="date_off",
    rows=["region"],
    columns=["order_date"],
    values=["Revenue"],
    auto_date_hierarchy=False,
)

# Explicit Original/raw opt-out for one field
st_pivot_table(
    df,
    key="date_original",
    rows=["region"],
    columns=["ship_date"],
    values=["Revenue"],
    date_grains={"ship_date": None},
)
```

Once a temporal field is active on an axis, open its header menu to:

- drill up or down through the default hierarchy,
- switch directly to `Week`,
- choose `Original` to persist a raw-date opt-out for that field.

When a temporal field is on `columns`, parent headers such as `2024` or `Q1 2024` can be collapsed with the inline +/- toggle. When a temporal field is on `rows`, collapsing a parent replaces the visible child rows with one summary row for that parent. Both are view-only collapses: exports still emit the full expanded leaf-level table.

Grouped buckets export as grouped labels such as `Jan 2024`, `Q1 2024`, or `2024-W03`; they are intentionally not exported as fake raw Excel dates.

### Row Layout Modes

Choose between two row presentation modes:

| Mode | Value | Description |
|------|-------|-------------|
| Table | `"table"` | Classic pivot layout with one visible row-header column per row dimension, plus expanded temporal levels as separate row-header columns when applicable. |
| Hierarchy | `"hierarchy"` | Compact tree layout with a single visible row hierarchy column, indentation by depth, breadcrumb controls, and inline expand/collapse. |

```python
st_pivot_table(
    df,
    key="row_layout_example",
    rows=["Region", "Category", "Customer"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    row_layout="hierarchy",
)
```

Behavior notes:

- `table` preserves the traditional multi-column row-axis layout and works naturally with `repeat_row_labels`.
- `hierarchy` renders parent groups before their children and uses a single visible row column rather than separate columns per row dimension. Indentation reflects depth and the top grouping level uses a subtle background tint; deeper levels rely on indentation plus group-boundary borders.
- **Auto-subtotals:** when `row_layout="hierarchy"` and `show_subtotals` is not explicitly set, subtotals are automatically enabled for all grouping levels so the tree exposes group aggregations out of the box. Pass `show_subtotals=False` or `show_subtotals=[...]` to override.
- **`repeat_row_labels` is ignored** in hierarchy mode because the row axis is a single indented column; the Settings Panel disables the toggle accordingly.
- Temporal date hierarchies work in both layouts. In `table`, date levels expand into separate row-header columns; in `hierarchy`, those same levels render as nested tree levels within the single hierarchy column.
- Export parity is preserved. CSV, TSV, clipboard, and XLSX outputs follow the selected row layout, including hierarchy indentation.
- Execution-mode parity is also preserved. `row_layout` works in both `client_only` and `threshold_hybrid`; the layout mostly affects rendering, not whether hybrid execution is allowed.

### Number Format Patterns

Patterns follow a lightweight d3-style syntax.

| Pattern | Example Output | Description |
|---------|---------------|-------------|
| `$,.0f` | $12,345 | US currency, no decimals |
| `,.2f` | 12,345.67 | Comma-grouped, 2 decimals |
| `.1%` | 34.5% | Percentage, 1 decimal |
| `€,.2f` | &euro;12,345.67 | Euro via symbol |
| `£,.0f` | &pound;12,345 | GBP |

A single string applies to all value fields. A dict maps field names to patterns. Use `"__all__"` as a dict key for a default pattern.

```python
# Per-field formatting
st_pivot_table(
    df,
    key="number_format_per_field_example",
    values=["Revenue", "Profit"],
    number_format={"Revenue": "$,.0f", "Profit": ",.2f"},
)

# Global format for all fields
st_pivot_table(
    df,
    key="number_format_global_example",
    values=["Revenue"],
    number_format="$,.0f",
)
```

`dimension_format` uses the same d3-style patterns but applies to row/column dimension labels instead of value cells. This is useful when a dimension is numeric but should display like an ID (for example, `Year` rendered as `2024` rather than `2,024.00`).

```python
st_pivot_table(
    df,
    key="dimension_format_example",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    dimension_format={"Year": ".0f"},
    number_format={"Revenue": "$,.0f"},
)
```

### Formats from `Styler` and `column_config`

The component can pick up sensible default formats from two upstream sources, so a single format declaration often flows through to the pivot without extra configuration.

**Pandas `Styler`.** If you pass a `Styler` as `data`, its per-column formatters are probed with a representative numeric value and the resulting output string is reverse-engineered into a d3-style `number_format` pattern (currency prefix, grouping, decimals, and percent suffix are all detected). The component extracts **number formats only** — dimension/date formatters on a Styler are not currently translated to `dimension_format`. Unrecognizable formatters are silently skipped.

```python
styled = df.style.format({"Revenue": "${:,.0f}", "Profit": "{:,.2f}"})
st_pivot_table(
    styled,
    key="styler_formats_example",
    rows=["Region"],
    values=["Revenue", "Profit"],
)
# -> number_format = {"Revenue": "$,.0f", "Profit": ",.2f"}
```

**`column_config`.** A dict mapping column names to a small subset of the Streamlit [`column_config`](https://docs.streamlit.io/develop/api-reference/data/st.column_config) shape. Each entry is read for:

- `format` — a format string. d3-style patterns (`",.2f"`, `"$,.0f"`) pass through as-is. Streamlit printf-style patterns (`"%,.2f"`) are normalized by stripping the leading `%`.
- `type` — if it resolves to `"date"`, `"datetime"`, or `"time"`, the pattern contributes to `dimension_format`; otherwise it contributes to `number_format`. Plain `type_config = {"type": ...}` nesting is also accepted.

```python
st_pivot_table(
    df,
    key="column_config_formats_example",
    rows=["Region"],
    columns=["Order Date"],
    values=["Revenue", "Units"],
    column_config={
        "Revenue": {"format": "$,.0f"},
        "Units": {"format": ",.0f"},
        "Order Date": {"format": "YYYY-MM-DD", "type": "date"},
    },
)
```

**Precedence.** `explicit number_format / dimension_format` > `column_config` > `Styler`. The lower-priority sources only fill gaps — any field already present in an explicit format dict keeps the caller-supplied pattern.

### Conditional Formatting

Apply visual formatting rules to value cells. Three rule types are supported:

#### Color Scale

Gradient fill between 2 or 3 colors based on min/mid/max values in the column.

```python
{
    "type": "color_scale",
    "apply_to": ["Revenue"],      # field names, or [] for all
    "min_color": "#ffffff",       # required
    "max_color": "#2e7d32",       # required
    "mid_color": "#a5d6a7",       # optional (3-color scale)
    "mid_value": 0,               # optional numeric anchor for the midpoint
    "include_totals": False,      # optional, default False
}
```

When `mid_color` is provided without `mid_value`, the gradient bends at the
visual midpoint of the observed column range (current default behavior).

When `mid_value` is also provided, the gradient is anchored at that numeric
value for a smooth Excel-like diverging scale — ideal for PnL or variance
columns where `0` should always be the neutral color:

```python
{
    "type": "color_scale",
    "apply_to": ["PnL"],
    "min_color": "#ff0000",       # darker red for more negative
    "mid_color": "#ffffff",       # white at 0
    "max_color": "#0000ff",       # darker blue for more positive
    "mid_value": 0,
}
```

`mid_value` is interpreted in the same numeric space as the underlying
**aggregated cell values** (i.e. the raw `agg.value()` used by all
conditional formatting rules), which is the same space as `min_color` /
`max_color`. This is the natural fit for typical use cases like PnL or
variance anchored at `0`. Conditional formatting runs **before** any
`show_values_as` transformation, so pairing `mid_value` with a mode such
as `"pct_of_total"` will anchor on the raw aggregate, not on the displayed
percentage. Values outside the observed column range (for example, grand
totals) clamp to the endpoint colors rather than extrapolating past them.

#### Data Bars

Horizontal bar fill proportional to the cell value.

```python
{
    "type": "data_bars",
    "apply_to": ["Revenue"],
    "color": "#1976d2",           # optional bar color
    "fill": "gradient",           # "gradient" or "solid"
}
```

#### Threshold

Highlight cells matching a numeric condition.

```python
{
    "type": "threshold",
    "apply_to": ["Profit"],
    "conditions": [
        {
            "operator": "gt",     # "gt", "gte", "lt", "lte", "eq", "between"
            "value": 5000,        # threshold value (or [lo, hi] for "between")
            "background": "#c8e6c9",
            "color": "#1b5e20",
            "bold": True,         # optional
        },
    ],
}
```

Multiple rules can be combined:

```python
st_pivot_table(
    df,
    key="conditional_formatting_example",
    values=["Revenue", "Profit", "Units"],
    conditional_formatting=[
        {"type": "data_bars", "apply_to": ["Revenue"], "color": "#1976d2", "fill": "gradient"},
        {"type": "color_scale", "apply_to": ["Profit"], "min_color": "#fff", "max_color": "#2e7d32"},
        {"type": "threshold", "apply_to": ["Units"], "conditions": [
            {"operator": "gt", "value": 250, "background": "#bbdefb", "color": "#0d47a1", "bold": True},
        ]},
    ],
)
```

### Null Handling

Control how null/NaN values in the source data are treated.

| Mode | Value | Description |
|------|-------|-------------|
| Exclude | `"exclude"` | Rows with null dimension values are excluded (default) |
| Zero | `"zero"` | Null measure values are treated as 0 |
| Separate | `"separate"` | Null dimension values are grouped as "(null)" |

```python
# Global mode
st_pivot_table(df, key="null_handling_global_example", null_handling="zero")

# Per-field modes
st_pivot_table(
    df,
    key="null_handling_per_field_example",
    null_handling={"Region": "separate", "Revenue": "zero"},
)
```

### Subtotals and Row Grouping

With 2+ row dimensions, enable subtotals to see group-level aggregations with collapsible groups.

```python
st_pivot_table(
    df,
    key="subtotals_example",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    show_subtotals=True,
    repeat_row_labels=False,
)
```

- Each group shows a subtotal row with a collapse/expand toggle (+/&minus;).
- Collapsed groups hide child rows but keep the subtotal visible.
- Expand All / Collapse All controls are available in the toolbar utility menu.

**Grouping vs. leaf dimensions:** When subtotals are on, all dimensions except
the innermost are *grouping dimensions*.  They define collapsible groups and
receive visual hierarchy cues:

- **Bold tinted cells** on grouping dimension columns to distinguish them from
  detail data.
- **Indented leaf cells** — the innermost dimension is visually subordinated
  within its parent group.
- **Group boundary borders** — a subtle top border appears between data rows
  that belong to different groups, reinforcing the hierarchy.
- **Inline collapse/expand toggles** on the first data row of each group (on
  the merged grouping cell), not just on subtotal rows.

Pass a list to `show_subtotals` to enable subtotals for specific dimensions
only (e.g. `show_subtotals=["Region"]`).

### Column Group Collapse/Expand

With 2+ column dimensions, column groups can be collapsed into subtotal columns.

```python
st_pivot_table(
    df,
    key="column_groups_example",
    rows=["Region"],
    columns=["Year", "Category"],
    values=["Revenue"],
)
```

Hover over a parent column header to reveal the collapse toggle. Temporal date hierarchies use the same pattern on the column axis, with inline +/- controls on parent date headers.

### Data Export

Export the pivot table as Excel, CSV, TSV, or copy to clipboard. Available via the toolbar utility menu (download icon) whenever the interactive toolbar is shown, including locked viewer mode.

- **Format**: Excel (.xlsx), CSV, TSV, or Clipboard (tab-separated for pasting into spreadsheets)
- **Content**: Formatted (display values with currency, percentages, etc.) or Raw (unformatted numbers)
- **Filename**: Customizable via `export_filename`. The date (`YYYY-MM-DD`) and file extension are appended automatically. Defaults to `"pivot-table"` (e.g. `pivot-table_2026-03-09.xlsx`).

Excel export produces a professionally styled workbook with merged column headers, bold totals/subtotals, number formatting, banded rows, frozen panes (headers stay visible when scrolling), and row dimension merging that matches the rendered table layout. Sort order, active filters, and show-values-as percentages are all preserved. Conditional formatting rules (color scales, data bars, and threshold highlights) are translated to native Excel conditional formatting, so the exported file renders them natively without macros.

Export always outputs the full expanded table regardless of any collapsed row/column groups, including collapsed temporal date parents.

### Drill-Down Detail Panel

Click any data or total cell to open an inline panel below the table showing the source records that contributed to that cell's aggregated value.

```python
result = st_pivot_table(
    df,
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    enable_drilldown=True,
    on_cell_click=lambda: None,
    key="my_pivot",
)
```

- The panel displays up to 500 matching records per page with pagination controls when there are more.
- **Column sorting:** Click any column header to sort the drilldown results. The sort cycles through ascending, descending, and unsorted (original order). Sorting applies to the **full** result set before pagination, so page boundaries reflect the global sort order.
- In `threshold_hybrid` mode, sorting triggers a server round-trip so the backend sorts the full filtered DataFrame before slicing the requested page.
- Close with the **&times;** button or by pressing **Escape**.
- Set `enable_drilldown=False` to disable (the `on_cell_click` callback still fires).

### Execution Mode

Controls how pivot aggregation is performed for large datasets. By default (`"auto"`), the component computes everything client-side unless the dataset is large enough to benefit from server-side pre-aggregation.

| Mode | Value | Description |
|------|-------|-------------|
| Auto | `"auto"` | Client-side unless the dataset exceeds row/cardinality thresholds (default) |
| Client Only | `"client_only"` | Always send raw rows to the frontend |
| Threshold Hybrid | `"threshold_hybrid"` | Force server-side pre-aggregation when the config is compatible |

```python
st_pivot_table(
    df,
    key="large_dataset_example",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    execution_mode="auto",
)
```

**Auto thresholds:** In `"auto"` mode, server-side pre-aggregation activates when the dataset has at least 100K rows (high-cardinality layouts) or 250K rows (moderate layouts) and the estimated pivot shape exceeds the client-side comfort budget.

**Supported aggregations:** All 10 aggregation types are supported in hybrid mode. `count` and `count_distinct` work on any column type; all other aggregations (`sum`, `avg`, `min`, `max`, `median`, `percentile_90`, `first`, `last`) coerce values to numeric and ignore non-numeric entries, consistent with client-only mode behavior. For non-decomposable aggregations (`avg`, `count_distinct`, `median`, `percentile_90`, `first`, `last`), the server computes correct totals and subtotals via a sidecar payload, ensuring accuracy that client-side re-aggregation alone cannot provide.

**Limitations:**
- Synthetic measures (including formulas) evaluate client-side in hybrid mode — source fields are aggregated locally while hybrid pre-computed totals are used for regular fields.

`row_layout` is supported in both execution paths. Switching between `table` and `hierarchy` does not by itself force a fallback out of `threshold_hybrid`.

### Locked Mode

Use `locked=True` for a viewer-mode experience with exploration enabled. The Settings Panel and toolbar config controls are locked so end-users cannot change rows, columns, values, or per-measure aggregation. Reset, Swap, and config import/export are hidden, while data export remains available. Expand/Collapse All group controls remain accessible in the toolbar utility menu. Header-menu sorting, filtering, and `Show Values As` remain available, and drill-down still works.

```python
st_pivot_table(
    df,
    key="locked_mode_example",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    locked=True,
)
```

### Toolbar and Settings Panel

When `interactive=True`, the toolbar displays read-only zone cards showing current **Rows**, **Columns**, and **Values** assignments. Hovering over the top-right reveals utility actions:

| Action | Description |
|--------|-------------|
| **Reset** | Resets to the original Python-supplied config (only visible when config has changed) |
| **Swap** | Transposes row and column dimensions |
| **Copy Config** | Copies the current config as JSON to clipboard |
| **Import Config** | Paste a JSON config to apply |
| **Export Data** | Open the export popover (Excel / CSV / TSV / Clipboard). Use `export_filename` to customize the download filename. |
| **Expand / Collapse All** | Expand or collapse all row/column groups (visible when subtotals are enabled or 2+ column dimensions exist) |
| **Fullscreen** (expand icon) | Toggles fullscreen mode — the table fills the entire viewport. Press Escape or click the collapse icon to exit. |
| **Settings** (pivot icon) | Opens the Settings Panel for full field configuration |

#### Settings Panel (Staged Commit UX)

The Settings Panel is the primary authoring surface for pivot configuration. Changes are staged locally and only applied when you click **Apply**. Click **Cancel** or press **Escape** to discard.

The panel contains:

- **Available Fields** — unassigned columns shown as draggable chips. Click a chip's menu to add it to Rows, Columns, or Values. When more than 8 fields are available, a search input appears.
- **Rows / Columns / Values** drop zones — drag chips to reorder within a zone, drag between zones, or use the `x` button to remove. Value chips show an aggregation picker (click the badge to change). Synthetic `fx` chips appear inline with raw value chips and can be reordered alongside them (the resulting order is persisted as `value_order`).
- **Synthetic Measures** — click **+ Add measure** to create derived metrics. Choose **Sum over Sum**, **Difference**, or **Formula** as the operation. Formula mode provides a text input for arbitrary expressions, clickable field-name chips for quick insertion, and a `?` tooltip listing supported operations. Existing synthetic chips expose an **✎ Edit** button that reopens the measure editor.
- **Display Toggles** — Row Totals, Column Totals, Subtotals, Repeat Labels, Row Layout, and Sticky Headers.
- **Invalid drop feedback** — if you drag a chip onto a zone that cannot accept it (e.g. a non-numeric field into Values), the zone turns red and shows an inline hint explaining why.

External config changes (toolbar DnD, Reset, Swap, config import) while the panel is open will close it and discard uncommitted edits. See [Locked Mode](#locked-mode) for viewer-mode behavior.

### Field Search

When the Settings Panel has more than **8 available fields**, a search input appears at the top of the Available Fields section. Typing filters the field chips in place. The container maintains its initial height even when search reduces the visible chips.

This is a frontend-only convenience feature; no Python parameter is needed to enable it.

### Drag-and-Drop Field Configuration

Drag-and-drop is available in two contexts:

**Toolbar DnD:** Each chip in the Rows, Columns, and Values toolbar zones has a **grip-dots drag handle**. Drag to reorder within a zone or move between zones. These are immediate (non-staged) changes.

**Settings Panel DnD:** Inside the Settings Panel, chips in Available Fields and all zone sections are draggable. Drag from Available Fields into a zone, reorder within zones, or move between zones. These changes are staged and applied on **Apply**.

**Visual feedback:**
- A floating overlay chip follows the cursor during drag.
- The source chip stays in place at reduced opacity (ghosted).
- When dragging over a **valid** target zone, the zone highlights with a dashed border and subtle tint.
- When dragging over an **invalid** target zone (see Constraints), the zone highlights in red with an inline hint such as "Only numeric fields can be added to Values"; dropping is blocked.
- Within-zone reorders show smooth shift animations as chips make room.

**Constraints:**
- `frozen_columns` render without drag handles and cannot be dragged.
- Non-numeric fields are rejected from the Values zone.
- Rows and Columns are mutually exclusive (a field cannot be in both).
- A field can be in Values and one dimension zone simultaneously.
- Synthetic `fx` chips may be reordered within the Values zone but cannot leave it.
- When `locked=True`, drag-and-drop is fully disabled.
- A 5 px activation distance distinguishes clicks from drags.

**Config cleanup on move:** When fields move between zones, related config properties (aggregation, sort, collapsed groups, subtotals, conditional formatting, show-values-as, per-measure totals, and `value_order`) are automatically synchronized. Orphan entries in `value_order` are dropped and newly added measures are appended.

No Python API parameter is required — drag-and-drop is a purely frontend interaction.

### Column Resize

Drag the **right edge of any column header** to resize that column. A thin resize handle appears on hover (cursor changes to `col-resize`). Minimum column width is 40 px.

- Works in both virtualized and non-virtualized rendering modes.
- Each column's width is tracked independently by slot position.
- Double-click the resize handle to auto-size a column to its content.
- No Python API parameter is required — column resize is a purely frontend interaction.

### Fullscreen Mode

Click the **expand icon** (⤢) in the toolbar utility menu to enter fullscreen mode. The pivot table fills the entire browser viewport as a fixed overlay. Press **Escape** or click the **collapse icon** (⤡) to exit.

- The table automatically re-measures to fill the viewport, including virtual scroll height.
- Works with both virtualized and non-virtualized rendering modes.
- No Python API parameter is required — fullscreen is a purely frontend interaction.

### Non-Interactive Mode

Set `interactive=False` to render a read-only pivot view. This hides the toolbar and disables header-menu config actions (sorting, filtering, and `Show Values As`). Cell clicks and drill-down remain available.

---

## Callbacks and State

This component uses Streamlit Components V2 (CCv2). Callbacks are called with **no arguments**. Read updated values from `st.session_state[key]` after the callback fires.

```python
def on_click():
    payload = st.session_state["my_pivot"].get("cell_click")
    st.write("Clicked:", payload)

def on_config():
    config = st.session_state["my_pivot"].get("config")
    st.write("Config changed:", config)

result = st_pivot_table(
    df,
    key="my_pivot",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    on_cell_click=on_click,
    on_config_change=on_config,
)
```

### Cell Click Payload

When a cell is clicked, the payload has this shape:

```python
{
    "rowKey": ["East"],           # row dimension values
    "colKey": ["2023"],           # column dimension values
    "value": 12345.0,            # aggregated cell value (or None)
    "valueField": "Revenue",     # clicked value field or synthetic measure id
    "filters": {                  # dimension filters for drill-down
        "Region": "East",
        "Year": "2023",
    },
}
```

For total cells, `rowKey` or `colKey` will be `["Total"]` and the corresponding dimension is omitted from `filters`.

### Config State

The returned `config` dict contains the current supported configuration state, including interactive changes such as rows, columns, values, aggregation, totals, sorting, filtering, collapsed groups, `value_order`, and display options. Use this to persist user customizations, serialize via the toolbar's **Copy Config** / **Import Config** actions, or synchronize multiple components.

### Performance Metrics

When the frontend finishes a render pass, it writes a `perf_metrics` entry into the component's session state alongside `config`. Read it from the return value or `st.session_state[key]` to observe pivot sizing and timing. The schema (all keys optional) is:

| Key | Type | Description |
|-----|------|-------------|
| `parseMs` | `float` | Time to parse incoming data |
| `pivotComputeMs` | `float` | Time to build the pivot structure |
| `renderMs` | `float` | Time to render the current view |
| `firstMountMs` | `float` | Time to the first painted frame after mount |
| `sourceRows` / `sourceCols` | `int` | Source DataFrame shape after `source_filters` |
| `totalRows` / `totalCols` / `totalCells` | `int` | Rendered pivot shape |
| `executionMode` | `str` | `"client_only"` or `"threshold_hybrid"` — the path actually used for the current render |
| `needsVirtualization` | `bool` | Whether the renderer switched to virtualized scrolling |
| `columnsTruncated` / `truncatedColumnCount` | `bool`, `int` | Whether the frontend capped columns for safety |
| `warnings` | `list[str]` | Non-fatal messages (e.g. fallbacks out of hybrid mode) |
| `lastAction` | `dict` | `{kind, elapsedMs, axis, field, totalCount}` describing the most recent user-driven action |

```python
result = st_pivot_table(df, key="my_pivot", rows=["Region"], values=["Revenue"])
metrics = result.get("perf_metrics") or {}
if metrics.get("executionMode") == "threshold_hybrid":
    st.caption(f"Pre-aggregated {metrics['sourceRows']} source rows on the server.")
```

---

## Keyboard Accessibility

The component follows WAI-ARIA patterns for all interactive elements:

- **Toolbar**: Arrow keys navigate between toolbar buttons (roving tabindex). Space/Enter activates.
- **Drag-and-drop**: Space to pick up a chip, arrow keys to move, Space to drop at the new position. Screen reader announcements provided by dnd-kit.
- **Header menus**: Escape closes. Arrow keys navigate options. Space/Enter selects.
- **Export/Import popovers**: Focus is automatically placed on the first interactive element when opened. Tab/Shift+Tab moves between controls; tabbing out closes the popover.
- **Settings Panel** (pivot icon): Focus moves into the panel on open. Escape closes and discards staged changes. Tab navigates between fields, zones, toggles, and buttons. Aggregation dropdowns support Enter/Space for keyboard selection.
- **Radio groups** (export format/content): Arrow keys move focus between options. Space/Enter selects.
- **Drill-down panel**: Focus moves to the close button on open. Escape closes. Column headers are clickable buttons that cycle sort direction (asc → desc → none).
- **Data cells**: Focusable via Tab. Space/Enter triggers cell click.

---

## Performance: Using Fragments

Streamlit reruns the entire script whenever a widget's state changes. In apps with multiple pivot tables or expensive data preparation, this means every toolbar change, sort, or filter in one table triggers a full rerun — including all other tables.

Wrapping each pivot table in [`@st.fragment`](https://docs.streamlit.io/develop/api-reference/execution-flow/st.fragment) scopes reruns to just the fragment that changed, leaving the rest of the app untouched.

### Basic pattern

```python
import streamlit as st
from streamlit_pivot import st_pivot_table

df = load_data()  # runs once per full rerun, not on fragment reruns

@st.fragment
def sales_pivot():
    result = st_pivot_table(df, key="sales", rows=["Region"], values=["Revenue"])
    if result and result.get("cell_click"):
        st.info(f"Clicked: {result['cell_click']}")

sales_pivot()

@st.fragment
def product_pivot():
    st_pivot_table(df, key="products", rows=["Product"], values=["Units"])

product_pivot()
```

Interacting with "sales" only re-executes `sales_pivot()` — the data load and `product_pivot()` are not re-executed.

### When fragments help

| Scenario | Benefit |
|---|---|
| App with multiple pivot tables | Interactions in one table don't re-execute the others |
| Expensive data loading / transformation | Data prep runs only on full reruns, not on every config change |
| Hybrid drilldown (`execution_mode="threshold_hybrid"`) | Server round-trips for drill-down are scoped to the fragment |

### Caveats

- **Return values**: Streamlit ignores fragment return values during fragment reruns. Code that reads the result of `st_pivot_table()` should live inside the same fragment, or use `st.session_state[key]` instead.
- **Data prep with randomness**: Keep DataFrame generation that uses random seeds outside the fragment to avoid non-deterministic data on fragment reruns.
- **Callbacks**: `on_config_change` and `on_cell_click` fire during fragment reruns, which is the expected behavior.

The demo app (`streamlit_app.py`) wraps each of its 19 sections in `@st.fragment` as a reference implementation.

---

## Development

### Development install (editable)

Install in editable mode with Streamlit so you can run the example app:

```sh
uv pip install -e '.[with-streamlit]' --force-reinstall
```

### Running the example app

```sh
uv run streamlit run streamlit_app.py
```

The example app (`streamlit_app.py`) contains 19 sections covering the major features and usage patterns with interactive examples and inline documentation.

### Building the frontend

```sh
cd streamlit_pivot/frontend
npm install
npm run build
```

### Running tests

```sh
cd streamlit_pivot/frontend
npx vitest run
```

### Build a wheel

Build the frontend first (see [Building the frontend](#building-the-frontend)), then:

```sh
uv build
```

Output: `dist/streamlit_pivot-<version>-py3-none-any.whl`

### Requirements

- Python >= 3.10
- Node.js >= 24 (LTS)
- Streamlit >= 1.51

## License

Apache 2.0
