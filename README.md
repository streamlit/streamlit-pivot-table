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
| `subtotal_position` | `"top" \| "bottom"` | `"bottom"` | Where subtotal rows appear relative to their group members. `"bottom"` (default) places the subtotal after members (Excel default). `"top"` places it before members as a collapsible group header. No effect when `show_subtotals` is `False` or `row_layout="hierarchy"`. |
| `repeat_row_labels` | `bool` | `False` | Repeat row dimension labels on every row instead of merging. |

#### Sorting

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `row_sort` | `dict \| list[dict] \| None` | `None` | Initial sort for rows. A single `SortConfig` dict or a list for multi-field (chained) sorting. See [Sort Configuration](#sort-configuration). |
| `col_sort` | `dict \| list[dict] \| None` | `None` | Initial sort for columns. Same shape as `row_sort` (without `col_key`). Accepts a single dict or a list. |

#### Display and Formatting

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `number_format` | `str \| dict[str, str] \| None` | `None` | Number format pattern(s) applied to value-field cells. See [Number Format Patterns](#number-format-patterns). |
| `dimension_format` | `str \| dict[str, str] \| None` | `None` | Number format pattern(s) applied to row/column dimension labels (e.g. to render a numeric `Year` as `2024` instead of `2,024.00`). A single string applies to every numeric dimension; a dict maps field names to patterns. Use `"__all__"` as a key for a default. |
| `column_alignment` | `dict[str, str] \| None` | `None` | Per-field text alignment: `"left"`, `"center"`, or `"right"`. |
| `show_values_as` | `dict[str, str] \| None` | `None` | Per-field display mode. See [Show Values As](#show-values-as). |
| `top_n_filters` | `list[TopNFilterFull] \| None` | `None` | Top N / Bottom N filters on row or column members. Per-parent ranking. Totals always reflect full data. See [Analytical Filters](#analytical-filters-top-n--value-filters). |
| `value_filters` | `list[ValueFilterFull] \| None` | `None` | Predicate filters that suppress members by aggregated measure. Per-parent evaluation. Totals always reflect full data. See [Analytical Filters](#analytical-filters-top-n--value-filters). |
| `conditional_formatting` | `list[dict] \| None` | `None` | Visual formatting rules. See [Conditional Formatting](#conditional-formatting). |
| `style` | `str \| PivotStyle \| list \| None` | `None` | Region-based table styling. Pass a preset name, a `PivotStyle` dict, or a list that composes presets + overrides. See [Styling](#styling). |
| `column_config` | `dict[str, Any] \| None` | `None` | Optional per-column display configuration, using a subset of the Streamlit [`column_config`](https://docs.streamlit.io/develop/api-reference/data/st.column_config) shape. Supported keys: `format`, `type`, `label`, `help`, `width` (`"small"` / `"medium"` / `"large"` / integer px), `pinned` (locks the field in the config UI; does not create a sticky column), `alignment` (`"left"` / `"center"` / `"right"`, unions with the `column_alignment` kwarg; explicit kwarg wins), and row-dim cell renderers via `type`: `"link"` (with optional `display_text`), `"image"`, `"checkbox"`, and `"text"` with `max_chars`. Explicit `number_format` / `dimension_format` / `column_alignment` parameters always win. See [Formats from `Styler` and `column_config`](#formats-from-styler-and-column_config). |
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

#### Filters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `filters` | `dict[str, dict[str, list[Any]]] \| None` | `None` | Initial user-facing dimension filters stored in the frontend config. Maps field name to `{"include": [...]}` or `{"exclude": [...]}`. Values are matched using **resolved-value semantics** (same as the interactive header-menu filter): for dates compare at the effective grain (e.g. `"2023 Q1"`), for null buckets use the display string (e.g. `"(empty)"`). Any filter key not already in `filter_fields` or the current `rows`/`columns` layout is automatically added to `filter_fields` so there is never a hidden active filter. Use `source_filters` for server-only filters that should not be exposed to the user. |
| `filter_fields` | `list[str] \| None` | `None` | Ordered list of dimension fields to place in the Filters zone. Fields appear as interactive chips in the FilterBar (when sections are expanded) and in the Settings panel's Filters zone. A field can be in both `rows`/`columns` and `filter_fields` simultaneously (dual-role). |
| `show_sections` | `bool \| None` | `True` | Whether the toolbar sections (Rows, Columns, Values cards and FilterBar) are expanded. `False` collapses them into a compact single-line summary that still shows active-filter count. Users can toggle interactively with the collapse/expand button. |
| `source_filters` | `dict[str, dict[str, list[Any]]] \| None` | `None` | Server-only report-level filters applied before any pivot processing. `include` takes precedence over `exclude`. `None` matches null-like values, `""` matches only literal empty strings, and no type coercion is performed. |

#### Data Control

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `null_handling` | `str \| dict[str, str] \| None` | `None` | How to treat null/NaN values. See [Null Handling](#null-handling). |
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

Sort rows or columns by label or by aggregated value. Pass a **single dict** for a single sort, or a **list of dicts** for multi-field (chained) sorting.

```python
# Single sort — sort rows by Revenue descending
row_sort = {
    "by": "value",            # "key" (alphabetical) or "value" (by measure)
    "direction": "desc",      # "asc" or "desc"
    "value_field": "Revenue", # required when by="value"
    "col_key": ["2023"],      # optional: sort within a specific column
    "dimension": "Category",  # optional: scope sort to this level and below
}

# Multi-field sort — primary by Revenue desc, secondary alphabetically asc
row_sort = [
    {"by": "value", "direction": "desc", "value_field": "Revenue"},
    {"by": "key",   "direction": "asc"},   # breaks ties
]

col_sort = {
    "by": "key",
    "direction": "asc",
}
```

**Multi-field sorting:** When a list is passed, sort configs are applied as a chained comparator — config[0] is primary, config[1] is secondary (activates only when config[0] produces a tie), and so on. This guarantees a deterministic order without relying on sort stability.

**Scoped sorting:** When `dimension` is set and subtotals are enabled, only the
targeted level and its children reorder — parent groups maintain their existing
order.  For example, with `rows=["Region", "Category", "Product"]` and
`dimension="Category"`, Region groups stay in their default (ascending by
subtotal) order while Categories within each Region sort descending.  Omit
`dimension` for a global sort that applies to all levels.

Users can also sort interactively via the column header menu (click the **&#8942;** icon).
When sorting from a specific dimension header, `dimension` is set automatically.

### Show Values As

Display measures as percentages, running totals, ranks, or other transformations instead of raw numbers.

#### Percentage and comparison modes

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

#### Analytical modes (0.5.0+)

| Mode | Value | Description |
|------|-------|-------------|
| Running Total | `"running_total"` | Cumulative sum along the row axis; resets at each distinct parent group |
| % Running Total | `"pct_running_total"` | Running total ÷ parent-group total for the same column (grand total for single-level pivots) |
| Rank | `"rank"` | Competition rank (1, 1, 3) per column, per parent group — matches Excel `RANK.EQ` |
| % of Parent | `"pct_of_parent"` | Cell ÷ immediate parent subtotal; for top-level rows, denominator is the column total |
| Index | `"index"` | `(cell / grand_total) / ((row_total / grand_total) × (col_total / grand_total))` — Excel INDEX |

**Totals / subtotals behaviour**: `running_total`, `pct_running_total`, and `rank` always display
the raw aggregate on total and subtotal rows — a running total at a subtotal equals the subtotal
itself, and 100% for pct_running_total would be misleading. `pct_of_parent` at a subtotal row
shows the subtotal relative to its own parent. `index` is always `null` for total rows.

**Null denominators**: all modes return the `empty_cell_value` when the denominator is null or zero.

**Export**: transformed values (not raw) are exported for all modes, matching display.

```python
# % of Grand Total (classic)
st_pivot_table(
    df,
    key="show_values_as_example",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    show_values_as={"Revenue": "pct_of_total"},
)

# Running total — accumulates per Region group, resets at each region
st_pivot_table(
    df,
    key="running_total_example",
    rows=["Region", "Product"],
    columns=["Quarter"],
    values=["Revenue"],
    show_values_as={"Revenue": "running_total"},
)

# Competition rank — largest revenue = rank 1 per column, per region
st_pivot_table(
    df,
    key="rank_example",
    rows=["Region", "Product"],
    columns=["Quarter"],
    values=["Revenue"],
    show_values_as={"Revenue": "rank"},
)
```

Users can also change this interactively via the value header menu (**&#8942;** icon on a value label header).
Synthetic measures are always rendered as raw derived values (`show_values_as` does not apply to them).
Period-comparison modes appear only when there is an active grouped temporal axis, whether that grouping came from auto hierarchy or an explicit `date_grains` override.
The analytical modes (`running_total`, `pct_running_total`, `rank`, `pct_of_parent`, `index`) are mutually exclusive with period-comparison modes per field.

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

Choose between two row presentation modes. Both modes display the same data — only the visual arrangement differs.

| Mode | Value | What you see |
|------|-------|-------------|
| Table | `"table"` | Classic pivot layout with one visible row-header column per row dimension. Region, Category, and Product each appear as their own column; repeated labels are merged by default (or repeated when `repeat_row_labels=True`). |
| Hierarchy | `"hierarchy"` | Compact tree layout with a **single visible row hierarchy column**. All row dimensions are collapsed into one indented column. A breadcrumb bar at the top shows the current drill path and lets you jump back to any ancestor level with a click. |

**When to use Table:** for 1–2 row dimensions; when each dimension should be a distinct visible column; or when `repeat_row_labels` behavior is needed.

**When to use Hierarchy:** for 3+ row dimensions or when horizontal space is limited. The single-column layout keeps value columns visible without side-scrolling. Subtotals are auto-enabled so every group node shows its aggregate immediately.

```python
# Default: one column per row dimension
st_pivot_table(
    df,
    key="table_mode",
    rows=["Region", "Category", "Customer"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    row_layout="table",      # default; can be omitted
    show_subtotals=True,
)

# Hierarchy: single indented tree column
# show_subtotals auto-enables when not explicitly set
st_pivot_table(
    df,
    key="hierarchy_mode",
    rows=["Region", "Category", "Customer"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    row_layout="hierarchy",
)
```

Behavior notes:

- **Auto-subtotals:** when `row_layout="hierarchy"` and `show_subtotals` is not explicitly set, subtotals are automatically enabled for all grouping levels so the tree exposes group aggregations out of the box. Pass `show_subtotals=False` or `show_subtotals=[...]` to override.
- **`repeat_row_labels` is ignored** in hierarchy mode because the row axis is a single indented column; the Settings Panel disables the toggle accordingly.
- In hierarchy mode, the top grouping level uses a subtle background tint and deeper levels use indentation plus group-boundary borders to reinforce depth.
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

**`column_config`.** A dict mapping column names to a small subset of the Streamlit [`column_config`](https://docs.streamlit.io/develop/api-reference/data/st.column_config) shape. Both plain dict literals and `st.column_config.*` typed objects are accepted. Supported keys:

- `format` — a format string. d3-style patterns (`",.2f"`, `"$,.0f"`) pass through as-is. Streamlit printf-style patterns (`"%,.2f"`) are normalized by stripping the leading `%`.
- `type` — if it resolves to `"date"`, `"datetime"`, or `"time"`, the pattern contributes to `dimension_format`; otherwise it contributes to `number_format`. Plain `type_config = {"type": ...}` nesting is also accepted.
- `label` — display name override for the field. Renames the field in row-dim headers, measure headers, chips (toolbar + settings panel), and exported header rows. The **underlying field id is unchanged** in the serialized config — sort, filter, and conditional-formatting rules still target the canonical id. Empty / whitespace-only labels fall back to the field id.
- `help` — text rendered as a native `title` tooltip on the corresponding dimension or measure header.
- `width` — either a preset (`"small"`=100px, `"medium"`=120px, `"large"`=200px) or an integer pixel value in the range `[20, 2000]`. Applies to row-dimension columns and measure columns (for the `col-single` header in single-value mode, and per-measure value-label cells in multi-value mode). Out-of-range / unparseable widths warn once per field and are skipped. **Interactive resize drags override the configured width at runtime but are not persisted to config**, so the width returns to the configured value after rerun/remount.
- `pinned` — when `True` or `"left"`, locks the field in the **config UI** (equivalent to adding it to `frozen_columns`): the field cannot be removed from its zone or reordered via drag-and-drop. This does **not** create a visually sticky column. `"right"` is currently warned and ignored.
- `alignment` — one of `"left"`, `"center"`, `"right"`. Unions with the `column_alignment` kwarg; when both set a value for the same field, the explicit `column_alignment` kwarg wins. Invalid values warn once per field and are skipped (unlike the `column_alignment` kwarg, which still raises on invalid values).
- `type` — in addition to the date/time / number role it plays for `format` resolution, a small set of `type` values produce **dimension cell renderers** that apply only to row-dimension cells. Measure cells are always numeric aggregates and ignore these types. On Total / Subtotal rows, `link`, `image`, and `checkbox` fall back to plain text because the cell value is a label rather than data; `text` with `max_chars` still truncates:
  - `"link"` — renders the row-dim value as an anchor (`href = <raw value>`). Accepts a `display_text` option (plain string, or a template containing `{}` which is substituted with the cell value, mirroring Streamlit's `LinkColumn` convention). Empty / null values, and values whose scheme isn't on the allowlist (`http:`, `https:`, `mailto:`, `tel:`, plus schemeless relative / protocol-relative URLs), fall back to plain text — hostile `javascript:` / `data:` / `file:` values never reach the DOM.
  - `"image"` — renders the row-dim value as an `<img>` (`src = <raw value>`) with `loading="lazy"` and a `max-height` guard so images don't blow out row height. Works in both `row_layout="table"` and `row_layout="hierarchy"` (the hierarchy breadcrumb applies a tighter 1em cap). Only `http:` / `https:` / schemeless URLs and `data:image/<raster-mime>` (png, jpeg, gif, webp, avif, bmp, ico) pass through; everything else — including `data:image/svg+xml` and non-image `data:` MIME types — falls back to plain text.
  - `"checkbox"` — renders truthy row-dim values as ☑ and falsy values as ☐. Accepts booleans (`True` / `False`), strings (`"true"` / `"false"` / `"yes"` / `"no"` / `"1"` / `"0"`, case-insensitive), and the numbers `0` / `1`. Unrecognized values fall back to plain text.
  - `"text"` with `max_chars` — truncates row-dim cell text to `max_chars` UTF-16 code units (matches JavaScript's native `String.length` / `slice`, which is also what Streamlit's `TextColumn(max_chars=...)` uses) with a trailing ellipsis. The full text is preserved in the cell's `title` attribute for hover inspection. Truncation applies on every row, including Total / Subtotal rows. Invalid `max_chars` values (non-positive, non-integer, or booleans) warn once per field and are skipped.

Unknown keys in dict literals warn once per `(field, key)` pair. Streamlit's internal defaults from typed `st.column_config.*` objects (`disabled`, `required`, `default`) are silently ignored. Recognized but unsupported column types (e.g. `line_chart`, `selectbox`) warn once per `(field, type)`.

```python
st_pivot_table(
    df,
    key="column_config_formats_example",
    rows=["Region"],
    columns=["Order Date"],
    values=["Revenue", "Units"],
    column_config={
        "Region": {"label": "Area", "help": "Geographic region", "width": "large", "pinned": True},
        "Revenue": {"format": "$,.0f", "label": "Rev", "width": 180, "alignment": "right"},
        "Units": {"format": ",.0f", "alignment": "center"},
        "Order Date": {"format": "YYYY-MM-DD", "type": "date"},
    },
)
```

```python
st_pivot_table(
    df,
    key="column_config_renderers_example",
    rows=["Homepage", "Poster", "Active", "Description"],
    values=["Revenue"],
    column_config={
        "Homepage": st.column_config.LinkColumn(
            "Homepage",
            display_text="Visit {}",
        ),
        "Poster": st.column_config.ImageColumn("Poster"),
        "Active": st.column_config.CheckboxColumn("Active"),
        "Description": st.column_config.TextColumn("Description", max_chars=40),
    },
)
```

**Precedence.** For format fields: `explicit number_format / dimension_format` > `column_config` > `Styler`. For alignment: `explicit column_alignment` > `column_config.alignment` > default (right-aligned measures, left-aligned dimensions). The lower-priority sources only fill gaps — any field already present in an explicit format or alignment dict keeps the caller-supplied value. `label`, `help`, and `width` are `column_config`-driven only (no legacy kwargs). `pinned` **unions** with `frozen_columns` / `hidden_from_drag_drop`.

### Analytical Filters: Top N / Value Filters

_(0.5.0)_ Two new display-only filter types let you focus on the most (or least) relevant dimension members without re-aggregating totals.

**Key semantics:**

- Filtering is **per-parent**: for a two-level hierarchy `[Region, Product]`, "Top 3 Products" keeps the 3 highest-revenue products **within each Region** independently.
- Ranking always uses the **grand-total column context** (sum across all column values). Contextual column scoping (`col_key`) is deferred to 0.6.0.
- Grand totals and subtotals are **not recalculated** — they always reflect the full unfiltered dataset. This is a deliberate product choice (simpler, avoids re-aggregation cost). The UI surfaces a note: *"Totals include all data, not just visible members."*
- Top N and value filters run **after** existing dimension member (include/exclude) filters.
- Members with a **null** aggregated measure are ranked last / excluded before the top-N cutoff and treated as failing all value filter predicates.

**`top_n_filters`**

```python
top_n_filters: list[TopNFilterFull] | None = None
```

Each filter dict may contain:

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `field` | `str` | ✓ | Dimension field to filter |
| `n` | `int` | ✓ | How many members to keep per parent group (must be ≥ 1) |
| `direction` | `"top"` \| `"bottom"` | ✓ | Keep highest or lowest N members |
| `by` | `str` | ✓ | Measure to rank by |
| `axis` | `"rows"` \| `"columns"` | — | Which axis to filter (default `"rows"`) |

```python
# Top 3 products by revenue within each region
st_pivot_table(
    df, key="top3",
    rows=["Region", "Product"],
    columns=["Quarter"],
    values=["Revenue"],
    top_n_filters=[
        {"field": "Product", "n": 3, "by": "Revenue", "direction": "top"}
    ],
)
```

**`value_filters`**

```python
value_filters: list[ValueFilterFull] | None = None
```

Each filter dict may contain:

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `field` | `str` | ✓ | Dimension field whose members are suppressed when predicate fails |
| `by` | `str` | ✓ | Measure to evaluate the predicate against |
| `operator` | `str` | ✓ | `"gt"`, `"gte"`, `"lt"`, `"lte"`, `"eq"`, `"neq"`, `"between"` |
| `value` | `float` | ✓ | Threshold (lower bound for `"between"`) |
| `value2` | `float` | — | Upper bound — required when `operator="between"` |
| `axis` | `"rows"` \| `"columns"` | — | Which axis to filter (default `"rows"`) |

```python
# Products with annual revenue > $1M within each region
st_pivot_table(
    df, key="gt1m",
    rows=["Region", "Product"],
    columns=["Quarter"],
    values=["Revenue"],
    value_filters=[
        {"field": "Product", "by": "Revenue", "operator": "gt", "value": 1_000_000}
    ],
)

# Revenue between $200K and $500K
st_pivot_table(
    df, key="between",
    rows=["Region", "Product"],
    columns=["Quarter"],
    values=["Revenue"],
    value_filters=[
        {
            "field": "Product", "by": "Revenue",
            "operator": "between", "value": 200_000, "value2": 500_000,
        }
    ],
)
```

**Interactive**: these filters are also accessible via the column header ⋮ menu — look for the **"Top / Bottom N"** and **"Filter by value"** sections on any row or column dimension header. Active filters are summarised in the Settings Panel (read-only; edit them from the header menu).

---

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

### Styling

> **Works with Streamlit theming.** `style=` is a per-table layer over the app's `[theme]` configuration. The pivot tracks the Streamlit theme automatically — including light/dark mode and custom fonts. Use `var(--st-...)` tokens as color string values for theme-aware custom colors.

```python
# Recommended: theme-aware color using a Streamlit token
style={"column_header": {"background_color": "var(--st-primary-color)"}}

# Not recommended: raw hex breaks dark mode compatibility
style={"column_header": {"background_color": "#1a73e8"}}
```

#### Presets

Six built-in presets are available as string shorthand. All preset colors use `color-mix(… var(--st-...))` — no raw hex — so they adapt to dark mode and custom themes automatically.

| Preset | Description |
|--------|-------------|
| `"default"` | No overrides; tracks Streamlit theme defaults. |
| `"striped"` | Alternating-row banding using the secondary background color. |
| `"minimal"` | Flat layout: no borders, no hover, no stripes. Good for static output. |
| `"compact"` | Tight padding + reduced virtualized row height. |
| `"comfortable"` | Generous padding — easier to scan on large monitors or in reports. |
| `"contrast"` | Bold emphasized headers, bold totals, subtle stripe. Power BI "Contrast" parity. |

```python
from streamlit_pivot import st_pivot_table, PivotStyle, RegionStyle, PIVOT_STYLE_PRESETS

st_pivot_table(df, key="p", rows=["Region"], values=["Revenue"], style="striped")
```

#### `PivotStyle` and `RegionStyle`

```python
class RegionStyle(TypedDict, total=False):
    background_color: str
    text_color: str
    font_weight: str   # "normal" | "bold"

class PivotStyle(TypedDict, total=False):
    # Table-wide
    density: str           # "compact" | "default" | "comfortable"
    font_size: str         # e.g. "13px", "0.875rem"
    background_color: str  # cascades to all regions
    text_color: str        # cascades to all regions
    stripe_color: str | None   # None = disable striping
    row_hover_color: str | None  # None = disable hover
    borders: str           # "all" | "outer" | "rows" | "columns" | "none"
    border_color: str

    # Region overrides
    column_header: RegionStyle
    row_header: RegionStyle
    data_cell: RegionStyle
    row_total: RegionStyle      # grand totals per row (rightmost column)
    column_total: RegionStyle   # grand totals per column (bottom row)
    subtotal: RegionStyle

    # Per-measure overrides for non-total data cells only
    data_cell_by_measure: dict[str, RegionStyle]
```

#### Cascade rules

Precedence from highest to lowest:

1. **Conditional formatting** (per-cell inline via CF rule)
2. **`data_cell_by_measure[field]`** (per-cell inline; non-total data cells only)
3. **Region overrides** — `column_header`, `row_header`, `data_cell`, `row_total`, `column_total`, `subtotal`
4. **Table-wide cascade** — `background_color` / `text_color` cascade to all regions
5. **Streamlit theme** (`--st-*` fallbacks)

#### Borders modes

| `borders` value | Appearance |
|-----------------|------------|
| `"all"` (default) | Full grid — all horizontal and vertical lines |
| `"outer"` | Outer frame only — no internal gridlines |
| `"rows"` | Horizontal rules only — financial / editorial style |
| `"columns"` | Vertical lines only |
| `"none"` | Completely flat — no borders at all |

#### Naming note: `row_total` vs. `column_total`

API names follow user intent, not CSS class names:

| API field | What it styles | CSS class |
|-----------|---------------|-----------|
| `row_total` | Grand total *of* each row — rightmost column | `.totalsCol` |
| `column_total` | Grand total *of* each column — bottom row | `.totalsRow` |

#### Per-measure styling

`data_cell_by_measure` applies background/text/weight overrides to non-total data cells of a specific measure. Total cells are **not** affected — they use `row_total` / `column_total` region overrides instead (matching Power BI's Values-only scoping).

```python
st_pivot_table(
    df, key="per_measure",
    values=["Revenue", "Profit"],
    style=PivotStyle(
        data_cell_by_measure={
            "Revenue": RegionStyle(
                background_color="color-mix(in srgb, var(--st-primary-color) 8%, transparent)"
            ),
            "Profit": RegionStyle(
                text_color="color-mix(in srgb, var(--st-text-color) 65%, transparent)",
                font_weight="bold",
            ),
        }
    ),
)
```

#### Composition

Pass a **list** to compose presets and custom overrides. Items are merged left-to-right; later items win:

```python
_tint = "color-mix(in srgb, var(--st-primary-color) 15%, var(--st-background-color))"
st_pivot_table(
    df, key="composed",
    style=[
        "compact",
        "contrast",
        PivotStyle(
            row_total=RegionStyle(background_color=_tint),
            column_total=RegionStyle(background_color=_tint),
        ),
    ],
)
```

#### Deferred from v1

The following are not yet supported and are planned for a future release:

- Font family (`--st-font` from Streamlit theme is always used)
- Per-region font size (only table-wide `font_size` is supported)
- Italic / `font_style`
- Per-level subtotal styling
- Hierarchy subtotal styling via the `subtotal` region
- Per-measure styling on totals (`row_total_by_measure`, `column_total_by_measure`)
- Thin / thick border widths
- CSS escape hatch (`class_name`)

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
| Auto | `"auto"` | Client-side unless any of three independent thresholds are exceeded (default) |
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

**Auto thresholds (0.5.0+):** In `"auto"` mode, the component evaluates three independent conditions in order and switches to server-side pre-aggregation when any one is met:

1. **Row ceiling** — dataset has ≥ 500,000 rows (regardless of cardinality or payload size)
2. **Payload size** — estimated Arrow payload is ≥ 50 MB (using shallow `memory_usage`; O(columns) cost)
3. **Pivot shape** — estimated visible cells > 5,000 or pivot groups > 10,000 after running `nunique()` on dimension columns

Each check produces a machine-readable reason code (`auto:row_ceiling`, `auto:payload`, `auto:pivot_shape`, or `auto:client_only`) that appears in the `executionReason` component metric for debugging. Forced modes return `forced:client_only` or `forced:threshold_hybrid`; incompatible configs (e.g. a field used as both a dimension and a value) return `incompatible:dim_value_overlap`.

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

The example app (`streamlit_app.py`) contains more than 20 sections covering the major features and usage patterns with interactive examples and inline documentation.

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

Licensed under the [Apache License, Version 2.0](LICENSE).
