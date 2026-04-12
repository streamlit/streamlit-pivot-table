# streamlit-pivot

Pivot table component for [Streamlit](https://streamlit.io). Built with Streamlit Components V2, React, and TypeScript.

Supports multi-dimensional pivoting, interactive sorting and filtering, subtotals with collapse/expand, conditional formatting, data export, drill-down detail panels, and more.

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

The `data` parameter accepts the same input types as `st.dataframe` — Pandas DataFrames, Polars DataFrames, NumPy arrays, dicts, lists of records, and more. Data is automatically converted to a Pandas DataFrame internally.

If `rows`, `columns`, and `values` are all omitted, the component auto-detects dimensions (categorical + low-cardinality numeric columns) and measures (high-cardinality numeric columns) from the data.

---

## API Reference

### `st_pivot_table(data, *, ...)`

Creates a pivot table component. All parameters except `data` are keyword-only.

Returns a `PivotTableResult` dict containing the current `config` state.

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
| `number_format` | `str \| dict[str, str] \| None` | `None` | Number format pattern(s). See [Number Format Patterns](#number-format-patterns). |
| `column_alignment` | `dict[str, str] \| None` | `None` | Per-field text alignment: `"left"`, `"center"`, or `"right"`. |
| `show_values_as` | `dict[str, str] \| None` | `None` | Per-field display mode. See [Show Values As](#show-values-as). |
| `conditional_formatting` | `list[dict] \| None` | `None` | Visual formatting rules. See [Conditional Formatting](#conditional-formatting). |
| `empty_cell_value` | `str` | `"-"` | Display string for cells with no data. |

#### Layout

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `height` | `int \| None` | `None` | Fixed height in pixels. `None` means auto-size (capped by `max_height`). |
| `max_height` | `int` | `500` | Maximum auto-size height in pixels. Table becomes scrollable when content exceeds this. Ignored when `height` is set. |
| `sticky_headers` | `bool` | `True` | Column headers stick to the top of the scroll container. |

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
| `frozen_columns` | `list[str] \| None` | `None` | Column names that cannot be removed from their toolbar zone and cannot be reordered or moved via drag-and-drop. |
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

In the interactive toolbar, aggregation is edited inside the `Values` dropdown, and raw measure chips display the selected aggregation inline in a compact name-first format such as `Revenue (Sum)`.

### Synthetic Measures (V1)

Synthetic measures let you render derived metrics alongside regular value fields. They are computed from source-field sums at each cell/total context.

Supported operations:

- `sum_over_sum` -> `sum(numerator) / sum(denominator)` (returns empty cell value when denominator is 0)
- `difference` -> `sum(numerator) - sum(denominator)`

Optional synthetic-measure fields:

- `format` -> number format pattern applied only to that synthetic measure (for example `.1%`, `$,.0f`, or `,.2f`)

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

Per-measure aggregation applies only to raw entries in `values`. Synthetic measures keep their current sum-based formula semantics, so `sum_over_sum` still means `sum(numerator) / sum(denominator)` even when nearby raw measures use `avg`, `count`, or other aggregations.

`aggregation="sum_over_sum"` is no longer supported as a table-wide aggregation mode. Use `synthetic_measures` for ratio-of-sums behavior.
In the interactive builder, the **Format** input includes presets (Percent, Currency, Number) and validates custom patterns before save.

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
    "include_totals": False,      # optional, default False
}
```

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
- Expand All / Collapse All controls are available in the Settings popover (gear icon in the toolbar).

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

Hover over a parent column header to reveal the collapse toggle.

### Data Export

Export the pivot table as Excel, CSV, TSV, or copy to clipboard. Available via the toolbar utility menu (download icon) whenever the interactive toolbar is shown, including locked viewer mode.

- **Format**: Excel (.xlsx), CSV, TSV, or Clipboard (tab-separated for pasting into spreadsheets)
- **Content**: Formatted (display values with currency, percentages, etc.) or Raw (unformatted numbers)
- **Filename**: Customizable via `export_filename`. The date (`YYYY-MM-DD`) and file extension are appended automatically. Defaults to `"pivot-table"` (e.g. `pivot-table_2026-03-09.xlsx`).

Excel export produces a professionally styled workbook with merged column headers, bold totals/subtotals, number formatting, banded rows, frozen panes (headers stay visible when scrolling), and row dimension merging that matches the rendered table layout. Sort order, active filters, and show-values-as percentages are all preserved. Conditional formatting rules (color scales, data bars, and threshold highlights) are translated to native Excel conditional formatting, so the exported file renders them natively without macros.

Export always outputs the full expanded table regardless of any collapsed row/column groups.

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

- The panel displays up to 500 matching records.
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
- Synthetic measures are not supported in hybrid mode (falls back to client-side).

### Locked Mode

Use `locked=True` for a viewer-mode experience with exploration enabled. Toolbar config controls stay locked so end-users cannot change rows, columns, values, per-measure aggregation, or settings toggles. Reset, Swap, and config import/export are hidden, while data export remains available and the Settings gear stays visible for read-only display status plus Expand/Collapse All group controls. Header-menu sorting, filtering, and `Show Values As` remain available, and drill-down still works.

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

### Toolbar Utility Menu

When `interactive=True`, hovering over the top-right of the toolbar reveals utility actions:

| Action | Description |
|--------|-------------|
| **Reset** | Resets to the original Python-supplied config (only visible when config has changed) |
| **Swap** | Transposes row and column dimensions |
| **Copy Config** | Copies the current config as JSON to clipboard |
| **Import Config** | Paste a JSON config to apply |
| **Export Data** | Open the export popover (Excel / CSV / TSV / Clipboard). Use `export_filename` to customize the download filename. |
| **Fullscreen** (expand icon) | Toggles fullscreen mode — the table fills the entire viewport. Press Escape or click the collapse icon to exit. |
| **Settings** (gear icon) | Opens a popover with display toggles: Row Totals, Column Totals, Subtotals, Repeat Labels, Sticky Headers, and Expand/Collapse All group controls |

In **locked mode**, Reset, Swap, and config import/export are hidden. `Export Data` remains available as a viewer action. The Settings gear remains visible, its popover shows read-only display status plus group expand/collapse actions, and header-menu sorting, filtering, and `Show Values As` stay enabled.

### Drag-and-Drop Field Configuration

When `interactive=True`, each chip in the Rows, Columns, and Values toolbar zones has a **grip-dots drag handle** on its left side. Drag chips to:

- **Reorder within a zone** — change the grouping hierarchy (e.g., swap which dimension is the outer vs. inner group in Rows).
- **Move between zones** — drag a chip from Rows to Columns (or vice versa), or between Rows/Columns and Values. The Values zone only accepts numeric columns; non-numeric drops are silently rejected.

**Visual feedback:**
- A floating overlay chip follows the cursor during drag.
- The source chip stays in place at reduced opacity (ghosted).
- When dragging over a valid target zone, the zone highlights with a dashed border and subtle tint.
- Within-zone reorders show smooth shift animations as chips make room.

**Constraints:**
- `frozen_columns` render without drag handles and cannot be dragged.
- Synthetic measures cannot be dragged to other zones.
- When `locked=True`, drag-and-drop is fully disabled.
- A 5 px activation distance distinguishes clicks from drags, so remove buttons and dropdown toggles work normally.

**Config cleanup on move:** When fields move between zones, related config properties (aggregation, sort, collapsed groups, subtotals, conditional formatting, show-values-as, per-measure totals) are automatically synchronized.

No Python API parameter is required — drag-and-drop is a purely frontend interaction.

### Column Resize

Drag the **right edge of any column header** to resize that column. A thin resize handle appears on hover (cursor changes to `col-resize`). Minimum column width is 40 px.

- Works in both virtualized and non-virtualized rendering modes.
- Each column's width is tracked independently.
- Widths reset when the pivot configuration changes (new rows, columns, or values).
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

The returned `config` dict contains the current supported configuration state, including interactive changes such as rows, columns, values, aggregation, totals, sorting, filtering, and display options. Use this to persist user customizations or synchronize multiple components.

---

## Keyboard Accessibility

The component follows WAI-ARIA patterns for all interactive elements:

- **Toolbar**: Arrow keys navigate between toolbar buttons (roving tabindex). Space/Enter activates.
- **Drag-and-drop**: Space to pick up a chip, arrow keys to move, Space to drop at the new position. Screen reader announcements provided by dnd-kit.
- **Header menus**: Escape closes. Arrow keys navigate options. Space/Enter selects.
- **Export/Import popovers**: Focus is automatically placed on the first interactive element when opened. Tab/Shift+Tab moves between controls; tabbing out closes the popover.
- **Settings popover** (gear icon): Focus moves to first checkbox on open. Escape closes. Tab navigates between toggles.
- **Radio groups** (export format/content): Arrow keys move focus between options. Space/Enter selects.
- **Drill-down panel**: Focus moves to the close button on open. Escape closes.
- **Data cells**: Focusable via Tab. Space/Enter triggers cell click.

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

The example app (`streamlit_app.py`) contains 17 sections covering the major features and usage patterns with interactive examples and inline documentation.

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

1. Build the frontend assets:

   ```sh
   cd streamlit_pivot/frontend
   npm install
   npm run build
   ```

2. Build the Python wheel:

   ```sh
   uv build
   ```

Output: `dist/streamlit_pivot-0.1.0-py3-none-any.whl`

### Requirements

- Python >= 3.10
- Node.js >= 24 (LTS)
- Streamlit >= 1.51

## License

Apache 2.0
