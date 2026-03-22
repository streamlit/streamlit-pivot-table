---
name: streamlit-pivot-table
description: "Build Streamlit apps with st_pivot_table — a pivot table component supporting multi-dimensional pivoting, sorting, filtering, subtotals, conditional formatting, data export, drill-down, and synthetic measures. Use when: user wants a pivot table in Streamlit, mentions streamlit_pivot_table or st_pivot_table, needs interactive data summarization, wants to deploy a pivot table to Snowflake SiS on SPCS, or has a .whl for a custom component. Triggers: pivot table, streamlit_pivot_table, st_pivot_table, pivot, crosstab, data summarization, whl, wheel, custom component, SiS component, SPCS."
---

# Streamlit Pivot Table Component

`streamlit_pivot_table` provides `st_pivot_table` — a pivot table component built with Streamlit Components V2, React, and TypeScript. Supports multi-dimensional pivoting, interactive sorting/filtering, subtotals with collapse/expand, conditional formatting, data export, drill-down detail panels, and synthetic measures.

**Requirements:** Python >= 3.10, Streamlit >= 1.51

## When to Use

- User wants to add a pivot table to a Streamlit app
- User mentions `streamlit_pivot_table` or `st_pivot_table`
- User needs interactive data summarization with row/column dimensions and aggregated measures
- User wants to deploy a pivot table component to Snowflake SiS on SPCS (see [Deploying to SiS on SPCS](#deploying-to-sis-on-spcs))

## Installation

```sh
pip install streamlit-pivot-table
```

## Quick Start

```python
import pandas as pd
import streamlit as st
from streamlit_pivot_table import st_pivot_table

df = pd.read_csv("sales.csv")

result = st_pivot_table(
    df,
    key="my_pivot",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    aggregation="sum",
    show_totals=True,
)
```

The `data` parameter accepts the same input types as `st.dataframe` — Pandas/Polars DataFrames, NumPy arrays, dicts, lists of records, PyArrow Tables, etc. Data is automatically converted to a Pandas DataFrame internally.

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
| `synthetic_measures` | `list[dict] \| None` | `None` | Derived measures computed from source-field sums (e.g., ratio of sums). See [Synthetic Measures](#synthetic-measures). |
| `aggregation` | `str` | `"sum"` | Aggregation function. See [Aggregation Functions](#aggregation-functions). |
| `interactive` | `bool` | `True` | Enable toolbar controls for reconfiguring the pivot. |

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
| `show_values_as` | `dict[str, str] \| None` | `None` | Per-field display mode. See [Show Values As](#show-values-as). Does not apply to synthetic measures. |
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
| `on_cell_click` | `Callable[[], None] \| None` | `None` | Called when a user clicks a data cell. Read the payload from `st.session_state[key]`. See [Cell Click Payload](#cell-click-payload). |
| `on_config_change` | `Callable[[], None] \| None` | `None` | Called when the user changes the pivot config via the toolbar. |
| `enable_drilldown` | `bool` | `True` | Show an inline drill-down panel with source records when a cell is clicked. |
| `locked` | `bool` | `False` | Freeze toolbar config controls (rows/columns/values/aggregation/settings). The entire utility menu is hidden. Sorting and filtering via header menus remain available. |
| `export_filename` | `str \| None` | `None` | Base filename (without extension) for exported files. Date and extension are appended automatically. Defaults to `"pivot-table"`. |

#### Data Control

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `null_handling` | `str \| dict[str, str] \| None` | `None` | How to treat null/NaN values. See [Null Handling](#null-handling). |
| `hidden_attributes` | `list[str] \| None` | `None` | Column names to hide entirely from the UI. |
| `hidden_from_aggregators` | `list[str] \| None` | `None` | Column names hidden from the values/aggregators dropdown only. |
| `frozen_columns` | `list[str] \| None` | `None` | Column names that cannot be removed from their toolbar zone. |
| `sorters` | `dict[str, list[str]] \| None` | `None` | Custom sort orderings per dimension. Maps column name to ordered list of values. |
| `menu_limit` | `int \| None` | `None` | Max items in the header-menu filter checklist. Defaults to 50. |

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

`"sum_over_sum"` is not supported as a table-wide aggregation. Use `synthetic_measures` for ratio-of-sums behavior.

### Synthetic Measures

Derived metrics computed from source-field sums at each cell/total context, rendered alongside regular value fields.

Supported operations:

- `sum_over_sum` — `sum(numerator) / sum(denominator)` (returns empty cell value when denominator is 0)
- `difference` — `sum(numerator) - sum(denominator)`

Each synthetic measure dict has:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `str` | yes | Unique identifier |
| `label` | `str` | yes | Display name |
| `operation` | `str` | yes | `"sum_over_sum"` or `"difference"` |
| `numerator` | `str` | yes | Source field name |
| `denominator` | `str` | yes | Source field name |
| `format` | `str` | no | Number format pattern for this measure only (e.g., `".1%"`, `"$,.0f"`) |

```python
st_pivot_table(
    df,
    key="synth_demo",
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
            "format": "$,.2f",
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

In the interactive toolbar, synthetic measures can be added via a builder UI. The **Format** input includes presets (Percent, Currency, Number) and validates custom patterns before save.

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
    key="show_as_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    show_values_as={"Revenue": "pct_of_total"},
)
```

Users can also change this interactively via the value header menu (**&#8942;** icon on a value label header). Synthetic measures are always rendered as raw derived values (`show_values_as` does not apply to them).

### Number Format Patterns

Patterns follow a lightweight d3-style syntax.

| Pattern | Example Output | Description |
|---------|---------------|-------------|
| `$,.0f` | $12,345 | US currency, no decimals |
| `,.2f` | 12,345.67 | Comma-grouped, 2 decimals |
| `.1%` | 34.5% | Percentage, 1 decimal |
| `€,.2f` | €12,345.67 | Euro via symbol |
| `£,.0f` | £12,345 | GBP |

A single string applies to all value fields. A dict maps field names to patterns. Use `"__all__"` as a dict key for a default pattern.

```python
# Per-field formatting
st_pivot_table(
    df,
    key="fmt_demo",
    values=["Revenue", "Profit"],
    number_format={"Revenue": "$,.0f", "Profit": ",.2f"},
)

# Global format for all fields
st_pivot_table(df, key="fmt_global", values=["Revenue"], number_format="$,.0f")
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
    key="cond_fmt_demo",
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
st_pivot_table(df, key="null_demo", null_handling="zero")

# Per-field modes
st_pivot_table(df, key="null_per_field", null_handling={"Region": "separate", "Revenue": "zero"})
```

### Subtotals and Row Grouping

With 2+ row dimensions, enable subtotals to see group-level aggregations with collapsible groups.

```python
st_pivot_table(
    df,
    key="subtotals_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    show_subtotals=True,
    repeat_row_labels=False,
)
```

- Each group shows a subtotal row with a collapse/expand toggle (+/−).
- Collapsed groups hide child rows but keep the subtotal visible.
- Expand All / Collapse All controls are available in the Settings popover (gear icon in the toolbar).
- Pass a list of dimension names to `show_subtotals` to enable subtotals only for specific levels (e.g., `show_subtotals=["Region"]`).

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

### Column Group Collapse/Expand

With 2+ column dimensions, column groups can be collapsed into subtotal columns.

```python
st_pivot_table(
    df,
    key="col_groups_demo",
    rows=["Region"],
    columns=["Year", "Category"],
    values=["Revenue"],
)
```

Hover over a parent column header to reveal the collapse toggle.

### Data Export

Export the pivot table as CSV, TSV, or copy to clipboard. Available via the toolbar utility menu (download icon) when `interactive=True`.

- **Format**: CSV, TSV, or Clipboard (tab-separated for pasting into spreadsheets)
- **Content**: Formatted (display values with currency, percentages, etc.) or Raw (unformatted numbers)
- **Filename**: Customizable via `export_filename`. The date (`YYYY-MM-DD`) and file extension are appended automatically. Defaults to `"pivot-table"` (e.g. `pivot-table_2026-03-09.csv`).

Export always outputs the full expanded table regardless of any collapsed row/column groups.

### Drill-Down Detail Panel

Click any data or total cell to open an inline panel below the table showing the source records that contributed to that cell's aggregated value.

```python
result = st_pivot_table(
    df,
    key="drilldown_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    enable_drilldown=True,
    on_cell_click=lambda: None,
)
```

- The panel displays up to 500 matching records.
- Close with the **×** button or by pressing **Escape**.
- Set `enable_drilldown=False` to disable (the `on_cell_click` callback still fires).

### Locked Mode

Freeze toolbar config controls so end-users cannot change rows, columns, values, aggregation, or display settings. The entire utility menu (reset, swap, config import/export, data export, settings) is hidden. Sorting and filtering via header menus remain available.

```python
st_pivot_table(
    df,
    key="locked_demo",
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
| **Export Data** | Open the export popover (CSV / TSV / Clipboard). Use `export_filename` to customize the download filename. |
| **Settings** (gear icon) | Opens a popover with display toggles: Row Totals, Column Totals, Subtotals, Repeat Labels, Sticky Headers, and Expand/Collapse All group controls |

In **locked mode**, Reset, Swap, config import/export, and data export are hidden. The Settings gear remains visible, its toggles are disabled, and sorting/filtering via header menus remain available.

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

The returned `config` dict contains the full current configuration including any changes the user made via the toolbar. Use this to persist user customizations or synchronize multiple components.

---

## Deploying to SiS on SPCS

This section covers deploying the pivot table component (as a `.whl` file) into a Streamlit in Snowflake (SiS) app running on SPCS (Snowpark Container Services). Custom `.whl` components are only supported when the SiS app runs on SPCS, not on the legacy Warehouse runtime.

### Prerequisites

- **Snow CLI** installed and configured with a Snowflake connection (`snow connection test` to verify). Install: `pip install snowflake-cli` or `brew install snowflake-cli`.
- **SiS on SPCS** enabled for the account — the app must use the SPCS runtime. Custom `.whl` components, `pyproject.toml`, and `requirements.txt` are only supported on SPCS.
- `PYPI_ACCESS_INTEGRATION` available in the Snowflake account.
- A compute pool available for running the SPCS-backed SiS app.

### Workflow

#### Step 1: Gather Information

**Ask the user:**

```
To set up your SiS on SPCS app with the pivot table component, I need:
1. The path to your .whl file (e.g., ~/Downloads/streamlit_pivot_table-0.1.0-py3-none-any.whl)
2. Do you have an existing SiS project directory with snowflake.yml, or should I create one from scratch?
3. What Snowflake table(s) will the app query?
4. What compute pool should the app run on? (e.g., MY_COMPUTE_POOL)
```

**STOP**: Do NOT proceed until the user provides the `.whl` file path and confirms whether they need a new project.

**After the user responds**, derive these values from the `.whl` filename and use them in ALL subsequent steps:

- **`WHL_FILENAME`**: The `.whl` file name (e.g., `streamlit_pivot_table-0.1.0-py3-none-any.whl`)
- **`PACKAGE_NAME`**: The portion before the first version segment, with hyphens replaced by underscores (e.g., `streamlit_pivot_table`)
- **`TABLE_NAMES`**: The Snowflake table(s) the user wants to query
- **`COMPUTE_POOL`**: The SPCS compute pool name for the app runtime

#### Step 2: Scaffold the SiS Project (if needed)

**Skip this step** if the user already has a project directory with `snowflake.yml` and `streamlit_app.py`.

```bash
snow init --template streamlit-python <app-name>
cd <app-name>
```

**Minimal `snowflake.yml`** (the `runtime.compute_pool` field is what makes this a SiS on SPCS app):

```yaml
definition_version: "2"
entities:
  my_streamlit_app:
    type: streamlit
    identifier:
      name: my_streamlit_app
    title: "My Streamlit App"
    query_warehouse: <WAREHOUSE_NAME>
    main_file: streamlit_app.py
    artifacts:
      - streamlit_app.py
      - pyproject.toml
      - requirements.txt
    external_access_integrations:
      - PYPI_ACCESS_INTEGRATION
    runtime:
      compute_pool: <COMPUTE_POOL_NAME>
```

**Minimal `pyproject.toml`:**

```toml
[project]
name = "my-sis-app"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = []
```

Create a blank `streamlit_app.py` and empty `requirements.txt` as well.

#### Step 3: Add the .whl and Dependencies

1. Copy the `.whl` file into the project directory (same level as `streamlit_app.py`).

2. Add `WHL_FILENAME` to the `artifacts` list in `snowflake.yml`:

```yaml
artifacts:
  - streamlit_app.py
  - pyproject.toml
  - requirements.txt
  - <WHL_FILENAME>
```

3. Ensure `external_access_integrations` includes `PYPI_ACCESS_INTEGRATION` in `snowflake.yml`.

4. Create/update `requirements.txt` with a local path reference:

```
./<WHL_FILENAME>
```

5. Add the component as a file dependency in `pyproject.toml`:

```toml
dependencies = [
    "<PACKAGE_NAME> @ file:./<WHL_FILENAME>",
]
```

#### Step 4: Inspect the Module

**CRITICAL**: Do NOT guess the function name. Install the `.whl` locally and inspect it:

```bash
pip install /full/path/to/<WHL_FILENAME> --quiet --no-deps
python -c "import <PACKAGE_NAME>; print([x for x in dir(<PACKAGE_NAME>) if not x.startswith('_')])"
```

For the pivot table component this outputs `['st_pivot_table']`. Optionally inspect the signature:

```bash
python -c "import inspect; from <PACKAGE_NAME> import <COMPONENT_FUNCTION>; print(inspect.signature(<COMPONENT_FUNCTION>))"
```

#### Step 5: Write streamlit_app.py

**IMPORTANT**: Always include a `key` parameter in every `st_pivot_table()` call.

```python
import streamlit as st
from snowflake.snowpark.context import get_active_session
from streamlit_pivot_table import st_pivot_table

session = get_active_session()
df = session.sql("SELECT * FROM <TABLE_NAME> LIMIT 1000").to_pandas()

st.title("My Pivot Table App")

result = st_pivot_table(
    df,
    key="my_pivot",
    rows=["REGION"],
    columns=["YEAR"],
    values=["REVENUE"],
    aggregation="sum",
)
```

Adapt `rows`, `columns`, and `values` to match the actual column names. If unsure, omit them — the component auto-detects dimensions and measures.

See the [API Reference](#api-reference) and [Feature Guide](#feature-guide) sections above for the full parameter documentation.

#### Step 6: Review and Deploy

**STOP**: Present this checklist to the user and get confirmation before deploying.

```
Before I deploy, here is what I have set up:

1. snowflake.yml — PYPI_ACCESS_INTEGRATION enabled, .whl listed in artifacts, compute_pool set
2. requirements.txt — references the .whl with ./ prefix
3. pyproject.toml — dependency entry for the component
4. streamlit_app.py — imports st_pivot_table and queries your table

Shall I proceed with deployment?
```

**Do NOT deploy until the user confirms.**

```bash
snow streamlit deploy --replace
```

#### Step 7: Verify Deployment

```bash
snow streamlit describe <entity_name>
```

If the app fails to load, check logs with `snow streamlit log <entity_name>`. Common issues:

- Import name mismatch
- Missing table permissions
- Column names in `rows`/`columns`/`values` not matching the actual table schema

### Stopping Points

- **Step 1**: Do NOT proceed until the user provides the `.whl` file path and confirms new vs. existing project.
- **Step 6**: Do NOT deploy until the user reviews and confirms the checklist.

### Troubleshooting

**Component not found after deploy?**
- Verify the `.whl` is listed in `artifacts` in `snowflake.yml`
- Check that `requirements.txt` uses `./` prefix for the local path

**Network/pip errors?**
- Ensure `PYPI_ACCESS_INTEGRATION` is in `external_access_integrations`
- Verify the integration exists: `snow sql -q "SHOW EXTERNAL ACCESS INTEGRATIONS"`

**Import errors?**
- Confirm the package name in `pyproject.toml` matches the module name in the `.whl`
- The `@ file:./` syntax requires the `.whl` to be in the same directory
