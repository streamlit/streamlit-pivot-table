---
name: streamlit-pivot
description: "Build Streamlit apps with st_pivot_table — a BI-focused pivot table component supporting multi-dimensional pivoting, subtotals, conditional formatting, Excel/CSV/TSV export, drill-down, drag-and-drop, date hierarchies with period-over-period comparisons, synthetic measures (including formula measures), and server-side pre-aggregation for large datasets. Use when: user wants a pivot table in Streamlit, mentions streamlit_pivot / st_pivot_table, needs interactive data summarization, or wants to deploy to Streamlit Community Cloud or Streamlit in Snowflake (SiS) on SPCS (installed from PyPI via PYPI_ACCESS_INTEGRATION). Triggers: pivot table, streamlit_pivot, st_pivot_table, pivot, crosstab, data summarization, SiS, SiS on SPCS, Snowflake streamlit, PYPI_ACCESS_INTEGRATION, Streamlit Community Cloud."
---

# Streamlit Pivot Table Component

`streamlit-pivot` provides `st_pivot_table` — a BI-focused pivot table component built with Streamlit Components V2, React, and TypeScript. It supports multi-dimensional pivoting, interactive sorting/filtering, subtotals with collapse/expand, conditional formatting, Excel/CSV/TSV/clipboard export, drill-down detail panels, drag-and-drop field configuration, synthetic (derived) measures with a formula engine, date/time hierarchies with period-over-period comparisons, hierarchical row layouts, column resize, fullscreen mode, and server-side pre-aggregation for large datasets.

**Current version:** 0.3.0
**Requirements:** Python >= 3.10, Streamlit >= 1.51

## When to Use

- User wants to add a pivot table to a Streamlit app
- User mentions `streamlit_pivot` or `st_pivot_table`
- User needs interactive data summarization with row/column dimensions and aggregated measures
- User wants to deploy a Streamlit app that includes the pivot table, whether locally, on **Streamlit Community Cloud**, in a **container** (Docker / ECS / Cloud Run / Kubernetes), or as **Streamlit in Snowflake (SiS) on SPCS**

## Table of Contents

1. [Quick Start](#quick-start)
2. [What `st_pivot_table` Returns](#what-st_pivot_table-returns)
3. [Deployment Paths](#deployment-paths) — [Local](#path-a--local-development) · [Community Cloud](#path-b--streamlit-community-cloud) · [Container](#path-c--custom-container-docker--ecs--cloud-run--k8s) · [SiS on SPCS](#deploying-to-sis-on-spcs)
4. [API Reference](#api-reference)
5. [Feature Guide](#feature-guide)
6. [Frontend UX: Toolbar, Settings Panel, and Interactions](#frontend-ux-toolbar-settings-panel-and-interactions)
7. [Theming](#theming)
8. [Keyboard Accessibility](#keyboard-accessibility)
9. [Callbacks and State](#callbacks-and-state)
10. [Integration Patterns](#integration-patterns) — [Widget filters](#pattern-1--sidebar--widget-filters-driving-a-pivot) · [Multiple pivots](#pattern-2--multiple-pivots-on-one-page) · [Linked pivots](#pattern-3--linked-pivots-drill-from-one-into-another) · [Tabs / columns / expanders](#pattern-4--tabs-columns-and-expanders) · [Persisting config](#pattern-5--persisting-config-across-sessions)
11. [Performance: `@st.fragment`](#performance-use-stfragment-with-multiple-pivots)
12. [Deploying to SiS on SPCS](#deploying-to-sis-on-spcs) (with end-to-end example)
13. [Replicating an Existing Pivot (Excel / Sigma / Tableau / Power BI)](#replicating-an-existing-pivot-excel--sigma--tableau--power-bi)
14. [Quick Reference Recipes](#quick-reference-recipes)

---

## Quick Start

```sh
pip install streamlit-pivot
```

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
    aggregation="sum",
)
```

- `data` accepts the same inputs as `st.dataframe` (Pandas/Polars DataFrames, NumPy arrays, dicts, lists of records, PyArrow Tables, Pandas `Styler`).
- If `rows`, `columns`, and `values` are all omitted, the component auto-detects dimensions (categorical + low-cardinality numeric) and measures (high-cardinality numeric).
- **Every call must pass a unique `key`** — required for state persistence across reruns and for multiple pivots on the same page.

## What `st_pivot_table` Returns

`st_pivot_table` is primarily a **display** component. The return value and `st.session_state[key]` give you the current **configuration and metadata**, not the aggregated pivot data.

```python
result = st_pivot_table(df, key="my_pivot", rows=["Region"], values=["Revenue"])

result["config"]           # current pivot config (rows, columns, values, sort, filters, ...)
result.get("perf_metrics") # render/pivot timing and shape stats (may be None)

# Cell-click payload is a *trigger value* — read it from session_state, not the return dict.
st.session_state["my_pivot"].get("cell_click")
```

Key consequences:

- **To get the aggregated data out of the app**, use the toolbar **Export Data** action (Excel / CSV / TSV / clipboard). There is no Python method that returns the pivoted DataFrame.
- **To get the source records for a cell**, use `on_cell_click` + the `cell_click` payload's `filters` dict and apply them yourself to your source DataFrame (or let the built-in drill-down panel do it).
- **To persist user customizations**, serialize `result["config"]` (matches the format produced by **Copy Config** in the toolbar) and pass it back as `**config` on the next run — see [Pattern 5](#pattern-5--persisting-config-across-sessions).

---

## Deployment Paths

In every environment, the component is installed **from PyPI as `streamlit-pivot`**. There is no custom-wheel workflow — `pip install streamlit-pivot` is the supported path everywhere, including Streamlit in Snowflake on SPCS (where PyPI access is granted via `PYPI_ACCESS_INTEGRATION`).

| Path | Environment | Install |
|------|-------------|---------|
| A | Local development | `pip install streamlit-pivot` |
| B | Streamlit Community Cloud | `streamlit-pivot` in `requirements.txt` |
| C | Custom container (Docker / ECS / Cloud Run / K8s) | `pip install streamlit-pivot` in the image |
| D | **Streamlit in Snowflake (SiS) on SPCS** | `streamlit-pivot` in `requirements.txt` + `PYPI_ACCESS_INTEGRATION` on the SiS entity. SPCS runtime is required. [Full workflow →](#deploying-to-sis-on-spcs) |

> **Decision question to ask the user:**
>
> "Where will your app run? (a) locally, (b) Streamlit Community Cloud, (c) a container you control (Docker / ECS / Cloud Run / K8s), or (d) Streamlit in Snowflake on SPCS?"

### Path A — Local Development

```sh
pip install streamlit-pivot
# or:
uv pip install streamlit-pivot

streamlit run streamlit_app.py
```

### Path B — Streamlit Community Cloud

1. Push the repo to GitHub with a top-level `streamlit_app.py` (or point the Community Cloud config to its path).
2. Add `requirements.txt`:

    ```
    streamlit>=1.51
    streamlit-pivot>=0.3.0
    pandas
    ```

3. Create the app at [share.streamlit.io](https://share.streamlit.io).

The component works out of the box on Community Cloud — no additional configuration, asset upload, or proxy setup is needed.

### Path C — Custom Container (Docker / ECS / Cloud Run / K8s)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.address=0.0.0.0", "--server.port=8501"]
```

`requirements.txt`:

```
streamlit>=1.51
streamlit-pivot>=0.3.0
pandas
```

The build environment needs outbound PyPI access. For air-gapped builds, mirror `streamlit-pivot` into your internal PyPI index and point `pip` at it.

### Path D — Streamlit in Snowflake (SiS) on SPCS

See the full guided workflow in [Deploying to SiS on SPCS](#deploying-to-sis-on-spcs) below.

---

## API Reference

### `st_pivot_table(data, *, ...)`

Creates a pivot table component. All parameters except `data` are keyword-only. Returns a `PivotTableResult` dict.

#### Core Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data` | DataFrame-like | *(required)* | Source data. Accepts the same types as `st.dataframe`: Pandas/Polars DataFrame or Series, NumPy array, dict, list of records, PyArrow Table, Pandas `Styler`, etc. |
| `key` | `str` | *(required)* | Unique component key. Each pivot on a page must have a distinct key. |
| `rows` | `list[str] \| None` | `None` | Row dimensions. |
| `columns` | `list[str] \| None` | `None` | Column dimensions. |
| `values` | `list[str] \| None` | `None` | Measure columns. |
| `synthetic_measures` | `list[dict] \| None` | `None` | Derived measures. See [Synthetic Measures](#synthetic-measures). |
| `aggregation` | `str \| dict[str, str]` | `"sum"` | Raw-value aggregation. See [Aggregation Functions](#aggregation-functions). |
| `auto_date_hierarchy` | `bool` | `True` | Auto-group typed date/datetime fields placed on rows or columns. |
| `date_grains` | `dict[str, str \| None] \| None` | `None` | Per-field grain: `"year"`, `"quarter"`, `"month"`, `"week"`, `"day"`, or `None` (raw). |
| `interactive` | `bool` | `True` | `False` hides the toolbar and disables header-menu sort/filter/show-values-as. |

#### Totals and Subtotals

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `show_totals` | `bool` | `True` | Grand totals default for row/column totals. |
| `show_row_totals` | `bool \| list[str] \| None` | `None` | `True`/`False`/list of measure names. |
| `show_column_totals` | `bool \| list[str] \| None` | `None` | Same semantics. |
| `show_subtotals` | `bool \| list[str]` | `False` | `True` all parent dimensions, list names specific levels. |
| `repeat_row_labels` | `bool` | `False` | Repeat row labels instead of merging. Ignored when `row_layout="hierarchy"`. |

#### Sorting

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `row_sort` | `dict \| None` | `None` | Initial row sort. See [Sort Configuration](#sort-configuration). |
| `col_sort` | `dict \| None` | `None` | Initial column sort (same shape, no `col_key`). |
| `sorters` | `dict[str, list[str]] \| None` | `None` | Custom sort ordering per dimension. |

#### Display and Formatting

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `number_format` | `str \| dict[str, str] \| None` | `None` | d3-style pattern(s) for value cells. `"__all__"` = default. |
| `dimension_format` | `str \| dict[str, str] \| None` | `None` | Same, for row/column dimension labels. |
| `column_alignment` | `dict[str, str] \| None` | `None` | `"left"` / `"center"` / `"right"`. |
| `show_values_as` | `dict[str, str] \| None` | `None` | Per-field display mode. See [Show Values As](#show-values-as). |
| `conditional_formatting` | `list[dict] \| None` | `None` | Color scales, data bars, thresholds. |
| `style` | `str \| PivotStyle \| list \| None` | `None` | Region-based table styling. Pass a preset name, a `PivotStyle` dict, or a list that composes presets + overrides. See [Styling](#styling-1). |
| `column_config` | `dict[str, Any] \| None` | `None` | Streamlit-style per-column display config. Keys: `format`, `type`, `label`, `help`, `width` (`"small"`/`"medium"`/`"large"`/int px ∈ [20, 2000]), `pinned` (config-UI lock only, not sticky), `alignment` (`"left"`/`"center"`/`"right"`; unions with `column_alignment` kwarg, explicit kwarg wins). Row-dim cell renderers via `type`: `"link"` (+ optional `display_text`), `"image"`, `"checkbox"`, `"text"` with `max_chars`. Both dict literals and `st.column_config.*` objects are accepted. |
| `empty_cell_value` | `str` | `"-"` | String for empty cells. |

#### Layout

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_height` | `int` | `500` | Max auto-size height (px). Table scrolls past this. |
| `height` | `int \| None` | `None` | **Deprecated.** Treated as `max_height` when provided. |
| `sticky_headers` | `bool` | `True` | Column headers stick to the top of the scroll container. |
| `row_layout` | `"table" \| "hierarchy"` | `"table"` | Controls how row dimensions are arranged. `"table"` gives one visible column per row dimension (traditional pivot look). `"hierarchy"` collapses all row dimensions into a single indented tree column with breadcrumb navigation, and auto-enables subtotals when `show_subtotals` is not explicitly set. See [Row Layout Modes](#row-layout-modes). |

#### Interactivity and Callbacks

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `on_cell_click` | `Callable[[], None] \| None` | `None` | Fires on data-cell click. Read payload from `st.session_state[key]["cell_click"]`. |
| `on_config_change` | `Callable[[], None] \| None` | `None` | Fires when the user changes config (toolbar, DnD, Settings Panel, header menu). |
| `enable_drilldown` | `bool` | `True` | Inline drill-down panel on cell click. |
| `locked` | `bool` | `False` | Viewer mode. Config locked; export, expand/collapse, drill-down, header-menu sort/filter remain. |
| `export_filename` | `str \| None` | `None` | Base filename for exports; date + extension auto-appended. |

#### Data Control

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `null_handling` | `str \| dict[str, str] \| None` | `None` | `"exclude"`, `"zero"`, or `"separate"`. Global or per-field. |
| `source_filters` | `dict[str, dict[str, list[Any]]] \| None` | `None` | Report-level filters applied **before** pivot processing. `include` wins over `exclude`. |
| `hidden_attributes` | `list[str] \| None` | `None` | Columns hidden from the UI entirely. |
| `hidden_from_aggregators` | `list[str] \| None` | `None` | Columns hidden from the Values/aggregators menu only. |
| `frozen_columns` | `list[str] \| None` | `None` | Pinned to zone; no drag, no remove. |
| `hidden_from_drag_drop` | `list[str] \| None` | `None` | **Deprecated** alias for `frozen_columns`. Prefer `frozen_columns` in new code. |
| `menu_limit` | `int \| None` | `None` | Max items in the header-menu filter checklist (default 50). |
| `execution_mode` | `str` | `"auto"` | `"auto"`, `"client_only"`, `"threshold_hybrid"`. |

---

## Feature Guide

### Aggregation Functions

Supported: `"sum"`, `"avg"`, `"count"`, `"min"`, `"max"`, `"count_distinct"`, `"median"`, `"percentile_90"`, `"first"`, `"last"`.

```python
st_pivot_table(
    df,
    key="agg_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Units", "Price"],
    aggregation={"Revenue": "sum", "Units": "count", "Price": "avg"},
)
```

`"sum_over_sum"` is **not** a valid table-wide aggregation — use `synthetic_measures` instead.

### Synthetic Measures

Three operations:

- `sum_over_sum` — `sum(numerator) / sum(denominator)` (empty when denominator is 0)
- `difference` — `sum(numerator) - sum(denominator)`
- `formula` — arbitrary expression referencing aggregated fields; each field uses its configured aggregation

```python
st_pivot_table(
    df,
    key="synth_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Cost", "Units"],
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
            "id": "margin_pct",
            "label": "Margin %",
            "operation": "formula",
            "formula": 'if("Revenue" > 0, ("Revenue" - "Cost") / "Revenue", 0)',
            "format": ".1%",
        },
    ],
)
```

Formula syntax: arithmetic (`+ - * / ^ %`), comparisons, `and` / `or` / `not`, `if(cond, then, else)`, and `abs() / min() / max() / round(x, n)`. CSP-safe (no `eval`). Division by zero returns null; `if()` short-circuits.

### Sort Configuration

```python
row_sort = {
    "by": "value",             # "key" or "value"
    "direction": "desc",       # "asc" or "desc"
    "value_field": "Revenue",  # required when by="value"
    "col_key": ["2023"],       # optional: sort within a specific column
    "dimension": "Category",   # optional: scope sort to a specific level
}
```

Scoped sorting (`dimension` set) only reorders that level and below when subtotals are enabled. Header-menu sort sets `dimension` automatically.

### Show Values As

| Mode | Value |
|------|-------|
| Raw | `"raw"` |
| % of Grand Total | `"pct_of_total"` |
| % of Row Total | `"pct_of_row"` |
| % of Column Total | `"pct_of_col"` |
| Diff vs Previous Period | `"diff_from_prev"` |
| % Diff vs Previous Period | `"pct_diff_from_prev"` |
| Diff vs Previous Year | `"diff_from_prev_year"` |
| % Diff vs Previous Year | `"pct_diff_from_prev_year"` |

Period-comparison modes require an active grouped temporal axis (auto hierarchy or explicit `date_grains`). Synthetic measures render as raw values and ignore `show_values_as`.

### Date Hierarchy and Time Comparisons

Typed `date` and `datetime` fields on `rows` or `columns` are hierarchy-capable.

- **Adaptive grain** (default): year (>2y) · quarter (>1y) · month (>2mo) · day (shorter).
- **Drill ladder**: Year → Quarter → Month → Day. Week is available via header menu.
- **Override precedence**: explicit `date_grains[field]` > interactive state > auto default.
- `date_grains[field] = None` preserves raw values for that field.

```python
st_pivot_table(
    df,
    key="date_demo",
    rows=["region"],
    columns=["order_date"],
    values=["Revenue"],
    date_grains={"order_date": "quarter"},
    show_values_as={"Revenue": "diff_from_prev_year"},
)
```

Parent headers (e.g. `2024`, `Q1 2024`) collapse/expand inline on both axes. Exports always emit the full expanded leaf-level table.

### Row Layout Modes

`row_layout` controls how row dimensions are visually arranged in the row header. Both modes display the same data — only the visual structure changes.

| Mode | Value | What you see |
|------|-------|-------------|
| Table | `"table"` | **One column per row dimension.** Region, Category, and Product each appear as their own header column. Repeated labels are merged by default (or repeated when `repeat_row_labels=True`). This is the traditional spreadsheet-pivot look. |
| Hierarchy | `"hierarchy"` | **A single indented tree column** for all row dimensions. Depth is shown through indentation. A breadcrumb bar at the top of the column shows the current drill path and lets users jump back to any ancestor level with a click. |

**When to use Table:** 1–2 row dimensions; when each dimension should be visually distinct in its own column; or when `repeat_row_labels=True` is needed.

**When to use Hierarchy:** 3+ row dimensions or when horizontal space is limited. The compact single-column layout keeps value columns visible without side-scrolling. Subtotals are auto-enabled so every group node shows its aggregate out of the box.

**Key behavior differences:**

- **Auto-subtotals:** when `row_layout="hierarchy"` and `show_subtotals` is not explicitly set, subtotals are automatically enabled for all grouping levels. Pass `show_subtotals=False` or `show_subtotals=[...]` to override. In `"table"` mode, `show_subtotals` defaults to `False` as usual.
- **`repeat_row_labels` is ignored** in hierarchy mode — the row axis is a single column, so there are no repeated labels to merge or repeat. The Settings Panel disables the Repeat Labels toggle automatically when hierarchy mode is active.
- Temporal date hierarchies (Year → Quarter → Month → Day) work in both layouts. In `"table"`, each date level expands as a separate row-header column; in `"hierarchy"`, the same levels render as nested tree levels within the single column.
- Export parity is preserved — CSV, TSV, clipboard, and XLSX outputs follow the selected row layout, including hierarchy indentation.

```python
# Default: one dedicated column per row dimension
st_pivot_table(
    df,
    key="table_mode",
    rows=["Region", "Category", "Product"],
    columns=["Year"],
    values=["Revenue"],
    show_subtotals=True,
    row_layout="table",   # default; can be omitted
)

# Hierarchy: single indented tree column
# show_subtotals auto-enables when not explicitly set
st_pivot_table(
    df,
    key="hierarchy_mode",
    rows=["Region", "Category", "Product"],
    columns=["Year"],
    values=["Revenue"],
    row_layout="hierarchy",
)

# Hierarchy with subtotals explicitly disabled
st_pivot_table(
    df,
    key="hierarchy_no_sub",
    rows=["Region", "Category", "Product"],
    columns=["Year"],
    values=["Revenue"],
    row_layout="hierarchy",
    show_subtotals=False,   # override the auto-enable
)
```

### Number Format Patterns

d3-style. Examples: `$,.0f` → `$12,345`; `,.2f` → `12,345.67`; `.1%` → `34.5%`; `€,.2f` → `€12,345.67`.

Use `"__all__"` in a dict for a default. Streamlit printf-style (`%,.2f`) works via `column_config`.

```python
st_pivot_table(
    df,
    key="fmt_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    number_format={"Revenue": "$,.0f", "Profit": ",.2f"},
    dimension_format={"Year": ".0f"},
)
```

**Auto-formats from upstream sources.** Precedence for format fields: explicit `number_format` / `dimension_format` > `column_config` > Pandas `Styler`. For alignment: explicit `column_alignment` > `column_config.alignment` > default (measures right, dims left). `label`, `help`, and `width` are `column_config`-driven only (no legacy kwargs). `pinned` **unions** with `frozen_columns`.

```python
styled = df.style.format({"Revenue": "${:,.0f}"})
st_pivot_table(styled, key="styler_demo", rows=["Region"], values=["Revenue"])
```

**Display-oriented `column_config` keys (Tier 1).**

```python
st_pivot_table(
    df,
    key="column_config_display",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    column_config={
        "Region": {"label": "Area", "help": "Geographic region", "width": "large", "pinned": True},
        "Revenue": {"label": "Rev", "help": "Total revenue (USD)", "width": 180, "format": "$,.0f", "alignment": "right"},
    },
)
```

- `label` overrides display text only. The **canonical field id is unchanged** in the serialized config; sort, filter, and conditional-formatting rules still target the id. Empty / whitespace-only labels fall back to the id.
- `help` renders as a native `title` tooltip on the dim/measure header.
- `width` presets: `"small"`=100px, `"medium"`=120px, `"large"`=200px. Integers must be in `[20, 2000]`; out-of-range values warn once per field and are skipped. **Interactive resize drags override the configured width at runtime but are not persisted to config** (width returns to the configured value after rerun/remount).
- `pinned=True` (or `"left"`) locks the field in the **config UI** — no drag, no remove. It does **not** create a visually sticky column. `"right"` warns and is ignored.
- `alignment` accepts `"left"`, `"center"`, or `"right"`. It **unions** with the `column_alignment` kwarg; if both set a value for the same field, the explicit `column_alignment` kwarg wins. Invalid values warn once per field and are skipped (unlike the `column_alignment` kwarg, which still raises on invalid input).
- Unknown keys in dict literals warn once per `(field, key)`; internal defaults on `st.column_config.*` objects (`disabled`, `required`, `default`) are silently ignored; recognized-but-unsupported column types (e.g. `line_chart`, `selectbox`) warn once per `(field, type)`.

**Row-dim cell renderers (Tier 2).** A small set of `type` values change how row-dim cell values render. These apply only to row-dim cells; measure cells are numeric aggregates and ignore them. On Total / Subtotal rows, `link` / `image` / `checkbox` fall back to plain text (cell value is a label, not data); `text` with `max_chars` still truncates.

```python
st_pivot_table(
    df,
    key="column_config_renderers",
    rows=["Homepage", "Poster", "Active", "Description"],
    values=["Revenue"],
    column_config={
        "Homepage":    st.column_config.LinkColumn("Homepage", display_text="Visit {}"),
        "Poster":      st.column_config.ImageColumn("Poster"),
        "Active":      st.column_config.CheckboxColumn("Active"),
        "Description": st.column_config.TextColumn("Description", max_chars=40),
    },
)
```

- `type="link"` — `href = <raw value>`. `display_text` can be a plain string or a `{}` template (substituted with the cell value, matching Streamlit's `LinkColumn`). Empty / null values fall back to plain text. **Scheme allowlist:** only `http:`, `https:`, `mailto:`, `tel:`, and schemeless (relative / protocol-relative) URLs render as anchors; `javascript:`, `data:`, `file:`, `vbscript:`, etc. fall back to plain text.
- `type="image"` — `src = <raw value>`. Uses `loading="lazy"` and a `max-height` guard so images don't blow out row height. In `row_layout="hierarchy"`, the breadcrumb variant applies a tighter 1em cap. **Src allowlist:** `http:` / `https:` / schemeless URLs plus raster `data:image/<mime>` (png, jpeg, gif, webp, avif, bmp, ico); everything else — notably `data:image/svg+xml` and non-image `data:` — falls back to plain text.
- `type="checkbox"` — truthy → ☑, falsy → ☐. Accepts booleans, `"true"/"false"/"yes"/"no"/"1"/"0"` (case-insensitive), and the numbers `0` / `1`. Unrecognized values fall back to plain text.
- `type="text"` + `max_chars` — truncates to `max_chars` UTF-16 code units (matches JS `String.length` / Streamlit's `TextColumn`) with an ellipsis; full text is preserved in the cell's `title` attribute. Truncation applies on every row, including Totals. Invalid `max_chars` (non-positive, non-integer, bool) warns once and is skipped.

### Conditional Formatting

```python
st_pivot_table(
    df,
    key="cond_fmt",
    values=["Revenue", "Profit", "Units"],
    conditional_formatting=[
        {"type": "data_bars", "apply_to": ["Revenue"], "color": "#1976d2", "fill": "gradient"},
        {"type": "color_scale", "apply_to": ["Profit"],
         "min_color": "#fff", "mid_color": "#ffffff", "max_color": "#2e7d32", "mid_value": 0},
        {"type": "threshold", "apply_to": ["Units"], "conditions": [
            {"operator": "gt", "value": 250, "background": "#bbdefb", "color": "#0d47a1", "bold": True},
        ]},
    ],
)
```

Color-scale `mid_value` anchors the gradient at a specific raw aggregate (e.g. `0` for PnL). Threshold operators: `gt`, `gte`, `lt`, `lte`, `eq`, `between`. Conditional formatting rules are translated into native Excel conditional formatting on export.

### Null Handling

`"exclude"` (default; null-dimension rows dropped) · `"zero"` (null measures → 0) · `"separate"` (null dimensions → "(null)" group).

```python
st_pivot_table(df, key="null_demo", null_handling={"Region": "separate", "Revenue": "zero"})
```

### Subtotals and Row Grouping

With 2+ row dimensions, `show_subtotals=True` exposes collapsible group subtotals with visual hierarchy cues (bold tinted group cells, indented leaves, group-boundary borders, inline +/− toggles). Pass a list to enable subtotals only for specific levels (`show_subtotals=["Region"]`).

### Column Group Collapse/Expand

With 2+ column dimensions (or an expanded temporal hierarchy), parent column headers show an inline +/− toggle on hover.

### Data Export

Excel (`.xlsx`), CSV, TSV, or clipboard via the toolbar download icon. Excel export is fully styled (merged headers, frozen panes, banded rows, number formats, native conditional formatting). Available in locked mode. Exports always emit the full expanded table.

Use `export_filename` to customize the base name (date + extension auto-appended).

### Drill-Down Detail Panel

Click any data or total cell to open an inline panel of source records (up to 500 per page, paginated for larger sets). Column headers sort the full filtered set before paging. Set `enable_drilldown=False` to disable the panel while keeping `on_cell_click`.

### Execution Mode

| Mode | Value | Description |
|------|-------|-------------|
| Auto | `"auto"` | Client-side unless the dataset exceeds row/cardinality thresholds (default). |
| Client Only | `"client_only"` | Always send raw rows to the frontend. |
| Threshold Hybrid | `"threshold_hybrid"` | Force server-side pre-aggregation when compatible. |

Auto-hybrid activates at ~100K rows (high-cardinality) or ~250K rows (moderate) when the estimated pivot shape exceeds the client budget. All 10 aggregations are supported in hybrid mode; non-decomposable aggregations use a server-computed sidecar for correct totals. Synthetic measures (including formulas) evaluate client-side, even in hybrid mode.

### Locked Mode

`locked=True` renders a viewer-mode experience: config controls are locked, but export, expand/collapse, drill-down, and header-menu sort/filter/show-values-as remain available.

### Non-Interactive Mode

`interactive=False` hides the toolbar and disables header-menu config actions. Cell clicks and drill-down still work.

---

## Frontend UX: Toolbar, Settings Panel, and Interactions

These are purely frontend behaviors — no Python parameters needed. All are active when `interactive=True` and disabled or restricted when `locked=True` or `interactive=False`.

### Toolbar

When `interactive=True`, the toolbar at the top of the pivot shows read-only **Rows**, **Columns**, and **Values** zones. Each chip has a drag handle (`⋮⋮`). Hovering the top-right reveals a utility menu:

| Action | Notes |
|--------|-------|
| **Reset** | Reverts to the original Python-supplied config. Only visible when the user has changed something. |
| **Swap** | Transposes row and column dimensions. |
| **Copy Config** | Copies the current config as JSON to the clipboard. |
| **Import Config** | Paste a JSON config to apply. |
| **Export Data** | Opens the export popover (Excel / CSV / TSV / Clipboard). Respects `export_filename`. |
| **Expand / Collapse All** | Expands or collapses all row/column groups. Visible when subtotals are on or 2+ column dimensions exist. |
| **Fullscreen** (⤢) | Fills the entire browser viewport. Escape or the collapse icon exits. |
| **Settings** (pivot icon) | Opens the Settings Panel (below). |

In **locked mode**, `Reset`, `Swap`, and config import/export are hidden; `Export Data` and the Settings gear remain visible but the Settings Panel is read-only for field layout.

### Settings Panel (staged-commit UX)

The Settings Panel is the primary authoring surface for pivot configuration. Changes are **staged locally** and only applied when the user clicks **Apply** (Escape / Cancel discards). Contents:

- **Available Fields** — unassigned columns as draggable chips. A search input appears when there are more than 8 fields.
- **Rows / Columns / Values** drop zones — drag to reorder within, drag to move between zones, or use the `x` to remove. Value chips expose an aggregation picker on the badge. Synthetic `fx` chips appear inline with raw value chips and can be reordered alongside them (saved as `value_order`).
- **Synthetic Measures** — `+ Add measure` opens the builder (**Sum over Sum**, **Difference**, or **Formula**). Formula mode provides an expression input, clickable field chips, and a `?` tooltip listing supported operators. Existing chips expose an `✎ Edit` action.
- **Display Toggles** — Row Totals, Column Totals, Subtotals, Repeat Labels, Row Layout, Sticky Headers.
- **Invalid-drop feedback** — dropping a non-numeric field into Values (for example) turns the zone red with an inline hint explaining why.

External changes while the panel is open (toolbar DnD, Reset, Swap, Import) close the panel and discard uncommitted edits.

### Drag-and-Drop Field Configuration

Two contexts:

- **Toolbar DnD** — the main toolbar chips can be reordered or moved between Rows / Columns / Values zones directly. Changes apply **immediately** (not staged).
- **Settings Panel DnD** — inside the panel, the same behavior, but changes are **staged until Apply**.

Constraints:

- `frozen_columns` render **without** drag handles and cannot be dragged or removed.
- Non-numeric fields are rejected from the **Values** zone (invalid-drop feedback appears).
- **Rows** and **Columns** are mutually exclusive for a given field.
- A field can be in **Values** and one of **Rows** / **Columns** simultaneously.
- Synthetic `fx` chips can be reordered within **Values** but cannot leave it.
- When `locked=True`, all drag-and-drop is disabled.
- A 5 px activation distance distinguishes clicks from drags.
- Space + arrow keys provide a keyboard-accessible DnD path (see [Keyboard Accessibility](#keyboard-accessibility)).

Config cleanup on move: when fields move between zones, related config properties (aggregation, sort, collapsed groups, subtotals, conditional formatting, show-values-as, per-measure totals, and `value_order`) are automatically synchronized. Orphan entries are dropped; newly added measures are appended.

### Column Resize

Drag the **right edge** of any column header (cursor becomes `col-resize`). Minimum width 40 px. **Double-click** the resize handle to auto-size to content. Works with both virtualized and non-virtualized rendering. Widths are tracked per slot position.

### Fullscreen

Click the **expand icon** (⤢) in the toolbar utility menu. The pivot fills the browser viewport as a fixed overlay; press Escape or click the collapse icon to exit. The table re-measures to fill the available height.

### Field Search

When the Settings Panel has more than 8 available fields, a search input appears at the top of the Available Fields list.

---

## Theming

The component inherits the Streamlit theme automatically. Its stylesheets are built on Streamlit's CSS custom properties — `--st-text-color`, `--st-background-color`, `--st-secondary-background-color`, `--st-dataframe-border-color`, `--st-font`, `--st-base-radius`, and related tokens — so the pivot picks up:

- **Light / dark mode** from `theme.base` in `.streamlit/config.toml` (or the user's Streamlit theme toggle).
- **Custom theme colors** from `primaryColor`, `backgroundColor`, `secondaryBackgroundColor`, `textColor`, and `font` in `[theme]`.
- **Border radius and font family** from the app's base settings, so the pivot matches the look of `st.dataframe` and other native widgets.

No Python parameters or theme configuration are needed inside `st_pivot_table` — it just works.

**One caveat for user-supplied colors.** `conditional_formatting` rules take literal colors (`min_color`, `max_color`, `mid_color`, data-bar `color`, threshold `background` / `color`). These are **not** theme-aware — they render exactly as specified in both light and dark mode. If you expect the app to switch themes, pick colors that read on both backgrounds, or keep intense backgrounds paired with explicit foreground `color` overrides so text stays legible.

```python
# Good: explicit text color paired with a vivid background so it works in dark mode too
{"type": "threshold", "apply_to": ["Profit"], "conditions": [
    {"operator": "gt", "value": 5000, "background": "#2e7d32", "color": "#ffffff", "bold": True},
]}
```

### Styling

The `style=` parameter is a **per-table layer** over the Streamlit theme. It does not replace theming — it lets individual pivot tables deviate from the app theme without breaking dark mode or custom theme compatibility. `style=None` (the default) means the pivot tracks the app theme exactly.

**Always use `var(--st-...)` tokens for theme-aware custom colors:**

```python
# ✅ Theme-aware: picks up dark mode and custom [theme] colors
style={"column_header": {"background_color": "var(--st-primary-color)"}}
style={"border_color": "color-mix(in srgb, var(--st-text-color) 20%, transparent)"}

# ❌ Not theme-aware: fixed hex breaks dark mode
style={"column_header": {"background_color": "#1a73e8"}}
```

**Presets (theme-aware, no raw hex):**

| Preset | Description |
|--------|-------------|
| `"default"` | No overrides; tracks Streamlit theme. Passing `"default"` equals `style=None`. |
| `"striped"` | Subtle alternating-row banding. |
| `"minimal"` | No borders, no hover, no stripes — good for static output. |
| `"compact"` | Tight padding + reduced virtualized row height. |
| `"contrast"` | Bold headers, bold totals, subtle stripe — Power BI Contrast parity. |

**`PivotStyle` and `RegionStyle` shapes:**

```python
from streamlit_pivot import PivotStyle, RegionStyle, PIVOT_STYLE_PRESETS

class RegionStyle(TypedDict, total=False):
    background_color: str
    text_color: str
    font_weight: str   # "normal" | "bold"

class PivotStyle(TypedDict, total=False):
    # Table-wide
    density: str           # "compact" | "default" | "comfortable"
    font_size: str         # e.g. "13px", "0.875rem"
    background_color: str  # cascades to all regions
    text_color: str
    stripe_color: str | None   # None = disable striping entirely
    row_hover_color: str | None  # None = disable hover entirely
    borders: str           # "all" | "outer" | "rows" | "columns" | "none"
    border_color: str
    # Region overrides
    column_header: RegionStyle
    row_header: RegionStyle
    data_cell: RegionStyle
    row_total: RegionStyle      # grand totals per row (rightmost col → .totalsCol)
    column_total: RegionStyle   # grand totals per col (bottom row → .totalsRow)
    subtotal: RegionStyle
    # Per-measure overrides (non-total data cells only)
    data_cell_by_measure: dict[str, RegionStyle]
```

**Naming note:** `row_total` styles cells in the rightmost (`.totalsCol`) column; `column_total` styles cells in the bottom (`.totalsRow`) row. This is intentional — API names follow user intent ("totals *of* each row"), not CSS class names.

**Precedence (highest → lowest):**

1. Conditional formatting (per-cell CF rule)
2. `data_cell_by_measure[field]` (per-cell; non-total data cells only)
3. Region overrides (`column_header`, `row_header`, `data_cell`, `row_total`, `column_total`, `subtotal`)
4. Table-wide cascade (`background_color`, `text_color`)
5. Streamlit theme (`--st-*` fallbacks)

**Composition (list merging):** Pass a list to merge presets and custom dicts left-to-right:

```python
# Compact layout + contrast headers + custom row-total tint
style=[
    "compact",
    "contrast",
    PivotStyle(row_total=RegionStyle(
        background_color="color-mix(in srgb, var(--st-primary-color) 15%, var(--st-background-color))"
    )),
]
```

---

## Keyboard Accessibility

The component follows WAI-ARIA patterns:

- **Toolbar** — arrow keys navigate between toolbar buttons (roving tabindex); Space/Enter activates.
- **Drag-and-drop** — Space to pick up a chip; arrow keys to move; Space again to drop. Screen reader announcements are provided by dnd-kit.
- **Header menus** — Escape closes; arrow keys navigate options; Space/Enter selects.
- **Export / Import popovers** — focus moves to the first control on open; tabbing out closes the popover.
- **Settings Panel** — focus moves in on open; Escape closes and discards staged changes; Tab navigates fields, zones, toggles, and buttons. Aggregation pickers respond to Enter/Space.
- **Radio groups** (export format/content) — arrow keys cycle; Space/Enter selects.
- **Drill-down panel** — focus moves to the close button on open; Escape closes; column headers are buttons that cycle sort (asc → desc → none).
- **Data cells** — focusable via Tab; Space/Enter triggers `on_cell_click`.

---

## Callbacks and State

Streamlit Components V2 callbacks are called with **no arguments**. Read updated state from `st.session_state[key]` after the callback fires.

```python
def on_click():
    payload = st.session_state["my_pivot"].get("cell_click")
    st.write("Clicked:", payload)

def on_config():
    config = st.session_state["my_pivot"].get("config")
    st.write("New config:", config)

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

```python
{
    "rowKey": ["East"],
    "colKey": ["2023"],
    "value": 12345.0,
    "valueField": "Revenue",
    "filters": {"Region": "East", "Year": "2023"},
}
```

Total cells use `["Total"]` for `rowKey` or `colKey`; the corresponding dimension is omitted from `filters`.

### Config State

`result["config"]` contains the live configuration (rows, columns, values, aggregations, totals, sort, filters, collapsed groups, `value_order`, display options). Serialize it to persist layouts or sync multiple pivots — this is the same shape the toolbar's **Copy Config** / **Import Config** produces.

### Performance Metrics

`result["perf_metrics"]` exposes render/pivot stats: `parseMs`, `pivotComputeMs`, `renderMs`, `firstMountMs`, `sourceRows`, `sourceCols`, `totalRows`, `totalCols`, `totalCells`, `executionMode`, `needsVirtualization`, `columnsTruncated` / `truncatedColumnCount`, `warnings`, `lastAction`.

```python
metrics = result.get("perf_metrics") or {}
if metrics.get("executionMode") == "threshold_hybrid":
    st.caption(f"Pre-aggregated {metrics['sourceRows']} rows on the server.")
```

---

## Integration Patterns

These are the most common real-world shapes a Streamlit app takes around `st_pivot_table`.

### Pattern 1 — Sidebar / Widget Filters Driving a Pivot

The typical dashboard pattern: Streamlit widgets filter the source data; the pivot table visualizes the result.

You have two equivalent choices for applying the filters:

**A. Filter the DataFrame before passing it in** — simplest, works with any data source.

```python
import streamlit as st
from streamlit_pivot import st_pivot_table

df = load_sales()

with st.sidebar:
    st.header("Filters")
    regions = st.multiselect("Region", sorted(df["Region"].unique()))
    years = st.multiselect("Year", sorted(df["Year"].unique()))
    min_rev = st.number_input("Min revenue", value=0)

filtered = df
if regions:
    filtered = filtered[filtered["Region"].isin(regions)]
if years:
    filtered = filtered[filtered["Year"].isin(years)]
filtered = filtered[filtered["Revenue"] >= min_rev]

st_pivot_table(
    filtered,
    key="sales_pivot",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
)
```

**B. Push the filters into `source_filters`** — applied inside the component *before* pivot aggregation. Useful when you want the unfiltered DataFrame to remain available for other UI (e.g. a total-count metric).

```python
source_filters = {}
if regions:
    source_filters["Region"] = {"include": regions}
if years:
    source_filters["Year"] = {"include": years}

st_pivot_table(
    df,
    key="sales_pivot",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    source_filters=source_filters,
)
```

> `source_filters` is categorical/set-membership only — it does **not** express numeric ranges like `Revenue >= X`. Use option A for numeric range filters.

### Pattern 2 — Multiple Pivots on One Page

Each pivot needs a distinct `key`. Wrap each in `@st.fragment` so interactions in one don't rerun the others.

```python
df = load_data()

@st.fragment
def by_region():
    st.subheader("By Region")
    st_pivot_table(df, key="pivot_region", rows=["Region"], values=["Revenue"])

@st.fragment
def by_product():
    st.subheader("By Product")
    st_pivot_table(df, key="pivot_product", rows=["Product"], values=["Units"])

by_region()
by_product()
```

### Pattern 3 — Linked Pivots (Drill From One Into Another)

Use `on_cell_click` on the summary pivot to drive filters on a detail pivot.

```python
if "drill" not in st.session_state:
    st.session_state["drill"] = None

def handle_click():
    payload = st.session_state["summary"].get("cell_click")
    if payload:
        st.session_state["drill"] = payload.get("filters") or {}

st_pivot_table(
    df,
    key="summary",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    on_cell_click=handle_click,
    enable_drilldown=False,  # we roll our own detail view
)

drill = st.session_state["drill"]
if drill:
    detail = df
    for field, value in drill.items():
        detail = detail[detail[field] == value]
    st.subheader("Detail")
    st_pivot_table(
        detail,
        key="detail",
        rows=["Category", "Product"],
        values=["Revenue", "Units"],
    )
```

### Pattern 4 — Tabs, Columns, and Expanders

`st_pivot_table` renders correctly inside `st.tabs`, `st.columns`, `st.expander`, and `st.container`. A few practical notes:

- **`st.columns`** — the pivot will respect the column width. If the table has many columns, prefer a single full-width column or enable fullscreen.
- **`st.sidebar`** — technically supported but the sidebar is narrow; usually a bad fit for a multi-column pivot. Keep the pivot in the main area and put controls in the sidebar.
- **`st.tabs`** — each tab is rendered up-front, so pivots in hidden tabs still pay their compute cost unless wrapped in `@st.fragment` and gated behind an explicit trigger.
- **`st.expander`** — fine, but pivot height measurement runs on first render; collapsing/reopening re-measures automatically.

```python
tab_summary, tab_detail = st.tabs(["Summary", "Detail"])

with tab_summary:
    st_pivot_table(df, key="t_summary", rows=["Region"], values=["Revenue"])

with tab_detail:
    st_pivot_table(df, key="t_detail", rows=["Region", "Product"], values=["Revenue", "Units"])
```

### Pattern 5 — Persisting Config Across Sessions

The `config` dict is serializable. Save it (to `st.session_state`, a file, or a database keyed by user) and pass it back on the next run.

```python
import json, pathlib

CONFIG_PATH = pathlib.Path("user_pivot_config.json")

def load_saved():
    if CONFIG_PATH.exists():
        return json.loads(CONFIG_PATH.read_text())
    return {}

def save_current():
    cfg = st.session_state["my_pivot"].get("config")
    if cfg:
        CONFIG_PATH.write_text(json.dumps(cfg, indent=2))

saved = load_saved()

result = st_pivot_table(
    df,
    key="my_pivot",
    on_config_change=save_current,
    **saved,  # unpack rows / columns / values / ... from the saved config
)
```

For multi-user apps, swap the file for a keyed row in your database and retrieve by user id.

---

## Performance: Use `@st.fragment` With Multiple Pivots

Streamlit reruns the entire script on any widget change. With multiple pivots or expensive data prep, wrap each in `@st.fragment` so interactions scope reruns to that fragment.

```python
@st.fragment
def sales_pivot():
    st_pivot_table(df, key="sales", rows=["Region"], values=["Revenue"])
    payload = st.session_state.get("sales", {}).get("cell_click")
    if payload:
        st.info(f"Clicked: {payload}")

sales_pivot()
```

**Caveats:**
- Streamlit ignores fragment return values during fragment reruns. Read results via `st.session_state[key]` if they need to be accessed outside the fragment.
- Keep random / non-deterministic data generation outside the fragment.
- `on_config_change` and `on_cell_click` fire on fragment reruns — as expected.

The reference example app (`streamlit_app.py` in this repo) wraps each section in `@st.fragment`.

---

## Deploying to SiS on SPCS

Use this workflow for a Streamlit in Snowflake app that uses `st_pivot_table`. The component is installed straight from PyPI via `PYPI_ACCESS_INTEGRATION` — there is no custom wheel to copy into the project.

> **SPCS runtime is required.** SiS apps on the legacy Warehouse runtime cannot install third-party PyPI packages. The app entity must set `runtime.compute_pool`.

### Prerequisites

- **Snow CLI** (`pip install snowflake-cli` or `brew install snowflake-cli`). Verify with `snow connection test`.
- **SiS on SPCS** enabled for the account.
- **`PYPI_ACCESS_INTEGRATION`** available (confirm: `snow sql -q "SHOW EXTERNAL ACCESS INTEGRATIONS"`). If it does not exist, ask the user's Snowflake admin to create one — see [Troubleshooting](#troubleshooting) for the SQL.
- An SPCS **compute pool** for the app runtime. A small `CPU_X64_XS` pool is fine for most pivots; for interactive use with 100K+ row DataFrames, move to `CPU_X64_S` or larger so the React runtime has enough memory.
- A **warehouse** for SQL queries the app runs.
- **Streamlit version ≥ 1.51** available on the target SiS runtime. SPCS runtimes regularly ship recent Streamlit versions, but verify after scaffolding — see Step 2.

### Workflow

#### Step 1 — Gather Information (STOP and ask the user)

```
To set up your SiS on SPCS app with the pivot table component, I need:
1. Do you have an existing SiS project directory with snowflake.yml, or should I scaffold one?
2. What Snowflake table(s) will the app query?
3. What compute pool should the app run on? (e.g. MY_COMPUTE_POOL)
4. What warehouse should be used for queries? (e.g. MY_WAREHOUSE)
5. Is `PYPI_ACCESS_INTEGRATION` (or an equivalent EAI allowing pypi.org) already created in the account?
```

**Do not proceed** until the user answers Q1 and confirms Q5.

#### Step 2 — Scaffold the SiS Project (skip if they already have one)

```bash
snow init --template streamlit-python <app-name>
cd <app-name>
```

Minimal `snowflake.yml` — `runtime.compute_pool` makes it an SPCS app; `external_access_integrations` enables PyPI:

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
      compute_pool: <COMPUTE_POOL>
```

Minimal `pyproject.toml`:

```toml
[project]
name = "my-sis-app"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "streamlit>=1.51",
    "streamlit-pivot>=0.3.0",
]
```

Minimal `requirements.txt`:

```
streamlit>=1.51
streamlit-pivot>=0.3.0
```

> Pin `streamlit>=1.51` explicitly. The component requires it, and the SiS runtime's default Streamlit version may be older than what is on PyPI.

#### Step 3 — Write `streamlit_app.py`

Import: `from streamlit_pivot import st_pivot_table` (distribution `streamlit-pivot`, module `streamlit_pivot`).

Minimal:

```python
import streamlit as st
from snowflake.snowpark.context import get_active_session
from streamlit_pivot import st_pivot_table

st.set_page_config(page_title="Pivot Explorer", layout="wide")
st.title("Pivot Explorer")

session = get_active_session()

@st.cache_data(ttl=300)
def load_df():
    return session.sql("SELECT * FROM <TABLE_NAME> LIMIT 100000").to_pandas()

@st.fragment
def pivot_fragment():
    st_pivot_table(
        load_df(),
        key="main_pivot",
        rows=["REGION"],
        columns=["YEAR"],
        values=["REVENUE"],
        aggregation="sum",
    )

pivot_fragment()
```

**Full end-to-end example** — sidebar filters, cached SQL, fragment-scoped pivot, linked drill-down:

```python
import streamlit as st
from snowflake.snowpark.context import get_active_session
from streamlit_pivot import st_pivot_table

st.set_page_config(page_title="Sales Explorer", layout="wide")
st.title("Sales Explorer")

session = get_active_session()

@st.cache_data(ttl=300)
def load_sales(region: str | None, year: int | None):
    # Parameterized query — never interpolate user input into SQL strings.
    return session.sql(
        """
        SELECT REGION, CATEGORY, PRODUCT, ORDER_DATE, REVENUE, COST, UNITS
        FROM SALES_FACT
        WHERE (? IS NULL OR REGION = ?)
          AND (? IS NULL OR YEAR(ORDER_DATE) = ?)
        """,
        params=[region, region, year, year],
    ).to_pandas()

with st.sidebar:
    st.header("Filters")
    region = st.selectbox("Region", ["", "East", "West", "North", "South"])
    year = st.number_input("Year", min_value=2020, max_value=2030, value=2025)

df = load_sales(region or None, year)
st.caption(f"{len(df):,} rows loaded")

if "drill" not in st.session_state:
    st.session_state["drill"] = None

def handle_click():
    payload = st.session_state["summary"].get("cell_click")
    st.session_state["drill"] = (payload or {}).get("filters") or None

@st.fragment
def summary():
    st_pivot_table(
        df,
        key="summary",
        rows=["REGION", "CATEGORY"],
        columns=["ORDER_DATE"],
        values=["REVENUE", "COST"],
        aggregation={"REVENUE": "sum", "COST": "sum"},
        synthetic_measures=[{
            "id": "margin_pct",
            "label": "Margin %",
            "operation": "formula",
            "formula": 'if("REVENUE" > 0, ("REVENUE" - "COST") / "REVENUE", 0)',
            "format": ".1%",
        }],
        show_subtotals=True,
        date_grains={"ORDER_DATE": "quarter"},
        number_format={"REVENUE": "$,.0f", "COST": "$,.0f"},
        conditional_formatting=[
            {"type": "data_bars", "apply_to": ["REVENUE"], "color": "#1976d2", "fill": "gradient"},
        ],
        on_cell_click=handle_click,
        enable_drilldown=False,
        execution_mode="auto",
        export_filename="sales_summary",
    )

summary()

drill = st.session_state["drill"]
if drill:
    filtered = df
    for field, value in drill.items():
        filtered = filtered[filtered[field] == value]
    st.subheader(f"Detail — {', '.join(f'{k}={v}' for k, v in drill.items())}")

    @st.fragment
    def detail():
        st_pivot_table(
            filtered,
            key="detail",
            rows=["PRODUCT"],
            columns=["ORDER_DATE"],
            values=["REVENUE", "UNITS"],
            date_grains={"ORDER_DATE": "month"},
            locked=True,
        )

    detail()
```

Notes:
- Snowflake columns are typically uppercase — match the actual schema.
- If you're unsure of columns, omit `rows` / `columns` / `values` and let auto-detection populate them.
- For very large tables, pre-aggregate in SQL or rely on `execution_mode="auto"` to flip to server-side pivoting inside the component.

#### Step 4 — Review Checklist, Then Deploy (STOP and ask)

```
Before I deploy, please confirm:

1. snowflake.yml
   - runtime.compute_pool: <COMPUTE_POOL>
   - query_warehouse: <WAREHOUSE_NAME>
   - external_access_integrations includes PYPI_ACCESS_INTEGRATION
2. requirements.txt includes streamlit>=1.51 and streamlit-pivot>=0.3.0
3. pyproject.toml dependencies include the same pins
4. streamlit_app.py imports st_pivot_table and queries <TABLE_NAMES>
5. The app role has SELECT on the target tables and USAGE on the warehouse + compute pool

Proceed with `snow streamlit deploy --replace`?
```

Only run the deploy after explicit confirmation:

```bash
snow streamlit deploy --replace
```

#### Step 5 — Verify

```bash
snow streamlit describe <entity_name>
snow streamlit log <entity_name>   # if the app fails to load
```

Open the app in Snowsight and confirm the pivot renders. The first load on a fresh compute pool will be slower while SPCS installs `streamlit-pivot` and its transitive dependencies — subsequent loads are cached.

### Stopping Points

- **Step 1** — Don't proceed until the user confirms a project exists (or should be scaffolded) and that `PYPI_ACCESS_INTEGRATION` is available.
- **Step 4** — Don't deploy until the user reviews the checklist.

### Troubleshooting

**`ModuleNotFoundError: streamlit_pivot` after deploy**
- Distribution name is `streamlit-pivot` (hyphen); module import is `streamlit_pivot` (underscore). Verify `requirements.txt` / `pyproject.toml` use the hyphenated name and `streamlit_app.py` imports the underscored module.
- `external_access_integrations` missing from `snowflake.yml` — without it, the SPCS container cannot reach PyPI.

**pip / network errors in `snow streamlit log`**
- `PYPI_ACCESS_INTEGRATION` doesn't exist in the account. Ask an admin to create it:

    ```sql
    SHOW EXTERNAL ACCESS INTEGRATIONS;
    -- If missing:
    CREATE OR REPLACE NETWORK RULE pypi_network_rule
      MODE = EGRESS TYPE = HOST_PORT
      VALUE_LIST = ('pypi.org', 'files.pythonhosted.org');
    CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION pypi_access_integration
      ALLOWED_NETWORK_RULES = (pypi_network_rule)
      ENABLED = TRUE;
    GRANT USAGE ON INTEGRATION pypi_access_integration TO ROLE <app_role>;
    ```

- The name in `snowflake.yml` must match the integration name exactly.

**`AttributeError: module 'streamlit' has no attribute 'fragment'` or similar**
- The SiS runtime is on an older Streamlit. Bump the version pin in `requirements.txt` and redeploy, or remove `@st.fragment` wrappers.

**App stuck in "Creating" / very slow first load**
- Normal on first deploy or after a compute-pool restart — SPCS is installing dependencies. Subsequent loads are cached.

**Table schema errors**
- Column names in `rows` / `columns` / `values` must match the DataFrame columns returned by the query (Snowflake is typically uppercase).
- Grant the app role `SELECT` on the target tables and `USAGE` on the warehouse and compute pool.

**Performance feels slow inside SiS**
- Wrap the SQL fetch in `@st.cache_data`.
- Wrap the pivot in `@st.fragment`.
- Let `execution_mode="auto"` pre-aggregate server-side for large tables, or pre-aggregate via SQL.
- Use `source_filters` (or a pre-filtered DataFrame) to shrink the input before pivoting.
- Consider a larger compute pool (`CPU_X64_S` or above) if the browser-side runtime is memory-pressed on 100K+ row inputs.

---

## Replicating an Existing Pivot (Excel / Sigma / Tableau / Power BI)

When a user asks you to reproduce a pivot from another tool, the hard parts aren't the obvious mappings (Row Fields → `rows`, Value Fields → `values`). They're (a) translating **format strings** from Excel/printf syntax to d3-style, and (b) recognizing **features this component doesn't support** so you can flag them explicitly instead of silently approximating.

### Format-string translation

`st_pivot_table` uses d3-style number formats (`number_format` / `dimension_format` / synthetic measure `format`). Translate source-tool formats with this cheat sheet:

| Source (Excel / Sigma / Tableau) | d3-style | Renders | Notes |
|---|---|---|---|
| `#,##0` | `,.0f` | `12,345` | Integer with thousands separator |
| `#,##0.00` | `,.2f` | `12,345.67` | Two decimals, comma-grouped |
| `$#,##0` | `$,.0f` | `$12,345` | US currency, no decimals |
| `$#,##0.00` | `$,.2f` | `$12,345.67` | US currency with cents |
| `0.0%` | `.1%` | `34.5%` | Percent, 1 decimal (value already fractional, e.g. 0.345) |
| `0.00%` | `.2%` | `34.52%` | Percent, 2 decimals |
| `0.00E+00` | `.2e` | `1.23e+4` | Scientific |
| `#,##0;(#,##0)` | `,.0f` + `color` override via `conditional_formatting` | `(12,345)` for negatives | d3 does not have built-in paren-for-negatives. Use a threshold rule `{"operator": "lt", "value": 0, "color": "#c62828"}` to color negatives red, or accept minus-sign formatting |
| `$#,##0;($#,##0)` | `$,.0f` + negative-highlight rule | `($12,345)` | Same limitation as above |
| `0` | `.0f` | `12345` | Plain integer, no grouping |
| `0.0` | `.1f` | `12345.7` | Fixed decimal, no grouping |
| Streamlit printf `"%,.2f"` | `,.2f` | `12,345.67` | Strip the leading `%`. `column_config` does this automatically. |

**Dates.** Typed `date` / `datetime` fields are formatted automatically by the date hierarchy — don't try to translate `yyyy-mm-dd` to `dimension_format`. Use `date_grains` (or `auto_date_hierarchy`) to pick the grain. For a numeric `Year` column, use `dimension_format={"Year": ".0f"}`.

### Unsupported source-tool features — flag these explicitly

Before telling the user "replicated," walk through this checklist. If any item in the source pivot matches, state the gap in your reply and offer the workaround.

| Source feature | Supported? | Workaround |
|---|---|---|
| **Running Total In / % Running Total** | No | None at the component level. Pre-compute a running-total column in Pandas (`df["cumulative"] = df.groupby(...)["Revenue"].cumsum()`) and add it as a regular value field. |
| **Rank (Smallest to Largest / Largest to Smallest)** | No | Pre-compute `df["rank"] = df["Revenue"].rank(...)` in Pandas and add as a value field. |
| **Index** (Excel's Show Values As → Index) | No | Not expressible as a single formula over aggregated fields. Compute in Pandas first. |
| **Icon sets** (Excel's green/yellow/red arrows, etc.) | No | Closest approximation: a `color_scale` with `mid_value` anchored at a neutral point, or multiple `threshold` rules with emoji-free colored backgrounds. |
| **Top N / Bottom N / Above Average conditional formatting** | No | Compute the cutoff in Pandas (`df["Revenue"].nlargest(10).min()` or `df["Revenue"].mean()`) and pass it as a `threshold` rule. |
| **Values laid out on rows** (measures on the row axis) | No | `st_pivot_table` always renders measures on the value axis inside the column hierarchy. If the source tool has measures on rows, reshape by swapping `rows` and `columns`. |
| **Per-cell manual formatting** (Excel clicking one cell and overriding) | No | Not supported. Apply rules by field (`number_format`) or by value range (`conditional_formatting`) instead. |
| **Grand-total placement (top vs bottom / left vs right)** | Not configurable | Grand totals render at the bottom of rows and the right of columns. |
| **Filter on measure values** (Excel's "Value Filters: Greater Than…") | No | `source_filters` is set-membership only. Pre-filter the DataFrame in Pandas before passing it in. |
| **Calculated items** (Excel's calculated-item feature, distinct from calculated fields) | No | Not supported. Express the logic as a `formula` synthetic measure if it reduces to aggregates, otherwise reshape source data. |

### Format-pane controls → `PivotStyle` fields

When a user shares a Sigma or Power BI format pane screenshot, map the controls to `PivotStyle` fields:

| BI tool control | `PivotStyle` field |
|---|---|
| Sigma: "Row banding" / Power BI: "Alternate rows" | `stripe_color` |
| Sigma: "Gridlines: Both / Horizontal / Vertical / None" | `borders: "all" / "rows" / "columns" / "none"` |
| Power BI: "Grid: Outline" | `borders: "outer"` |
| Sigma / Power BI: "Column headers" format pane | `column_header` region |
| Sigma / Power BI: "Values" format pane | `data_cell` + `data_cell_by_measure` |
| Sigma / Power BI: "Totals" column / row | `column_total` / `row_total` |
| Sigma / Power BI: "Subtotals" format pane | `subtotal` |
| Sigma: "Density: Compact / Standard / Comfortable" | `density: "compact" / "default" / "comfortable"` |
| Hover row highlight | `row_hover_color` |

### Replication workflow the agent should follow

1. **Inventory the source pivot.** Ask the user for rows, columns, values, aggregations, any calculated fields/measures, number formats, conditional-formatting rules, sort order, filters, and subtotal/grand-total settings. If they can share a screenshot or the `.xlsx` / workbook, use it.
2. **Translate structurally** — map to `rows`, `columns`, `values`, `aggregation`, `show_subtotals`, `show_totals`, `row_sort`, `col_sort`, `sorters`, `source_filters`.
3. **Translate calculated fields** — each becomes a `synthetic_measures` entry with `operation: "formula"`. Quote field references in double quotes inside the formula.
4. **Translate number formats** using the table above.
5. **Translate style / format-pane settings** — use the `style=PivotStyle(...)` parameter. Map BI format-pane controls using the table above. Always use `var(--st-...)` tokens or `color-mix(…)` for theme-aware colors.
6. **Translate conditional formatting** to `color_scale`, `data_bars`, or `threshold` rules. If the source uses icon sets / Top N / Above Average, apply the workarounds above.
7. **Walk the unsupported-feature checklist.** Anything that matches, tell the user explicitly — don't silently approximate.
8. **Rebuild slicers / report filters as Streamlit widgets** (see [Pattern 1](#pattern-1--sidebar--widget-filters-driving-a-pivot)) and wire them via `source_filters` or DataFrame pre-filtering.
9. **Show the final call to the user** and confirm before deploying.

---

## Quick Reference Recipes

**Interactive dashboard with drill-down and export**

```python
st_pivot_table(
    df,
    key="dash",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    show_subtotals=True,
    conditional_formatting=[
        {"type": "data_bars", "apply_to": ["Revenue"], "color": "#1976d2", "fill": "gradient"},
    ],
    enable_drilldown=True,
    export_filename="sales_dashboard",
)
```

**Read-only executive view**

```python
st_pivot_table(
    df,
    key="exec_view",
    rows=["Region"],
    values=["Revenue"],
    locked=True,
)
```

**Time-series with YoY**

```python
st_pivot_table(
    df,
    key="yoy",
    rows=["Product"],
    columns=["order_date"],
    values=["Revenue"],
    date_grains={"order_date": "month"},
    show_values_as={"Revenue": "pct_diff_from_prev_year"},
    number_format={"Revenue": "$,.0f"},
)
```

**Large dataset (Snowflake)**

```python
@st.cache_data(ttl=600)
def load():
    return get_active_session().sql("SELECT * FROM BIG_FACT").to_pandas()

@st.fragment
def pivot():
    st_pivot_table(
        load(),
        key="big",
        rows=["REGION"],
        columns=["YEAR"],
        values=["REVENUE"],
        execution_mode="auto",
    )

pivot()
```

**Sidebar filters + pivot**

```python
with st.sidebar:
    regions = st.multiselect("Region", sorted(df["Region"].unique()))

filtered = df if not regions else df[df["Region"].isin(regions)]

st_pivot_table(filtered, key="p", rows=["Region", "Category"], values=["Revenue"])
```

**Persisting user layout**

```python
saved = st.session_state.get("pivot_cfg", {})

def remember():
    st.session_state["pivot_cfg"] = st.session_state["p"].get("config")

st_pivot_table(df, key="p", on_config_change=remember, **saved)
```

---

**Styling — preset string**

```python
from streamlit_pivot import st_pivot_table

st_pivot_table(df, key="p", rows=["Region"], values=["Revenue"], style="striped")
```

**Styling — theme-aware custom style using `var(--st-...)` tokens**

```python
from streamlit_pivot import st_pivot_table, PivotStyle, RegionStyle

st_pivot_table(
    df, key="p",
    rows=["Region"], columns=["Category"], values=["Revenue"],
    style=PivotStyle(
        column_header=RegionStyle(
            background_color="color-mix(in srgb, var(--st-primary-color) 80%, var(--st-background-color))",
            text_color="var(--st-background-color)",
            font_weight="bold",
        ),
        borders="rows",
        stripe_color="color-mix(in srgb, var(--st-text-color) 5%, transparent)",
    ),
)
```

**Styling — per-measure with "totals don't inherit" caveat**

```python
from streamlit_pivot import st_pivot_table, PivotStyle, RegionStyle

# Revenue data cells tinted blue; Revenue TOTAL cells are NOT affected.
# To tint totals too, add row_total / column_total region overrides separately.
st_pivot_table(
    df, key="p",
    rows=["Region"], columns=["Category"], values=["Revenue", "Profit"],
    style=PivotStyle(
        data_cell_by_measure={
            "Revenue": RegionStyle(
                background_color="color-mix(in srgb, var(--st-primary-color) 8%, transparent)"
            ),
        }
    ),
)
```
