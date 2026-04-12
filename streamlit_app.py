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

"""Streamlit Pivot Table — Feature Documentation & Examples

Community Cloud entry point. Also runnable locally:
    streamlit run streamlit_app.py
"""

from pathlib import Path
from typing import Any

import pandas as pd
from streamlit_pivot import st_pivot_table

import streamlit as st

st.set_page_config(page_title="Pivot Table — Feature Guide", layout="wide")
st.title("Streamlit Pivot Table — Feature Guide")
st.caption(
    "Each section below demonstrates a cohort of features with its own pivot table. "
    "Interact with the tables to explore the functionality described."
)

_DATA_DIR = Path(__file__).parent / "tests" / "golden_data"
df = pd.read_csv(_DATA_DIR / "small.csv")
df_medium = pd.read_csv(_DATA_DIR / "medium.csv")

# ---------------------------------------------------------------------------
# Section 1: Getting Started — Basic Pivot
# ---------------------------------------------------------------------------
st.divider()
st.subheader("1. Getting Started — Basic Pivot")

st.markdown(
    """
A basic pivot table needs three things: **row dimensions**, **column dimensions**,
and **value fields** to aggregate.

**Try it:**
- Use the **Rows / Columns / Values** dropdowns in the toolbar to add or remove fields.
- Change the **Aggregation** (e.g. Sum → Average) in the toolbar.
- Omit `rows`, `columns`, and `values` entirely to let the component auto-detect dimensions and measures.
- Click any data cell — the cell coordinates will appear below the table.
- Hover over the top-right of the toolbar to reveal the **utility menu**:
  - **Reset** (↺) — resets the config to the original Python-supplied values
    (only visible when the config has been changed).
  - **Swap** (↔) — transposes row and column dimensions.
  - **Copy Config** — copies the current config as JSON to your clipboard.
  - **Import Config** — paste a JSON config to apply it.
  - **Export Data** (↓) — export the table as Excel, CSV, TSV, or copy to clipboard
    (see Section 11 for details).
  - **Settings** (⚙) — opens a popover with display toggles (e.g. Row Totals,
    Column Totals). More options appear here as you add features — see
    Sections 4 and 10.
"""
)

result_basic = st_pivot_table(
    df,
    key="basic",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    aggregation="sum",
    show_totals=True,
    interactive=True,
    on_cell_click=lambda: None,
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="basic",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    aggregation="sum",
    show_totals=True,
    interactive=True,
)
""",
        language="python",
    )

basic_cell_click = st.session_state.get("basic", {}).get("cell_click")
if basic_cell_click:
    st.info(f"Last cell click: {basic_cell_click}")

st.markdown("#### Auto-Detect Layout")
st.markdown(
    """
If you omit `rows`, `columns`, and `values`, the component auto-detects dimensions
and measures from the input data. This is useful for quick exploration when you
want a sensible starting layout without pre-configuring the pivot.
"""
)
st_pivot_table(
    df,
    key="basic_auto_detect",
)


# ---------------------------------------------------------------------------
# Section 2: Multiple Measures and Sorting
# ---------------------------------------------------------------------------
st.divider()
st.subheader("2. Multiple Measures and Sorting")

st.markdown(
    """
Add multiple value fields to compare measures side-by-side.  Sorting lets you
rank rows or columns by label or by value.

**Try it:**
- The table shows both **Revenue** and **Profit** — notice the value label row
  below the column headers.
- Open the **Values** dropdown to see that each raw measure has its own
  aggregation control.
- Notice the selected value chips use a compact inline format like
  **Revenue (Sum)**.
- Click the **⋮** menu icon on a row/column header to open the **header menu**.
  Choose **Sort A→Z**, **Sort Z→A**, or **Sort by value ↑/↓**.
- When sorting by value, pick which measure and which column to sort against.

**API parameters used:** `row_sort`, `col_sort`
"""
)

st_pivot_table(
    df,
    key="sorting",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "avg"},
    row_sort={"by": "value", "direction": "desc", "value_field": "Revenue"},
    col_sort={"by": "key", "direction": "asc"},
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="sorting",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "avg"},
    row_sort={"by": "value", "direction": "desc", "value_field": "Revenue"},
    col_sort={"by": "key", "direction": "asc"},
)
""",
        language="python",
    )


# ---------------------------------------------------------------------------
# Section 3: Filtering, Locked Mode, and Non-Interactive Mode
# ---------------------------------------------------------------------------
st.divider()
st.subheader("3. Filtering, Locked Mode, and Non-Interactive Mode")

st.markdown(
    """
Control which values appear via **header menu filters** or the Python API.
**Locked mode** freezes the toolbar config controls so end-users cannot change
rows, columns, values, or per-measure aggregation — but sorting and filtering via header
menus still work, **Show Values As** remains available on value headers, and
export still stays available as a viewer action. **Custom sorters** enforce a specific dimension order.

**Non-interactive mode** (`interactive=False`) is the true read-only mode:
the toolbar is hidden, header-menu config actions are disabled, but cell clicks
and drill-down still work.

**Try it (left table):**
- Click the **⋮** menu icon on the "Region" header → uncheck regions to filter them out.
- Use the search box to find specific values quickly.

**Middle table** is **locked** — authoring actions like reset, swap, and config import/export
are hidden, but **Export Data** remains available, the **Settings** gear shows
read-only view status plus **Expand/Collapse All** group controls, and you can
still sort, filter, and change **Show Values As** from the header menus.

**Right table** is **non-interactive** — there is no toolbar and no header-menu
config UI, but cell clicks still work and drill-down remains enabled.

**API parameters used:** `hidden_from_aggregators`, `sorters`, `locked`, `interactive`
"""
)

col_left, col_middle, col_right = st.columns(3)

with col_left:
    st.caption("Interactive (with custom sorters)")
    st_pivot_table(
        df,
        key="filtering",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        sorters={"Region": ["North", "South", "East", "West"]},
        null_handling="zero",
    )

with col_middle:
    st.caption("Locked mode")
    st_pivot_table(
        df,
        key="locked",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        locked=True,
        hidden_from_aggregators=["Year", "Region"],
        show_subtotals=True,
    )

with col_right:
    st.caption("Non-interactive mode")
    result_noninteractive = st_pivot_table(
        df,
        key="noninteractive",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        interactive=False,
        on_cell_click=lambda: None,
    )

with st.expander("View Code"):
    st.code(
        """
# Interactive with custom dimension ordering
st_pivot_table(
    df,
    key="filtering",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    sorters={"Region": ["North", "South", "East", "West"]},
    null_handling="zero",
)

# Locked — users cannot change configuration
st_pivot_table(
    df,
    key="locked",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    locked=True,
    hidden_from_aggregators=["Year", "Region"],
    show_subtotals=True,
)

# Non-interactive — no toolbar or header-menu config actions
st_pivot_table(
    df,
    key="noninteractive",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    interactive=False,
    on_cell_click=lambda: None,
)
""",
        language="python",
    )

noninteractive_cell_click = st.session_state.get("noninteractive", {}).get("cell_click")
if noninteractive_cell_click:
    st.info(f"Non-interactive cell click: {noninteractive_cell_click}")


# ---------------------------------------------------------------------------
# Section 4: Subtotals and Grouping
# ---------------------------------------------------------------------------
st.divider()
st.subheader("4. Subtotals and Grouping")

st.markdown(
    """
When you have **two or more row dimensions**, enable subtotals to see
aggregated values at each group level.

**Try it:**
- Each **Region** group has a subtotal row (e.g. "North Total") with a
  **collapse/expand toggle** (+/−).
- Click the toggle to collapse a group — its child rows are hidden but the
  subtotal stays visible.
- **Dimension-level collapse:** Click the **dimension header** label (e.g.
  "Region ›") to collapse or expand **all groups at that level** at once.
  This is available on both row and column headers when there are 2+ dimensions.
- Click the **Settings** gear icon (⚙) in the utility menu to open the display
  toggles. Use **Expand All** / **Collapse All** buttons, and toggle the
  **Subtotals** and **Repeat Labels** checkboxes.

**API parameters used:** `show_subtotals`, `repeat_row_labels`
"""
)

st_pivot_table(
    df,
    key="subtotals",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    aggregation="sum",
    show_subtotals=True,
    repeat_row_labels=False,
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="subtotals",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    show_subtotals=True,
    repeat_row_labels=False,
)
""",
        language="python",
    )

st.markdown("#### Per-Dimension Subtotals")
st.markdown(
    """
Pass a list of dimension names to `show_subtotals` to control which levels get subtotal rows.
Only listed dimensions will have subtotals; others are skipped.
"""
)
st_pivot_table(
    df_medium,
    key="demo_per_dim_subtotals",
    rows=["Region", "Category", "Product"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    show_subtotals=["Region"],
)

st.markdown("#### Per-Measure Row Totals")
st.markdown(
    """
Pass a list of measure names to `show_row_totals` to control which measures appear
in the rightmost Total column. Excluded measures show `–` (dash).
"""
)
st_pivot_table(
    df,
    key="demo_per_measure_row_totals",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    show_row_totals=["Revenue"],
)

st.markdown("#### Per-Measure Column Totals")
st.markdown(
    """
Pass a list of measure names to `show_column_totals` to control which measures appear
in the Grand Total row. Excluded measures show `–` (dash) in the Grand Total.
Subtotals are independent — they still show all measures regardless of this setting.
"""
)
st_pivot_table(
    df_medium,
    key="demo_per_measure_col_totals",
    rows=["Region", "Category", "Product"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    show_column_totals=["Revenue"],
    show_subtotals=True,
)


# ---------------------------------------------------------------------------
# Section 5: Advanced Aggregators and Show Values As
# ---------------------------------------------------------------------------
st.divider()
st.subheader("5. Advanced Aggregators and Show Values As")

st.markdown(
    """
Beyond Sum, Average, Count, Min, Max — the component supports **advanced
aggregators**: Count Distinct, Median, Percentile (90th), First, and Last.

**Show Values As** lets you display measures as **% of Grand Total**,
**% of Row Total**, or **% of Column Total** instead of raw numbers.

**Try it:**
- Open the **Values** dropdown in the toolbar and use each measure's own
  aggregation control.
- Selected value chips show aggregation inline, using the same name-first
  pattern as **Revenue (Sum)**.
- Click the **⋮** menu icon on a **value label** header (e.g. "Revenue") to open
  its **Display** menu. Choose between Raw Value, % of Grand Total, % of Row
  Total, or % of Column Total.
- When a % mode is active, a small **%** badge appears on the toolbar chip.

**API parameters used:** `aggregation`, `show_values_as`
"""
)

st_pivot_table(
    df,
    key="advanced_agg",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation="sum",
    show_values_as={"Revenue": "pct_of_total"},
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="advanced_agg",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation="sum",
    show_values_as={"Revenue": "pct_of_total"},
)
""",
        language="python",
    )


# ---------------------------------------------------------------------------
# Section 6: Conditional Formatting
# ---------------------------------------------------------------------------
st.divider()
st.subheader("6. Conditional Formatting")

st.markdown(
    """
Apply visual formatting rules to cells based on their values. Three rule types
are supported:

| Rule Type | Description |
|-----------|-------------|
| **Color Scale** | Gradient between 2 or 3 colors based on min/mid/max |
| **Data Bars** | Horizontal bar fill proportional to the cell value |
| **Threshold** | Highlight cells that meet a numeric condition (>, <, =, etc.) |

**Try it:**
- Revenue cells show **data bars** — a gradient fill proportional to the cell value.
- Profit cells show a **green color scale** — a dark-to-bright gradient.
- Units cells above 250 are highlighted in **bold blue** (threshold rule).
- Text color auto-adjusts for readability against colored backgrounds.
- Rules are configured via the Python API (not interactive yet).
- **Excel export** preserves these rules as native Excel conditional formatting —
  use the Download icon (↓) to export and open in Excel or Google Sheets.

**API parameter used:** `conditional_formatting`
"""
)

cond_fmt_rules: list[dict[str, Any]] = [
    {
        "type": "data_bars",
        "apply_to": ["Revenue"],
        "color": "#1976d2",
        "fill": "gradient",
    },
    {
        "type": "color_scale",
        "apply_to": ["Profit"],
        "min_color": "#1b2e1b",
        "max_color": "#4caf50",
    },
    {
        "type": "threshold",
        "apply_to": ["Units"],
        "conditions": [
            {"operator": "gt", "value": 250, "background": "#1565c0", "bold": True},
        ],
    },
]

st_pivot_table(
    df,
    key="cond_fmt",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit", "Units"],
    aggregation="sum",
    conditional_formatting=cond_fmt_rules,
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="cond_fmt",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit", "Units"],
    conditional_formatting=[
        {
            "type": "data_bars",
            "apply_to": ["Revenue"],
            "color": "#1976d2",
            "fill": "gradient",
        },
        {
            "type": "color_scale",
            "apply_to": ["Profit"],
            "min_color": "#1b2e1b",
            "max_color": "#4caf50",
        },
        {
            "type": "threshold",
            "apply_to": ["Units"],
            "conditions": [
                {"operator": "gt", "value": 250,
                 "background": "#1565c0", "bold": True},
            ],
        },
    ],
)
""",
        language="python",
    )


# ---------------------------------------------------------------------------
# Section 7: Number Formatting and Layout
# ---------------------------------------------------------------------------
st.divider()
st.subheader("7. Number Formatting and Layout")

st.markdown(
    """
Control how numbers are displayed and align columns.

| Format Pattern | Example Output | Description |
|----------------|---------------|-------------|
| `$,.0f` | $12,345 | Currency, no decimals |
| `,.2f` | 12,345.67 | Comma-grouped, 2 decimals |
| `.1%` | 34.5% | Percentage with 1 decimal |

**Try it:**
- Revenue is formatted as **currency** (`$,.0f`), Profit uses **two decimals** (`,.2f`).
- Both columns are **right-aligned** for numeric readability.
- Other currency patterns: `$EUR,.2f` (Euro), `€,.2f` (Euro via symbol), `£,.0f` (GBP), `¥,.0f` (JPY).

**API parameters used:** `number_format`, `column_alignment`
"""
)

st_pivot_table(
    df,
    key="formatting",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation="sum",
    number_format={"Revenue": "$,.0f", "Profit": ",.2f"},
    column_alignment={"Revenue": "right", "Profit": "right"},
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="formatting",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    number_format={"Revenue": "$,.0f", "Profit": ",.2f"},
    column_alignment={"Revenue": "right", "Profit": "right"},
)
""",
        language="python",
    )


# ---------------------------------------------------------------------------
# Section 8: Column Group Collapse/Expand
# ---------------------------------------------------------------------------
st.divider()
st.subheader("8. Column Group Collapse/Expand")

st.markdown(
    """
When you have **two or more column dimensions**, column groups can be
collapsed just like row groups.

**Try it:**
- The table below pivots by **Region** (rows) and **Year × Category** (columns).
- Hover over a **Year** column header — a **−** toggle appears.
- Click it to **collapse** that year's sub-columns into a single subtotal column.
- Click **+** to expand again.

**Note:** Column collapse/expand works alongside row subtotals and row grouping.
"""
)

st_pivot_table(
    df,
    key="col_collapse",
    rows=["Region"],
    columns=["Year", "Category"],
    values=["Revenue"],
    aggregation="sum",
    show_totals=True,
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="col_collapse",
    rows=["Region"],
    columns=["Year", "Category"],
    values=["Revenue"],
    aggregation="sum",
    show_totals=True,
)
""",
        language="python",
    )


# ---------------------------------------------------------------------------
# Section 9: Synthetic Measures (V1)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("9. Synthetic Measures (V1)")

st.markdown(
    """
Synthetic measures let you combine source metrics using fixed operations while
still keeping regular measures in the same table.

**V1 operations:**
- **Ratio of sums**: `sum(A) / sum(B)`
- **Difference of sums**: `sum(A) - sum(B)`

**Try it:**
- Keep **Total PRs** as a regular measure.
- Add synthetic measures in the **Values** panel using **+ Add measure**.
- Compare **PRs / Person** and **PRs - People** alongside raw values.
- Notice denominator-zero cells render as `–`.
- In the builder, try **Format presets** (Percent/Currency/Number) or enter a custom format pattern.

Synthetic measures currently do not support **Show Values As** transformations,
and they continue to use sum-based source semantics even if raw measures in the
same pivot use different aggregations.
"""
)

df_synth_demo = pd.DataFrame(
    {
        "Region": ["East", "East", "West", "West", "North"],
        "Year": [2023, 2024, 2023, 2024, 2024],
        "Total PRs": [20, 30, 10, 25, 5],
        "People": [5, 0, 2, 5, 1],
    }
)

st_pivot_table(
    df_synth_demo,
    key="synthetic_measures_v1",
    rows=["Region"],
    columns=["Year"],
    values=["Total PRs", "People"],
    aggregation={"Total PRs": "sum", "People": "count"},
    synthetic_measures=[
        {
            "id": "prs_per_person",
            "label": "PRs / Person",
            "operation": "sum_over_sum",
            "numerator": "Total PRs",
            "denominator": "People",
            "format": ".1%",
        },
        {
            "id": "prs_minus_people",
            "label": "PRs - People",
            "operation": "difference",
            "numerator": "Total PRs",
            "denominator": "People",
            "format": ",.1f",
        },
    ],
    number_format={"Total PRs": ",.0f", "People": ",.0f", "__all__": ",.2f"},
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df_synth_demo,
    key="synthetic_measures_v1",
    rows=["Region"],
    columns=["Year"],
    values=["Total PRs", "People"],
    aggregation={"Total PRs": "sum", "People": "count"},
    synthetic_measures=[
        {
            "id": "prs_per_person",
            "label": "PRs / Person",
            "operation": "sum_over_sum",
            "numerator": "Total PRs",
            "denominator": "People",
            "format": ".1%",
        },
        {
            "id": "prs_minus_people",
            "label": "PRs - People",
            "operation": "difference",
            "numerator": "Total PRs",
            "denominator": "People",
            "format": ",.1f",
        },
    ],
)
""",
        language="python",
    )


# ---------------------------------------------------------------------------
# Section 10: Sticky Headers, Height, and Max Height
# ---------------------------------------------------------------------------
st.divider()
st.subheader("10. Sticky Headers, Height, and Max Height")

st.markdown(
    """
By default, column headers **stick** to the top of the table as you scroll.
You can disable this behavior with `sticky_headers=False` or toggle it at
runtime via the **Sticky Headers** checkbox in the **Settings** popover (gear icon in the utility menu).

The table container size is controlled by `max_height` (default ``500``).
The table auto-sizes up to this limit, then becomes scrollable with sticky
headers. The sticky headers checkbox appears when content exceeds this limit.

**Try it:**
- The table below has `max_height=700` and sticky headers **disabled** — scroll
  down and notice the headers scroll away.
- Hover over the top-right of the toolbar, click the **Settings** gear icon,
  and toggle the **Sticky Headers** checkbox to re-enable.

**API parameters used:** `sticky_headers`, `max_height`
"""
)

st_pivot_table(
    df,
    key="sticky_off",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation="sum",
    sticky_headers=False,
    show_subtotals=True,
    max_height=700,
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="sticky_off",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    sticky_headers=False,
    show_subtotals=True,
    max_height=700,
)
""",
        language="python",
    )


# ---------------------------------------------------------------------------
# Section 11: Data Export
# ---------------------------------------------------------------------------
st.divider()
st.subheader("11. Data Export")

st.markdown(
    """
Export the pivot table data as **Excel** (.xlsx), **CSV**, **TSV**, or copy to
**clipboard** for pasting into spreadsheets.

Excel export produces a professionally styled workbook with merged column
headers, bold totals/subtotals, number formatting, banded rows, and frozen
panes — matching the quality you'd expect from BI tools like Sigma.
Conditional formatting rules (color scales, data bars, and threshold
highlights) are translated to native Excel conditional formatting — try
exporting the table in **Section 6** to see this in action.

**Try it:**
- Use the top-right utility menu in the toolbar.
- Click the **Download** icon (↓) to open the export popover.
- Choose a **Format**: Excel, CSV, TSV, or Clipboard.
- Choose **Content**: Formatted (display values including currency, percentages)
  or Raw (unformatted aggregated numbers).
- Click **Export** (downloads a file) or **Copy** (copies to clipboard as
  tab-separated values for easy paste into spreadsheets).

**API notes:**
- Export is always available when the toolbar is visible (``interactive=True``).
- Use ``export_filename`` to customize the downloaded file name. The date
  (``YYYY-MM-DD``) and file extension are appended automatically.
  Defaults to ``"pivot-table"`` (e.g. ``pivot-table_2026-03-09.xlsx``).
- This demo sets `export_filename="sales-export-demo"` so you can see the custom
  filename behavior in the downloaded file.
"""
)

st_pivot_table(
    df,
    key="export_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation="sum",
    number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
    show_totals=True,
    interactive=True,
    export_filename="sales-export-demo",
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="export_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
    show_totals=True,
    export_filename="sales-export-demo",
)
# Then use the Download icon in the toolbar utility menu.
""",
        language="python",
    )

# ---------------------------------------------------------------------------
# Section 12: Drill-Down Detail Panel
# ---------------------------------------------------------------------------
st.divider()
st.subheader("12. Drill-Down Detail Panel")

st.markdown(
    """
Click any **data cell** (or total cell) to open an inline **drill-down panel**
below the pivot table. The panel displays the **source records** that
contributed to the clicked cell's aggregated value.

**Try it:**
- Click any numeric cell in the table below.
- A detail panel slides in below the table showing all matching source records.
- The header shows the dimension filters (e.g. "Region: East, Year: 2023")
  and the record count.
- Click the **✕** button or press **Escape** to close the panel.
- Click a different cell to replace the panel with new records.

**API parameter used:** `enable_drilldown` (default ``True``)

Set ``enable_drilldown=False`` to disable the drill-down panel (the
``on_cell_click`` callback still fires).
"""
)

result_drilldown = st_pivot_table(
    df,
    key="drilldown_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    aggregation="sum",
    show_totals=True,
    enable_drilldown=True,
    on_cell_click=lambda: None,
)

with st.expander("View Code"):
    st.code(
        """
result = st_pivot_table(
    df,
    key="drilldown_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    enable_drilldown=True,
    on_cell_click=lambda: None,
)
# Click any cell to see the contributing source records.
""",
        language="python",
    )

# ---------------------------------------------------------------------------
# Section 13: Grouping Hierarchy and Sorting
# ---------------------------------------------------------------------------
st.divider()
st.subheader("13. Grouping Hierarchy and Scoped Sorting")

st.markdown(
    """
When **subtotals** are enabled with multiple row dimensions, the component
treats the data as a **grouped hierarchy**:

- **Grouping dimensions** (all but the innermost row) define collapsible
  groups. Their cells use bolder styling and a subtle background tint.
- The **leaf dimension** (the innermost row) shows detail data, visually
  indented within its parent group.
- **Group boundary borders** appear between consecutive data rows that belong
  to different groups, giving clear visual separation.
- **Scoped sorting:** When you sort by value from a specific dimension header,
  only that level and its children reorder — parent groups stay put.

Compare the tables below to see the hierarchy in action.
"""
)

hier_cols = st.columns(2)

with hier_cols[0]:
    st.markdown("**Subtotals ON — 3-level hierarchy**")
    st_pivot_table(
        df_medium,
        key="hier_subtotals_on",
        rows=["Region", "Category", "Product"],
        columns=[],
        values=["Revenue"],
        aggregation="sum",
        show_subtotals=True,
        show_totals=True,
        max_height=500,
    )

with hier_cols[1]:
    st.markdown("**Subtotals OFF — flat table for comparison**")
    st_pivot_table(
        df_medium,
        key="hier_subtotals_off",
        rows=["Region", "Category", "Product"],
        columns=[],
        values=["Revenue"],
        aggregation="sum",
        show_subtotals=False,
        show_totals=True,
        max_height=500,
    )

st.markdown("---")
st.markdown("**Scoped value sort — sort Category desc, Regions stay in place**")
st_pivot_table(
    df_medium,
    key="hier_scoped_sort",
    rows=["Region", "Category", "Product"],
    columns=[],
    values=["Revenue"],
    aggregation="sum",
    show_subtotals=True,
    show_totals=True,
    row_sort={
        "by": "value",
        "direction": "desc",
        "value_field": "Revenue",
        "dimension": "Category",
    },
    max_height=500,
)

st.markdown(
    """
↑ **What to observe:** Region groups are in their default (ascending by
subtotal) order. Categories *within* each Region are sorted descending by
Revenue subtotal. Products within each Category are also descending.

Compare with the **global** value sort below — all levels sort descending:
"""
)

st_pivot_table(
    df_medium,
    key="hier_global_sort",
    rows=["Region", "Category", "Product"],
    columns=[],
    values=["Revenue"],
    aggregation="sum",
    show_subtotals=True,
    show_totals=True,
    row_sort={"by": "value", "direction": "desc", "value_field": "Revenue"},
    max_height=500,
)

st.markdown("---")
st.markdown("**Per-dimension subtotals — only Region grouped**")
st_pivot_table(
    df_medium,
    key="hier_partial_subtotals",
    rows=["Region", "Category", "Product"],
    columns=[],
    values=["Revenue"],
    aggregation="sum",
    show_subtotals=["Region"],
    show_totals=True,
    max_height=500,
)

st.markdown(
    """
↑ Only Region has subtotals. Category and Product are both leaf attributes
within each Region group. The Region column gets grouping-dimension styling
while Category and Product are plain detail data.
"""
)

with st.expander("View Code"):
    st.code(
        """
# 3-level hierarchy with full subtotals
st_pivot_table(
    df_medium,
    key="hier_subtotals_on",
    rows=["Region", "Category", "Product"],
    values=["Revenue"],
    show_subtotals=True,
)

# Scoped value sort: Category desc, Regions stay in default order
st_pivot_table(
    df_medium,
    key="hier_scoped_sort",
    rows=["Region", "Category", "Product"],
    values=["Revenue"],
    show_subtotals=True,
    row_sort={
        "by": "value",
        "direction": "desc",
        "value_field": "Revenue",
        "dimension": "Category",  # <-- scoped to Category level
    },
)

# Per-dimension subtotals: only Region level
st_pivot_table(
    df_medium,
    key="hier_partial_subtotals",
    rows=["Region", "Category", "Product"],
    values=["Revenue"],
    show_subtotals=["Region"],
)
""",
        language="python",
    )

# ---------------------------------------------------------------------------
# Section 14: Server-Side Drill-Down (Hybrid Mode)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("14. Server-Side Drill-Down (Hybrid Mode)")

st.markdown(
    """
When datasets are large enough to trigger **threshold_hybrid** mode, the pivot
data is pre-aggregated on the server before being sent to the browser. In this
mode, drill-down works via a **server round-trip**: clicking a cell sends a
request to Python, which filters the *original* un-aggregated DataFrame and
returns the matching rows.

Because large cells can match thousands of rows, the results are **paginated**
(500 rows per page) with **Prev / Next** controls.

**Try it:**
- Click any data cell — after a brief round-trip the drill-down panel appears.
- If the cell has more than 500 matching rows, use the **← Prev / Next →**
  buttons at the bottom of the panel to page through all results.
- The header shows a range like "1–500 of 2,340 records" and the current page.

**API parameter used:** `execution_mode` (set to `"threshold_hybrid"` here to
force hybrid mode on a smaller dataset for demonstration purposes)
"""
)

import numpy as np  # noqa: E402

_rng = np.random.default_rng(42)
_n = 50_000
df_hybrid = pd.DataFrame(
    {
        "Region": _rng.choice(["North", "South", "East", "West"], _n),
        "Category": _rng.choice(
            ["Electronics", "Clothing", "Food", "Furniture", "Toys"], _n
        ),
        "Year": _rng.choice([2022, 2023, 2024], _n),
        "Channel": _rng.choice(["Online", "Retail", "Wholesale"], _n),
        "Revenue": _rng.uniform(10, 5000, _n).round(2),
        "Profit": _rng.uniform(-500, 2000, _n).round(2),
    }
)

st_pivot_table(
    df_hybrid,
    key="hybrid_drilldown_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
    show_totals=True,
    show_subtotals=True,
    enable_drilldown=True,
    execution_mode="threshold_hybrid",
)

with st.expander("View Code"):
    st.code(
        """
import numpy as np

rng = np.random.default_rng(42)
n = 50_000
df_hybrid = pd.DataFrame({
    "Region": rng.choice(["North", "South", "East", "West"], n),
    "Category": rng.choice(["Electronics", "Clothing", "Food", "Furniture", "Toys"], n),
    "Year": rng.choice([2022, 2023, 2024], n),
    "Channel": rng.choice(["Online", "Retail", "Wholesale"], n),
    "Revenue": rng.uniform(10, 5000, n).round(2),
    "Profit": rng.uniform(-500, 2000, n).round(2),
})

st_pivot_table(
    df_hybrid,
    key="hybrid_drilldown_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
    show_totals=True,
    show_subtotals=True,
    enable_drilldown=True,
    execution_mode="threshold_hybrid",
)
# Click any cell to see paginated server-side drill-down.
""",
        language="python",
    )

st.caption(
    f"Dataset: {len(df_hybrid):,} rows × {len(df_hybrid.columns)} columns — "
    "forced to threshold_hybrid mode for demonstration."
)


# ---------------------------------------------------------------------------
# Section 15: Drag-and-Drop Field Configuration
# ---------------------------------------------------------------------------
st.divider()
st.subheader("15. Drag-and-Drop Field Configuration")

st.markdown(
    """
Each chip in the **Rows**, **Columns**, and **Values** toolbar zones has a
**grip-dots drag handle** on its left side. Use it to:

- **Reorder within a zone** — change the grouping hierarchy (e.g., swap
  which dimension is the outer vs. inner group).
- **Move between zones** — drag a Row chip into Columns, or move a numeric
  field into Values. Non-numeric fields are rejected from the Values zone.

**Try it** on the pivot below (two row dimensions so you can reorder):
- Drag the grip handle on **Category** and drop it before **Region** to
  reverse the grouping order.
- Drag **Category** from Rows into the Columns zone.
- Drag a numeric chip into/out of Values.

Frozen columns (set via `frozen_columns`) cannot be dragged. When `locked=True`,
drag-and-drop is fully disabled.
"""
)

st_pivot_table(
    df,
    key="drag_and_drop_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    show_totals=True,
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="drag_and_drop_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    show_totals=True,
)
# Drag the grip-dots handle on any chip to reorder or move between zones.
""",
        language="python",
    )

# ---------------------------------------------------------------------------
# Section 16: Column Resize
# ---------------------------------------------------------------------------
st.divider()
st.subheader("16. Column Resize")

st.markdown(
    """
Drag the **right edge of any column header** to resize that column.
The resize handle appears as a thin highlight strip when you hover the
column border. Minimum width is 40 px. Works in both virtualized and
non-virtualized rendering modes.

**Try it:**
- Hover the right edge of a column header until the cursor changes to
  a **col-resize** handle.
- Drag left or right to change the column width.
- Resize multiple columns — each column remembers its width independently.
- Widths reset when the pivot config changes (new rows, columns, values, etc.).

Column resize is a **purely frontend interaction** — no Python API parameter
is required.
"""
)

st_pivot_table(
    df,
    key="column_resize_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
    show_totals=True,
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="column_resize_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
    show_totals=True,
)
# Drag any column header edge to resize.
""",
        language="python",
    )

# ---------------------------------------------------------------------------
# Section 17: Fullscreen Mode
# ---------------------------------------------------------------------------
st.divider()
st.subheader("17. Fullscreen Mode")

st.markdown(
    """
Click the **expand icon** (⤢) in the toolbar utility menu to enter
**fullscreen mode**. The pivot table fills the entire browser viewport
as a fixed overlay. Press **Escape** or click the **collapse icon** (⤡)
to exit.

**Try it:**
- Hover over the toolbar area to reveal the utility buttons.
- Click the expand icon (rightmost group, before the gear icon).
- The table expands to fill the full viewport — virtual scrolling
  automatically adjusts to the new height.
- Press **Escape** or click the collapse icon to return to normal view.

Fullscreen mode is a **purely frontend interaction** — no Python API
parameter is required. It works with both virtualized and non-virtualized
tables.
"""
)

st_pivot_table(
    df,
    key="fullscreen_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
    show_totals=True,
    show_subtotals=True,
    max_height=350,
)

with st.expander("View Code"):
    st.code(
        """
st_pivot_table(
    df,
    key="fullscreen_demo",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
    show_totals=True,
    show_subtotals=True,
    max_height=350,
)
# Click the expand icon in the toolbar to go fullscreen.
# Press Escape to exit.
""",
        language="python",
    )

# ---------------------------------------------------------------------------
# Footer: Raw Data
# ---------------------------------------------------------------------------
st.divider()
with st.expander("View Source Data"):
    st.dataframe(df, width="stretch")
    st.caption(f"{len(df)} rows × {len(df.columns)} columns")
