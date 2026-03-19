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
from streamlit_pivot_table import st_pivot_table

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
- Click any data cell — the cell coordinates will appear below the table.
- Hover over the top-right of the toolbar to reveal the **utility menu**:
  - **Reset** (↺) — resets the config to the original Python-supplied values
    (only visible when the config has been changed).
  - **Swap** (↔) — transposes row and column dimensions.
  - **Copy Config** — copies the current config as JSON to your clipboard.
  - **Import Config** — paste a JSON config to apply it.
  - **Export Data** (↓) — export the table as CSV, TSV, or copy to clipboard
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

if result_basic.get("cell_click"):
    st.info(f"Last cell click: {result_basic['cell_click']}")


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
    aggregation="sum",
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
    row_sort={"by": "value", "direction": "desc", "value_field": "Revenue"},
    col_sort={"by": "key", "direction": "asc"},
)
""",
        language="python",
    )


# ---------------------------------------------------------------------------
# Section 3: Filtering and Locked Mode
# ---------------------------------------------------------------------------
st.divider()
st.subheader("3. Filtering and Locked Mode")

st.markdown(
    """
Control which values appear via **header menu filters** or the Python API.
**Locked mode** freezes the toolbar config controls so end-users cannot change
rows, columns, values, or aggregation — but sorting and filtering via header
menus still work. **Custom sorters** enforce a specific dimension order.

**Try it (left table):**
- Click the **⋮** menu icon on the "Region" header → uncheck regions to filter them out.
- Use the search box to find specific values quickly.

**Right table** is **locked** — the toolbar config controls and utility menu
(reset, swap, import/export, settings) are all hidden, but you can still
sort and filter via the column header menus.

**API parameters used:** `hidden_from_aggregators`, `sorters`, `locked`
"""
)

col_left, col_right = st.columns(2)

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

with col_right:
    st.caption("Locked mode")
    st_pivot_table(
        df,
        key="locked",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        locked=True,
        hidden_from_aggregators=["Year", "Region"],
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
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    locked=True,
    hidden_from_aggregators=["Year", "Region"],
)
""",
        language="python",
    )


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
- Open the **Aggregation** dropdown in the toolbar — notice the **Basic** and
  **Advanced** groups.
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

Synthetic measures currently do not support **Show Values As** transformations.
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

The table container size is controlled by two parameters:
- **`max_height`** (default ``500``) — the table auto-sizes up to this height,
  then becomes scrollable. The sticky headers checkbox appears when the table
  exceeds this limit.
- **`height`** — an explicit fixed height in pixels. When set, it overrides
  ``max_height``.

**Try it:**
- The table below has `height=700` and sticky headers **disabled** — scroll
  down and notice the headers scroll away.
- Hover over the top-right of the toolbar, click the **Settings** gear icon,
  and toggle the **Sticky Headers** checkbox to re-enable.

**API parameters used:** `sticky_headers`, `height`, `max_height`
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
    height=700,
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
    height=700,
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
Export the pivot table data as **CSV**, **TSV**, or copy to **clipboard** for
pasting into Excel or Google Sheets.

**Try it:**
- Use the top-right utility menu in the toolbar.
- Click the **Download** icon (↓) to open the export popover.
- Choose a **Format**: CSV, TSV, or Clipboard.
- Choose **Content**: Formatted (display values including currency, percentages)
  or Raw (unformatted aggregated numbers).
- Click **Export** (downloads a file) or **Copy** (copies to clipboard as
  tab-separated values for easy paste into spreadsheets).

**API notes:**
- Export is always available when the toolbar is visible (``interactive=True``).
- Use ``export_filename`` to customize the downloaded file name. The date
  (``YYYY-MM-DD``) and file extension are appended automatically.
  Defaults to ``"pivot-table"`` (e.g. ``pivot-table_2026-03-09.csv``).
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
# Footer: Raw Data
# ---------------------------------------------------------------------------
st.divider()
with st.expander("View Source Data"):
    st.dataframe(df, use_container_width=True)
    st.caption(f"{len(df)} rows × {len(df.columns)} columns")
