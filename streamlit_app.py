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

import datetime as _dt
import random as _rnd
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from streamlit_pivot import st_pivot_table, PivotStyle, RegionStyle, PIVOT_STYLE_PRESETS

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
df_field_search = df_medium.assign(
    Cost=(df_medium["Revenue"] - df_medium["Profit"]).round(2),
    RevenuePerUnit=(df_medium["Revenue"] / df_medium["Units"]).round(2),
    DiscountAmount=(df_medium["Revenue"] * df_medium["Discount"]).round(2),
    ProfitMargin=(df_medium["Profit"] / df_medium["Revenue"]).round(3),
)

_rnd.seed(42)
_date_regions = ["US", "EU", "APAC"]
_date_base = {
    "US": [120, 110, 135, 150, 160, 175, 190, 185, 170, 155, 140, 130],
    "EU": [90, 85, 100, 115, 125, 140, 150, 145, 130, 120, 105, 95],
    "APAC": [70, 65, 80, 95, 105, 120, 130, 125, 110, 100, 85, 75],
}
_date_records = []
for year in [2023, 2024]:
    for month_idx in range(12):
        for region in _date_regions:
            base = _date_base[region][month_idx]
            growth = 1.08 if year == 2024 else 1.0
            rev = int(base * growth + _rnd.randint(-10, 10))
            d = _dt.date(year, month_idx + 1, 1)
            _date_records.append(
                {
                    "region": region,
                    "order_date": d,
                    "ship_date": d + _dt.timedelta(days=3),
                    "Revenue": rev,
                }
            )
df_dates = pd.DataFrame(_date_records)

# ---------------------------------------------------------------------------
# Section 1: Getting Started — Basic Pivot
# ---------------------------------------------------------------------------
st.divider()
st.subheader("1. Getting Started — Basic Pivot")


@st.fragment
def section_basic():
    st.markdown(
        """
A basic pivot table needs three things: **row dimensions**, **column dimensions**,
and **value fields** to aggregate.

**Try it:**
- Click the **Settings** icon in the toolbar utility menu to open the **Settings Panel**
  — this is the primary surface for adding, removing, and rearranging fields.
  Make changes, then click **Apply** (or **Cancel** / Escape to discard).
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
  - **Settings** — opens the full Settings Panel for field configuration,
    aggregation, synthetic measures, and display toggles (see Section 19).
"""
    )

    st_pivot_table(
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

    st.markdown("#### Field Search in Settings Panel")
    st.markdown(
        """
When the Settings Panel has more than **8 available fields**, a search input
appears at the top of the **Available Fields** section. This wider demo includes
enough dimensions and numeric measures to surface the search.

**Try it:**
- Open the **Settings Panel** (pivot icon) and type part of a field name in the
  search box above the Available Fields chips.
- The chip list filters in place while the container maintains its original height.
"""
    )
    st_pivot_table(
        df_field_search,
        key="basic_field_search",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        interactive=True,
        show_totals=True,
    )


section_basic()

# ---------------------------------------------------------------------------
# Section 2: Multiple Measures and Sorting
# ---------------------------------------------------------------------------
st.divider()
st.subheader("2. Multiple Measures and Sorting")


@st.fragment
def section_sorting():
    st.markdown(
        """
Add multiple value fields to compare measures side-by-side.  Sorting lets you
rank rows or columns by label or by value.

**Try it:**
- The table shows both **Revenue** and **Profit** — notice the value label row
  below the column headers.
- Open the **Settings Panel** (pivot icon) to see the Values zone with each
  measure's aggregation control (click the badge, e.g. "Sum", to change it).
- Notice the toolbar value chips use a compact inline format like
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


section_sorting()

# ---------------------------------------------------------------------------
# Section 3: Filtering, Locked Mode, and Non-Interactive Mode
# ---------------------------------------------------------------------------
st.divider()
st.subheader("3. Filtering, Locked Mode, and Non-Interactive Mode")


@st.fragment
def section_filtering():
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
are hidden, but **Export Data** remains available, and you can
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
        st_pivot_table(
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

    noninteractive_cell_click = st.session_state.get("noninteractive", {}).get(
        "cell_click"
    )
    if noninteractive_cell_click:
        st.info(f"Non-interactive cell click: {noninteractive_cell_click}")


section_filtering()

# ---------------------------------------------------------------------------
# Section 4: Subtotals and Grouping
# ---------------------------------------------------------------------------
st.divider()
st.subheader("4. Subtotals and Grouping")


@st.fragment
def section_subtotals():
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
- Use the **Expand All** / **Collapse All** buttons in the toolbar utility menu.
- Open the **Settings Panel** (pivot icon) and toggle **Subtotals** and
  **Repeat Labels** checkboxes, then click **Apply**.

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


section_subtotals()

# ---------------------------------------------------------------------------
# Section 5: Advanced Aggregators and Show Values As
# ---------------------------------------------------------------------------
st.divider()
st.subheader("5. Advanced Aggregators and Show Values As")


@st.fragment
def section_aggregators():
    st.markdown(
        """
Beyond Sum, Average, Count, Min, Max — the component supports **advanced
aggregators**: Count Distinct, Median, Percentile (90th), First, and Last.

**Show Values As** lets you display measures as **% of Grand Total**,
**% of Row Total**, or **% of Column Total** instead of raw numbers.

**Try it:**
- Open the **Settings Panel** (pivot icon) and click the aggregation badge on a
  value chip to switch between Sum, Average, Count, Min, Max, and advanced
  aggregators like Median, Percentile 90, Count Distinct, First, Last.
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


section_aggregators()

# ---------------------------------------------------------------------------
# Section 5b: Show Values As — Analytical Modes (0.5.0)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("5b. Show Values As — Running Total, Rank, % of Parent (0.5.0)")


@st.fragment
def section_show_values_as_analytical():
    import pandas as pd

    sva_df = pd.DataFrame(
        {
            "Region": ["US", "US", "US", "EU", "EU", "EU", "APAC", "APAC", "APAC"],
            "Product": [
                "Widget",
                "Gadget",
                "Doohickey",
                "Widget",
                "Gadget",
                "Doohickey",
                "Widget",
                "Gadget",
                "Doohickey",
            ],
            "Quarter": [
                "Q1",
                "Q1",
                "Q1",
                "Q1",
                "Q1",
                "Q1",
                "Q1",
                "Q1",
                "Q1",
            ],
            "Revenue": [120, 80, 40, 200, 150, 50, 90, 60, 30],
        }
    )

    st.markdown(
        """
**0.5.0** adds five new analytical **Show Values As** modes, accessible via the
**⋮** menu on a value header or via the `show_values_as` API parameter:

| Mode | Description |
|---|---|
| `running_total` | Cumulative sum along the row axis; resets per parent group |
| `pct_running_total` | Running total ÷ parent-group total for the same column |
| `rank` | Competition rank (1, 1, 3) per column, per parent group |
| `pct_of_parent` | Cell ÷ immediate parent subtotal |
| `index` | Excel INDEX formula: `cell × grand_total / (row_total × col_total)` |

The example below shows **running total** by product within each region.
**Totals and subtotals always display raw aggregates** — a running total at a
subtotal equals the subtotal itself, so transforming it would be misleading.
"""
    )

    col1, col2 = st.columns(2)
    with col1:
        st.caption("Running Total")
        st_pivot_table(
            sva_df,
            key="sva_running_total",
            rows=["Region", "Product"],
            columns=["Quarter"],
            values=["Revenue"],
            show_values_as={"Revenue": "running_total"},
        )
    with col2:
        st.caption("% of Parent Subtotal")
        st_pivot_table(
            sva_df,
            key="sva_pct_of_parent",
            rows=["Region", "Product"],
            columns=["Quarter"],
            values=["Revenue"],
            show_values_as={"Revenue": "pct_of_parent"},
        )

    with st.expander("View Code"):
        st.code(
            """
# Running total — resets per Region group
st_pivot_table(
    df,
    rows=["Region", "Product"],
    columns=["Quarter"],
    values=["Revenue"],
    show_values_as={"Revenue": "running_total"},
)

# % of Parent — each Product ÷ its Region subtotal
st_pivot_table(
    df,
    rows=["Region", "Product"],
    columns=["Quarter"],
    values=["Revenue"],
    show_values_as={"Revenue": "pct_of_parent"},
)
""",
            language="python",
        )


section_show_values_as_analytical()

# ---------------------------------------------------------------------------
# Section 5c: Top N / Value Filters (0.5.0)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("5c. Top N / Bottom N and Value Filters (0.5.0)")


@st.cache_data
def _section_filters_data():
    import pandas as pd
    import numpy as np

    rng = np.random.default_rng(42)
    regions = ["North", "South", "East", "West", "Central"]
    products = [
        "Widget A",
        "Widget B",
        "Widget C",
        "Gadget X",
        "Gadget Y",
        "Gadget Z",
        "Tool 1",
        "Tool 2",
        "Tool 3",
        "Tool 4",
    ]
    quarters = ["Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024"]
    rows = []
    for region in regions:
        for product in products:
            for quarter in quarters:
                revenue = int(rng.integers(50_000, 500_000))
                rows.append(
                    {
                        "Region": region,
                        "Product": product,
                        "Quarter": quarter,
                        "Revenue": revenue,
                    }
                )
    return pd.DataFrame(rows)


def section_top_n_value_filters():
    df = _section_filters_data()

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            """
**Top N / Bottom N** filters hide dimension members outside the top (or bottom) N
ranked by a measure. Ranking is **per-parent**: for a two-level hierarchy like
`[Region, Product]`, "Top 3 Products by Revenue" keeps the 3 highest-revenue
products **within each Region** independently.

The `top_n_filters` parameter accepts a list of dicts with keys:
- `field` — dimension to filter
- `n` — how many members to keep
- `direction` — `"top"` or `"bottom"`
- `by` — measure to rank by
- `axis` — `"rows"` or `"columns"` (default `"rows"`)

> ⚠️ Grand totals and subtotals always reflect **all data**, not just visible
> members. This is a deliberate product choice (simpler, avoids re-aggregation
> cost). The table shows a footnote to communicate this.

**Interactive**: open any row-dimension header menu to set a Top N or value
filter without code.
            """
        )
    with col2:
        st.markdown(
            """
**Value Filters** suppress dimension members whose aggregated measure fails a
predicate at the grand-total column context. Evaluation is also **per-parent**.

The `value_filters` parameter accepts dicts with:
- `field` — dimension to filter
- `by` — measure to evaluate
- `operator` — `"gt"`, `"gte"`, `"lt"`, `"lte"`, `"eq"`, `"neq"`, `"between"`
- `value` — threshold (lower bound for `"between"`)
- `value2` — upper bound (required for `"between"`)
- `axis` — `"rows"` or `"columns"` (default `"rows"`)

Members with a **null** aggregated measure fail all predicates and are excluded.

**Hint**: these filters are also accessible via the column header ⋮ menu
("Top / Bottom N" and "Filter by value" sections) — no code required.
            """
        )

    st.markdown("##### Top 3 Products by Revenue per Region")
    st.caption(
        "50 products × 5 regions; only the top 3 revenue products per region are shown. "
        "Grand totals include all products."
    )
    from streamlit_pivot import st_pivot_table

    st_pivot_table(
        df,
        key="demo_top3_products",
        rows=["Region", "Product"],
        columns=["Quarter"],
        values=["Revenue"],
        number_format={"Revenue": "$,.0f"},
        top_n_filters=[
            {"field": "Product", "n": 3, "by": "Revenue", "direction": "top"}
        ],
    )

    st.markdown("##### Revenue > $1M (annual total) — Value Filter")
    st.caption(
        "Products whose total annual Revenue ≤ $1M are hidden within each Region."
    )
    st_pivot_table(
        df,
        key="demo_value_filter_revenue",
        rows=["Region", "Product"],
        columns=["Quarter"],
        values=["Revenue"],
        number_format={"Revenue": "$,.0f"},
        value_filters=[
            {"field": "Product", "by": "Revenue", "operator": "gt", "value": 1_000_000}
        ],
    )

    st.markdown("##### Bottom 2 Products per Region")
    st.caption(
        "Bottom 2 products by revenue within each region — useful for identifying "
        "underperformers."
    )
    st_pivot_table(
        df,
        key="demo_bottom2_products",
        rows=["Region", "Product"],
        columns=["Quarter"],
        values=["Revenue"],
        number_format={"Revenue": "$,.0f"},
        top_n_filters=[
            {"field": "Product", "n": 2, "by": "Revenue", "direction": "bottom"}
        ],
    )

    with st.expander("Code", expanded=False):
        st.code(
            """\
from streamlit_pivot import st_pivot_table

# Top 3 Products by Revenue per Region
st_pivot_table(
    df,
    key="top3",
    rows=["Region", "Product"],
    columns=["Quarter"],
    values=["Revenue"],
    top_n_filters=[
        {"field": "Product", "n": 3, "by": "Revenue", "direction": "top"}
    ],
)

# Products with Revenue > $1M (per-region grand-total column context)
st_pivot_table(
    df,
    key="gt1m",
    rows=["Region", "Product"],
    columns=["Quarter"],
    values=["Revenue"],
    value_filters=[
        {"field": "Product", "by": "Revenue", "operator": "gt", "value": 1_000_000}
    ],
)
""",
            language="python",
        )


section_top_n_value_filters()

# ---------------------------------------------------------------------------
# Section 5d: Multi-field Sorting + Subtotal Position (0.5.0)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("5d. Multi-field Sorting + Subtotal Position (0.5.0)")


@st.fragment
def section_sort_and_subtotals():
    st.markdown(
        """
**Multi-field sorting** lets you chain sort criteria: the primary sort orders
rows/columns, and the secondary (tertiary, …) kicks in only when values are
equal, guaranteeing a deterministic, stable order.

**Subtotal position** (`"top"` or `"bottom"`) controls whether subtotal rows
appear as group *headers* (above members) or group *footers* (after members,
the default — matching Excel's default pivot behavior).

| Feature | Parameter | Default |
|---------|-----------|---------|
| Multi-field row sort | `row_sort=[{…}, {…}]` | single sort |
| Multi-field column sort | `col_sort=[{…}, {…}]` | single sort |
| Subtotal placement | `subtotal_position` | `"bottom"` |

**API parameters used:** `row_sort` (list), `subtotal_position`
"""
    )

    import pandas as pd  # noqa: PLC0415

    sort_data = pd.DataFrame(
        {
            "Region": ["East"] * 4 + ["West"] * 4,
            "Category": ["Electronics", "Furniture", "Electronics", "Furniture"] * 2,
            "Year": ["2023", "2023", "2024", "2024"] * 2,
            "Revenue": [500, 500, 700, 300, 600, 200, 800, 400],
            "Units": [10, 20, 14, 6, 12, 4, 16, 8],
        }
    )

    st.markdown("##### Multi-field Sort: Primary Revenue desc, Secondary Category asc")
    st.caption(
        "East/2024 and West/2024 rows have different revenues so primary sort orders "
        "them. Rows with equal revenue (East/2023 Electronics = West/2023 Electronics "
        "= 500 and 600) fall back to Category asc for a stable, deterministic order."
    )
    st_pivot_table(
        sort_data,
        key="demo_multi_sort",
        rows=["Region", "Category"],
        columns=["Year"],
        values=["Revenue", "Units"],
        row_sort=[
            {
                "by": "value",
                "direction": "desc",
                "dimension": "Region",
                "value_field": "Revenue",
            },
            {"by": "key", "direction": "asc", "dimension": "Category"},
        ],
        show_subtotals=True,
        number_format={"Revenue": "$,.0f"},
    )

    subtotal_data = pd.DataFrame(
        {
            "Region": ["East", "East", "East", "West", "West", "West"],
            "Category": ["Electronics", "Furniture", "Clothing"] * 2,
            "Revenue": [400, 300, 200, 350, 450, 150],
        }
    )

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("##### Subtotals at Bottom (default)")
        st.caption("Subtotal row appears after group members — Excel default behavior.")
        st_pivot_table(
            subtotal_data,
            key="demo_subtotal_bottom",
            rows=["Region", "Category"],
            columns=[],
            values=["Revenue"],
            show_subtotals=True,
            subtotal_position="bottom",
            number_format={"Revenue": "$,.0f"},
        )
    with col2:
        st.markdown("##### Subtotals at Top")
        st.caption(
            "Subtotal row appears before group members — acts as a collapsible "
            "group header."
        )
        st_pivot_table(
            subtotal_data,
            key="demo_subtotal_top",
            rows=["Region", "Category"],
            columns=[],
            values=["Revenue"],
            show_subtotals=True,
            subtotal_position="top",
            number_format={"Revenue": "$,.0f"},
        )

    with st.expander("Code", expanded=False):
        st.code(
            """\
# Multi-field sorting
st_pivot_table(
    df,
    key="demo_multi_sort",
    rows=["Region", "Category"],
    columns=["Year"],
    values=["Revenue"],
    row_sort=[
        {"by": "value", "direction": "desc", "dimension": "Region", "value_field": "Revenue"},
        {"by": "key", "direction": "asc", "dimension": "Category"},  # breaks ties
    ],
)

# Subtotals at top (group headers)
st_pivot_table(
    df,
    key="demo_subtotal_top",
    rows=["Region", "Category"],
    values=["Revenue"],
    show_subtotals=True,
    subtotal_position="top",
)
""",
            language="python",
        )


section_sort_and_subtotals()


# ---------------------------------------------------------------------------
# Section 5e: Values Axis Placement (0.5.0)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("5e. Values Axis Placement (0.5.0)")


@st.fragment
def section_values_axis():
    st.markdown(
        """
By default, multiple value fields appear as a **"Σ Values"** column-group in the
column headers — each column slot is repeated once per measure. Setting
`values_axis="rows"` moves measures onto the **row axis** instead: each
dimension row is split into one sub-row per measure, and a **Values** header
column labels each measure.

This layout mirrors how financial statements (income statements, balance sheets)
and accounting reports are typically presented — rows for line items, columns
for time periods.

| Feature | Parameter | Default |
|---------|-----------|---------|
| Place measures on rows | `values_axis="rows"` | `"columns"` |
| Incompatible with | period comparison `show_values_as`, temporal hierarchies | — |

**API parameter:** `values_axis`
"""
    )

    import pandas as pd  # noqa: PLC0415

    # --- Example 1: side-by-side comparison ---
    st.markdown("##### Side-by-side: Values on Columns vs. Values on Rows")
    st.caption(
        "Both tables show the same data. On the left, Revenue and Units share "
        "column slots (the default). On the right, each metric occupies its own row."
    )

    cmp_data = pd.DataFrame(
        {
            "Region": ["East", "East", "West", "West"],
            "Category": ["Electronics", "Furniture", "Electronics", "Furniture"],
            "Year": ["2023", "2024", "2023", "2024"],
            "Revenue": [500, 700, 600, 800],
            "Units": [10, 14, 12, 16],
        }
    )

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Values on Columns** (default)")
        st_pivot_table(
            cmp_data,
            key="demo_values_axis_cols",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue", "Units"],
            show_totals=True,
            number_format={"Revenue": "$,.0f"},
        )
    with col2:
        st.markdown("**Values on Rows**")
        st_pivot_table(
            cmp_data,
            key="demo_values_axis_rows",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue", "Units"],
            values_axis="rows",
            show_totals=True,
            number_format={"Revenue": "$,.0f"},
        )

    # --- Example 2: Income Statement layout ---
    st.markdown("##### Income Statement Layout")
    st.caption(
        "A classic financial-report layout: line items (Revenue, COGS, Gross Profit, "
        "OpEx, Operating Income) as rows, quarters as columns."
    )

    income_data = pd.DataFrame(
        {
            "Account": [
                "Revenue",
                "Revenue",
                "Revenue",
                "Revenue",
                "COGS",
                "COGS",
                "COGS",
                "COGS",
                "Gross Profit",
                "Gross Profit",
                "Gross Profit",
                "Gross Profit",
                "OpEx",
                "OpEx",
                "OpEx",
                "OpEx",
                "Operating Income",
                "Operating Income",
                "Operating Income",
                "Operating Income",
            ],
            "Quarter": ["Q1", "Q2", "Q3", "Q4"] * 5,
            "Amount": [
                1_200_000,
                1_350_000,
                1_500_000,
                1_650_000,  # Revenue
                720_000,
                810_000,
                900_000,
                990_000,  # COGS
                480_000,
                540_000,
                600_000,
                660_000,  # Gross Profit
                200_000,
                210_000,
                220_000,
                230_000,  # OpEx
                280_000,
                330_000,
                380_000,
                430_000,  # Operating Income
            ],
        }
    )

    st_pivot_table(
        income_data,
        key="demo_income_statement",
        rows=["Account"],
        columns=["Quarter"],
        values=["Amount"],
        values_axis="rows",
        show_totals=False,
        number_format={"Amount": "$,.0f"},
    )

    # --- Example 3: Multi-dim rows with values on rows + subtotals ---
    st.markdown("##### Multi-Dimension Rows + Subtotals")
    st.caption(
        "Two row dimensions with subtotals enabled. Each subtotal row also "
        "gets its own Revenue and Units sub-rows."
    )

    multi_data = pd.DataFrame(
        {
            "Region": ["East"] * 4 + ["West"] * 4,
            "Category": ["Electronics", "Electronics", "Furniture", "Furniture"] * 2,
            "Quarter": ["Q1", "Q2", "Q1", "Q2"] * 2,
            "Revenue": [300, 350, 200, 220, 400, 450, 250, 270],
            "Units": [6, 7, 4, 5, 8, 9, 5, 6],
        }
    )

    st_pivot_table(
        multi_data,
        key="demo_values_axis_subtotals",
        rows=["Region", "Category"],
        columns=["Quarter"],
        values=["Revenue", "Units"],
        values_axis="rows",
        show_subtotals=True,
        show_totals=True,
        number_format={"Revenue": "$,.0f"},
    )

    with st.expander("Code", expanded=False):
        st.code(
            """\
# Values on rows — basic
st_pivot_table(
    df,
    key="demo_values_axis_rows",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Units"],
    values_axis="rows",
    show_totals=True,
)

# Income Statement layout
st_pivot_table(
    income_df,
    key="demo_income_statement",
    rows=["Account"],
    columns=["Quarter"],
    values=["Amount"],
    values_axis="rows",
    show_totals=False,
    number_format={"Amount": "$,.0f"},
)

# Multi-dimension rows with subtotals
st_pivot_table(
    df,
    key="demo_values_axis_subtotals",
    rows=["Region", "Category"],
    columns=["Quarter"],
    values=["Revenue", "Units"],
    values_axis="rows",
    show_subtotals=True,
    show_totals=True,
)
""",
            language="python",
        )


section_values_axis()


# ---------------------------------------------------------------------------
# Section 6: Conditional Formatting
# ---------------------------------------------------------------------------
st.divider()
st.subheader("6. Conditional Formatting")


@st.fragment
def section_cond_fmt():
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

    st.markdown(
        """
#### Diverging color scale anchored at a numeric midpoint (`mid_value`)

Pass `mid_color` with a numeric `mid_value` to anchor a smooth diverging
gradient at a meaningful midpoint (e.g. `0` for PnL, or a target value).
Below, the average-profit column is rendered red-white-blue around the
overall average so cells below average appear red and cells above appear
blue, regardless of the column's min/max.
"""
    )

    avg_profit = float(df["Profit"].mean())
    st_pivot_table(
        df,
        key="cond_fmt_mid_value",
        rows=["Region"],
        columns=["Year"],
        values=["Profit"],
        aggregation="avg",
        number_format={"Profit": ",.0f"},
        conditional_formatting=[
            {
                "type": "color_scale",
                "apply_to": ["Profit"],
                "min_color": "#c62828",
                "mid_color": "#ffffff",
                "max_color": "#1565c0",
                "mid_value": avg_profit,
            },
        ],
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

# Diverging color scale anchored at a numeric midpoint:
st_pivot_table(
    df,
    key="cond_fmt_mid_value",
    rows=["Region"],
    columns=["Year"],
    values=["Profit"],
    aggregation="avg",
    conditional_formatting=[
        {
            "type": "color_scale",
            "apply_to": ["Profit"],
            "min_color": "#c62828",   # red for below-midpoint
            "mid_color": "#ffffff",   # neutral at mid_value
            "max_color": "#1565c0",   # blue for above-midpoint
            "mid_value": df["Profit"].mean(),  # anchor at overall average
        },
    ],
)
""",
            language="python",
        )


section_cond_fmt()

# ---------------------------------------------------------------------------
# Section 7: Number Formatting and Layout
# ---------------------------------------------------------------------------
st.divider()
st.subheader("7. Number Formatting and Layout")


@st.fragment
def section_formatting():
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


section_formatting()

# ---------------------------------------------------------------------------
# Section 8: Column Group Collapse/Expand
# ---------------------------------------------------------------------------
st.divider()
st.subheader("8. Column Group Collapse/Expand")


@st.fragment
def section_col_collapse():
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


section_col_collapse()

# ---------------------------------------------------------------------------
# Section 9: Synthetic Measures (V1)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("9. Synthetic Measures (V1)")


@st.fragment
def section_synthetic():
    st.markdown(
        """
Synthetic measures let you combine source metrics using fixed operations while
still keeping regular measures in the same table.

**V1 operations:**
- **Ratio of sums**: `sum(A) / sum(B)`
- **Difference of sums**: `sum(A) - sum(B)`

**Try it:**
- Keep **Total PRs** as a regular measure.
- Open the **Settings Panel** (pivot icon) and click **+ Add measure** in the
  Calculated Measures section to create new synthetic measures interactively.
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


section_synthetic()

# ---------------------------------------------------------------------------
# Section 9b: Formula-Based Calculated Measures
# ---------------------------------------------------------------------------
st.divider()
st.subheader("9b. Formula-Based Calculated Measures")


@st.fragment
def section_formula():
    st.markdown(
        """
Formula measures extend synthetic measures with a general expression evaluator.
Instead of picking a fixed operation (Ratio/Difference), you write an arbitrary
arithmetic expression that references aggregated fields.

**Key points:**
- Field references are quoted strings: `"Revenue" / "Cost"`.
- Fields use their configured aggregation (default: Sum).
- Built-in functions: `abs()`, `min()`, `max()`, `round()`, `if()`.
- Null propagation: if any field is null, the result is null.
- Division by zero returns null.
- No `eval()` or `new Function()` — CSP-safe AST evaluation.

**Try it:**
- Open the **Settings Panel** and click **+ Add measure**.
- Select **Formula** from the Operation dropdown.
- Type `"Revenue" / "Headcount"` or `if("Headcount" > 0, "Revenue" / "Headcount", 0)`.
"""
    )

    df_formula = pd.DataFrame(
        {
            "Region": ["East", "East", "West", "West", "North"],
            "Year": [2023, 2024, 2023, 2024, 2024],
            "Revenue": [100, 200, 150, 300, 50],
            "Cost": [40, 80, 60, 100, 20],
            "Headcount": [5, 8, 3, 10, 2],
        }
    )

    st_pivot_table(
        df_formula,
        key="formula_measures",
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

    with st.expander("View Code"):
        st.code(
            """
st_pivot_table(
    df_formula,
    key="formula_measures",
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
""",
            language="python",
        )


section_formula()

# ---------------------------------------------------------------------------
# Section 10: Sticky Headers, Height, and Max Height
# ---------------------------------------------------------------------------
st.divider()
st.subheader("10. Sticky Headers, Height, and Max Height")


@st.fragment
def section_sticky():
    st.markdown(
        """
By default, column headers **stick** to the top of the table as you scroll.
You can disable this behavior with `sticky_headers=False` or toggle it at
runtime via the **Sticky Headers** checkbox in the **Settings Panel** (pivot icon).

The table container size is controlled by `max_height` (default ``500``).
The table auto-sizes up to this limit, then becomes scrollable with sticky
headers. The sticky headers checkbox appears when content exceeds this limit.

**Try it:**
- The table below has `max_height=700` and sticky headers **disabled** — scroll
  down and notice the headers scroll away.
- Open the **Settings Panel** (pivot icon), enable **Sticky Headers**, and
  click **Apply** to re-enable.

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


section_sticky()

# ---------------------------------------------------------------------------
# Section 11: Data Export
# ---------------------------------------------------------------------------
st.divider()
st.subheader("11. Data Export")


@st.fragment
def section_export():
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


section_export()

# ---------------------------------------------------------------------------
# Section 12: Drill-Down Detail Panel
# ---------------------------------------------------------------------------
st.divider()
st.subheader("12. Drill-Down Detail Panel")


@st.fragment
def section_drilldown():
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
- **Click any column header** in the drill-down panel to sort the results.
  Click again to toggle between ascending, descending, and original order.
  Sorting applies to the **full** result set before pagination.
- Click the **✕** button or press **Escape** to close the panel.
- Click a different cell to replace the panel with new records.

**API parameter used:** `enable_drilldown` (default ``True``)

Set ``enable_drilldown=False`` to disable the drill-down panel (the
``on_cell_click`` callback still fires).
"""
    )

    st_pivot_table(
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
# Click a column header in the drill-down panel to sort results.
""",
            language="python",
        )


section_drilldown()

# ---------------------------------------------------------------------------
# Section 13: Grouping Hierarchy and Sorting
# ---------------------------------------------------------------------------
st.divider()
st.subheader("13. Grouping Hierarchy and Scoped Sorting")


@st.fragment
def section_hierarchy():
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


section_hierarchy()

# ---------------------------------------------------------------------------
# Section 13b: Row Layout Modes (Table vs. Hierarchy)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("13b. Row Layout Modes (Table vs. Hierarchy)")


@st.fragment
def section_row_layout():
    st.markdown(
        """
The `row_layout` parameter controls how row dimensions are arranged in the row header.
Both modes display the **same data** — only the visual arrangement differs.

| Mode | `row_layout=` | What you see |
|------|--------------|--------------|
| **Table** (default) | `"table"` | One dedicated column per row dimension. Region, Category, and Product each get their own header column; repeated labels are merged by default. |
| **Hierarchy** | `"hierarchy"` | A **single indented tree column** for all row dimensions. Depth is shown through indentation. A breadcrumb bar at the top of the column shows the current drill path and lets you jump back to any ancestor level. |

**When to use Table:** for 1–2 row dimensions, when each dimension label should be visually distinct in its own column, or when `repeat_row_labels=True` is needed.

**When to use Hierarchy:** for 3+ row dimensions or when horizontal space is limited.
The compact single-column layout keeps wide value columns visible without side-scrolling,
and the breadcrumb bar makes deep nesting easy to navigate.
"""
    )

    row_layout_cols = st.columns(2)

    with row_layout_cols[0]:
        st.markdown('**`row_layout="table"` (default)**')
        st.caption("Each row dimension gets its own column.")
        st_pivot_table(
            df_medium,
            key="row_layout_table",
            rows=["Region", "Category", "Product"],
            columns=["Year"],
            values=["Revenue"],
            aggregation="sum",
            show_subtotals=True,
            row_layout="table",
            max_height=400,
        )

    with row_layout_cols[1]:
        st.markdown('**`row_layout="hierarchy"`**')
        st.caption(
            "All row dimensions in one indented tree column. " "Subtotals auto-enable."
        )
        st_pivot_table(
            df_medium,
            key="row_layout_hierarchy",
            rows=["Region", "Category", "Product"],
            columns=["Year"],
            values=["Revenue"],
            aggregation="sum",
            row_layout="hierarchy",
            max_height=400,
        )

    st.info(
        '**Auto-subtotals in hierarchy mode:** When `row_layout="hierarchy"` and '
        "`show_subtotals` is not explicitly set, subtotals are automatically enabled for "
        "all grouping levels so every group node shows its aggregate. "
        "Pass `show_subtotals=False` to opt out."
    )

    st.markdown("#### Breadcrumb navigation and `repeat_row_labels`")
    st.markdown(
        """
The hierarchy column header contains a **breadcrumb bar** that tracks your current drill level:

- Click a breadcrumb label to **collapse all descendants** back to that ancestor level.
- The **+/−** toggle on each group row expands or collapses that individual branch.
- Temporal date hierarchies (Year → Quarter → Month → Day) render as nested tree levels
  within the single hierarchy column — identical in behavior to any other dimension nesting.

`repeat_row_labels` is **ignored** in hierarchy mode — the row axis is a single column, so
there are no labels to repeat across columns. The Settings Panel disables the toggle automatically
when hierarchy mode is active.
"""
    )

    with st.expander("View Code"):
        st.code(
            """
# Default: one column per row dimension
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
""",
            language="python",
        )


section_row_layout()

# ---------------------------------------------------------------------------
# Section 14: Server-Side Drill-Down (Hybrid Mode)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("14. Server-Side Drill-Down (Hybrid Mode)")

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


@st.fragment
def section_hybrid():
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
- **Click a column header** to sort the drill-down results. In hybrid mode,
  each sort triggers a server round-trip so the backend sorts the full
  filtered result before slicing the requested page. Navigate to page 2 to
  confirm the sort applies globally, not just within the visible page.

**API parameter used:** `execution_mode` (set to `"threshold_hybrid"` here to
force hybrid mode on a smaller dataset for demonstration purposes)
"""
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
# Click a column header to sort — sort is applied server-side before pagination.
""",
            language="python",
        )

    st.caption(
        f"Dataset: {len(df_hybrid):,} rows × {len(df_hybrid.columns)} columns — "
        "forced to threshold_hybrid mode for demonstration."
    )

    st.markdown(
        """
**Non-decomposable aggregations in hybrid mode:** All 10 aggregation types are
supported in hybrid mode, including `median`, `count_distinct`, `percentile_90`,
`first`, and `last`. The server computes correct totals via a sidecar payload.
`count` and `count_distinct` work on any column type; all other aggregations
coerce values to numeric and ignore non-numeric entries.
"""
    )

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Hybrid Median Pivot**")
        st_pivot_table(
            df_hybrid,
            key="hybrid_median_demo",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
            aggregation="median",
            number_format={"Revenue": "$,.2f"},
            show_totals=True,
            execution_mode="threshold_hybrid",
        )
    with col_b:
        st.markdown("**Hybrid Count Distinct Pivot**")
        st_pivot_table(
            df_hybrid,
            key="hybrid_count_distinct_demo",
            rows=["Region"],
            columns=["Year"],
            values=["Category"],
            aggregation="count_distinct",
            show_totals=True,
            execution_mode="threshold_hybrid",
        )

    with st.expander("View Code — Non-decomposable Aggregations"):
        st.code(
            """
# Median: server computes correct grand/row/col totals
st_pivot_table(
    df_hybrid,
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    aggregation="median",
    show_totals=True,
    execution_mode="threshold_hybrid",
)

# Count Distinct: works on any column type (e.g. Category strings)
st_pivot_table(
    df_hybrid,
    rows=["Region"],
    columns=["Year"],
    values=["Category"],
    aggregation="count_distinct",
    show_totals=True,
    execution_mode="threshold_hybrid",
)
""",
            language="python",
        )


section_hybrid()

# ---------------------------------------------------------------------------
# Section 15: Drag-and-Drop Field Configuration
# ---------------------------------------------------------------------------
st.divider()
st.subheader("15. Drag-and-Drop Field Configuration")


@st.fragment
def section_dnd():
    st.markdown(
        """
Drag-and-drop is available in **two contexts**:

**Toolbar DnD:** Each chip in the Rows, Columns, and Values toolbar zones has a
**grip-dots drag handle**. Drag to reorder within a zone or move between zones.
These are immediate changes.

**Settings Panel DnD:** Inside the Settings Panel (pivot icon), chips in
Available Fields and all zone sections are draggable. Drag from Available Fields
into a zone, reorder within zones, or move between zones. These changes are
staged and applied on **Apply**.

**Try it** on the pivot below (two row dimensions so you can reorder):
- Drag the grip handle on **Category** and drop it before **Region** to
  reverse the grouping order.
- Drag **Category** from Rows into the Columns zone.
- Open the **Settings Panel** and drag fields from Available Fields into zones.

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


section_dnd()

# ---------------------------------------------------------------------------
# Section 16: Column Resize
# ---------------------------------------------------------------------------
st.divider()
st.subheader("16. Column Resize")


@st.fragment
def section_resize():
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


section_resize()

# ---------------------------------------------------------------------------
# Section 17: Fullscreen Mode
# ---------------------------------------------------------------------------
st.divider()
st.subheader("17. Fullscreen Mode")


@st.fragment
def section_fullscreen():
    st.markdown(
        """
Click the **expand icon** (⤢) in the toolbar utility menu to enter
**fullscreen mode**. The pivot table fills the entire browser viewport
as a fixed overlay. Press **Escape** or click the **collapse icon** (⤡)
to exit.

**Try it:**
- Hover over the toolbar area to reveal the utility buttons.
- Click the expand icon in the utility menu (before the Settings icon).
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


section_fullscreen()

# ---------------------------------------------------------------------------
# Section 18: Date Hierarchy and Time Comparisons
# ---------------------------------------------------------------------------
st.divider()
st.subheader("18. Date Hierarchy and Time Comparisons")

_short_records = []
for day in range(1, 15):
    for region in ["US", "EU"]:
        base = 80 if region == "EU" else 120
        _short_records.append(
            {
                "region": region,
                "order_date": _dt.date(2024, 3, day),
                "Revenue": base + _rnd.randint(-15, 25),
            }
        )
df_dates_short = pd.DataFrame(_short_records)


@st.fragment
def section_dates():
    st.markdown(
        """
Typed date and datetime fields now auto-behave like BI-style time hierarchies.
When a temporal field is placed on rows or columns, the pivot **adapts the
default grouping grain** to the date range in the source data:

| Date Span | Default Grain |
|---|---|
| >2 years | Year |
| >1 year | Quarter |
| >2 months | Month |
| ≤2 months | Day |

This means a 5-year dataset starts at Year, while a 2-month dataset starts at
Day — no configuration needed.  The default is still overridable via
`date_grains` or the interactive header menu.

Drill controls expose **Year → Quarter → Month → Day**, and **Week** is
available as an alternate grouping. Period-over-period display modes
(previous-period, previous-year) are unlocked automatically.

The Excel/Power BI-style parent collapse/expand UI now works on **both axes**.
On columns, parent headers collapse into summary columns. On rows, collapsing a
parent replaces its visible descendants with one synthetic summary row. Exports
still keep the full leaf-level date structure regardless of collapse state.

**Try it:**
- In the first table, notice the adaptive default grain based on the data range.
- Open the `order_date` header menu to drill up/down, switch to Week, or choose Original.
- Use the +/- toggle on a parent date group to collapse or expand it on either axis.
- Open the `Revenue` value header menu to switch between raw values and period comparisons.
- Compare the other tables for explicit override, global opt-out, per-field opt-out, and the row-side collapse example in the bottom-right.

**API parameters used:** `auto_date_hierarchy`, `date_grains`, `show_values_as`
"""
    )

    date_col_1, date_col_2 = st.columns(2)

    with date_col_1:
        st.caption("2-year span → adaptive default: Quarter")
        st_pivot_table(
            df_dates,
            key="date_hierarchy_auto",
            rows=["region"],
            columns=["order_date"],
            values=["Revenue"],
            aggregation="sum",
            show_totals=True,
        )

    with date_col_2:
        st.caption("2-week span → adaptive default: Day")
        st_pivot_table(
            df_dates_short,
            key="date_hierarchy_adaptive_day",
            rows=["region"],
            columns=["order_date"],
            values=["Revenue"],
            aggregation="sum",
            show_totals=True,
        )

    date_col_3, date_col_4 = st.columns(2)

    with date_col_3:
        st.caption("Explicit override: Month grain (raw sums)")
        st_pivot_table(
            df_dates,
            key="date_hierarchy_quarter",
            rows=["region"],
            columns=["order_date"],
            values=["Revenue"],
            aggregation="sum",
            show_totals=True,
            date_grains={"order_date": "month"},
        )

    with date_col_4:
        st.caption("Global auto hierarchy off")
        st_pivot_table(
            df_dates,
            key="date_hierarchy_auto_off",
            rows=["region"],
            columns=["ship_date"],
            values=["Revenue"],
            aggregation="sum",
            show_totals=True,
            auto_date_hierarchy=False,
        )

    date_col_5, date_col_6 = st.columns(2)

    with date_col_5:
        st.caption("Per-field Original opt-out")
        st_pivot_table(
            df_dates,
            key="date_hierarchy_original",
            rows=["region"],
            columns=["ship_date"],
            values=["Revenue"],
            aggregation="sum",
            show_totals=True,
            date_grains={"ship_date": None},
        )

    with date_col_6:
        st.caption(
            "Row-side hierarchy with collapsible parent rows (click the +/- in the row headers)"
        )
        st_pivot_table(
            df_dates,
            key="date_hierarchy_rows",
            rows=["order_date"],
            columns=["region"],
            values=["Revenue"],
            aggregation="sum",
            show_totals=True,
        )

    with st.expander("View Code"):
        st.code(
            """
# Adaptive default: 2-year span → Quarter
st_pivot_table(
    df_dates,  # spans 2023-2024
    key="date_hierarchy_auto",
    rows=["region"],
    columns=["order_date"],
    values=["Revenue"],
)

# Adaptive default: 2-week span → Day
st_pivot_table(
    df_dates_short,  # spans Mar 1-14
    key="date_hierarchy_adaptive_day",
    rows=["region"],
    columns=["order_date"],
    values=["Revenue"],
)

# Explicit override: month grain (raw sums)
st_pivot_table(
    df_dates,
    rows=["region"],
    columns=["order_date"],
    values=["Revenue"],
    date_grains={"order_date": "month"},
)

st_pivot_table(
    df_dates,
    key="date_hierarchy_auto_off",
    rows=["region"],
    columns=["ship_date"],
    values=["Revenue"],
    auto_date_hierarchy=False,
)

st_pivot_table(
    df_dates,
    key="date_hierarchy_original",
    rows=["region"],
    columns=["ship_date"],
    values=["Revenue"],
    date_grains={"ship_date": None},
)

st_pivot_table(
    df_dates,
    key="date_hierarchy_rows",
    rows=["order_date"],
    columns=["region"],
    values=["Revenue"],
)
""",
            language="python",
        )


section_dates()

# ---------------------------------------------------------------------------
# Section 19: Settings Panel
# ---------------------------------------------------------------------------
st.divider()
st.subheader("19. Settings Panel (Staged Commit UX)")


@st.fragment
def section_settings():
    st.markdown(
        """
The **Settings Panel** is the primary authoring surface for pivot field
configuration. It uses a **staged commit** model: changes are made locally
and only applied when you click **Apply**.

**Panel sections:**
- **Available Fields** — unassigned columns shown as draggable chips with
  context menus for adding to Rows, Columns, or Values. A search input
  appears when more than 8 fields are available.
- **Rows / Columns / Values** — drop zones with drag-and-drop reordering,
  remove buttons, and aggregation pickers on value chips.
- **Calculated Measures** — click **+ Add measure** to build synthetic
  metrics (ratio of sums, difference of sums) with format patterns.
- **Display** — toggles for Row Totals, Column Totals, Subtotals, Repeat
  Labels, and Sticky Headers.
- **Apply / Cancel** — commit or discard all staged changes.

**Key behaviors:**
- The **Apply** button is disabled when no changes have been made.
- **Escape** or clicking outside the panel discards staged changes.
- If the toolbar config changes externally (Reset, Swap, DnD, import) while
  the panel is open, it automatically closes and discards uncommitted edits.
- Frozen columns appear in Available Fields if unassigned, but once placed in
  a zone their chips are non-draggable and non-removable.

**Try it:**
- Click the **Settings** icon in the toolbar utility menu to open the panel.
- Drag fields from Available Fields into Rows, Columns, or Values.
- Change an aggregation by clicking the badge on a value chip.
- Add a synthetic measure via **+ Add measure**.
- Toggle display settings, then click **Apply** to see the changes.
- Click **Cancel** or press **Escape** to discard.
"""
    )

    st_pivot_table(
        df_field_search,
        key="settings_panel_demo",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue"],
        aggregation="sum",
        show_totals=True,
        interactive=True,
    )

    with st.expander("View Code"):
        st.code(
            """
st_pivot_table(
    df_field_search,
    key="settings_panel_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    aggregation="sum",
    show_totals=True,
    interactive=True,
)
# Click the Settings icon in the toolbar to open the Settings Panel.
# Make changes, then click Apply or Cancel.
""",
            language="python",
        )


section_settings()

# ---------------------------------------------------------------------------
# Section 20: column_config (Tier 1: label, help, width, pinned, alignment)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("20. Column Config (label, help, width, pinned, alignment)")


@st.fragment
def section_column_config():
    st.markdown(
        """
Pass a ``column_config`` dict to customize **display** properties per field
without changing the underlying data identity. Supported Tier 1 keys:

- ``label`` — override the display name shown in all headers, chips, and
  exported header rows. Canonical field ids in the serialized config are
  **unchanged** (sort/filter/conditional formatting still target ids).
  Empty / whitespace-only labels fall back to the field id.
- ``help`` — text rendered as a native ``title`` tooltip on dimension and
  measure headers.
- ``width`` — either a preset (``"small"``=100px, ``"medium"``=120px,
  ``"large"``=200px) or an integer pixel value in the range ``[20, 2000]``.
  Applies to row dimension columns and measure columns. Interactive resize
  drags override this at runtime (but are **not** persisted to config).
- ``pinned`` — when ``True`` or ``"left"``, locks the field in the config
  UI (equivalent to adding it to ``frozen_columns``). This does **not**
  create a visually sticky column — that's a separate concern.
- ``alignment`` — ``"left"``, ``"center"``, or ``"right"``. Unions with the
  ``column_alignment`` kwarg; explicit kwarg wins on conflicts.

Both plain dict literals and ``st.column_config.*`` typed objects are
accepted. Unknown keys in dict literals warn once per (field, key);
Streamlit's internal defaults from typed objects are silently ignored.
"""
    )

    st_pivot_table(
        df,
        key="column_config_demo",
        rows=["Region"],
        columns=["Year"],
        values=["Revenue", "Profit"],
        aggregation={"Revenue": "sum", "Profit": "sum"},
        column_config={
            "Region": {
                "label": "Area",
                "help": "Geographic region",
                "width": "large",
                "pinned": True,
                "alignment": "left",
            },
            "Revenue": {
                "label": "Rev",
                "help": "Total revenue in USD",
                "width": 180,
                "alignment": "right",
            },
            "Profit": {
                "label": "Net",
                "help": "Net profit in USD",
                "alignment": "center",
            },
        },
        show_totals=True,
        interactive=True,
    )

    with st.expander("View Code"):
        st.code(
            """
st_pivot_table(
    df,
    key="column_config_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    aggregation={"Revenue": "sum", "Profit": "sum"},
    column_config={
        "Region": {
            "label": "Area",
            "help": "Geographic region",
            "width": "large",
            "pinned": True,
            "alignment": "left",
        },
        "Revenue": {
            "label": "Rev",
            "help": "Total revenue in USD",
            "width": 180,
            "alignment": "right",
        },
        "Profit": {
            "label": "Net",
            "help": "Net profit in USD",
            "alignment": "center",
        },
    },
    show_totals=True,
    interactive=True,
)
""",
            language="python",
        )


section_column_config()

# ---------------------------------------------------------------------------
# Section 21: column_config (Tier 2: link, image, checkbox, text max_chars)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("21. Column Config — Cell Renderers (link, image, checkbox, text)")


@st.fragment
def section_column_config_renderers():
    st.markdown(
        """
A small set of ``type`` values in ``column_config`` produce **row-dimension
cell renderers**. Measure cells are numeric aggregates and ignore these
types. On Total / Subtotal rows, ``link`` / ``image`` / ``checkbox`` fall back
to plain text because the cell value is a label rather than data; ``text``
with ``max_chars`` still truncates.

- ``type="link"`` — renders the cell value as an ``<a>`` (``href`` = raw
  value). ``display_text`` can be a plain string or a ``{}`` template
  (substituted with the cell value, mirroring Streamlit's ``LinkColumn``).
- ``type="image"`` — renders the cell value as an ``<img>`` (``src`` = raw
  value) with ``loading="lazy"`` and a ``max-height`` guard.
- ``type="checkbox"`` — truthy → ☑, falsy → ☐. Accepts ``True`` / ``False``,
  the strings ``"true"`` / ``"false"`` / ``"yes"`` / ``"no"`` / ``"1"`` /
  ``"0"`` (case-insensitive), and the numbers ``0`` / ``1``.
- ``type="text"`` with ``max_chars`` — truncates with ellipsis; full text is
  preserved in the cell's ``title`` attribute.
"""
    )

    renderer_df = pd.DataFrame(
        {
            "Product": [
                "Widget",
                "Gadget",
                "Gizmo",
                "Sprocket",
            ],
            "Homepage": [
                "https://example.com/widget",
                "https://example.com/gadget",
                "https://example.com/gizmo",
                "https://example.com/sprocket",
            ],
            "Poster": [
                "https://streamlit.io/images/brand/streamlit-mark-color.png",
                "https://streamlit.io/images/brand/streamlit-mark-color.png",
                "https://streamlit.io/images/brand/streamlit-mark-color.png",
                "https://streamlit.io/images/brand/streamlit-mark-color.png",
            ],
            "Active": [True, False, True, True],
            "Description": [
                "A small but mighty widget with a very long description "
                "that should definitely be truncated for readability.",
                "Gadget with a medium-length description that may or may not fit.",
                "Gizmo — short.",
                "Sprocket description that keeps going and going and going.",
            ],
            "Revenue": [1200.0, 850.5, 430.25, 2100.75],
        }
    )

    st_pivot_table(
        renderer_df,
        key="column_config_renderers_demo",
        rows=["Product", "Homepage", "Poster", "Active", "Description"],
        values=["Revenue"],
        aggregation={"Revenue": "sum"},
        number_format={"Revenue": "$,.2f"},
        column_config={
            "Homepage": st.column_config.LinkColumn(
                "Homepage",
                display_text="Visit {}",
            ),
            "Poster": st.column_config.ImageColumn("Poster", width="small"),
            "Active": st.column_config.CheckboxColumn("Active"),
            "Description": st.column_config.TextColumn(
                "Description",
                max_chars=40,
            ),
        },
        show_totals=True,
        interactive=True,
    )

    with st.expander("View Code"):
        st.code(
            """
st_pivot_table(
    renderer_df,
    key="column_config_renderers_demo",
    rows=["Product", "Homepage", "Poster", "Active", "Description"],
    values=["Revenue"],
    aggregation={"Revenue": "sum"},
    number_format={"Revenue": "$,.2f"},
    column_config={
        "Homepage":    st.column_config.LinkColumn("Homepage", display_text="Visit {}"),
        "Poster":      st.column_config.ImageColumn("Poster", width="small"),
        "Active":      st.column_config.CheckboxColumn("Active"),
        "Description": st.column_config.TextColumn("Description", max_chars=40),
    },
    show_totals=True,
    interactive=True,
)
""",
            language="python",
        )


section_column_config_renderers()


# ---------------------------------------------------------------------------
# Section N: Styling API
# ---------------------------------------------------------------------------
def section_styling() -> None:
    st.divider()
    st.subheader("Styling API")
    st.markdown(
        """
The `style=` parameter accepts a **preset name**, a `PivotStyle` dict, or a **list** that
composes presets and overrides (merged left-to-right). Styles are a thin per-table layer over
Streamlit's `[theme]` — they automatically adapt to light/dark mode and custom themes.

> **Tip:** Use `var(--st-...)` tokens for theme-aware custom colors.
        """
    )

    _style_df = df_medium.copy()

    # --- 1. Preset gallery ---
    st.markdown("#### 1. Preset gallery")
    st.caption(
        "All built-in presets shown side-by-side. Every color value is a "
        "`var(--st-...)` token — no raw hex — so presets adapt to light/dark "
        "mode and custom `[theme]` configs automatically."
    )
    preset_items = list(PIVOT_STYLE_PRESETS.items())
    for row_presets in [preset_items[:3], preset_items[3:]]:
        preset_cols = st.columns(3)
        for col, (preset_name, _) in zip(preset_cols, row_presets):
            with col:
                st.caption(f"**`{preset_name!r}`**")
                st_pivot_table(
                    _style_df,
                    key=f"style_preset_{preset_name}",
                    rows=["Region"],
                    columns=["Category"],
                    values=["Revenue"],
                    aggregation={"Revenue": "sum"},
                    number_format={"Revenue": "$,.0f"},
                    show_totals=True,
                    interactive=False,
                    style=preset_name,
                )

    with st.expander("View Code"):
        st.code(
            """
# Pick any preset by name
st_pivot_table(df, key="my_pivot", ..., style="striped")
st_pivot_table(df, key="my_pivot2", ..., style="compact")
st_pivot_table(df, key="my_pivot3", ..., style="minimal")
""",
            language="python",
        )

    # --- 2. Streamlit theme match ---
    st.markdown("#### 2. Theme-aware custom colors")
    st.caption(
        "Use `var(--st-...)` tokens as color values so your customizations "
        "track light/dark mode and custom `[theme]` configs automatically."
    )
    st_pivot_table(
        _style_df,
        key="style_theme_match",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue", "Profit"],
        aggregation={"Revenue": "sum", "Profit": "sum"},
        number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
        show_totals=True,
        interactive=False,
        style=PivotStyle(
            background_color="var(--st-secondary-background-color)",
            column_header=RegionStyle(
                background_color="color-mix(in srgb, var(--st-primary-color) 80%, var(--st-background-color))",
                text_color="var(--st-background-color)",
                font_weight="bold",
            ),
            row_total=RegionStyle(
                background_color="color-mix(in srgb, var(--st-primary-color) 15%, var(--st-background-color))",
                font_weight="bold",
            ),
            column_total=RegionStyle(
                background_color="color-mix(in srgb, var(--st-primary-color) 15%, var(--st-background-color))",
                font_weight="bold",
            ),
        ),
    )
    with st.expander("View Code"):
        st.code(
            """
from streamlit_pivot import PivotStyle, RegionStyle

st_pivot_table(
    df, key="theme_match",
    rows=["Region"], columns=["Category"], values=["Revenue", "Profit"],
    style=PivotStyle(
        background_color="var(--st-secondary-background-color)",
        column_header=RegionStyle(
            background_color="color-mix(in srgb, var(--st-primary-color) 80%, var(--st-background-color))",
            text_color="var(--st-background-color)",
            font_weight="bold",
        ),
        row_total=RegionStyle(
            background_color="color-mix(in srgb, var(--st-primary-color) 15%, var(--st-background-color))",
            font_weight="bold",
        ),
        column_total=RegionStyle(
            background_color="color-mix(in srgb, var(--st-primary-color) 15%, var(--st-background-color))",
            font_weight="bold",
        ),
    ),
)
""",
            language="python",
        )

    # --- 3. Financial / editorial (borders="rows") ---
    st.markdown("#### 3. Financial / editorial style — horizontal rules only")
    st.caption(
        "Horizontal-rules-only border mode with no hover or stripes gives a "
        "clean editorial look suited to financial reports."
    )
    st_pivot_table(
        _style_df,
        key="style_financial",
        rows=["Region", "Product"],
        columns=["Category"],
        values=["Revenue", "Profit"],
        aggregation={"Revenue": "sum", "Profit": "sum"},
        number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
        show_totals=True,
        interactive=False,
        style=PivotStyle(
            borders="rows",
            row_hover_color=None,
            stripe_color=None,
            border_color="color-mix(in srgb, var(--st-text-color) 15%, transparent)",
        ),
    )
    with st.expander("View Code"):
        st.code(
            """
st_pivot_table(
    df, key="financial",
    rows=["Region", "Product"], columns=["Category"],
    values=["Revenue", "Profit"],
    style=PivotStyle(
        borders="rows",
        row_hover_color=None,
        stripe_color=None,
        border_color="color-mix(in srgb, var(--st-text-color) 15%, transparent)",
    ),
)
""",
            language="python",
        )

    # --- 4. Per-measure styling ---
    st.markdown("#### 4. Per-measure data cell styling")
    st.caption(
        "`data_cell_by_measure` applies background/text/weight overrides to non-total "
        "data cells of a specific measure. Total cells use `row_total` / `column_total` "
        "region overrides instead — this matches Power BI's Values-only scoping."
    )
    st_pivot_table(
        _style_df,
        key="style_per_measure",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue", "Profit"],
        aggregation={"Revenue": "sum", "Profit": "sum"},
        number_format={"Revenue": "$,.0f", "Profit": "$,.0f"},
        show_totals=True,
        interactive=False,
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
    with st.expander("View Code"):
        st.code(
            """
st_pivot_table(
    df, key="per_measure",
    rows=["Region"], columns=["Category"],
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
# Note: Revenue/Profit TOTAL cells are NOT affected — they use row_total / column_total styling instead.
""",
            language="python",
        )

    # --- 5. Composition ---
    st.markdown("#### 5. Composition — list merging")
    st.caption(
        "Pass a **list** to compose presets and custom overrides. Items are merged "
        "left-to-right; later values win. Region dicts are merged at the field level. "
        "Note: the Grand Total / Total corner cell takes `column_total` styling — "
        "set both `row_total` and `column_total` to keep that cell consistent."
    )
    _tint = (
        "color-mix(in srgb, var(--st-primary-color) 15%, var(--st-background-color))"
    )
    st_pivot_table(
        _style_df,
        key="style_composition",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue"],
        aggregation={"Revenue": "sum"},
        number_format={"Revenue": "$,.0f"},
        show_totals=True,
        interactive=False,
        style=[
            "compact",
            "contrast",
            PivotStyle(
                row_total=RegionStyle(background_color=_tint),
                column_total=RegionStyle(background_color=_tint),
            ),
        ],
    )
    with st.expander("View Code"):
        st.code(
            """
tint = "color-mix(in srgb, var(--st-primary-color) 15%, var(--st-background-color))"
st_pivot_table(
    df, key="composition",
    rows=["Region"], columns=["Category"], values=["Revenue"],
    style=[
        "compact",          # tight padding
        "contrast",         # bold headers + stripe
        PivotStyle(         # per-table override: tint all total cells
            row_total=RegionStyle(background_color=tint),
            column_total=RegionStyle(background_color=tint),
            # Note: the Grand Total / Total corner takes column_total styling,
            # so set both to keep all total cells visually consistent.
        ),
    ],
)
""",
            language="python",
        )

    # --- 6. Density ---
    st.markdown("#### 6. Density")
    st.caption(
        "`density` controls cell padding (and virtualized row height). "
        "Three values: `'compact'` (3px / 6px), `'default'` (6px / 10px), "
        "and `'comfortable'` (10px / 14px)."
    )
    density_cols = st.columns(3)
    for col, density_val in zip(density_cols, ["compact", "default", "comfortable"]):
        with col:
            st.caption(f"**`density='{density_val}'`**")
            st_pivot_table(
                _style_df,
                key=f"style_density_{density_val}",
                rows=["Region"],
                columns=["Category"],
                values=["Revenue"],
                aggregation={"Revenue": "sum"},
                number_format={"Revenue": "$,.0f"},
                show_totals=True,
                interactive=False,
                style=PivotStyle(density=density_val),
            )
    with st.expander("View Code"):
        st.code(
            """
# density controls padding (and row height in virtualized/large tables)
st_pivot_table(df, key="tight",  ..., style=PivotStyle(density="compact"))
st_pivot_table(df, key="normal", ..., style=PivotStyle(density="default"))
st_pivot_table(df, key="airy",   ..., style=PivotStyle(density="comfortable"))

# same as using the built-in presets:
st_pivot_table(df, key="tight2", ..., style="compact")
st_pivot_table(df, key="airy2",  ..., style="comfortable")
""",
            language="python",
        )

    # --- 7. CF precedence ---
    st.markdown("#### 7. Conditional formatting wins over per-measure style")
    st.caption(
        "When both `data_cell_by_measure` and `conditional_formatting` apply to "
        "the same cell, conditional formatting always wins."
    )
    st_pivot_table(
        _style_df,
        key="style_cf_precedence",
        rows=["Region"],
        columns=["Category"],
        values=["Revenue"],
        aggregation={"Revenue": "sum"},
        number_format={"Revenue": "$,.0f"},
        show_totals=True,
        interactive=False,
        conditional_formatting=[
            {
                "type": "color_scale",
                "apply_to": ["Revenue"],
                "min_color": "rgba(63,185,80,0.15)",
                "max_color": "rgba(63,185,80,0.70)",
            }
        ],
        style=PivotStyle(
            data_cell_by_measure={
                "Revenue": RegionStyle(background_color="rgba(255,0,0,0.25)")
            }
        ),
    )
    with st.expander("View Code"):
        st.code(
            """
# CF color_scale wins over the red per-measure background
st_pivot_table(
    df, key="cf_wins",
    conditional_formatting=[{
        "type": "color_scale",
        "apply_to": ["Revenue"],
        "min_color": "rgba(63,185,80,0.15)",
        "max_color": "rgba(63,185,80,0.70)",
    }],
    style=PivotStyle(
        data_cell_by_measure={"Revenue": RegionStyle(background_color="rgba(255,0,0,0.25)")}
    ),
)
""",
            language="python",
        )


section_styling()

# ---------------------------------------------------------------------------
# Section 22: Filters Zone + FilterBar (report-level filtering)
# ---------------------------------------------------------------------------

st.subheader("22. Filters Zone + FilterBar (Report-Level Filtering)")


@st.fragment
def section_filter_bar():
    st.markdown(
        """
The **Filters zone** in the Settings panel lets users filter on any dimension field —
even one that is **not placed in Rows or Columns**. This is report-level (page) filtering,
similar to the filter shelf in Excel or Tableau.

Once a field is placed in the Filters zone and the config is applied, a **FilterBar** appears
above the pivot table (when sections are expanded). Each chip shows the field name; active
selections are indicated by a highlighted chip background. Clicking a chip opens a value
picker with search, Select All, and Clear All. Use the **×** button on a chip to remove the
field from the Filters zone entirely.

**Toolbar sections visibility (`show_sections`):**
- The **Collapse Sections** button in the toolbar collapses the Rows/Columns/Values cards and FilterBar
  into a compact single-line summary, e.g. `Rows: Region | Cols: Year | Values: Revenue, Profit ● 1 filter`.
- The summary dot shows the total number of active filters — both FilterBar filters and any header-menu filters applied to row/column dimensions.
- Set `show_sections=False` in Python to start collapsed by default.

**Filter chips in Settings panel:**
- Each chip in the Filters zone is **clickable**: clicking opens the value picker inline
  (same picker as the FilterBar above the table). Active filters highlight the chip background.
- Use the **×** button to remove a field from the Filters zone.

**Pre-configured example:**
- `Category` is in the Filters zone (off-axis) with an initial include filter.
- `Region` is in **both** Rows and the Filters zone (dual-role): its header-menu filter and
  FilterBar chip share the same underlying `config.filters["Region"]` state.

**Hybrid mode note:** In `threshold_hybrid` execution, unique values for off-axis filter fields
are pre-computed on the server (`filter_field_values` sidecar) so the picker is always
fully populated. Dual-role fields read unique values from the current data — a dependent-filter
effect where one active filter can narrow the options shown for others (standard BI behavior).

**Try it:**
1. Click the `Category` chip in the FilterBar → open picker → change the selection.
2. Open Settings → click a chip in the Filters zone to set values without leaving Settings.
3. Use the **Collapse Sections** button in the toolbar to hide sections; note the filter dot in the summary.
4. Drag `Category` from Available Fields directly into the Filters drop zone.
"""
    )

    col_left, col_right = st.columns(2)

    with col_left:
        st.caption("Pre-configured FilterBar (Category off-axis + Region dual-role)")
        st_pivot_table(
            df,
            key="filter_bar_demo",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue", "Profit"],
            filter_fields=["Category", "Region"],  # ordered Filters zone fields
            filters={"Category": {"include": ["Electronics", "Furniture"]}},
            show_totals=True,
        )

    with col_right:
        st.caption("Start from scratch — open Settings and use the Filters zone")
        st_pivot_table(
            df_medium,
            key="filter_bar_blank",
            rows=["Region"],
            columns=["Year"],
            values=["Revenue"],
        )

    with st.expander("View Code"):
        st.code(
            """
# Pre-configured: Category in Filters zone (off-axis) + Region in both Rows and Filters
st_pivot_table(
    df,
    key="filter_bar_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue", "Profit"],
    filter_fields=["Category", "Region"],  # ordered list of filter-bar fields
    filters={"Category": {"include": ["Electronics", "Furniture"]}},
    show_totals=True,
)

# Start collapsed (sections hidden, compact summary shown):
st_pivot_table(
    df,
    key="compact_demo",
    rows=["Region"],
    columns=["Year"],
    values=["Revenue"],
    filter_fields=["Category"],
    show_sections=False,   # collapses Rows/Columns/Values cards + FilterBar
)

# Users can also add fields interactively:
# 1. Open Settings → drag a field into the Filters zone (or use click menu)
# 2. Click the chip to set filter values without leaving Settings
# 3. Click Apply → FilterBar appears above the table (when sections expanded)
# 4. Use the ⊟ button in the toolbar to collapse all sections
""",
            language="python",
        )

    # Show what's currently in the config for the pre-configured demo
    demo_state = st.session_state.get("filter_bar_demo", {})
    if demo_state.get("config"):
        cfg = demo_state["config"]
        active_filters = cfg.get("filters") or {}
        filter_fields = cfg.get("filter_fields") or []
        if filter_fields:
            summaries = []
            for f in filter_fields:
                flt = active_filters.get(f)
                if not flt:
                    summaries.append(f"**{f}**: All")
                elif flt.get("include"):
                    summaries.append(f"**{f}**: {len(flt['include'])} selected")
                elif flt.get("exclude"):
                    summaries.append(f"**{f}**: {len(flt['exclude'])} excluded")
            st.caption("Active filter state: " + " · ".join(summaries))


section_filter_bar()

# ---------------------------------------------------------------------------
# Footer: Raw Data
# ---------------------------------------------------------------------------
st.divider()
with st.expander("View Source Data"):
    st.dataframe(df, width="stretch")
    st.caption(f"{len(df)} rows × {len(df.columns)} columns")
