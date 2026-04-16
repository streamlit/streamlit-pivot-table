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

"""Standalone demo app for comparing row layout modes.

Run locally with:
    streamlit run streamlit_layout_demo.py
"""

from __future__ import annotations

import datetime as dt
from typing import Literal, cast

import pandas as pd  # type: ignore[import-untyped]
import streamlit as st

from streamlit_pivot import st_pivot_table


def make_sales_demo_data() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    scenarios = [
        ("BI Tools", "Streamlit", "Enterprise", "Acme Corp", 2024, 420_000, 138_000),
        ("BI Tools", "Streamlit", "Enterprise", "Globex", 2024, 355_000, 122_000),
        ("BI Tools", "Sigma", "Enterprise", "Acme Corp", 2024, 390_000, 129_000),
        ("BI Tools", "Sigma", "Mid Market", "Initech", 2024, 210_000, 71_000),
        ("BI Tools", "Power BI", "Enterprise", "Umbrella", 2024, 330_000, 111_000),
        ("BI Tools", "Power BI", "Mid Market", "Soylent", 2024, 180_000, 58_000),
        (
            "AI Apps",
            "Cortex Analyst",
            "Enterprise",
            "Acme Corp",
            2024,
            510_000,
            189_000,
        ),
        ("AI Apps", "Cortex Analyst", "Enterprise", "Globex", 2024, 470_000, 172_000),
        ("AI Apps", "Copilot", "Mid Market", "Initech", 2024, 260_000, 88_000),
        ("AI Apps", "Copilot", "SMB", "Initrode", 2024, 120_000, 36_000),
        ("AI Apps", "Notebook UX", "Enterprise", "Umbrella", 2024, 300_000, 102_000),
        ("AI Apps", "Notebook UX", "SMB", "Soylent", 2024, 95_000, 29_000),
        ("BI Tools", "Streamlit", "Enterprise", "Acme Corp", 2025, 465_000, 151_000),
        ("BI Tools", "Streamlit", "Enterprise", "Globex", 2025, 380_000, 131_000),
        ("BI Tools", "Sigma", "Enterprise", "Acme Corp", 2025, 410_000, 135_000),
        ("BI Tools", "Sigma", "Mid Market", "Initech", 2025, 232_000, 79_000),
        ("BI Tools", "Power BI", "Enterprise", "Umbrella", 2025, 348_000, 118_000),
        ("BI Tools", "Power BI", "Mid Market", "Soylent", 2025, 192_000, 61_000),
        (
            "AI Apps",
            "Cortex Analyst",
            "Enterprise",
            "Acme Corp",
            2025,
            560_000,
            205_000,
        ),
        ("AI Apps", "Cortex Analyst", "Enterprise", "Globex", 2025, 498_000, 181_000),
        ("AI Apps", "Copilot", "Mid Market", "Initech", 2025, 279_000, 95_000),
        ("AI Apps", "Copilot", "SMB", "Initrode", 2025, 130_000, 39_000),
        ("AI Apps", "Notebook UX", "Enterprise", "Umbrella", 2025, 322_000, 110_000),
        ("AI Apps", "Notebook UX", "SMB", "Soylent", 2025, 108_000, 33_000),
    ]
    for use_case, product, segment, customer, year, arr, profit in scenarios:
        rows.append(
            {
                "Use Case": use_case,
                "Product": product,
                "Segment": segment,
                "Customer": customer,
                "Year": year,
                "ARR": arr,
                "Profit": profit,
                "Deals": 1,
            }
        )
    return pd.DataFrame(rows)


def make_temporal_demo_data() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for region, customers, base in [
        ("North America", ["Acme Corp", "Globex"], 120_000),
        ("Europe", ["Initech", "Umbrella"], 95_000),
    ]:
        for month_index, month in enumerate([1, 2, 3, 4, 5, 6], start=1):
            for customer_index, customer in enumerate(customers, start=1):
                booking = base + month_index * 8_000 + customer_index * 3_500
                rows.append(
                    {
                        "Order Date": dt.date(2024, month, 1),
                        "Region": region,
                        "Customer": customer,
                        "Bookings": booking,
                        "Pipeline": round(booking * 1.45),
                    }
                )
    return pd.DataFrame(rows)


st.set_page_config(page_title="Pivot Row Layout Demo", layout="wide")
st.title("Pivot Row Layout Demo")
st.caption(
    "This standalone app compares the two row rendering modes: "
    '`row_layout="table"` and `row_layout="hierarchy"`.'
)

st.markdown(
    """
Use this page to compare the same pivot configuration rendered in two ways:

- **Table**: one visible row-header column per row field
- **Hierarchy**: a single indented tree-style first column with parent rows and inline toggles

Open the **Settings** panel on either interactive pivot to switch the layout at runtime.
"""
)

sales_df = make_sales_demo_data()
temporal_df = make_temporal_demo_data()

st.divider()
st.subheader("Business Hierarchy Comparison")
st.markdown(
    """
This example uses a business hierarchy with four row levels so the layout difference is obvious.
The underlying grouping is identical in both pivots; only the row presentation changes.
"""
)

st.markdown("#### Table Layout")
st_pivot_table(
    sales_df,
    key="layout_demo_table",
    rows=["Use Case", "Product", "Segment", "Customer"],
    columns=["Year"],
    values=["ARR", "Profit"],
    aggregation={"ARR": "sum", "Profit": "sum"},
    show_totals=True,
    show_subtotals=True,
    repeat_row_labels=False,
    row_layout="table",
    number_format={"ARR": "$,.0f", "Profit": "$,.0f"},
    max_height=420,
)

st.markdown("#### Hierarchy Layout")
st_pivot_table(
    sales_df,
    key="layout_demo_hierarchy",
    rows=["Use Case", "Product", "Segment", "Customer"],
    columns=["Year"],
    values=["ARR", "Profit"],
    aggregation={"ARR": "sum", "Profit": "sum"},
    show_totals=True,
    row_layout="hierarchy",
    number_format={"ARR": "$,.0f", "Profit": "$,.0f"},
    max_height=420,
)

with st.expander("View code for the business hierarchy example"):
    st.code(
        """
st_pivot_table(
    sales_df,
    key="layout_demo_table",
    rows=["Use Case", "Product", "Segment", "Customer"],
    columns=["Year"],
    values=["ARR", "Profit"],
    aggregation={"ARR": "sum", "Profit": "sum"},
    show_totals=True,
    show_subtotals=True,
    repeat_row_labels=False,
    row_layout="table",
)

st_pivot_table(
    sales_df,
    key="layout_demo_hierarchy",
    rows=["Use Case", "Product", "Segment", "Customer"],
    columns=["Year"],
    values=["ARR", "Profit"],
    aggregation={"ARR": "sum", "Profit": "sum"},
    show_totals=True,
    row_layout="hierarchy",
)
""",
        language="python",
    )

st.divider()
st.subheader("Temporal Hierarchy Comparison")
st.markdown(
    """
This example puts a date field on rows so you can compare how temporal parents appear.
In **table** mode the renderer expands the date hierarchy into visible row-header columns.
In **hierarchy** mode those same levels render as a single indented tree.
"""
)

st.markdown("#### Table Layout with Row Date Hierarchy")
st_pivot_table(
    temporal_df,
    key="layout_demo_temporal_table",
    rows=["Order Date", "Region", "Customer"],
    columns=[],
    values=["Bookings", "Pipeline"],
    aggregation={"Bookings": "sum", "Pipeline": "sum"},
    show_totals=True,
    row_layout="table",
    number_format={"Bookings": "$,.0f", "Pipeline": "$,.0f"},
    max_height=420,
)

st.markdown("#### Hierarchy Layout with Row Date Hierarchy")
st_pivot_table(
    temporal_df,
    key="layout_demo_temporal_hierarchy",
    rows=["Order Date", "Region", "Customer"],
    columns=[],
    values=["Bookings", "Pipeline"],
    aggregation={"Bookings": "sum", "Pipeline": "sum"},
    show_totals=True,
    row_layout="hierarchy",
    number_format={"Bookings": "$,.0f", "Pipeline": "$,.0f"},
    max_height=420,
)

st.divider()
st.subheader("Interactive Layout Switch")
st.markdown(
    """
This final pivot starts in whichever mode you choose below so you can inspect the
same sample data with the exact config parameter that application code would set.
"""
)

selected_layout = st.radio(
    "Initial row layout",
    options=["table", "hierarchy"],
    horizontal=True,
)
selected_layout = cast(Literal["table", "hierarchy"], selected_layout)

st_pivot_table(
    sales_df,
    key="layout_demo_switchable",
    rows=["Use Case", "Product", "Segment", "Customer"],
    columns=["Year"],
    values=["ARR"],
    aggregation="sum",
    show_totals=True,
    row_layout=selected_layout,
    number_format={"ARR": "$,.0f"},
    max_height=420,
)

with st.expander("View sample data"):
    left, right = st.columns(2)
    with left:
        st.markdown("**Business hierarchy sample**")
        st.dataframe(sales_df, width="stretch")
    with right:
        st.markdown("**Temporal hierarchy sample**")
        st.dataframe(temporal_df, width="stretch")
