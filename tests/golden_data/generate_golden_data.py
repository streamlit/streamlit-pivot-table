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

"""Generate golden datasets for tests, demos, and performance benchmarks.

Run once to create the CSV files:
    python tests/golden_data/generate_golden_data.py

Datasets:
    small.csv   – 100 rows, 6 cols (3 dims, 3 measures); unit tests & demo
    medium.csv  – 10,000 rows, 12 cols; integration tests
    large.csv   – 200,000 rows, 15 cols; performance benchmarks only
    edge_*.csv  – Edge case files for specific test scenarios
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

OUT = Path(__file__).parent
RNG = np.random.default_rng(42)

# ── Dimension pools ──────────────────────────────────────────────────────

REGIONS = ["North", "South", "East", "West"]
CATEGORIES = ["Electronics", "Clothing", "Food", "Furniture", "Sports"]
PRODUCTS = [
    "Laptop",
    "Phone",
    "Tablet",
    "TV",
    "Headphones",
    "Shirt",
    "Pants",
    "Jacket",
    "Shoes",
    "Hat",
    "Bread",
    "Milk",
    "Cheese",
    "Fruit",
    "Snacks",
    "Desk",
    "Chair",
    "Shelf",
    "Lamp",
    "Rug",
    "Ball",
    "Racket",
    "Gloves",
    "Bike",
    "Helmet",
]
YEARS = ["2022", "2023", "2024"]
QUARTERS = ["Q1", "Q2", "Q3", "Q4"]
CHANNELS = ["Online", "Retail", "Wholesale"]
SEGMENTS = ["Consumer", "Business", "Government"]
PRIORITIES = ["High", "Medium", "Low"]
STATUSES = ["Delivered", "Shipped", "Processing", "Returned"]
COUNTRIES = [
    "US",
    "CA",
    "UK",
    "DE",
    "FR",
    "JP",
    "AU",
    "BR",
    "IN",
    "MX",
    "IT",
    "ES",
    "NL",
    "SE",
    "NO",
    "KR",
    "SG",
    "NZ",
    "ZA",
    "AE",
]
CITIES = [f"City_{i:03d}" for i in range(250)]


def _small() -> pd.DataFrame:
    """100 rows, 6 columns (3 dimensions, 3 measures)."""
    n = 100
    return pd.DataFrame(
        {
            "Region": RNG.choice(REGIONS, n),
            "Category": RNG.choice(CATEGORIES, n),
            "Year": RNG.choice(YEARS, n),
            "Revenue": RNG.uniform(100, 10000, n).round(2),
            "Units": RNG.integers(1, 500, n),
            "Profit": RNG.uniform(-500, 5000, n).round(2),
        }
    )


def _medium() -> pd.DataFrame:
    """10,000 rows, 12 columns."""
    n = 10_000
    return pd.DataFrame(
        {
            "Region": RNG.choice(REGIONS, n),
            "Country": RNG.choice(COUNTRIES[:10], n),
            "Category": RNG.choice(CATEGORIES, n),
            "Product": RNG.choice(PRODUCTS, n),
            "Year": RNG.choice(YEARS, n),
            "Quarter": RNG.choice(QUARTERS, n),
            "Channel": RNG.choice(CHANNELS, n),
            "Revenue": RNG.uniform(10, 50000, n).round(2),
            "Units": RNG.integers(1, 1000, n),
            "Profit": RNG.uniform(-2000, 20000, n).round(2),
            "Discount": RNG.uniform(0, 0.5, n).round(3),
            "Rating": RNG.uniform(1, 5, n).round(1),
        }
    )


def _large() -> pd.DataFrame:
    """200,000 rows, 15 columns."""
    n = 200_000
    return pd.DataFrame(
        {
            "Region": RNG.choice(REGIONS, n),
            "Country": RNG.choice(COUNTRIES, n),
            "City": RNG.choice(CITIES, n),
            "Category": RNG.choice(CATEGORIES, n),
            "Product": RNG.choice(PRODUCTS, n),
            "Year": RNG.choice(YEARS, n),
            "Quarter": RNG.choice(QUARTERS, n),
            "Channel": RNG.choice(CHANNELS, n),
            "Segment": RNG.choice(SEGMENTS, n),
            "Priority": RNG.choice(PRIORITIES, n),
            "Revenue": RNG.uniform(1, 100000, n).round(2),
            "Units": RNG.integers(1, 5000, n),
            "Profit": RNG.uniform(-10000, 50000, n).round(2),
            "Discount": RNG.uniform(0, 0.7, n).round(3),
            "Rating": RNG.uniform(1, 5, n).round(1),
        }
    )


# ── Edge cases ───────────────────────────────────────────────────────────


def _edge_nulls() -> pd.DataFrame:
    """Rows with nulls in dimensions and measures."""
    df = _small().copy()
    null_idx = RNG.choice(len(df), size=20, replace=False)
    df.loc[null_idx[:7], "Region"] = None
    df.loc[null_idx[7:14], "Revenue"] = None
    df.loc[null_idx[14:], "Profit"] = None
    return df


def _edge_single_row() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Region": ["North"],
            "Category": ["Electronics"],
            "Year": ["2024"],
            "Revenue": [1234.56],
            "Units": [42],
            "Profit": [567.89],
        }
    )


def _edge_single_column() -> pd.DataFrame:
    """Only one dimension and one measure."""
    n = 50
    return pd.DataFrame(
        {
            "Group": RNG.choice(["A", "B", "C"], n),
            "Value": RNG.uniform(0, 100, n).round(2),
        }
    )


def _edge_all_nulls() -> pd.DataFrame:
    """All measure values are null."""
    n = 20
    return pd.DataFrame(
        {
            "Region": RNG.choice(REGIONS, n),
            "Category": RNG.choice(CATEGORIES[:3], n),
            "Revenue": [None] * n,
            "Units": [None] * n,
        }
    )


def _edge_high_cardinality() -> pd.DataFrame:
    """High-cardinality dimension (250 unique cities) to test column cap."""
    n = 2000
    return pd.DataFrame(
        {
            "City": RNG.choice(CITIES, n),
            "Category": RNG.choice(CATEGORIES, n),
            "Revenue": RNG.uniform(100, 10000, n).round(2),
        }
    )


def _edge_mixed_types() -> pd.DataFrame:
    """Dimensions with numeric-looking strings and actual numbers."""
    n = 50
    return pd.DataFrame(
        {
            "Code": RNG.choice(["100", "200", "ABC", "300", "DEF", "010"], n),
            "Flag": RNG.choice([True, False], n),
            "Amount": RNG.uniform(0, 1000, n).round(2),
        }
    )


def _edge_single_value_groups() -> pd.DataFrame:
    """Every group has exactly one row."""
    items = [f"Item_{i}" for i in range(30)]
    return pd.DataFrame(
        {
            "Item": items,
            "Region": RNG.choice(REGIONS, 30),
            "Value": RNG.uniform(10, 500, 30).round(2),
        }
    )


if __name__ == "__main__":
    datasets = {
        "small.csv": _small,
        "medium.csv": _medium,
        "large.csv": _large,
        "edge_nulls.csv": _edge_nulls,
        "edge_single_row.csv": _edge_single_row,
        "edge_single_column.csv": _edge_single_column,
        "edge_all_nulls.csv": _edge_all_nulls,
        "edge_high_cardinality.csv": _edge_high_cardinality,
        "edge_mixed_types.csv": _edge_mixed_types,
        "edge_single_value_groups.csv": _edge_single_value_groups,
    }

    for name, factory in datasets.items():
        path = OUT / name
        df = factory()
        df.to_csv(path, index=False)
        print(
            f"  {name:35s} {len(df):>7,} rows  {len(df.columns):>2} cols  ({os.path.getsize(path):>10,} bytes)"
        )

    print(f"\nGenerated {len(datasets)} datasets in {OUT}")
