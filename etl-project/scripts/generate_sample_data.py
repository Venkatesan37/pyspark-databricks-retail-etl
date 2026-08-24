"""
generate_sample_data.py

Generates a synthetic, intentionally-messy retail sales dataset that mimics
real-world raw data problems: missing values, duplicate rows, inconsistent
date formats, inconsistent casing, and stray whitespace.

This raw file is what the Bronze layer notebook (01_bronze_ingestion.py)
ingests. Run this once to (re)create data/sample/retail_sales_raw.csv.

Usage:
    python scripts/generate_sample_data.py
"""

import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

RANDOM_SEED = 42
NUM_ROWS = 5000
OUTPUT_PATH = "data/sample/retail_sales_raw.csv"

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

REGIONS = ["North", "South", "East", "West", "north", "SOUTH", " East ", None]
CATEGORIES = [
    "Electronics",
    "Groceries",
    "Apparel",
    "Home & Kitchen",
    "Beauty",
    "Sports",
]
PRODUCTS = {
    "Electronics": ["Wireless Mouse", "USB-C Cable", "Bluetooth Speaker", "Power Bank"],
    "Groceries": ["Basmati Rice 5kg", "Sunflower Oil 1L", "Green Tea Pack", "Almonds 500g"],
    "Apparel": ["Cotton T-Shirt", "Denim Jeans", "Running Shoes", "Formal Shirt"],
    "Home & Kitchen": ["Non-stick Pan", "LED Bulb", "Storage Box", "Cutlery Set"],
    "Beauty": ["Face Wash", "Sunscreen SPF50", "Shampoo 400ml", "Lip Balm"],
    "Sports": ["Yoga Mat", "Cricket Bat", "Football", "Resistance Bands"],
}
PAYMENT_MODES = ["UPI", "Credit Card", "Debit Card", "Cash on Delivery", "upi", "COD"]

DATE_FORMATS = [
    "%Y-%m-%d",       # 2025-06-14
    "%d/%m/%Y",       # 14/06/2025
    "%d-%b-%Y",       # 14-Jun-2025
    "%m/%d/%Y",       # 06/14/2025
]


def random_date(start: datetime, end: datetime) -> str:
    """Return a random date between start/end, formatted inconsistently
    on purpose to simulate real-world messy source data."""
    delta_days = (end - start).days
    d = start + timedelta(days=random.randint(0, delta_days))
    fmt = random.choice(DATE_FORMATS)
    return d.strftime(fmt)


def build_dataset(num_rows: int) -> pd.DataFrame:
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 12, 31)

    rows = []
    for i in range(1, num_rows + 1):
        category = random.choice(CATEGORIES)
        product = random.choice(PRODUCTS[category])

        unit_price = round(random.uniform(99, 4999), 2)
        quantity = random.choice([1, 1, 1, 2, 2, 3, 4, 5, -1])  # -1 = data error
        region = random.choice(REGIONS)
        payment_mode = random.choice(PAYMENT_MODES)
        customer_id = f"CUST{random.randint(1000, 1999)}"

        # Inject missing values ~6% of the time on a few columns
        if random.random() < 0.05:
            unit_price = None
        if random.random() < 0.04:
            quantity = None
        if random.random() < 0.03:
            payment_mode = None

        row = {
            "order_id": f"ORD{100000 + i}",
            "order_date": random_date(start_date, end_date),
            "customer_id": customer_id,
            "region": region,
            "category": category,
            "product_name": f"  {product}  " if random.random() < 0.1 else product,
            "quantity": quantity,
            "unit_price": unit_price,
            "payment_mode": payment_mode,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Inject ~2% exact duplicate rows to simulate upstream duplication
    dup_count = int(num_rows * 0.02)
    dup_rows = df.sample(n=dup_count, random_state=RANDOM_SEED)
    df = pd.concat([df, dup_rows], ignore_index=True)

    # Shuffle so duplicates aren't neatly grouped
    df = df.sample(frac=1, random_state=RANDOM_SEED).reset_index(drop=True)
    return df


if __name__ == "__main__":
    dataset = build_dataset(NUM_ROWS)
    dataset.to_csv(OUTPUT_PATH, index=False)
    print(f"Generated {len(dataset)} rows -> {OUTPUT_PATH}")
    print(dataset.head(10).to_string())
