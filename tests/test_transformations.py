"""
test_transformations.py

Unit tests for src/transformations.py using a local PySpark session.

Run with:
    pytest tests/ -v

Requires `pyspark` installed locally (see requirements.txt). No Databricks
cluster or cloud account is needed -- these tests run against a local,
in-process Spark session.
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pyspark.sql import SparkSession

from schemas import PAYMENT_MODE_LOOKUP, REGION_LOOKUP
from transformations import (
    add_total_amount,
    cast_numeric_columns,
    parse_order_date,
    standardize_column,
    validity_flag,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-retail-etl")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_standardize_region_handles_case_and_whitespace(spark):
    df = spark.createDataFrame(
        [("north",), ("SOUTH",), (" East ",), ("West",), (None,)], ["region"]
    )
    result = standardize_column(df, "region", REGION_LOOKUP).collect()
    values = [r["region"] for r in result]

    assert "North" in values
    assert "South" in values
    assert "East" in values
    assert "West" in values


def test_standardize_payment_mode(spark):
    df = spark.createDataFrame([("upi",), ("COD",), ("Credit Card",)], ["payment_mode"])
    result = standardize_column(df, "payment_mode", PAYMENT_MODE_LOOKUP).collect()
    values = [r["payment_mode"] for r in result]

    assert "UPI" in values
    assert "Cash on Delivery" in values
    assert "Credit Card" in values


def test_parse_order_date_handles_multiple_formats(spark):
    df = spark.createDataFrame(
        [
            ("2025-06-14",),
            ("14/06/2025",),
            ("14-Jun-2025",),
            ("06/14/2025",),
            ("not-a-date",),
        ],
        ["order_date"],
    )
    result = parse_order_date(df, "order_date").collect()
    parsed = [r["order_date"] for r in result]

    # First four should all parse successfully (non-null)
    assert all(d is not None for d in parsed[:4])
    # The garbage value should fail to parse -> null, caught later by validity_flag
    assert parsed[4] is None


def test_cast_numeric_columns(spark):
    df = spark.createDataFrame(
        [("3", "199.99"), ("-1", "50.0"), (None, "20.0")],
        ["quantity", "unit_price"],
    )
    result = cast_numeric_columns(df).collect()

    assert result[0]["quantity"] == 3
    assert result[0]["unit_price"] == 199.99
    assert result[1]["quantity"] == -1  # cast succeeds; validity_flag rejects it later
    assert result[2]["quantity"] is None


def test_validity_flag_rejects_bad_rows(spark):
    df = spark.createDataFrame(
        [
            ("2025-01-01", 2, 100.0),   # valid
            (None, 2, 100.0),           # invalid: no date
            ("2025-01-01", -1, 100.0),  # invalid: negative quantity
            ("2025-01-01", 2, None),    # invalid: null price
        ],
        ["order_date", "quantity", "unit_price"],
    )
    df = df.withColumn("order_date", df["order_date"].cast("date"))

    result = df.withColumn("is_valid", validity_flag(df)).collect()
    flags = [r["is_valid"] for r in result]

    assert flags == [True, False, False, False]


def test_add_total_amount(spark):
    df = spark.createDataFrame([(3, 10.0), (2, 5.5)], ["quantity", "unit_price"])
    result = add_total_amount(df).collect()

    assert result[0]["total_amount"] == 30.0
    assert result[1]["total_amount"] == 11.0
