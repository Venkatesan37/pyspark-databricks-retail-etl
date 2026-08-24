"""
schemas.py

Central place for schema definitions and small shared helpers used by the
Bronze / Silver / Gold notebooks. Keeping schemas here (rather than letting
Spark infer them ad-hoc in every notebook) makes the pipeline deterministic
and easier to unit test.
"""

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# Raw source data is read entirely as strings. We deliberately do NOT cast
# types at ingestion time -- that happens explicitly in the Silver layer so
# that malformed values (e.g. "-1" quantity, blank prices) are handled with
# visible, intentional logic rather than silently coerced/nulled by Spark's
# CSV reader.
RAW_RETAIL_SALES_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("payment_mode", StringType(), True),
    ]
)

# Canonical region names we standardize messy source values into.
REGION_LOOKUP = {
    "north": "North",
    "south": "South",
    "east": "East",
    "west": "West",
}

# Canonical payment mode names.
PAYMENT_MODE_LOOKUP = {
    "upi": "UPI",
    "cod": "Cash on Delivery",
    "cash on delivery": "Cash on Delivery",
    "credit card": "Credit Card",
    "debit card": "Debit Card",
}
