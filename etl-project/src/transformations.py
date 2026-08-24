"""
transformations.py

Reusable PySpark transformation functions used by the Silver layer notebook
(02_silver_cleaning_transformation.py). Pulling this logic out of the
notebook and into a plain importable module means it can be unit tested
with pytest (see tests/test_transformations.py) instead of only being
verifiable by eyeballing notebook output.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "dd-MMM-yyyy", "MM/dd/yyyy"]


def standardize_column(df: DataFrame, col_name: str, lookup: dict) -> DataFrame:
    """Trim whitespace and map messy values (varying case/spacing) to a
    single canonical spelling using `lookup`. Values with no lookup match
    are trimmed and title-cased rather than dropped, so unexpected new
    values degrade gracefully instead of disappearing silently.
    """
    trimmed = F.trim(F.col(col_name))
    lower = F.lower(trimmed)
    mapping_expr = F.create_map([F.lit(x) for pair in lookup.items() for x in pair])

    return df.withColumn(col_name, F.coalesce(mapping_expr[lower], F.initcap(trimmed)))


def parse_order_date(df: DataFrame, col_name: str = "order_date") -> DataFrame:
    """Parse a string date column that may arrive in any of DATE_FORMATS.
    Tries each format in order and keeps the first successful parse;
    returns null if none match (caught later by validation)."""
    parsed = F.coalesce(*[F.to_date(F.col(col_name), fmt) for fmt in DATE_FORMATS])
    return df.withColumn(col_name, parsed)


def cast_numeric_columns(df: DataFrame) -> DataFrame:
    """Cast quantity/unit_price from string to proper numeric types."""
    return df.withColumn("quantity", F.col("quantity").cast(IntegerType())).withColumn(
        "unit_price", F.col("unit_price").cast(DoubleType())
    )


def validity_flag(df: DataFrame):
    """Return the boolean column expression used to split valid vs.
    rejected rows. Centralizing this rule means the Silver notebook and
    any tests always apply the exact same business rule."""
    return (
        F.col("order_date").isNotNull()
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("unit_price").isNotNull()
        & (F.col("unit_price") > 0)
    )


def add_total_amount(df: DataFrame) -> DataFrame:
    """Add the derived total_amount = quantity * unit_price column."""
    return df.withColumn(
        "total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )
