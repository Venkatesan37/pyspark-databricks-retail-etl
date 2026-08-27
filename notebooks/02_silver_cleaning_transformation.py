# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer: Cleaning & Transformation
# MAGIC
# MAGIC **Purpose:** Take the raw Bronze table and produce a clean, typed,
# MAGIC de-duplicated table that downstream analytics can trust.
# MAGIC
# MAGIC **Cleaning steps applied:**
# MAGIC 1. Drop exact duplicate rows
# MAGIC 2. Standardize `region` and `payment_mode` (casing, whitespace)
# MAGIC 3. Parse `order_date` from multiple inconsistent formats into a single `DateType`
# MAGIC 4. Cast `quantity` and `unit_price` to numeric types
# MAGIC 5. Flag and remove invalid rows (negative quantity, null price after cast, unparsable dates)
# MAGIC 6. Trim stray whitespace from text fields
# MAGIC
# MAGIC **Input:**  `bronze.retail_sales_raw`
# MAGIC **Output:** Delta table `silver.retail_sales_clean` (+ a `silver.retail_sales_rejected` quarantine table)

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import sys

sys.path.append("../src")

from pyspark.sql import functions as F

from schemas import PAYMENT_MODE_LOOKUP, REGION_LOOKUP
from transformations import (
    add_total_amount,
    cast_numeric_columns,
    parse_order_date,
    standardize_column,
    validity_flag,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Bronze data

# COMMAND ----------

bronze_df = spark.table(f"{catalog}.bronze.retail_sales_raw")
print(f"Bronze row count (incl. duplicates): {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Drop exact duplicates
# MAGIC We compare only the business columns (excluding ingestion metadata,
# MAGIC which differs per run) so true duplicate orders are caught.

# COMMAND ----------

business_cols = [
    "order_id",
    "order_date",
    "customer_id",
    "region",
    "category",
    "product_name",
    "quantity",
    "unit_price",
    "payment_mode",
]

deduped_df = bronze_df.dropDuplicates(business_cols)
print(f"Rows after de-duplication: {deduped_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Standardize text fields
# MAGIC Trims whitespace, and maps messy region / payment values (e.g.
# MAGIC `"north"`, `" East "`, `"upi"`, `"COD"`) to a single canonical spelling
# MAGIC using the lookup tables defined in `src/schemas.py`.

# COMMAND ----------

clean_df = deduped_df
clean_df = standardize_column(clean_df, "region", REGION_LOOKUP)
clean_df = standardize_column(clean_df, "payment_mode", PAYMENT_MODE_LOOKUP)
clean_df = clean_df.withColumn("product_name", F.trim(F.col("product_name")))
clean_df = clean_df.withColumn("customer_id", F.trim(F.col("customer_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Parse inconsistent date formats
# MAGIC The source system has sent dates in at least four formats over time.
# MAGIC We try each known format in order and keep the first successful parse.

# COMMAND ----------

clean_df = parse_order_date(clean_df, "order_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Cast numeric columns

# COMMAND ----------

clean_df = cast_numeric_columns(clean_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Quarantine invalid rows
# MAGIC A row is invalid if any of the following are true:
# MAGIC - `order_date` failed to parse (null after Step 3)
# MAGIC - `quantity` is null or <= 0 (e.g. the `-1` data-entry errors seen in source)
# MAGIC - `unit_price` is null or <= 0
# MAGIC
# MAGIC Invalid rows are written to a separate `_rejected` table rather than
# MAGIC silently dropped, so they remain auditable and can be investigated or
# MAGIC re-processed later.

# COMMAND ----------

is_valid = validity_flag(clean_df)

valid_df = clean_df.filter(is_valid).withColumn("_cleaned_at", F.current_timestamp())
rejected_df = clean_df.filter(~is_valid).withColumn(
    "_rejected_at", F.current_timestamp()
)

print(f"Valid rows:    {valid_df.count()}")
print(f"Rejected rows: {rejected_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Add a derived column
# MAGIC `total_amount` is computed once here so every downstream consumer uses
# MAGIC the same calculation instead of recomputing `quantity * unit_price`
# MAGIC independently (and potentially inconsistently).

# COMMAND ----------

valid_df = add_total_amount(valid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Silver tables

# COMMAND ----------

(
    valid_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.retail_sales_clean")
)

(
    rejected_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.retail_sales_rejected")
)

print("Silver layer write complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data quality summary
# MAGIC A quick printed report -- in a production job this would be pushed to
# MAGIC a monitoring table or alerting channel instead of just printed.

# COMMAND ----------

total_bronze = bronze_df.count()
total_valid = valid_df.count()
total_rejected = rejected_df.count()

print("===== Data Quality Summary =====")
print(f"Bronze rows (raw, incl. dupes): {total_bronze}")
print(f"Silver valid rows:              {total_valid}")
print(f"Silver rejected rows:           {total_rejected}")
print(f"Reject rate:                    {round(100 * total_rejected / total_bronze, 2)}%")
