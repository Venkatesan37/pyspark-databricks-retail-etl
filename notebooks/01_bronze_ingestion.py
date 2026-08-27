# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Raw Ingestion
# MAGIC
# MAGIC **Purpose:** Land the raw retail sales CSV into a Delta table exactly as
# MAGIC received, with no cleaning or type-casting. This preserves an auditable
# MAGIC copy of the source data (the "Bronze" layer in a medallion architecture)
# MAGIC so that any downstream cleaning bug can always be re-run from the
# MAGIC original data.
# MAGIC
# MAGIC **Input:**  `data/sample/retail_sales_raw.csv` (uploaded to DBFS/Volume)
# MAGIC **Output:** Delta table `bronze.retail_sales_raw`
# MAGIC
# MAGIC **Adding metadata columns:**
# MAGIC - `_ingested_at`   : timestamp this notebook ran
# MAGIC - `_source_file`   : originating file name, for traceability

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets (parameters)
# MAGIC Using widgets makes this notebook runnable as a Databricks Job with
# MAGIC different input paths without editing code.

# COMMAND ----------

dbutils.widgets.text("source_path", "/Volumes/main/default/raw/retail_sales_raw.csv", "Source CSV path")
dbutils.widgets.text("catalog", "main", "Unity Catalog name")
dbutils.widgets.text("schema", "bronze", "Target schema/database")

source_path = dbutils.widgets.get("source_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"source_path = {source_path}")
print(f"catalog     = {catalog}")
print(f"schema      = {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports & schema
# MAGIC We import the shared raw schema from `src/schemas.py` so the same
# MAGIC schema definition is version-controlled and reused everywhere it's
# MAGIC needed (rather than re-typed in every notebook).

# COMMAND ----------

import sys

sys.path.append("../src")

from pyspark.sql import functions as F

from schemas import RAW_RETAIL_SALES_SCHEMA

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read raw CSV
# MAGIC Every column is read as a string on purpose -- see `schemas.py` for why.

# COMMAND ----------

raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(RAW_RETAIL_SALES_SCHEMA)
    .load(source_path)
)

print(f"Rows read: {raw_df.count()}")
raw_df.printSchema()
display(raw_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add ingestion metadata
# MAGIC These columns make every row traceable back to the run and file that
# MAGIC produced it -- useful for debugging and for basic data lineage.

# COMMAND ----------

bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp()).withColumn(
    "_source_file", F.input_file_name()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Bronze Delta table
# MAGIC `mergeSchema` is enabled so that adding a new source column in the
# MAGIC future doesn't break the pipeline -- it simply appends to the schema.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.retail_sales_raw")
)

print(f"Wrote {bronze_df.count()} rows to {catalog}.{schema}.retail_sales_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick sanity check

# COMMAND ----------

display(
    spark.sql(f"SELECT * FROM {catalog}.{schema}.retail_sales_raw LIMIT 20")
)
