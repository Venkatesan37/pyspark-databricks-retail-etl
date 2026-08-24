# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer: Business Aggregations
# MAGIC
# MAGIC **Purpose:** Build the reporting-ready tables that a BI tool (Power BI /
# MAGIC Databricks SQL dashboard) would query directly. Each table answers one
# MAGIC specific business question, pre-aggregated so dashboards stay fast.
# MAGIC
# MAGIC **Input:**  `silver.retail_sales_clean`
# MAGIC **Output:**
# MAGIC - `gold.monthly_revenue_by_region`
# MAGIC - `gold.top_products_by_revenue`
# MAGIC - `gold.payment_mode_breakdown`
# MAGIC - `gold.customer_order_summary`

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Unity Catalog name")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

silver_df = spark.table(f"{catalog}.silver.retail_sales_clean")
print(f"Silver rows: {silver_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold table 1 - Monthly revenue by region
# MAGIC Powers a time-series chart: revenue trend per region over time.

# COMMAND ----------

monthly_revenue_by_region = (
    silver_df.withColumn("order_month", F.date_trunc("month", F.col("order_date")))
    .groupBy("order_month", "region")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("order_id").alias("num_orders"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
    )
    .orderBy("order_month", "region")
)

display(monthly_revenue_by_region)

(
    monthly_revenue_by_region.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.monthly_revenue_by_region")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold table 2 - Top products by revenue
# MAGIC Ranks products within each category using a window function -- powers
# MAGIC a "top 5 products per category" chart.

# COMMAND ----------

product_revenue = silver_df.groupBy("category", "product_name").agg(
    F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    F.sum("quantity").alias("total_units_sold"),
)

rank_window = Window.partitionBy("category").orderBy(F.desc("total_revenue"))

top_products_by_revenue = product_revenue.withColumn(
    "rank_in_category", F.rank().over(rank_window)
).orderBy("category", "rank_in_category")

display(top_products_by_revenue.filter(F.col("rank_in_category") <= 5))

(
    top_products_by_revenue.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.top_products_by_revenue")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold table 3 - Payment mode breakdown
# MAGIC Powers a simple pie/bar chart of revenue share by payment method.

# COMMAND ----------

payment_mode_breakdown = (
    silver_df.groupBy("payment_mode")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.count("order_id").alias("num_orders"),
    )
    .withColumn(
        "revenue_share_pct",
        F.round(
            100 * F.col("total_revenue") / F.sum("total_revenue").over(Window.partitionBy()),
            2,
        ),
    )
    .orderBy(F.desc("total_revenue"))
)

display(payment_mode_breakdown)

(
    payment_mode_breakdown.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.payment_mode_breakdown")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold table 4 - Customer order summary
# MAGIC A per-customer rollup: lifetime spend, order count, first/last order
# MAGIC date. Useful for a customer-value table in the dashboard.

# COMMAND ----------

customer_order_summary = silver_df.groupBy("customer_id").agg(
    F.count("order_id").alias("total_orders"),
    F.round(F.sum("total_amount"), 2).alias("lifetime_spend"),
    F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
    F.min("order_date").alias("first_order_date"),
    F.max("order_date").alias("last_order_date"),
).orderBy(F.desc("lifetime_spend"))

display(customer_order_summary.limit(20))

(
    customer_order_summary.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.gold.customer_order_summary")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC All four Gold tables are now ready to be connected to Databricks SQL
# MAGIC dashboards or an external BI tool (e.g. Power BI via the Databricks
# MAGIC connector). See `notebooks/04_analysis_queries.sql` for example queries.
print("Gold layer build complete.")
