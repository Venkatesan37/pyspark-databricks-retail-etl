-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 - Analysis Queries (Databricks SQL)
-- MAGIC
-- MAGIC Example queries to run in a Databricks SQL editor or wire directly
-- MAGIC into dashboard visualizations / Power BI (via the Databricks SQL
-- MAGIC connector). All queries read only from the Gold layer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Revenue trend by region, last 12 months

-- COMMAND ----------

SELECT
  order_month,
  region,
  total_revenue,
  num_orders,
  avg_order_value
FROM main.gold.monthly_revenue_by_region
WHERE order_month >= add_months(current_date(), -12)
ORDER BY order_month, region;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Top 5 products per category by revenue

-- COMMAND ----------

SELECT
  category,
  product_name,
  total_revenue,
  total_units_sold,
  rank_in_category
FROM main.gold.top_products_by_revenue
WHERE rank_in_category <= 5
ORDER BY category, rank_in_category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Revenue share by payment mode

-- COMMAND ----------

SELECT
  payment_mode,
  total_revenue,
  num_orders,
  revenue_share_pct
FROM main.gold.payment_mode_breakdown
ORDER BY total_revenue DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Top 20 customers by lifetime spend

-- COMMAND ----------

SELECT
  customer_id,
  total_orders,
  lifetime_spend,
  avg_order_value,
  first_order_date,
  last_order_date
FROM main.gold.customer_order_summary
ORDER BY lifetime_spend DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Month-over-month overall revenue growth %
-- MAGIC Demonstrates a window function in SQL (LAG) on top of the Gold table.

-- COMMAND ----------

WITH monthly_totals AS (
  SELECT
    order_month,
    SUM(total_revenue) AS total_revenue
  FROM main.gold.monthly_revenue_by_region
  GROUP BY order_month
)
SELECT
  order_month,
  total_revenue,
  LAG(total_revenue) OVER (ORDER BY order_month) AS prev_month_revenue,
  ROUND(
    100 * (total_revenue - LAG(total_revenue) OVER (ORDER BY order_month))
      / LAG(total_revenue) OVER (ORDER BY order_month),
    2
  ) AS mom_growth_pct
FROM monthly_totals
ORDER BY order_month;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Data quality check: rejected rows by reason
-- MAGIC Quick query against the quarantine table from the Silver layer to
-- MAGIC monitor how much data is failing validation over time.

-- COMMAND ----------

SELECT
  CASE
    WHEN order_date IS NULL THEN 'unparsable_date'
    WHEN quantity IS NULL OR quantity <= 0 THEN 'invalid_quantity'
    WHEN unit_price IS NULL OR unit_price <= 0 THEN 'invalid_price'
    ELSE 'other'
  END AS rejection_reason,
  COUNT(*) AS num_rows
FROM main.silver.retail_sales_rejected
GROUP BY rejection_reason
ORDER BY num_rows DESC;
