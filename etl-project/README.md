# Retail Sales ETL Pipeline — PySpark + Databricks

An end-to-end batch ETL pipeline that ingests messy raw retail sales data,
cleans and validates it, and produces business-ready aggregate tables —
built entirely with **PySpark** and **Delta Lake**, designed to run on
**Databricks** (Community Edition or higher).

This project follows a **medallion architecture** (Bronze → Silver →
Gold), the standard pattern for production data engineering pipelines.
See [`docs/architecture.md`](docs/architecture.md) for the full diagram
and design notes.

## Why this project

Real source data is never clean. This pipeline was built around a
synthetic-but-realistic raw dataset that intentionally contains the kinds
of problems seen in production systems:

- Inconsistent casing and whitespace (`"north"`, `"SOUTH"`, `" East "`)
- Four different date formats in the same column
- Missing values in price, quantity, and payment fields
- Data-entry errors (negative quantities)
- ~2% duplicate rows

The pipeline detects, standardizes, and quarantines these issues
explicitly and auditably, rather than silently dropping or miscomputing
data.

## Tech stack

- **PySpark** — DataFrame transformations, window functions
- **Delta Lake** — ACID table storage, schema evolution, time travel
- **Databricks** (Community Edition) — notebook execution, Unity
  Catalog / Hive metastore, Databricks SQL for dashboarding
- **pytest** — unit tests for the cleaning logic, runnable locally
  without a Databricks cluster

## Project structure

```
.
├── README.md
├── requirements.txt
├── data/
│   └── sample/
│       └── retail_sales_raw.csv        # sample messy input data
├── scripts/
│   └── generate_sample_data.py         # regenerates the sample dataset
├── src/
│   ├── schemas.py                      # shared schema + lookup tables
│   └── transformations.py              # reusable, unit-testable cleaning logic
├── notebooks/
│   ├── 01_bronze_ingestion.py          # raw ingestion -> Bronze Delta table
│   ├── 02_silver_cleaning_transformation.py  # cleaning -> Silver Delta table
│   ├── 03_gold_aggregation.py          # business aggregates -> Gold Delta tables
│   └── 04_analysis_queries.sql         # dashboard-ready SQL queries
├── tests/
│   └── test_transformations.py         # pytest unit tests (local Spark session)
└── docs/
    └── architecture.md                 # architecture diagram + design notes
```

## Pipeline stages

1. **Bronze — `01_bronze_ingestion.py`**
   Reads the raw CSV with an explicit string schema (no silent type
   inference), tags every row with `_ingested_at` / `_source_file`
   metadata, and writes it untouched to a Delta table.

2. **Silver — `02_silver_cleaning_transformation.py`**
   - Drops exact duplicate rows
   - Standardizes `region` and `payment_mode` to canonical values
   - Parses `order_date` across 4 known source formats
   - Casts `quantity` / `unit_price` to proper numeric types
   - Splits rows into `silver.retail_sales_clean` (valid) and
     `silver.retail_sales_rejected` (quarantined, with reason
     queryable) instead of silently dropping bad data
   - Adds a derived `total_amount` column

3. **Gold — `03_gold_aggregation.py`**
   Builds four reporting tables:
   - `gold.monthly_revenue_by_region`
   - `gold.top_products_by_revenue` (ranked per category with a window function)
   - `gold.payment_mode_breakdown`
   - `gold.customer_order_summary`

4. **Analysis — `04_analysis_queries.sql`**
   Example Databricks SQL queries for dashboarding, including a
   month-over-month growth calculation and a data-quality/reject-reason
   report.

## How to run it on Databricks Community Edition

1. Sign up for a free [Databricks Community Edition](https://community.cloud.databricks.com/) account.
2. Clone this repo, then in Databricks: **Workspace → Import** each file
   under `notebooks/` (they're saved in Databricks' source `.py`/`.sql`
   notebook format, so they import as ready-to-run notebooks with the
   markdown cells and cell breaks intact).
3. Upload `data/sample/retail_sales_raw.csv` to a Unity Catalog Volume
   (or DBFS) and update the `source_path` widget in
   `01_bronze_ingestion.py` to match.
4. Attach all notebooks to a cluster and run in order: `01` → `02` → `03`
   → `04`.
5. (Optional) Build a Databricks SQL dashboard on top of the four Gold
   tables, or connect Power BI using the
   [Databricks SQL connector](https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi).

## Running the tests locally (no Databricks needed)

```bash
pip install -r requirements.txt
python scripts/generate_sample_data.py   # (re)generate the sample CSV
pytest tests/ -v
```

The tests spin up a local, in-process Spark session and validate the
cleaning/transformation logic in `src/transformations.py` directly —
region/payment standardization, multi-format date parsing, numeric
casting, the valid/invalid row split, and the derived `total_amount`
calculation.

## Data quality approach

Invalid rows are never silently dropped. They're written to
`silver.retail_sales_rejected` with a timestamp, and query #6 in
`04_analysis_queries.sql` breaks down *why* each row was rejected
(bad date, bad quantity, bad price). This makes the pipeline's data loss
fully auditable — an important property for any real production ETL job.

## Known limitation

Date parsing tries formats in a fixed order (`yyyy-MM-dd` →
`dd/MM/yyyy` → `dd-MMM-yyyy` → `MM/dd/yyyy`). For a small number of
genuinely ambiguous slash-dates (e.g. `07/08/2025`, valid as both 7 Aug
and Jul 8), the first matching format in that order wins. This is a
common real-world data engineering trade-off — flagged here rather than
hidden, since resolving it fully would require a source-system format
contract that a raw CSV export doesn't provide.

## Possible extensions

- Convert the batch Bronze ingestion to Databricks Auto Loader for
  incremental/streaming ingestion
- Add Delta Live Tables (DLT) expectations instead of manual validity
  filtering
- Schedule the three notebooks as a Databricks Workflow (Job) with
  task dependencies
- Add Great Expectations or `dbt` tests for additional data quality
  coverage

## Author

K Venkatesan — [LinkedIn](https://www.linkedin.com/in/venkatesan-k-5151042b5)
