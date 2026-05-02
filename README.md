# ecommerce-sales-dbt-pipeline

A production-style ETL pipeline built on 1,067,371 real retail transactions from a UK online store. Raw messy data goes in — clean, queryable business metrics come out.

The pipeline strips out cancellations, missing customers, duplicates, and bad prices, leaving 779,425 verified rows loaded into a local DuckDB warehouse. dbt models sit on top to answer three business questions: who are the best customers, which products drive the most revenue, and how does revenue trend month over month.

---

## Architecture

Kaggle CSV  →  extract.py  →  transform.py  →  load.py  →  DuckDB
↓
dbt staging model (view)
↓
dbt mart models (tables)
┌───────────────┬──────────────────┐
customer          product            monthly
metrics           metrics            revenue

**Stack:** Python · Pandas · DuckDB · dbt Core

---

## What the pipeline does

Raw data enters with real problems: 19,494 cancelled transactions mixed into revenue figures, 243,007 rows with no customer ID, 26,124 duplicates, and customer IDs stored as floats. The transform step handles each category explicitly and logs exactly how many rows each step removes.

After cleaning, 779,425 rows load into DuckDB in under 2 seconds. Three dbt mart models then aggregate that data into customer-level spend metrics, product-level sales rankings, and a month-over-month revenue trend table with growth percentages.

---

## Results

| Metric | Value |
|---|---|
| Raw rows ingested | 1,067,371 |
| Clean rows after transform | 779,425 |
| Rows removed (cancellations, missing IDs, duplicates) | 287,946 |
| Total verified revenue | £17,374,804 |
| Unique customers | 5,878 |
| Date range | Dec 2009 – Dec 2011 |
| Pipeline runtime | ~5 seconds |

---

## Project structure

ecommerce-sales-dbt-pipeline/
├── data/
│   ├── raw/              ← source CSV (downloaded via Kaggle CLI, not committed)
│   └── processed/        ← reserved for future exports
├── notebooks/
│   └── explore_data.py   ← initial data profiling script
├── src/
│   ├── extract.py        ← reads raw CSV into a DataFrame
│   ├── transform.py      ← 10-step cleaning pipeline with row-level logging
│   ├── load.py           ← writes clean data to DuckDB
│   └── pipeline.py       ← orchestrates extract → transform → load
└── ecommerce_dbt/
└── models/
├── staging/
│   └── stg_transactions.sql     ← standardised base layer over raw table
└── marts/
├── mart_customer_metrics.sql    ← spend, orders, tenure per customer
├── mart_product_metrics.sql     ← revenue, units, reach per product
└── mart_monthly_revenue.sql     ← revenue trends with MoM growth %


---

## Setup

**Requirements:** Python 3.11+, Git, Kaggle account (free)

```bash
# Clone the repo
git clone https://github.com/LihleDon/ecommerce-sales-dbt-pipeline.git
cd ecommerce-sales-dbt-pipeline

# Create and activate virtual environment
python -m venv venv
venv\Scripts\Activate.ps1        # Windows
# source venv/bin/activate        # Mac/Linux

# Install dependencies
pip install -r requirements.txt

# Download the dataset (requires Kaggle API credentials at ~/.kaggle/kaggle.json)
kaggle datasets download -d mashlyn/online-retail-ii-uci -p data/raw --unzip

# Run the pipeline
cd src
python pipeline.py
```

**Run dbt models:**

```bash
# Configure your DuckDB path in ~/.dbt/profiles.yml first
cd ecommerce_dbt
dbt run
```

---

## Key dbt models

**`stg_transactions`** — a view over `raw_transactions` that adds a surrogate key and confirms column types. All mart models build from this layer, never directly from the raw table.

**`mart_customer_metrics`** — one row per customer. Includes total orders, total revenue, average order value, first and last order dates, and tenure in days. Ordered by revenue descending.

**`mart_product_metrics`** — one row per stock code. Includes units sold, total revenue, average unit price, and number of countries the product was ordered from.

**`mart_monthly_revenue`** — one row per calendar month. Includes order volume, active customers, total revenue, and month-over-month revenue growth percentage.

---

## Data quality decisions

**Cancelled transactions removed** — invoices starting with `C` are returns. Including them in revenue totals would undercount actual sales.

**Rows without Customer ID dropped** — 243,007 rows (23% of raw data) have no customer attached. Keeping them would corrupt any customer-level aggregation.

**Duplicates removed** — 26,124 exact duplicate rows likely from repeated data exports. Each unique transaction should appear once.

**Customer ID type fixed** — the raw CSV stores IDs as floats (`13085.0`). Stripped to clean integers (`13085`) during transform.

---

## Dataset

UCI Online Retail II dataset via Kaggle — real transactions from a UK-based online retailer, December 2009 to December 2011. Licensed CC0 (public domain).