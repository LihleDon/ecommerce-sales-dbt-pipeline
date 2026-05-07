import duckdb
import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

DB_PATH = os.path.join("data", "ecommerce.duckdb")
TABLE_NAME = "raw_transactions"


def load(df: pd.DataFrame) -> None:
    """
    Load the cleaned DataFrame into DuckDB.
    CREATE OR REPLACE TABLE makes the pipeline idempotent — safe to re-run.
    """

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    logger.info(f"Connecting to DuckDB at: {DB_PATH}")

    con = duckdb.connect(DB_PATH)

    logger.info(f"Loading {len(df):,} rows into table '{TABLE_NAME}'")

    # DuckDB reads the pandas DataFrame directly from Python memory.
    # No intermediate CSV export needed.
    con.execute(f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} AS
        SELECT * FROM df
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    logger.info(f"Verified: {row_count:,} rows now in '{TABLE_NAME}'")

    sample = con.execute(f"""
        SELECT
            MIN(invoicedate)             AS earliest_date,
            MAX(invoicedate)             AS latest_date,
            COUNT(DISTINCT customer_id)  AS unique_customers,
            ROUND(SUM(line_revenue), 2)  AS total_revenue
        FROM {TABLE_NAME}
    """).fetchdf()

    logger.info("Summary of loaded data:")
    logger.info(f"\n{sample.to_string(index=False)}")

    # Close explicitly — on Windows an open DuckDB connection locks the file,
    # blocking any other process from reading or writing it.
    con.close()

    logger.info("Load complete. DuckDB connection closed.")


if __name__ == "__main__":
    from extract import extract
    from transform import transform
    df_raw = extract()
    df_clean = transform(df_raw)
    load(df_clean)
