import duckdb    # the DuckDB Python client
import pandas as pd  # for type hints — the input is a DataFrame
import os        # for creating directories and building paths
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Path where the DuckDB database file will be saved
# This is a single file that acts as your entire local warehouse
DB_PATH = os.path.join("data", "ecommerce.duckdb")

# The name of the table we will create inside DuckDB
TABLE_NAME = "raw_transactions"


def load(df: pd.DataFrame) -> None:
    """
    Load the cleaned DataFrame into a DuckDB table.
    Creates or replaces the table on each run — this makes the pipeline idempotent.
    Idempotent means: running it once or ten times produces the same result.
    """

    # Create the data directory if it does not already exist
    # The DuckDB file lives at data/ecommerce.duckdb
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    # os.makedirs creates the directory and all parents if needed
    # exist_ok=True means no error if the directory already exists

    logger.info(f"Connecting to DuckDB at: {DB_PATH}")

    # Open a connection to the DuckDB file
    # If the file does not exist, DuckDB creates it automatically
    # duckdb.connect returns a connection object we use to run queries
    con = duckdb.connect(DB_PATH)

    logger.info(f"Loading {len(df):,} rows into table '{TABLE_NAME}'")

    # Load the DataFrame into DuckDB
    # DuckDB can read a pandas DataFrame directly — no need to convert to CSV first
    # CREATE OR REPLACE TABLE drops the old table if it exists and creates a fresh one
    # This is safe for a pipeline that runs repeatedly — you always get the latest data
    con.execute(f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} AS
        SELECT * FROM df
    """)
    # In this SQL: df refers to the pandas DataFrame variable in Python's local scope
    # DuckDB automatically sees it because it shares Python's memory — this is one of
    # DuckDB's most useful features for data engineering work

    # Verify the load by counting rows in the database
    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    # .fetchone() returns the first row of the result as a tuple
    # [0] gets the first (and only) value in that tuple — the row count
    logger.info(f"Verified: {row_count:,} rows now in '{TABLE_NAME}'")

    # Run a quick sanity check query and log the result
    sample = con.execute(f"""
        SELECT
            MIN(invoicedate) AS earliest_date,
            MAX(invoicedate) AS latest_date,
            COUNT(DISTINCT customer_id) AS unique_customers,
            ROUND(SUM(line_revenue), 2) AS total_revenue
        FROM {TABLE_NAME}
    """).fetchdf()
    # .fetchdf() returns the result as a pandas DataFrame — convenient for logging

    logger.info("Summary of loaded data:")
    logger.info(f"\n{sample.to_string(index=False)}")
    # to_string(index=False) prints the DataFrame without the row number index

    # Always close the database connection when you are done
    # Leaving connections open can cause file locking issues on Windows
    con.close()

    logger.info("Load complete. DuckDB connection closed.")


# Allow direct execution for testing the load step in isolation
if __name__ == "__main__":
    from extract import extract
    from transform import transform
    df_raw = extract()
    df_clean = transform(df_raw)
    load(df_clean)