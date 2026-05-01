import pandas as pd   # for all DataFrame operations
import logging        # for structured log output

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def log_shape(df: pd.DataFrame, step: str) -> None:
    """
    Helper function that logs the row count after each cleaning step.
    This makes it easy to see exactly how many rows each step removes.
    Useful for debugging and for explaining your choices in an interview.
    """
    logger.info(f"  After '{step}': {len(df):,} rows remain")


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardise the raw retail DataFrame.
    Each step removes a specific category of bad data.
    Returns a clean DataFrame ready to load into DuckDB.
    """

    logger.info(f"Starting transform — input shape: {df.shape}")

    # --- Step 1: Standardise column names ---
    # Replace spaces in column names with underscores, make everything lowercase
    # "Customer ID" becomes "customer_id", "StockCode" becomes "stockcode"
    # Consistent snake_case names are easier to query in SQL and dbt
    df.columns = (
        df.columns
        .str.strip()       # remove any leading/trailing whitespace from column names
        .str.lower()       # lowercase everything
        .str.replace(" ", "_", regex=False)  # replace spaces with underscores
    )
    logger.info(f"Columns renamed: {list(df.columns)}")

    # --- Step 2: Remove cancelled transactions ---
    # Cancelled invoices start with 'C' in this dataset (e.g. C536379)
    # These represent returns/refunds — they are not forward sales
    # We exclude them for this pipeline which focuses on completed sales
    before = len(df)
    df = df[~df["invoice"].astype(str).str.startswith("C")]
    # ~ is the NOT operator — it inverts the boolean mask
    # so we keep rows where the invoice does NOT start with 'C'
    log_shape(df, "remove cancelled invoices")
    logger.info(f"  Removed {before - len(df):,} cancelled transactions")

    # --- Step 3: Remove rows with missing Customer ID ---
    # About 25% of rows have no customer ID — they cannot be attributed to a customer
    # For customer-level analytics these rows are useless
    before = len(df)
    df = df.dropna(subset=["customer_id"])
    # dropna removes rows where any of the listed columns contain NaN
    # subset=["customer_id"] means only check that column, not all columns
    log_shape(df, "drop missing customer_id")
    logger.info(f"  Removed {before - len(df):,} rows with no customer ID")

    # --- Step 4: Remove rows with missing or empty Description ---
    before = len(df)
    df = df.dropna(subset=["description"])                       # remove NaN descriptions
    df = df[df["description"].str.strip() != ""]                 # remove whitespace-only descriptions
    # str.strip() removes leading and trailing whitespace
    # != "" filters out rows where the description was only spaces
    log_shape(df, "drop missing/empty description")
    logger.info(f"  Removed {before - len(df):,} rows with no description")

    # --- Step 5: Remove rows with non-positive Quantity ---
    # Quantity should be a positive whole number for a completed sale
    # Zero or negative quantities are errors or adjustments, not real transactions
    before = len(df)
    df = df[df["quantity"] > 0]
    log_shape(df, "remove non-positive quantity")
    logger.info(f"  Removed {before - len(df):,} rows with quantity <= 0")

    # --- Step 6: Remove rows with non-positive Price ---
    # A unit price of 0 or less is not a real product sale
    before = len(df)
    df = df[df["price"] > 0]
    log_shape(df, "remove non-positive price")
    logger.info(f"  Removed {before - len(df):,} rows with price <= 0")

    # --- Step 7: Remove duplicate rows ---
    # Full duplicates can appear when sheets are combined or from data entry errors
    before = len(df)
    df = df.drop_duplicates()
    # drop_duplicates removes rows where every column value is identical to another row
    log_shape(df, "drop full duplicates")
    logger.info(f"  Removed {before - len(df):,} duplicate rows")

    # --- Step 8: Fix data types ---
    # Parse InvoiceDate as a proper datetime object — currently it is a string or object type
    df["invoicedate"] = pd.to_datetime(df["invoicedate"])
    # pd.to_datetime converts strings like "2010-12-01 08:26:00" into datetime objects
    # This enables date filtering, grouping by month, and date arithmetic later

    # Ensure customer_id has no trailing .0 (Excel sometimes adds this)
    df["customer_id"] = df["customer_id"].str.strip()
# str.strip() removes any accidental whitespace

    df["customer_id"] = df["customer_id"].str.replace(r"\.0$", "", regex=True)
# The raw CSV stores customer IDs as "13085.0" — a float that got saved as text
# This regex removes the trailing ".0" so we get clean IDs like "13085"
# r"\.0$" means: literal dot, followed by 0, at the end of the string ($)
    # str.strip() removes any accidental whitespace

    # Ensure quantity and price are the correct numeric types
    df["quantity"] = df["quantity"].astype(int)
    df["price"] = df["price"].astype(float)

    # --- Step 9: Add a calculated column — revenue per line ---
    # This is a derived field: quantity × price = revenue for that transaction line
    # Adding it here means dbt models and SQL queries can use it without recalculating
    df["line_revenue"] = df["quantity"] * df["price"]
    # The result is a new column with the revenue for each individual transaction row

    # --- Step 10: Add date part columns for easier SQL grouping later ---
    df["invoice_year"] = df["invoicedate"].dt.year    # extract the year as an integer
    df["invoice_month"] = df["invoicedate"].dt.month  # extract the month as 1–12
    # .dt is the datetime accessor on a pandas Series
    # .year and .month extract the relevant part

    logger.info(f"Transform complete — output shape: {df.shape}")

    return df