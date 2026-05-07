import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def log_shape(df: pd.DataFrame, step: str) -> None:
    logger.info(f"  After '{step}': {len(df):,} rows remain")


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardise the raw retail DataFrame.
    Returns a DataFrame ready to load into DuckDB.
    """

    logger.info(f"Starting transform — input shape: {df.shape}")

    # Standardise to snake_case so column names are consistent across
    # Python, SQL, and dbt without quoting or case-guessing
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    logger.info(f"Columns renamed to: {list(df.columns)}")

    # Cancelled invoices start with 'C' and represent returns, not sales.
    # Including them inflates gross revenue and breaks net revenue calculations.
    before = len(df)
    df = df[~df["invoice"].astype(str).str.startswith("C")]
    log_shape(df, "remove cancelled invoices")
    logger.info(f"  Removed {before - len(df):,} cancelled transactions")

    # Rows without a customer ID cannot be attributed to anyone.
    # They are valid revenue but unusable for any customer-level model.
    before = len(df)
    df = df.dropna(subset=["customer_id"])
    log_shape(df, "drop missing customer_id")
    logger.info(f"  Removed {before - len(df):,} rows with no customer ID")

    before = len(df)
    df = df.dropna(subset=["description"])
    df = df[df["description"].str.strip() != ""]
    log_shape(df, "drop missing/empty description")
    logger.info(f"  Removed {before - len(df):,} rows with no description")

    # Zero or negative quantities are returns or ledger adjustments, not sales.
    before = len(df)
    df = df[df["quantity"] > 0]
    log_shape(df, "remove non-positive quantity")
    logger.info(f"  Removed {before - len(df):,} rows with quantity <= 0")

    before = len(df)
    df = df[df["price"] > 0]
    log_shape(df, "remove non-positive price")
    logger.info(f"  Removed {before - len(df):,} rows with price <= 0")

    before = len(df)
    df = df.drop_duplicates()
    log_shape(df, "drop full duplicates")
    logger.info(f"  Removed {before - len(df):,} duplicate rows")

    df["invoicedate"] = pd.to_datetime(df["invoicedate"])

    # The raw CSV stores customer IDs as floats (13085.0).
    # Strip whitespace first, then remove the trailing .0 with a regex.
    df["customer_id"] = df["customer_id"].str.strip()
    df["customer_id"] = df["customer_id"].str.replace(r"\.0$", "", regex=True)

    df["quantity"] = df["quantity"].astype(int)
    df["price"] = df["price"].astype(float)

    df["line_revenue"] = df["quantity"] * df["price"]

    df["invoice_year"] = df["invoicedate"].dt.year
    df["invoice_month"] = df["invoicedate"].dt.month

    logger.info(f"Transform complete — output shape: {df.shape}")

    return df
