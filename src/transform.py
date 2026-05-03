import pandas as pd   
import logging       

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

    # --- Standardise column names ---
    
    df.columns = (
        df.columns
        .str.strip()       
        .str.lower()       
        .str.replace(" ", "_", regex=False)  
    logger.info(f"Columns renamed: {list(df.columns)}")

    # --- Remove cancelled transactions ---
    before = len(df)
    df = df[~df["invoice"].astype(str).str.startswith("C")]
    
    log_shape(df, "remove cancelled invoices")
    logger.info(f"  Removed {before - len(df):,} cancelled transactions")

    # --- Remove rows with missing Customer ID ---
    before = len(df)
    df = df.dropna(subset=["customer_id"])
    
    log_shape(df, "drop missing customer_id")
    logger.info(f"  Removed {before - len(df):,} rows with no customer ID")

    # --- Remove rows with missing or empty Description ---
    before = len(df)
    df = df.dropna(subset=["description"])                     
    df = df[df["description"].str.strip() != ""]                 
    
    log_shape(df, "drop missing/empty description")
    logger.info(f"  Removed {before - len(df):,} rows with no description")

    # --- Remove rows with non-positive Quantity ---
    before = len(df)
    df = df[df["quantity"] > 0]
    log_shape(df, "remove non-positive quantity")
    logger.info(f"  Removed {before - len(df):,} rows with quantity <= 0")

    # --- Remove rows with non-positive Price ---
    before = len(df)
    df = df[df["price"] > 0]
    log_shape(df, "remove non-positive price")
    logger.info(f"  Removed {before - len(df):,} rows with price <= 0")

    # --- Remove duplicate rows ---
    before = len(df)
    df = df.drop_duplicates()
    
    log_shape(df, "drop full duplicates")
    logger.info(f"  Removed {before - len(df):,} duplicate rows")

    # --- Fix data types ---
    df["invoicedate"] = pd.to_datetime(df["invoicedate"])
    
    df["customer_id"] = df["customer_id"].str.strip()

    df["customer_id"] = df["customer_id"].str.replace(r"\.0$", "", regex=True)

    df["quantity"] = df["quantity"].astype(int)
    df["price"] = df["price"].astype(float)

    # --- Add a calculated column — revenue per line ---
    df["line_revenue"] = df["quantity"] * df["price"]

    # --- Add date part columns for easier SQL grouping later ---
    df["invoice_year"] = df["invoicedate"].dt.year    
    df["invoice_month"] = df["invoicedate"].dt.month  

    logger.info(f"Transform complete — output shape: {df.shape}")

    return df
