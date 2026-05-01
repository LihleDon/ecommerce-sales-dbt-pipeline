import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

# Updated from .xlsx to .csv — the Kaggle dataset now ships as a single CSV file
RAW_FILE = os.path.join("data", "raw", "online_retail_II.csv")

def extract() -> pd.DataFrame:
    """
    Extract raw data from the CSV file and return a DataFrame.
    The original dataset had two Excel sheets — the CSV version is a single file
    containing the same data. Deduplication happens in transform.py.
    """
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(
            f"Raw data file not found: {RAW_FILE}\n"
            "Run the Kaggle download command before running the pipeline."
        )

    logger.info(f"Reading raw file: {RAW_FILE}")

    # encoding="latin-1" handles special characters in product descriptions
    # and country names that would cause a UTF-8 decode error
    # dtype={"Customer ID": str} prevents Pandas from reading the ID as 13085.0
    # — without this, any column with missing values gets cast to float automatically
    df = pd.read_csv(
        RAW_FILE,
        encoding="latin-1",
        dtype={"Customer ID": str}
    )

    logger.info(f"Loaded {len(df):,} rows from CSV")
    return df

if __name__ == "__main__":
    df = extract()
    print(df.head())
    print(f"\nShape: {df.shape}")