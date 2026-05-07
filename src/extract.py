import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

RAW_FILE = os.path.join("data", "raw", "online_retail_II.csv")


def extract() -> pd.DataFrame:
    """
    Read the raw CSV and return it as a DataFrame.
    No cleaning happens here — that is transform.py's job.
    """

    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(
            f"Raw data file not found: {RAW_FILE}\n"
            "Run the Kaggle download command before running the pipeline."
        )

    logger.info(f"Reading raw file: {RAW_FILE}")

    # encoding="latin-1" is required — the file contains special characters in
    # product descriptions and country names that break UTF-8.
    # dtype={"Customer ID": str} prevents pandas from casting the column to float64
    # when it encounters missing values, which would corrupt every ID with a trailing .0
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
