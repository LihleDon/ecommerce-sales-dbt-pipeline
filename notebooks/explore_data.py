import pandas as pd  # pandas handles all tabular data operations in this script
import os            # os lets us build file paths that work on both Windows and Linux

# Define the path to the raw CSV file
# os.path.join builds the path correctly for whatever OS is running the script
# On Windows this produces: data\raw\online_retail_II.csv
# On Linux/Mac it produces: data/raw/online_retail_II.csv
RAW_FILE = os.path.join("data", "raw", "online_retail_II.csv")


def explore() -> None:
    """
    Load the raw CSV and print a summary of its structure and data quality issues.
    This script is for exploration only — it does not modify any data.
    """

    # Check the file exists before trying to open it
    # If we skip this check and the file is missing, pandas gives a confusing error
    if not os.path.exists(RAW_FILE):
        print(f"ERROR: File not found at {RAW_FILE}")
        print("Make sure you have run the Kaggle download step.")
        return  # exit early — nothing else can run without the file

    print(f"\n{'='*60}")
    print("RAW DATA EXPLORATION")
    print(f"{'='*60}")

    # Read the CSV into a pandas DataFrame
    # encoding="latin-1" handles special characters in product descriptions
    # and European country names that would cause a UTF-8 decode error
    # Without this argument pandas uses UTF-8 by default and crashes on certain rows
    # dtype={"Customer ID": str} prevents pandas from reading the ID as a float
    # Pandas sees a numeric column with missing values and converts it to float64
    # which turns 13085 into 13085.0 — setting dtype=str keeps it as text
    df = pd.read_csv(RAW_FILE, encoding="latin-1", dtype={"Customer ID": str})

    # Print the total number of rows and columns
    # df.shape returns a tuple: (number_of_rows, number_of_columns)
    print(f"\nShape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    # The :, inside the format spec adds comma separators to large numbers
    # 1067371 becomes 1,067,371 — much easier to read

    # Print column names and their data types
    print("\nColumn names and data types:")
    print(df.dtypes)
    # dtypes shows you what pandas inferred for each column
    # This is important because a column read as the wrong type causes bugs later

    # Print the first 5 rows to see what the actual data looks like
    print("\nFirst 5 rows:")
    print(df.head())
    # head() returns the first N rows — default is 5

    # Count missing values per column
    # df.isnull() returns a DataFrame of True/False — True where a value is missing
    # .sum() counts the True values per column (True = 1, False = 0)
    missing = df.isnull().sum()
    print("\nMissing values per column:")
    print(missing[missing > 0])  # filter to only show columns that actually have gaps

    # Count cancelled transactions
    # In this dataset, cancelled invoices start with the letter 'C'
    # For example: C536379 is the cancellation of invoice 536379
    cancelled = df[df["Invoice"].astype(str).str.startswith("C")]
    # .astype(str) converts the column to string type so .str methods work
    # .str.startswith("C") returns True for every row where Invoice starts with C
    print(f"\nCancelled transactions (Invoice starts with 'C'): {len(cancelled):,}")

    # Show the date range of the dataset
    print(f"\nDate range: {df['InvoiceDate'].min()} → {df['InvoiceDate'].max()}")

    # Count unique values for key identifier columns
    print(f"\nUnique invoices:    {df['Invoice'].nunique():,}")
    print(f"Unique customers:   {df['Customer ID'].nunique():,}")
    print(f"Unique stock codes: {df['StockCode'].nunique():,}")
    print(f"Unique countries:   {df['Country'].nunique():,}")
    # nunique() counts distinct non-null values in the column

    # Count rows with negative quantity — these are returns or data entry errors
    negative_qty = df[df["Quantity"] < 0]
    print(f"\nRows with negative quantity: {len(negative_qty):,}")


# This block only runs when you execute this file directly with: python explore_data.py
# It does NOT run if another script imports anything from this file
# This is a Python best practice — always guard your script entry point
if __name__ == "__main__":
    explore()