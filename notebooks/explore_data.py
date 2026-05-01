import pandas as pd  # pandas is the main data manipulation library
import os            # os lets us work with file paths and directories

# Define the path to the raw data file
# os.path.join builds a path that works correctly on both Windows and Linux
# On Windows this produces: data\raw\online_retail_II.xlsx
RAW_FILE = os.path.join("data", "raw", "online_retail_II.csv")

def explore_sheet(sheet_name: str) -> pd.DataFrame:
    """
    Load one sheet from the Excel file and print summary statistics.
    Returns the loaded DataFrame so we can inspect it further.
    """

    print(f"\n{'='*60}")
    print(f"Sheet: {sheet_name}")
    print(f"{'='*60}")

    # Read the Excel sheet into a DataFrame
    # sheet_name tells pandas which tab inside the Excel file to read
    # dtype={"Customer ID": str} prevents pandas from reading the ID as a float
    # (which would add .0 to every value — a common bug with numeric ID columns)
    df = pd.read_csv(RAW_FILE, encoding="latin-1")

    # Print the shape — number of rows and columns
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    # Print column names and their data types
    print("\nColumn names and types:")
    print(df.dtypes)

    # Print the first 5 rows so we can see what the data actually looks like
    print("\nFirst 5 rows:")
    print(df.head())

    # Count and print missing values per column
    # df.isnull() returns True/False for each cell
    # .sum() counts the Trues (missing values) per column
    missing = df.isnull().sum()
    print("\nMissing values per column:")
    print(missing[missing > 0])  # only print columns that actually have missing values

    # Show how many rows have cancelled orders
    # Cancelled invoice numbers start with 'C' in this dataset
    # str.startswith returns a boolean Series — True where the condition matches
    cancelled = df[df["Invoice"].astype(str).str.startswith("C")]
    print(f"\nCancelled transactions: {len(cancelled):,}")

    # Show the range of transaction dates
    print(f"\nDate range: {df['InvoiceDate'].min()} → {df['InvoiceDate'].max()}")

    # Show unique counts for key columns
    print(f"Unique invoices: {df['Invoice'].nunique():,}")
    print(f"Unique customers: {df['Customer ID'].nunique():,}")
    print(f"Unique stock codes: {df['StockCode'].nunique():,}")
    print(f"Unique countries: {df['Country'].nunique():,}")

    # Show negative quantities — these are returns or data errors
    negative_qty = df[df["Quantity"] < 0]
    print(f"Rows with negative quantity: {len(negative_qty):,}")

    return df


def main():
    """Entry point — explore both sheets in the Excel file."""

    # Check the file exists before trying to open it
    if not os.path.exists(RAW_FILE):
        print(f"ERROR: File not found at {RAW_FILE}")
        print("Make sure you have run the Kaggle download step.")
        return  # exit the function early — nothing else can run without the file

    # The Excel file has two sheets — explore both
    df_year1 = explore_sheet("Year 2009-2010")
    df_year2 = explore_sheet("Year 2010-2011")

    # Combined row count across both sheets
    total_rows = len(df_year1) + len(df_year2)
    print(f"\n{'='*60}")
    print(f"TOTAL ROWS ACROSS BOTH SHEETS: {total_rows:,}")
    print(f"{'='*60}\n")


# This block only runs when you execute this file directly
# It does NOT run if another file imports this module
# This is a Python best practice — always guard your entry point
if __name__ == "__main__":
    main()