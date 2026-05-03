import pandas as pd  
import os           

RAW_FILE = os.path.join("data", "raw", "online_retail_II.csv")


def explore() -> None:
    """
    Load the raw CSV and print a summary of its structure and data quality issues.
    This script is for exploration only — it does not modify any data.
    """

    if not os.path.exists(RAW_FILE):
        print(f"ERROR: File not found at {RAW_FILE}")
        print("Make sure you have run the Kaggle download step.")
        return  

    print(f"\n{'='*60}")
    print("RAW DATA EXPLORATION")
    print(f"{'='*60}")

 
    df = pd.read_csv(RAW_FILE, encoding="latin-1", dtype={"Customer ID": str})

   
    print(f"\nShape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    
    print("\nColumn names and data types:")
    print(df.dtypes)
    print("\nFirst 5 rows:")
    print(df.head())
    
    missing = df.isnull().sum()
    print("\nMissing values per column:")
    print(missing[missing > 0])  # filter to only show columns that actually have gaps

    cancelled = df[df["Invoice"].astype(str).str.startswith("C")]
    
    print(f"\nCancelled transactions (Invoice starts with 'C'): {len(cancelled):,}")

    # Show the date range of the dataset
    print(f"\nDate range: {df['InvoiceDate'].min()} → {df['InvoiceDate'].max()}")

    # Count unique values for key identifier columns
    print(f"\nUnique invoices:    {df['Invoice'].nunique():,}")
    print(f"Unique customers:   {df['Customer ID'].nunique():,}")
    print(f"Unique stock codes: {df['StockCode'].nunique():,}")
    print(f"Unique countries:   {df['Country'].nunique():,}")
    
    # Count rows with negative quantity — these are returns or data entry errors
    negative_qty = df[df["Quantity"] < 0]
    print(f"\nRows with negative quantity: {len(negative_qty):,}")


if __name__ == "__main__":
    explore()
