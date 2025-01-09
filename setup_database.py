import duckdb
import pandas as pd
from pathlib import Path

def setup_database():
    """Set up DuckDB database and import transaction data"""
    try:
        print("Setting up DuckDB database...")
        
        con = duckdb.connect('trades.duckdb')
        
        print("Loading transaction data...")
        df = pd.read_csv("data/all_transactions.csv")
        
        print("Creating trades table...")
        con.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                transaction_date DATE,
                representative VARCHAR,
                district VARCHAR,
                ticker VARCHAR,
                asset_description VARCHAR,
                asset_type VARCHAR,
                type VARCHAR,
                amount VARCHAR,
                comment VARCHAR,
                ptr_link VARCHAR,
                disclosure_date DATE,
                transaction_type VARCHAR
            )
        """)
        
        print("Importing data into trades table...")
        con.execute("DELETE FROM trades")  # Clear existing data
        con.register('df', df)  # Register DataFrame
        con.execute("INSERT INTO trades SELECT * FROM df")
        
        # Verify data import
        count = con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        print(f"\nSuccessfully imported {count} records")
        
        # Run some basic validation queries
        print("\nRunning validation queries...")
        
        # Check distinct representatives
        result = con.execute("""
            SELECT COUNT(DISTINCT representative) as rep_count 
            FROM trades
        """).fetchone()
        print(f"Number of distinct representatives: {result[0]}")
        
        # Check date range
        date_range = con.execute("""
            SELECT 
                MIN(transaction_date) as earliest_date,
                MAX(transaction_date) as latest_date
            FROM trades
        """).fetchone()
        print(f"Date range: {date_range[0]} to {date_range[1]}")
        
        # Check most common tickers
        print("\nTop 5 most traded tickers:")
        top_tickers = con.execute("""
            SELECT ticker, COUNT(*) as count
            FROM trades
            WHERE ticker IS NOT NULL
            GROUP BY ticker
            ORDER BY count DESC
            LIMIT 5
        """).fetchall()
        for ticker, count in top_tickers:
            print(f"  {ticker}: {count} trades")
        
        con.close()
        print("\nDatabase setup completed successfully!")
        
    except Exception as e:
        print(f"Error setting up database: {e}")
        if 'con' in locals():
            con.close()
        return False
    
    return True

if __name__ == "__main__":
    # First make sure we have the required packages
    try:
        import duckdb
    except ImportError:
        print("DuckDB not found. Installing required packages...")
        import subprocess
        subprocess.check_call(["pip", "install", "duckdb"])
        
    setup_database() 