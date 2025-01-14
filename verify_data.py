import duckdb

def verify_stock_prices_data():
    try:
        # Connect to the database
        con = duckdb.connect('databases/stock_prices.duckdb', read_only=True)
        
        # Check table structure
        print("\nChecking table structure:")
        structure = con.execute("DESCRIBE daily_prices;").fetchdf()
        print(structure)
        
        # Check sample data
        print("\nChecking sample data:")
        sample = con.execute("SELECT * FROM daily_prices LIMIT 5;").fetchdf()
        print(sample)
        
        # Check unique tickers
        print("\nChecking unique tickers:")
        tickers = con.execute("SELECT DISTINCT ticker FROM daily_prices;").fetchdf()
        print(f"Found {len(tickers)} unique tickers")
        print(tickers)
        
        # Check date range
        print("\nChecking date range:")
        date_range = con.execute("""
            SELECT 
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                COUNT(*) as total_records
            FROM daily_prices;
        """).fetchdf()
        print(date_range)
        
        con.close()
        
    except Exception as e:
        print(f"Error during verification: {e}")

if __name__ == "__main__":
    verify_stock_prices_data() 