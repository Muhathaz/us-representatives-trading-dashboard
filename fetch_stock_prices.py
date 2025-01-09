import yfinance as yf
import pandas as pd
import json
from datetime import datetime, timedelta
import duckdb
from pathlib import Path

def fetch_stock_prices():
    """Fetch historical stock prices for all tickers in the dataset"""
    try:
        print("Connecting to databases...")
        con_trans = duckdb.connect('databases/transactions.duckdb')
        con_prices = duckdb.connect('databases/stock_prices.duckdb')
        
        # Get unique tickers and date range
        print("Getting unique tickers and date range...")
        tickers = con_trans.execute("""
            SELECT DISTINCT ticker 
            FROM transactions 
            WHERE ticker IS NOT NULL
            AND ticker != ''
        """).fetchall()
        
        # Get date range and convert to proper format
        date_range = con_trans.execute("""
            SELECT 
                MIN(transaction_date)::DATE as start_date,
                MAX(transaction_date)::DATE as end_date
            FROM transactions
            WHERE transaction_date IS NOT NULL
        """).fetchone()
        
        # Convert to list of tickers
        tickers = [t[0] for t in tickers]
        
        # Convert dates to datetime objects
        start_date = date_range[0]
        end_date = date_range[1] + timedelta(days=180)  # 6 months after last trade
        
        print(f"Found {len(tickers)} unique tickers")
        print(f"Date range: {start_date} to {end_date}")
        
        # Fetch and store prices for each ticker
        print("\nFetching stock prices...")
        failed_tickers = []
        
        for i, ticker in enumerate(tickers, 1):
            try:
                print(f"Processing {ticker} ({i}/{len(tickers)})...")
                
                # Get stock data from Yahoo Finance
                stock = yf.Ticker(ticker)
                hist = stock.history(start=start_date, end=end_date)
                
                if hist.empty:
                    print(f"No data found for {ticker}")
                    failed_tickers.append(ticker)
                    continue
                
                # Prepare data for database
                hist.reset_index(inplace=True)
                hist['ticker'] = ticker
                hist.rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                }, inplace=True)
                
                # Insert into database
                con_prices.execute("DELETE FROM daily_prices WHERE ticker = ?", [ticker])
                con_prices.register('hist_df', hist)
                con_prices.execute("""
                    INSERT INTO daily_prices 
                    SELECT ticker, date, open, high, low, close, volume
                    FROM hist_df
                """)
                
            except Exception as e:
                print(f"Error processing {ticker}: {str(e)}")
                failed_tickers.append(ticker)
        
        # Save failed tickers for reference
        if failed_tickers:
            Path("data").mkdir(exist_ok=True)
            with open("data/failed_tickers.json", "w") as f:
                json.dump(failed_tickers, f, indent=4)
            print(f"\nFailed to fetch data for {len(failed_tickers)} tickers")
            print("Failed tickers saved to data/failed_tickers.json")
        
        # Verify data
        count = con_prices.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
        ticker_count = con_prices.execute("SELECT COUNT(DISTINCT ticker) FROM daily_prices").fetchone()[0]
        print(f"\nSuccessfully stored {count} price records for {ticker_count} tickers")
        
        con_trans.close()
        con_prices.close()
        return True
        
    except Exception as e:
        print(f"Error fetching stock prices: {str(e)}")
        if 'con_trans' in locals():
            con_trans.close()
        if 'con_prices' in locals():
            con_prices.close()
        return False

if __name__ == "__main__":
    fetch_stock_prices() 