import yfinance as yf
import pandas as pd
import json
from datetime import datetime
import duckdb
from pathlib import Path
import time

def fetch_stock_details():
    """Fetch company information for all tickers in the dataset"""
    try:
        print("Connecting to database...")
        con_transactions = duckdb.connect('databases/transactions.duckdb')
        con_details = duckdb.connect('databases/stock_details.duckdb')
        
        # Get unique tickers
        print("Getting unique tickers...")
        tickers = con_transactions.execute("""
            SELECT DISTINCT ticker 
            FROM transactions 
            WHERE ticker IS NOT NULL
            AND ticker != ''
        """).fetchall()
        
        # Convert to list of tickers
        tickers = [t[0] for t in tickers]
        print(f"Found {len(tickers)} unique tickers")
        
        # Prepare data storage
        stock_details = []
        failed_tickers = []
        
        # Fetch details for each ticker
        print("\nFetching stock details...")
        for i, ticker in enumerate(tickers, 1):
            try:
                print(f"Processing {ticker} ({i}/{len(tickers)})...")
                
                # Get stock info from Yahoo Finance
                stock = yf.Ticker(ticker)
                info = stock.info
                
                # Extract relevant information
                details = {
                    'ticker': ticker,
                    'company_name': info.get('longName', None),
                    'sector': info.get('sector', None),
                    'industry': info.get('industry', None),
                    'country': info.get('country', None),
                    'market_cap': info.get('marketCap', None),
                    'description': info.get('longBusinessSummary', None),
                    'website': info.get('website', None),
                    'exchange': info.get('exchange', None),
                    'currency': info.get('currency', None),
                    'last_updated_date': datetime.now().date()
                }
                
                stock_details.append(details)
                
                # Insert into database every 10 stocks or at the end
                if len(stock_details) >= 10 or i == len(tickers):
                    df = pd.DataFrame(stock_details)
                    con_details.execute("DELETE FROM stocks WHERE ticker IN (SELECT ticker FROM df)")
                    con_details.register('df', df)
                    con_details.execute("""
                        INSERT INTO stocks 
                        SELECT * FROM df
                    """)
                    stock_details = []  # Clear the list
                
                # Add delay to avoid rate limiting
                time.sleep(1)
                
            except Exception as e:
                print(f"Error processing {ticker}: {str(e)}")
                failed_tickers.append({'ticker': ticker, 'error': str(e)})
        
        # Save failed tickers for reference
        if failed_tickers:
            Path("data").mkdir(exist_ok=True)
            with open("data/failed_stock_details.json", "w") as f:
                json.dump(failed_tickers, f, indent=4)
            print(f"\nFailed to fetch data for {len(failed_tickers)} tickers")
            print("Failed tickers saved to data/failed_stock_details.json")
        
        # Verify data
        count = con_details.execute("SELECT COUNT(*) FROM stocks").fetchone()[0]
        print(f"\nSuccessfully stored details for {count} stocks")
        
        con_transactions.close()
        con_details.close()
        return True
        
    except Exception as e:
        print(f"Error fetching stock details: {str(e)}")
        if 'con_transactions' in locals():
            con_transactions.close()
        if 'con_details' in locals():
            con_details.close()
        return False

if __name__ == "__main__":
    fetch_stock_details() 