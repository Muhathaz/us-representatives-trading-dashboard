from fetch_data import fetch_transaction_data, fetch_stock_prices, fetch_stock_details
from validate_data import validate_all_data
from setup_database import create_database_schema, load_data_to_database
from create_views import create_dashboard_views
from datetime import datetime, timedelta

def run_pipeline():
    """Run the complete data pipeline"""
    try:
        print("Starting pipeline...")
        
        # Step 1: Create database schema
        print("\nCreating database schema...")
        if not create_database_schema():
            print("Failed to create database schema. Aborting.")
            return False

        # Step 2: Fetch data
        print("\nFetching transaction data...")
        transactions = fetch_transaction_data()
        if not transactions:
            print("Failed to fetch transaction data. Aborting.")
            return False

        print("\nFetching stock data...")
        tickers = set(t.get('ticker') for t in transactions if t.get('ticker'))
        start_date = datetime.now() - timedelta(days=365*2)  # 2 years of data
        end_date = datetime.now()
        
        stock_prices = fetch_stock_prices(tickers, start_date, end_date)
        stock_details = fetch_stock_details(tickers)

        # Step 3: Validate data
        print("\nValidating data...")
        validated_data = validate_all_data(transactions, stock_prices, stock_details)

        # Step 4: Load data into database
        print("\nLoading data into database...")
        if not load_data_to_database(validated_data):
            print("Failed to load data into database. Aborting.")
            return False

        # Step 5: Create views
        print("\nCreating database views...")
        if not create_dashboard_views():
            print("Failed to create views. Aborting.")
            return False

        print("\nPipeline completed successfully!")
        return True

    except Exception as e:
        print(f"\nError in pipeline: {e}")
        return False

if __name__ == "__main__":
    run_pipeline() 