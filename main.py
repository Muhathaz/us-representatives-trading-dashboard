import os
from pathlib import Path
from collect_data import fetch_transaction_data
from validate_data import validate_transactions
from setup_database_schema import setup_database_schema
from fetch_stock_prices import fetch_stock_prices
from fetch_stock_details import fetch_stock_details

def create_directories():
    """Create necessary directories if they don't exist"""
    Path("data").mkdir(exist_ok=True)
    Path("databases").mkdir(exist_ok=True)
    print("Directory structure verified.")

def run_pipeline():
    """Execute the complete data pipeline"""
    try:
        print("\n=== Starting Data Pipeline ===\n")
        
        # Step 1: Create directories
        print("Step 1: Creating directories...")
        create_directories()
        
        # Step 2: Collect Data
        print("\nStep 2: Collecting transaction data...")
        df = fetch_transaction_data()
        if df is None:
            raise Exception("Data collection failed")
        print(f"Successfully collected {len(df)} transactions")
        
        # Step 3: Validate Data
        print("\nStep 3: Validating data...")
        validation_results = validate_transactions()
        if validation_results is None:
            raise Exception("Data validation failed")
        
        # Step 4: Setup Database Schema
        print("\nStep 4: Setting up database schema...")
        if not setup_database_schema():
            raise Exception("Database schema setup failed")
            
        # Step 5: Fetch Stock Prices
        print("\nStep 5: Fetching stock prices...")
        if not fetch_stock_prices():
            raise Exception("Stock price fetching failed")
            
        # Step 6: Fetch Stock Details
        print("\nStep 6: Fetching stock details...")
        if not fetch_stock_details():
            raise Exception("Stock details fetching failed")
        
        print("\n=== Pipeline Completed Successfully ===")
        return True
        
    except Exception as e:
        print(f"\nPipeline failed: {str(e)}")
        return False

if __name__ == "__main__":
    run_pipeline() 