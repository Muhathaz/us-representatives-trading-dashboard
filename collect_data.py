import requests
import json
import pandas as pd
from pathlib import Path

def fetch_transaction_data():
    """Fetch transaction data from the House Stock Watcher API"""
    url = "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json"
    
    try:
        # Fetch data from URL
        print("Fetching data from API...")
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse JSON data
        data = response.json()
        
        # Create data directory if it doesn't exist
        Path("data").mkdir(exist_ok=True)
        
        # Save raw JSON data
        print("Saving raw JSON data...")
        with open("data/all_transactions.json", "w") as f:
            json.dump(data, f, indent=4)
            
        # Convert to DataFrame for validation
        df = pd.DataFrame(data)
        
        # Validate critical fields
        critical_fields = ['transaction_date', 'representative', 'ticker', 'amount', 'transaction_type']
        missing_fields = [field for field in critical_fields if field not in df.columns]
        
        if missing_fields:
            print(f"Warning: Missing critical fields: {missing_fields}")
        
        # Check for missing values in critical fields
        for field in critical_fields:
            if field in df.columns:
                missing_count = df[field].isna().sum()
                if missing_count > 0:
                    print(f"Warning: {missing_count} missing values in {field}")
        
        # Save as CSV for easier viewing
        print("Saving data as CSV...")
        df.to_csv("data/all_transactions.csv", index=False)
        
        print(f"Successfully downloaded {len(df)} transactions")
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

if __name__ == "__main__":
    fetch_transaction_data() 