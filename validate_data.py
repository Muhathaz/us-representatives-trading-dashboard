import pandas as pd
import json
from datetime import datetime
import re
import numpy as np

def validate_transactions():
    """Validate the transaction data for missing or malformed entries"""
    try:
        # Load the data
        print("Loading transaction data...")
        df = pd.read_csv("data/all_transactions.csv")
        
        # Print column names to debug
        print("\nAvailable columns:", df.columns.tolist())
        
        # Define critical fields and their validation rules
        validation_rules = {
            'ticker': {
                'check_empty': True,
                'pattern': r'^[A-Z]+$',  # Tickers should be uppercase letters
                'max_length': 5
            },
            'transaction_date': {
                'check_empty': True,
                'is_date': True,
                'format': '%Y-%m-%d'
            },
            'type': {
                'check_empty': True,
                'valid_values': ['purchase', 'sale', 'exchange']
            },
            'amount': {
                'check_empty': True,
                'pattern': r'^[\$\d,\.-]+$'  # Should contain only $, digits, commas, dots
            },
            'representative': {
                'check_empty': True
            }
        }

        validation_results = {
            'missing_values': {},
            'malformed_values': {},
            'total_records': int(len(df))  # Convert to standard Python int
        }

        # Perform validation for each field
        for field, rules in validation_rules.items():
            print(f"\nValidating {field}...")
            
            # Check if field exists in dataframe
            if field not in df.columns:
                print(f"Warning: Field '{field}' not found in data")
                validation_results['missing_values'][field] = int(len(df))
                validation_results['malformed_values'][field] = 0
                continue
            
            # Check for missing values
            if rules.get('check_empty'):
                missing_count = int(df[field].isna().sum())  # Convert to standard Python int
                validation_results['missing_values'][field] = missing_count
                print(f"Missing values: {missing_count}")

            # Skip malformed checks for missing values
            valid_rows = df[df[field].notna()]
            malformed_count = 0

            try:
                if field == 'transaction_date' and rules.get('is_date'):
                    malformed_count = sum(1 for value in valid_rows[field] if not is_valid_date(value, rules['format']))

                elif field == 'type' and 'valid_values' in rules:
                    malformed_count = int(sum(~valid_rows[field].str.lower().isin(rules['valid_values'])))

                elif 'pattern' in rules:
                    pattern = re.compile(rules['pattern'])
                    malformed_count = sum(1 for value in valid_rows[field].astype(str) 
                                        if not bool(pattern.match(str(value))))

                validation_results['malformed_values'][field] = int(malformed_count)  # Convert to standard Python int
                
                if malformed_count > 0:
                    print(f"Malformed values: {malformed_count}")
                    
            except Exception as e:
                print(f"Error validating {field}: {str(e)}")
                validation_results['malformed_values'][field] = 0

        # Convert any remaining numpy types to Python types
        validation_results = convert_to_serializable(validation_results)

        # Save validation results
        print("\nSaving validation results...")
        with open("data/validation_results.json", "w") as f:
            json.dump(validation_results, f, indent=4)

        # Print summary
        print("\nValidation Summary:")
        print(f"Total records: {validation_results['total_records']}")
        print("\nMissing Values:")
        for field, count in validation_results['missing_values'].items():
            print(f"  {field}: {count}")
        print("\nMalformed Values:")
        for field, count in validation_results['malformed_values'].items():
            print(f"  {field}: {count}")

        return validation_results

    except FileNotFoundError:
        print("Error: Transaction data file not found. Please run collect_data.py first.")
        return None
    except Exception as e:
        print(f"Error during validation: {e}")
        return None

def is_valid_date(date_str, date_format):
    """Helper function to validate date strings"""
    try:
        datetime.strptime(str(date_str), date_format)
        return True
    except ValueError:
        return False

def convert_to_serializable(obj):
    """Convert numpy types to standard Python types"""
    if isinstance(obj, dict):
        return {key: convert_to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_serializable(item) for item in obj]
    elif isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8,
                         np.uint64, np.uint32, np.uint16, np.uint8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    return obj

if __name__ == "__main__":
    validate_transactions() 