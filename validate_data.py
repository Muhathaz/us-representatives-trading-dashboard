import pandas as pd
import json
from datetime import datetime

def validate_transactions():
    """Validate the transaction data for missing or malformed entries"""
    try:
        # Load the data
        print("Loading transaction data...")
        df = pd.read_csv("data/all_transactions.csv")
        
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
            'transaction_type': {
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
            'total_records': len(df)
        }

        # Perform validation for each field
        for field, rules in validation_rules.items():
            print(f"\nValidating {field}...")
            
            # Check for missing values
            if rules.get('check_empty'):
                missing_count = df[field].isna().sum()
                validation_results['missing_values'][field] = missing_count
                print(f"Missing values: {missing_count}")

            # Skip malformed checks for missing values
            valid_rows = df[df[field].notna()]

            # Check for malformed values
            malformed = []
            
            if field == 'transaction_date' and rules.get('is_date'):
                for idx, value in valid_rows[field].items():
                    try:
                        datetime.strptime(str(value), rules['format'])
                    except ValueError:
                        malformed.append(idx)

            elif field == 'transaction_type' and 'valid_values' in rules:
                malformed = valid_rows[~valid_rows[field].str.lower().isin(rules['valid_values'])].index

            elif 'pattern' in rules:
                import re
                pattern = re.compile(rules['pattern'])
                malformed = valid_rows[~valid_rows[field].astype(str).str.match(pattern)].index

            validation_results['malformed_values'][field] = len(malformed)
            
            if malformed:
                print(f"Malformed values: {len(malformed)}")
                print("Sample of malformed entries:")
                for idx in malformed[:5]:  # Show first 5 malformed entries
                    print(f"  Row {idx}: {df.loc[idx, field]}")

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

if __name__ == "__main__":
    validate_transactions() 