import duckdb
import pandas as pd
from pathlib import Path

def setup_database_schema():
    """Set up properly modeled database schema"""
    try:
        print("Setting up database schema...")
        Path("databases").mkdir(exist_ok=True)
        
        # Step 1: Create tables in all databases
        databases = {
            'transactions': create_transactions_tables(),
            'stock_prices': create_stock_prices_tables(),
            'representatives': create_representatives_tables(),
            'stock_details': create_stock_details_tables()
        }
        
        if not all(databases.values()):
            return False
        
        # Step 2: Import initial data
        print("\nImporting data into databases...")
        if not import_initial_data():
            return False
        
        return True
        
    except Exception as e:
        print(f"Error setting up database schema: {e}")
        return False

def create_transactions_tables():
    """Create tables in transactions database"""
    try:
        print("\nSetting up transactions database...")
        con = duckdb.connect('databases/transactions.duckdb')
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                disclosure_year INTEGER,
                disclosure_date DATE,
                transaction_date DATE,
                owner VARCHAR,
                ticker VARCHAR,
                asset_description VARCHAR,
                type VARCHAR,
                amount VARCHAR,
                representative VARCHAR,
                district VARCHAR,
                state VARCHAR,
                ptr_link VARCHAR,
                cap_gains_over_200_usd BOOLEAN,
                industry VARCHAR,
                sector VARCHAR,
                party VARCHAR
            )
        """)
        
        con.close()
        print("Transactions database setup complete")
        return True
    except Exception as e:
        print(f"Error setting up transactions database: {e}")
        return False

def create_stock_prices_tables():
    """Create tables in stock prices database"""
    try:
        print("\nSetting up stock prices database...")
        con = duckdb.connect('databases/stock_prices.duckdb')
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                ticker VARCHAR,
                date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                PRIMARY KEY (ticker, date)
            )
        """)
        
        con.close()
        print("Stock prices database setup complete")
        return True
    except Exception as e:
        print(f"Error setting up stock prices database: {e}")
        return False

def create_stock_details_tables():
    """Create tables in stock details database"""
    try:
        print("\nSetting up stock details database...")
        con = duckdb.connect('databases/stock_details.duckdb')
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                ticker VARCHAR PRIMARY KEY,
                company_name VARCHAR,
                sector VARCHAR,
                industry VARCHAR,
                country VARCHAR,
                market_cap DOUBLE,
                description TEXT,
                website VARCHAR,
                exchange VARCHAR,
                currency VARCHAR,
                last_updated_date DATE
            )
        """)
        
        con.close()
        print("Stock details database setup complete")
        return True
    except Exception as e:
        print(f"Error setting up stock details database: {e}")
        return False

def create_representatives_tables():
    """Create tables in representatives database"""
    try:
        print("\nSetting up representatives database...")
        con = duckdb.connect('databases/representatives.duckdb')
        
        con.execute("""
            CREATE TABLE IF NOT EXISTS representatives (
                representative_id INTEGER PRIMARY KEY,
                name VARCHAR,
                district VARCHAR,
                state VARCHAR,
                party VARCHAR
            )
        """)
        
        con.close()
        print("Representatives database setup complete")
        return True
    except Exception as e:
        print(f"Error setting up representatives database: {e}")
        return False

def import_initial_data():
    """Import data from CSV into appropriate databases"""
    try:
        print("Loading transaction data...")
        df = pd.read_csv("data/all_transactions.csv")
        
        # Convert date columns to proper format
        print("Converting date formats...")
        
        # Function to safely convert dates
        def convert_date(date_str):
            try:
                # Try parsing with multiple formats
                for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y']:
                    try:
                        return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
                    except:
                        continue
                # If all formats fail, return None
                return None
            except:
                return None
        
        # Convert date columns
        date_columns = ['disclosure_date', 'transaction_date']
        for col in date_columns:
            df[col] = df[col].apply(convert_date)
            # Fill invalid dates with None
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        
        # Convert disclosure_year to integer
        df['disclosure_year'] = pd.to_numeric(df['disclosure_year'], errors='coerce').fillna(0).astype(int)
        
        # Convert boolean column
        df['cap_gains_over_200_usd'] = df['cap_gains_over_200_usd'].fillna(False)
        
        # Fill NaN values with None for proper SQL handling
        df = df.replace({pd.NA: None})
        
        # Connect to databases
        con_trans = duckdb.connect('databases/transactions.duckdb')
        con_rep = duckdb.connect('databases/representatives.duckdb')
        
        # Import transactions
        print("Importing transactions...")
        con_trans.execute("DELETE FROM transactions")
        con_trans.register('df', df)
        con_trans.execute("""
            INSERT INTO transactions 
            SELECT * FROM df
        """)
        
        # Extract unique representatives
        print("Extracting representative information...")
        rep_df = df[['representative', 'district', 'state', 'party']].drop_duplicates()
        rep_df['representative_id'] = range(1, len(rep_df) + 1)
        
        # Import representatives
        print("Importing representatives...")
        con_rep.execute("DELETE FROM representatives")
        con_rep.register('rep_df', rep_df)
        con_rep.execute("""
            INSERT INTO representatives (representative_id, name, district, state, party)
            SELECT representative_id, representative, district, state, party
            FROM rep_df
        """)
        
        con_trans.close()
        con_rep.close()
        print("Data import complete")
        return True
        
    except Exception as e:
        print(f"Error importing data: {e}")
        if 'con_trans' in locals():
            con_trans.close()
        if 'con_rep' in locals():
            con_rep.close()
        return False

if __name__ == "__main__":
    setup_database_schema() 