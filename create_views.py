import duckdb
import pandas as pd

def create_dashboard_views():
    try:
        con = duckdb.connect('databases/transactions.duckdb')
        con.execute("ATTACH 'databases/stock_prices.duckdb' AS prices (READ_ONLY)")
        con.execute("ATTACH 'databases/stock_details.duckdb' AS details (READ_ONLY)")

        # Representative Overview View
        print("Creating representative overview view...")
        con.execute("""
            DROP VIEW IF EXISTS rep_overview;
            CREATE VIEW rep_overview AS
            SELECT 
                t.representative,
                COUNT(*) as total_trades,
                COUNT(DISTINCT t.ticker) as unique_stocks,
                CAST(DATEDIFF('YEAR', MIN(t.transaction_date), MAX(t.transaction_date)) AS INTEGER) + 1 as years_active,
                SUM(CASE WHEN t.type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
                SUM(CASE WHEN t.type = 'sale' THEN 1 ELSE 0 END) as total_sales,
                COUNT(DISTINCT sd.industry) as unique_sectors,
                MAX(t.party) as party
            FROM transactions t
            LEFT JOIN details.stocks sd ON t.ticker = sd.ticker
            GROUP BY t.representative;
        """)

        # Representative Sector Analysis View
        print("Creating sector analysis view...")
        con.execute("""
            DROP VIEW IF EXISTS representative_sector_analysis;
            CREATE VIEW representative_sector_analysis AS
            SELECT 
                t.representative,
                COALESCE(sd.sector, 'Unknown') as sector,
                COUNT(*) as transaction_count
            FROM transactions t
            LEFT JOIN details.stocks sd ON t.ticker = sd.ticker
            GROUP BY t.representative, sd.sector
            HAVING transaction_count > 0
            ORDER BY transaction_count DESC;
        """)

        # Current Positions View
        print("Creating current positions view...")
        con.execute("""
            DROP VIEW IF EXISTS current_positions;
            CREATE VIEW current_positions AS
            WITH trade_values AS (
                SELECT 
                    representative,
                    ticker,
                    transaction_date,
                    type,
                    CASE 
                        WHEN amount = '< $1,000' THEN 500
                        WHEN amount = '$1,001 - $15,000' THEN 8000
                        WHEN amount = '$15,001 - $50,000' THEN 32500
                        WHEN amount = '$50,001 - $100,000' THEN 75000
                        WHEN amount = '$100,001 - $250,000' THEN 175000
                        WHEN amount = '$250,001 - $500,000' THEN 375000
                        WHEN amount = '$500,001 - $1,000,000' THEN 750000
                        WHEN amount = '$1,000,001 - $5,000,000' THEN 3000000
                        WHEN amount = '> $5,000,000' THEN 5000000
                        ELSE 0
                    END as estimated_value
                FROM transactions
            )
            SELECT 
                t.representative,
                t.ticker,
                COALESCE(sd.sector, 'Unknown') as sector,
                SUM(CASE WHEN t.type = 'purchase' THEN t.estimated_value
                         WHEN t.type = 'sale' THEN -t.estimated_value
                    ELSE 0 END) as current_value
            FROM trade_values t
            LEFT JOIN details.stocks sd ON t.ticker = sd.ticker
            GROUP BY t.representative, t.ticker, sd.sector
            HAVING current_value > 0
            ORDER BY current_value DESC;
        """)

        # Trading Timeline View
        print("Creating trading timeline view...")
        con.execute("""
            DROP VIEW IF EXISTS trading_timeline;
            CREATE VIEW trading_timeline AS
            SELECT 
                t.ticker,
                t.transaction_date,
                t.type,
                t.representative,
                t.party,
                t.amount,
                p.close as price_at_trade,
                sd.sector
            FROM transactions t
            LEFT JOIN prices.daily_prices p ON t.ticker = p.ticker AND t.transaction_date = p.date
            LEFT JOIN details.stocks sd ON t.ticker = sd.ticker
            ORDER BY t.transaction_date;
        """)

        con.close()
        print("Successfully created all views!")
        return True

    except Exception as e:
        print(f"Error creating views: {e}")
        if 'con' in locals():
            con.close()
        return False

if __name__ == "__main__":
    create_dashboard_views() 