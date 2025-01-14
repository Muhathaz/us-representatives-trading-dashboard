import duckdb
import pandas as pd

def create_dashboard_views():
    try:
        con = duckdb.connect('databases/transactions.duckdb')
        con.execute("ATTACH 'databases/stock_prices.duckdb' AS prices (READ_ONLY)")
        con.execute("ATTACH 'databases/stock_details.duckdb' AS details (READ_ONLY)")
        con.execute("ATTACH 'databases/representatives.duckdb' AS reps (READ_ONLY)")

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
                COUNT(DISTINCT sd.industry) as unique_sectors
            FROM transactions t
            LEFT JOIN details.stocks sd ON t.ticker = sd.ticker
            GROUP BY t.representative;
        """)

        # Representative Sector Analysis View
        print("Creating sector analysis view...")
        con.execute("""
            DROP VIEW IF EXISTS representative_sector_analysis;
            CREATE VIEW representative_sector_analysis AS
            WITH trade_values AS (
                SELECT 
                    t.representative,
                    t.ticker,
                    t.type,
                    sd.industry as sector,
                    CASE 
                        WHEN t.amount LIKE '%$1,001 - $15,000%' THEN 8000.5
                        WHEN t.amount LIKE '%$15,001 - $50,000%' THEN 32500.5
                        WHEN t.amount LIKE '%$50,001 - $100,000%' THEN 75000.5
                        WHEN t.amount LIKE '%$100,001 - $250,000%' THEN 175000.5
                        WHEN t.amount LIKE '%$250,001 - $500,000%' THEN 375000.5
                        WHEN t.amount LIKE '%$500,001 - $1,000,000%' THEN 750000.5
                        WHEN t.amount LIKE '%$1,000,001 - $5,000,000%' THEN 3000000.5
                        ELSE 0
                    END as trade_value
                FROM transactions t
                LEFT JOIN details.stocks sd ON t.ticker = sd.ticker
            )
            SELECT 
                representative,
                COALESCE(sector, 'Unknown') as sector,
                COUNT(*) as transaction_count,
                SUM(CASE WHEN type = 'purchase' THEN trade_value ELSE 0 END) as purchase_value,
                SUM(CASE WHEN type = 'sale' THEN trade_value ELSE 0 END) as sale_value,
                SUM(CASE 
                    WHEN type = 'purchase' THEN trade_value 
                    WHEN type = 'sale' THEN -trade_value 
                    ELSE 0 
                END) as net_value
            FROM trade_values
            GROUP BY representative, sector
            HAVING net_value != 0
            ORDER BY ABS(net_value) DESC;
        """)

        # Current Positions View
        print("Creating current positions view...")
        con.execute("""
            DROP VIEW IF EXISTS current_positions;
            CREATE VIEW current_positions AS
            WITH position_values AS (
                SELECT 
                    t.representative,
                    t.ticker,
                    sd.industry as sector,
                    CASE 
                        WHEN t.type = 'purchase' THEN 
                            CASE 
                                WHEN t.amount LIKE '%$1,001 - $15,000%' THEN 8000.5
                                WHEN t.amount LIKE '%$15,001 - $50,000%' THEN 32500.5
                                WHEN t.amount LIKE '%$50,001 - $100,000%' THEN 75000.5
                                WHEN t.amount LIKE '%$100,001 - $250,000%' THEN 175000.5
                                WHEN t.amount LIKE '%$250,001 - $500,000%' THEN 375000.5
                                WHEN t.amount LIKE '%$500,001 - $1,000,000%' THEN 750000.5
                                WHEN t.amount LIKE '%$1,000,001 - $5,000,000%' THEN 3000000.5
                                ELSE 0
                            END
                        WHEN t.type = 'sale' THEN 
                            -1 * CASE 
                                WHEN t.amount LIKE '%$1,001 - $15,000%' THEN 8000.5
                                WHEN t.amount LIKE '%$15,001 - $50,000%' THEN 32500.5
                                WHEN t.amount LIKE '%$50,001 - $100,000%' THEN 75000.5
                                WHEN t.amount LIKE '%$100,001 - $250,000%' THEN 175000.5
                                WHEN t.amount LIKE '%$250,001 - $500,000%' THEN 375000.5
                                WHEN t.amount LIKE '%$500,001 - $1,000,000%' THEN 750000.5
                                WHEN t.amount LIKE '%$1,000,001 - $5,000,000%' THEN 3000000.5
                                ELSE 0
                            END
                        ELSE 0
                    END as position_value
                FROM transactions t
                LEFT JOIN details.stocks sd ON t.ticker = sd.ticker
            )
            SELECT 
                representative,
                ticker,
                COALESCE(sector, 'Unknown') as sector,
                SUM(position_value) as current_value
            FROM position_values
            GROUP BY representative, ticker, sector
            HAVING SUM(position_value) > 0
            ORDER BY current_value DESC;
        """)

        # Portfolio Value Analysis View
        print("Creating portfolio value analysis view...")
        con.execute("""
            DROP VIEW IF EXISTS portfolio_value_analysis;
            CREATE VIEW portfolio_value_analysis AS
            WITH parsed_amounts AS (
                SELECT 
                    t.representative,
                    t.transaction_date,
                    t.ticker,
                    t.type,
                    sd.industry as sector,
                    CASE 
                        WHEN t.amount LIKE '%$1,001 - $15,000%' THEN 8000.5
                        WHEN t.amount LIKE '%$15,001 - $50,000%' THEN 32500.5
                        WHEN t.amount LIKE '%$50,001 - $100,000%' THEN 75000.5
                        WHEN t.amount LIKE '%$100,001 - $250,000%' THEN 175000.5
                        WHEN t.amount LIKE '%$250,001 - $500,000%' THEN 375000.5
                        WHEN t.amount LIKE '%$500,001 - $1,000,000%' THEN 750000.5
                        WHEN t.amount LIKE '%$1,000,001 - $5,000,000%' THEN 3000000.5
                        ELSE 0
                    END as estimated_value
                FROM transactions t
                LEFT JOIN details.stocks sd ON t.ticker = sd.ticker
            )
            SELECT 
                representative,
                transaction_date,
                ticker,
                COALESCE(sector, 'Unknown') as sector,
                SUM(CASE 
                    WHEN type = 'purchase' THEN estimated_value
                    WHEN type = 'sale' THEN -estimated_value
                    ELSE 0
                END) OVER (
                    PARTITION BY representative, ticker
                    ORDER BY transaction_date
                ) as stock_value,
                SUM(CASE 
                    WHEN type = 'purchase' THEN estimated_value
                    WHEN type = 'sale' THEN -estimated_value
                    ELSE 0
                END) OVER (
                    PARTITION BY representative
                    ORDER BY transaction_date
                ) as total_portfolio_value
            FROM parsed_amounts
            ORDER BY representative, transaction_date;
        """)

        # Trading Timeline View
        print("Creating trading timeline view...")
        con.execute("""
            DROP VIEW IF EXISTS trading_timeline;
            CREATE VIEW trading_timeline AS
            SELECT 
                t.representative,
                t.transaction_date,
                t.ticker,
                t.type,
                t.amount,
                COALESCE(sd.industry, 'Unknown') as sector
            FROM transactions t
            LEFT JOIN details.stocks sd ON t.ticker = sd.ticker
            ORDER BY t.transaction_date;
        """)

        # Stock Overview View
        print("Creating stock overview view...")
        con.execute("""
            DROP VIEW IF EXISTS stock_overview;
            CREATE VIEW stock_overview AS
            WITH trade_durations AS (
                SELECT 
                    t1.ticker,
                    t1.representative,
                    t1.transaction_date as purchase_date,
                    t1.amount as purchase_amount,
                    p1.close as purchase_price,
                    COALESCE(MIN(t2.transaction_date), CURRENT_DATE) as sale_date,
                    CASE 
                        WHEN t1.amount LIKE '%$1,001 - $15,000%' THEN 8000.5
                        WHEN t1.amount LIKE '%$15,001 - $50,000%' THEN 32500.5
                        WHEN t1.amount LIKE '%$50,001 - $100,000%' THEN 75000.5
                        WHEN t1.amount LIKE '%$100,001 - $250,000%' THEN 175000.5
                        WHEN t1.amount LIKE '%$250,001 - $500,000%' THEN 375000.5
                        WHEN t1.amount LIKE '%$500,001 - $1,000,000%' THEN 750000.5
                        WHEN t1.amount LIKE '%$1,000,001 - $5,000,000%' THEN 3000000.5
                        ELSE 0
                    END / NULLIF(p1.close, 0) as estimated_volume
                FROM transactions t1
                LEFT JOIN prices.daily_prices p1 ON t1.ticker = p1.ticker 
                    AND t1.transaction_date = p1.date
                LEFT JOIN transactions t2 ON t1.ticker = t2.ticker 
                    AND t1.representative = t2.representative
                    AND t2.type = 'sale'
                    AND t2.transaction_date > t1.transaction_date
                WHERE t1.type = 'purchase'
                GROUP BY t1.ticker, t1.representative, t1.transaction_date, t1.amount, p1.close
            )
            SELECT 
                ticker,
                COUNT(DISTINCT representative) as active_representatives,
                COUNT(*) as total_trades,
                AVG(DATEDIFF('day', purchase_date, sale_date)) as avg_holding_days,
                SUM(estimated_volume) as total_volume
            FROM trade_durations
            GROUP BY ticker;
        """)

        # Stock Active Positions View
        print("Creating stock positions view...")
        con.execute("""
            DROP VIEW IF EXISTS stock_positions;
            CREATE VIEW stock_positions AS
            WITH current_holdings AS (
                SELECT 
                    t.ticker,
                    t.representative,
                    r.party,
                    SUM(CASE 
                        WHEN t.type = 'purchase' THEN 
                            CASE 
                                WHEN t.amount LIKE '%$1,001 - $15,000%' THEN 8000.5
                                WHEN t.amount LIKE '%$15,001 - $50,000%' THEN 32500.5
                                WHEN t.amount LIKE '%$50,001 - $100,000%' THEN 75000.5
                                WHEN t.amount LIKE '%$100,001 - $250,000%' THEN 175000.5
                                WHEN t.amount LIKE '%$250,001 - $500,000%' THEN 375000.5
                                WHEN t.amount LIKE '%$500,001 - $1,000,000%' THEN 750000.5
                                WHEN t.amount LIKE '%$1,000,001 - $5,000,000%' THEN 3000000.5
                                ELSE 0
                            END
                        WHEN t.type = 'sale' THEN 
                            -1 * CASE 
                                WHEN t.amount LIKE '%$1,001 - $15,000%' THEN 8000.5
                                WHEN t.amount LIKE '%$15,001 - $50,000%' THEN 32500.5
                                WHEN t.amount LIKE '%$50,001 - $100,000%' THEN 75000.5
                                WHEN t.amount LIKE '%$100,001 - $250,000%' THEN 175000.5
                                WHEN t.amount LIKE '%$250,001 - $500,000%' THEN 375000.5
                                WHEN t.amount LIKE '%$500,001 - $1,000,000%' THEN 750000.5
                                WHEN t.amount LIKE '%$1,000,001 - $5,000,000%' THEN 3000000.5
                                ELSE 0
                            END
                        ELSE 0
                    END) as position_value
                FROM transactions t
                LEFT JOIN reps.representatives r ON t.representative = r.name
                GROUP BY t.ticker, t.representative, r.party
                HAVING position_value > 0
            )
            SELECT 
                ticker,
                representative,
                party,
                position_value
            FROM current_holdings
            ORDER BY position_value DESC;
        """)

        # Stock Trading Timeline View
        print("Creating stock trading timeline view...")
        con.execute("""
            DROP VIEW IF EXISTS stock_trading_timeline;
            CREATE VIEW stock_trading_timeline AS
            SELECT 
                t.ticker,
                t.transaction_date,
                t.type,
                t.representative,
                r.party,
                t.amount,
                p.close as price_at_trade
            FROM transactions t
            LEFT JOIN reps.representatives r ON t.representative = r.name
            LEFT JOIN prices.daily_prices p ON t.ticker = p.ticker AND t.transaction_date = p.date
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