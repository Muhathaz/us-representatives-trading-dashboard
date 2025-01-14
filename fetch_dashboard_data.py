import duckdb
import pandas as pd

class DashboardData:
    def __init__(self):
        """Initialize database connections"""
        self.con = None
        self._connect()

    def _connect(self):
        """Create a new connection"""
        try:
            if self.con:
                try:
                    self.con.close()
                except:
                    pass
            self.con = duckdb.connect('databases/transactions.duckdb', read_only=True)
            self.con.execute("ATTACH 'databases/stock_prices.duckdb' AS prices (READ_ONLY)")
            self.con.execute("ATTACH 'databases/stock_details.duckdb' AS details (READ_ONLY)")
            self.con.execute("ATTACH 'databases/representatives.duckdb' AS reps (READ_ONLY)")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            self.con = None

    def close(self):
        """Close database connection"""
        if self.con:
            try:
                self.con.close()
            except:
                pass
            self.con = None

    def _check_connection(self):
        """Ensure connection is active"""
        try:
            if self.con:
                # Try a simple query to test connection
                self.con.execute("SELECT 1").fetchone()
            else:
                self._connect()
        except:
            self._connect()

    def get_representative_overview(self, representative_name=None):
        """Fetch representative overview data"""
        try:
            self._check_connection()
            if representative_name:
                result = self.con.execute("""
                    SELECT * FROM rep_overview 
                    WHERE representative = ?
                """, [representative_name]).fetchdf()
            else:
                result = self.con.execute("SELECT * FROM rep_overview").fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching representative overview: {e}")
            return pd.DataFrame()

    def get_representative_sector_analysis(self, representative_name=None):
        """Fetch sector analysis data"""
        try:
            self._check_connection()
            if representative_name:
                result = self.con.execute("""
                    SELECT * FROM representative_sector_analysis 
                    WHERE representative = ?
                """, [representative_name]).fetchdf()
            else:
                result = self.con.execute("SELECT * FROM representative_sector_analysis").fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching sector analysis: {e}")
            return pd.DataFrame()

    def get_portfolio_value_analysis(self, representative_name=None):
        """Fetch portfolio value analysis data"""
        try:
            self._check_connection()
            if representative_name:
                result = self.con.execute("""
                    SELECT * FROM portfolio_value_analysis 
                    WHERE representative = ?
                    ORDER BY transaction_date
                """, [representative_name]).fetchdf()
            else:
                result = self.con.execute("SELECT * FROM portfolio_value_analysis").fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching portfolio value analysis: {e}")
            return pd.DataFrame()

    def get_current_positions(self, representative_name=None):
        """Fetch current positions data"""
        try:
            self._check_connection()
            if representative_name:
                result = self.con.execute("""
                    SELECT * FROM current_positions 
                    WHERE representative = ?
                    ORDER BY current_value DESC
                """, [representative_name]).fetchdf()
            else:
                result = self.con.execute("SELECT * FROM current_positions").fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching current positions: {e}")
            return pd.DataFrame()

    def get_trading_timeline(self, start_date=None, end_date=None):
        """Fetch trading timeline data"""
        try:
            self._check_connection()
            query = "SELECT * FROM trading_timeline WHERE 1=1"
            params = []
            
            if start_date:
                query += " AND transaction_date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND transaction_date <= ?"
                params.append(end_date)
            
            result = self.con.execute(query, params).fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching trading timeline: {e}")
            return pd.DataFrame()

    def get_all_representatives(self):
        """Get list of all representatives"""
        try:
            self._check_connection()
            result = self.con.execute("""
                SELECT DISTINCT representative as name 
                FROM transactions 
                ORDER BY representative
            """).fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching representatives: {e}")
            return pd.DataFrame(columns=['name'])

    def get_all_tickers(self):
        """Get list of all tickers"""
        try:
            self._check_connection()
            result = self.con.execute("""
                SELECT DISTINCT ticker 
                FROM transactions 
                ORDER BY ticker
            """).fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching tickers: {e}")
            return pd.DataFrame(columns=['ticker'])

    def get_all_stocks(self):
        """Get list of all stocks with price data"""
        try:
            self._check_connection()
            # Only return stocks that have price data
            result = self.con.execute("""
                SELECT DISTINCT t.ticker 
                FROM transactions t
                INNER JOIN prices.daily_prices p ON t.ticker = p.ticker
                ORDER BY t.ticker;
            """).fetchdf()
            
            # Debug print
            print(f"Found {len(result)} stocks with price data")
            
            return result
        except Exception as e:
            print(f"Error fetching stocks: {e}")
            return pd.DataFrame(columns=['ticker'])

    def get_stock_prices(self, ticker):
        """Get historical prices for a stock"""
        try:
            self._check_connection()
            # First, let's verify if the ticker exists in our database
            check_query = """
                SELECT COUNT(*) 
                FROM prices.daily_prices 
                WHERE ticker = ?;
            """
            count = self.con.execute(check_query, [ticker]).fetchone()[0]
            
            if count == 0:
                print(f"No price data found for ticker: {ticker}")
                return pd.DataFrame()
            
            # If ticker exists, fetch the data
            result = self.con.execute("""
                SELECT 
                    date,
                    close
                FROM prices.daily_prices
                WHERE ticker = ?
                ORDER BY date;
            """, [ticker]).fetchdf()
            
            # Debug print
            print(f"Found {len(result)} price records for {ticker}")
            
            return result
        except Exception as e:
            print(f"Error fetching stock prices: {e}")
            return pd.DataFrame()

    def get_stock_overview(self, ticker):
        """Get overview statistics for a stock"""
        try:
            self._check_connection()
            result = self.con.execute("""
                SELECT *
                FROM stock_overview
                WHERE ticker = ?;
            """, [ticker]).fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching stock overview: {e}")
            return pd.DataFrame()

    def get_stock_positions(self, ticker):
        """Get current positions for a stock"""
        try:
            self._check_connection()
            result = self.con.execute("""
                SELECT *
                FROM stock_positions
                WHERE ticker = ?
                ORDER BY position_value DESC;
            """, [ticker]).fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching stock positions: {e}")
            return pd.DataFrame()

    def get_stock_trading_timeline(self, ticker):
        """Get trading timeline for a stock"""
        try:
            self._check_connection()
            result = self.con.execute("""
                SELECT *
                FROM stock_trading_timeline
                WHERE ticker = ?
                ORDER BY transaction_date;
            """, [ticker]).fetchdf()
            return result
        except Exception as e:
            print(f"Error fetching stock trading timeline: {e}")
            return pd.DataFrame()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close() 