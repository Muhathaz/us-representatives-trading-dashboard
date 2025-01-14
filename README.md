# US Representatives Trading Dashboard

A data pipeline and analysis tool for tracking US Representatives' stock trading activities.

## Overview

This project collects, validates, and analyzes stock trading data from US Representatives, storing it in a structured database system for analysis. It includes historical stock prices and company details for comprehensive trading analysis.

## Features

- Automated data collection from House Stock Watcher API
- Data validation and cleaning
- Multiple DuckDB databases for efficient data organization:
  - Transaction records
  - Stock prices
  - Company details
  - Representative information
- Historical stock price tracking
- Company information retrieval

## Prerequisites

- Python 3.8 or higher
- Git
- pip (Python package installer)

## Project Structure

```
us-representatives-trading-dashboard/
├── data/                    # Data storage directory
│   ├── all_transactions.json
│   ├── all_transactions.csv
│   ├── validation_results.json
│   └── failed_tickers.json
├── databases/              # Database directory
│   ├── transactions.duckdb
│   ├── stock_prices.duckdb
│   ├── stock_details.duckdb
│   └── representatives.duckdb
├── main.py                 # Pipeline orchestrator
├── collect_data.py         # Data collection script
├── validate_data.py        # Data validation script
├── setup_database_schema.py # Database schema setup
├── fetch_stock_prices.py   # Stock price fetching script
├── fetch_stock_details.py  # Stock details fetching script
├── requirements.txt        # Project dependencies
└── README.md              # Project documentation
```

## Database Schema

### transactions.duckdb

- Stores all trading transactions
- Fields: disclosure_year, disclosure_date, transaction_date, owner, ticker, asset_description, type, amount, representative, district, state, ptr_link, cap_gains_over_200_usd, industry, sector, party

### stock_prices.duckdb

- Stores historical daily stock prices
- Fields: ticker, date, open, high, low, close, volume

### stock_details.duckdb

- Stores company information
- Fields: ticker, company_name, sector, industry, country, market_cap, description, website, exchange, currency, last_updated_date

### representatives.duckdb

- Stores representative information
- Fields: representative_id, name, district, state, party

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/us-representatives-trading-dashboard.git
cd us-representatives-trading-dashboard
```

2. Create and activate virtual environment:

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/MacOS
python3 -m venv venv
source venv/bin/activate
```

3. Install required packages:

```bash
pip install -r requirements.txt
```

## Usage

### Running the Complete Pipeline

To run the entire data pipeline:

```bash
python main.py
```

This will:

1. Create necessary directories
2. Collect transaction data
3. Validate the data
4. Set up database schema
5. Fetch historical stock prices
6. Fetch company details

### Running Individual Components

To run specific components separately:

```bash
python collect_data.py      # Collect transaction data
python validate_data.py     # Validate collected data
python fetch_stock_prices.py # Update stock prices
python fetch_stock_details.py # Update company details
```

## Data Sources

- Transaction data: [House Stock Watcher API](https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json)
- Stock prices: [Yahoo Finance](https://finance.yahoo.com/)
- Company information: [Yahoo Finance API](https://finance.yahoo.com/)

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

Muhathaz - mails2muhathaz@gmail.com

Project Link: [https://github.com/yourusername/us-representatives-trading-dashboard](https://github.com/yourusername/us-representatives-trading-dashboard)
