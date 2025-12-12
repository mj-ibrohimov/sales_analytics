# Sales Analytics Dashboard

A FastAPI-based Business Intelligence dashboard for analyzing sales data from multiple sources.

## Features

- Data processing and ETL pipeline for books, customers, and transactions
- Revenue analytics with daily trends
- Customer deduplication and reconciliation
- Author set analysis
- Interactive dashboard with three data source tabs

## Requirements

- Python 3.8+
- PostgreSQL database
- Environment variables configured (see `.env` file)

## Quick Start

**For detailed setup instructions, see [SETUP_GUIDE.md](SETUP_GUIDE.md)**

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Create `.env` file** (copy from `.env.example`):
```env
DB_HOST=localhost
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
```

3. **Run the application:**
```bash
python main.py
```

The application will be available at `http://127.0.0.1:8000`

## Project Structure

```
.
├── app/
│   ├── api/          # API routes
│   ├── core/          # Core components (database, models)
│   └── services/      # Business logic services
├── templates/         # HTML templates
├── static/           # Static files (images, CSS)
├── DATAs/            # Data sources (DATA1, DATA2, DATA3)
├── main.py           # FastAPI application entry point
└── asgi.py           # ASGI application for deployment
```

## API Endpoints

- `GET /` - Main dashboard (HTML)
- `GET /health` - Health check endpoint

## Data Processing

The application automatically processes data from DATA1, DATA2, and DATA3 folders on first access. Data is stored in PostgreSQL with the following tables:

- `analytics_metrics` - Calculated metrics
- `book_catalog` - Book information
- `customer_profiles` - Customer data with deduplication
- `transaction_records` - Order/transaction data

## Dashboard Features

- Top 5 revenue days (YYYY-MM-dd format)
- Unique customer count (with deduplication)
- Unique author set count
- Most popular author by books sold
- Top customer with linked IDs
- Daily revenue trend chart

## Screenshots

Homepage preview:
![Dashboard Homepage](static/images/homepage.png)

Example chart:
![Revenue Chart](static/images/example-chart.png)

