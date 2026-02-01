"""
Configuration for ETL Pipeline
Stores database connections and settings
"""

import os
from pathlib import Path

# Project paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Create directories if they don't exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'etl_db'),
    'user': os.getenv('DB_USER', 'etl_user'),
    'password': os.getenv('DB_PASSWORD', 'etl_password')
}

# Data source configuration
DATA_SOURCES = {
    'transactions': 'https://api.example.com/transactions',
    'customers': 'https://api.example.com/customers'
}

# Quality thresholds
QUALITY_THRESHOLDS = {
    'max_null_percentage': 0.05,  # Max 5% null values
    'min_row_count': 100,          # Minimum expected rows
    'duplicate_threshold': 0.01    # Max 1% duplicates
}
