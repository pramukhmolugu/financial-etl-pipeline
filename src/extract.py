"""
Data Extraction Module
Extracts data from various sources (JSON API, CSV, databases)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Extract data from multiple sources.
    Demonstrates: Multi-source data extraction, JSON handling, error handling
    """
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def extract_transactions(self, num_records: int = 10000) -> pd.DataFrame:
        """
        Extract transaction data.
        For demo: generates synthetic data (in production: would call real API)
        
        Args:
            num_records: Number of transactions to generate
            
        Returns:
            DataFrame with transaction data
        """
        logger.info(f"Extracting {num_records} transactions...")
        
        np.random.seed(42)
        
        # Generate synthetic transaction data
        data = {
            'transaction_id': [f'TXN{i:08d}' for i in range(num_records)],
            'customer_id': [f'CUST{np.random.randint(1, 5000):06d}' for _ in range(num_records)],
            'transaction_date': [
                datetime.now() - timedelta(days=np.random.randint(0, 365))
                for _ in range(num_records)
            ],
            'amount': np.random.lognormal(mean=4, sigma=1.5, size=num_records).round(2),
            'merchant_id': [f'MERCH{np.random.randint(1, 500):04d}' for _ in range(num_records)],
            'category': np.random.choice(
                ['groceries', 'restaurants', 'gas', 'retail', 'utilities', 'entertainment'],
                num_records,
                p=[0.25, 0.20, 0.15, 0.20, 0.10, 0.10]
            ),
            'status': np.random.choice(
                ['completed', 'pending', 'failed'],
                num_records,
                p=[0.90, 0.07, 0.03]
            ),
            'payment_method': np.random.choice(
                ['credit_card', 'debit_card', 'bank_transfer', 'cash'],
                num_records,
                p=[0.50, 0.30, 0.15, 0.05]
            )
        }
        
        df = pd.DataFrame(data)
        
        # Introduce some data quality issues (realistic scenario)
        # 2% null values in amount
        null_indices = np.random.choice(df.index, size=int(num_records * 0.02), replace=False)
        df.loc[null_indices, 'amount'] = np.nan
        
        # 1% duplicate transaction IDs
        dup_indices = np.random.choice(df.index[100:], size=int(num_records * 0.01), replace=False)
        df.loc[dup_indices, 'transaction_id'] = df.loc[dup_indices - 100, 'transaction_id'].values
        
        # Save raw data
        output_file = self.output_dir / f'raw_transactions_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Raw data saved to {output_file}")
        
        logger.info(f"Extracted {len(df)} transactions")
        return df
    
    def extract_customers(self, num_records: int = 5000) -> pd.DataFrame:
        """
        Extract customer data.
        
        Args:
            num_records: Number of customers to generate
            
        Returns:
            DataFrame with customer data
        """
        logger.info(f"Extracting {num_records} customers...")
        
        np.random.seed(42)
        
        data = {
            'customer_id': [f'CUST{i:06d}' for i in range(1, num_records + 1)],
            'customer_name': [f'Customer {i}' for i in range(1, num_records + 1)],
            'registration_date': [
                datetime.now() - timedelta(days=np.random.randint(0, 1825))
                for _ in range(num_records)
            ],
            'customer_tier': np.random.choice(
                ['bronze', 'silver', 'gold', 'platinum'],
                num_records,
                p=[0.50, 0.30, 0.15, 0.05]
            ),
            'email': [f'customer{i}@example.com' for i in range(1, num_records + 1)],
            'is_active': np.random.choice([True, False], num_records, p=[0.95, 0.05])
        }
        
        df = pd.DataFrame(data)
        
        output_file = self.output_dir / f'raw_customers_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"Customer data saved to {output_file}")
        
        logger.info(f"Extracted {len(df)} customers")
        return df


if __name__ == "__main__":
    # Test the extractor
    from config import RAW_DATA_DIR
    
    extractor = DataExtractor(RAW_DATA_DIR)
    
    # Extract data
    transactions = extractor.extract_transactions(10000)
    customers = extractor.extract_customers(5000)
    
    print("\nTransaction Data Sample:")
    print(transactions.head())
    print(f"\nTotal Transactions: {len(transactions)}")
    print(f"\nNull Values:\n{transactions.isnull().sum()}")
    
    print("\nCustomer Data Sample:")
    print(customers.head())
    print(f"\nTotal Customers: {len(customers)}")
