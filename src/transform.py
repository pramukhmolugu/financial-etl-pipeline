"""
Data Transformation Module
Cleans, validates, and transforms raw data for loading
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transform and validate extracted data.
    Demonstrates: Data cleaning, validation, quality controls, business rules
    """
    
    def __init__(self):
        self.quality_report = {}
        
    def transform_transactions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Complete transformation pipeline for transactions.
        
        Steps:
        1. Remove duplicates
        2. Handle missing values
        3. Validate data types
        4. Apply business rules
        5. Add derived fields
        6. Generate quality report
        
        Args:
            df: Raw transaction DataFrame
            
        Returns:
            Cleaned and transformed DataFrame
        """
        logger.info("Starting transaction transformation...")
        
        # Store initial row count
        initial_count = len(df)
        
        # Step 1: Remove duplicates
        df = self._remove_duplicates(df)
        
        # Step 2: Handle missing values
        df = self._handle_missing_values(df)
        
        # Step 3: Validate and fix data types
        df = self._validate_data_types(df)
        
        # Step 4: Apply business rules
        df = self._apply_business_rules(df)
        
        # Step 5: Add derived fields
        df = self._add_derived_fields(df)
        
        # Step 6: Validate final data
        df = self._final_validation(df)
        
        # Generate quality report
        final_count = len(df)
        self.quality_report['transactions'] = {
            'initial_records': initial_count,
            'final_records': final_count,
            'records_removed': initial_count - final_count,
            'removal_percentage': ((initial_count - final_count) / initial_count * 100) if initial_count > 0 else 0,
            'null_counts': df.isnull().sum().to_dict(),
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Transformation complete. Records: {initial_count} → {final_count}")
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate transaction IDs.
        Keep the first occurrence.
        """
        initial_count = len(df)
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        duplicates_removed = initial_count - len(df)
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate transactions")
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values according to business rules.
        
        Rules:
        - Critical fields (transaction_id, customer_id): Remove record
        - Amount: Remove record (can't process without amount)
        - Category: Fill with 'unknown'
        - Merchant: Fill with 'UNKNOWN'
        """
        initial_count = len(df)
        
        # Remove records with missing critical fields
        critical_fields = ['transaction_id', 'customer_id', 'amount']
        for field in critical_fields:
            before = len(df)
            df = df[df[field].notna()]
            removed = before - len(df)
            if removed > 0:
                logger.warning(f"Removed {removed} records with missing {field}")
        
        # Fill non-critical missing values
        df['category'] = df['category'].fillna('unknown')
        df['merchant_id'] = df['merchant_id'].fillna('MERCH0000')
        
        return df
    
    def _validate_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ensure correct data types and fix formatting.
        """
        # Convert transaction_date to datetime
        if df['transaction_date'].dtype != 'datetime64[ns]':
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
        
        # Ensure amount is numeric
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        
        # Remove any records where conversion failed
        df = df[df['transaction_date'].notna()]
        df = df[df['amount'].notna()]
        
        # Standardize string fields
        df['transaction_id'] = df['transaction_id'].astype(str).str.strip().str.upper()
        df['customer_id'] = df['customer_id'].astype(str).str.strip().str.upper()
        df['merchant_id'] = df['merchant_id'].astype(str).str.strip().str.upper()
        df['status'] = df['status'].astype(str).str.strip().str.lower()
        df['category'] = df['category'].astype(str).str.strip().str.lower()
        df['payment_method'] = df['payment_method'].astype(str).str.strip().str.lower()
        
        return df
    
    def _apply_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply business validation rules.
        
        Rules:
        1. Amount must be positive
        2. Amount must be <= $1,000,000 (fraud threshold)
        3. Status must be valid
        4. Transaction date cannot be in future
        5. Transaction date cannot be older than 2 years
        """
        initial_count = len(df)
        
        # Rule 1: Amount must be positive
        df = df[df['amount'] > 0]
        logger.info(f"Filtered negative amounts. Records removed: {initial_count - len(df)}")
        
        # Rule 2: Amount must be reasonable (<= $1M)
        initial_count = len(df)
        df = df[df['amount'] <= 1_000_000]
        if initial_count - len(df) > 0:
            logger.warning(f"Removed {initial_count - len(df)} transactions with amount > $1M")
        
        # Rule 3: Status must be valid
        valid_statuses = ['completed', 'pending', 'failed']
        initial_count = len(df)
        df = df[df['status'].isin(valid_statuses)]
        if initial_count - len(df) > 0:
            logger.warning(f"Removed {initial_count - len(df)} transactions with invalid status")
        
        # Rule 4: Transaction date not in future
        initial_count = len(df)
        df = df[df['transaction_date'] <= datetime.now()]
        if initial_count - len(df) > 0:
            logger.warning(f"Removed {initial_count - len(df)} future-dated transactions")
        
        # Rule 5: Transaction date not older than 2 years
        two_years_ago = datetime.now() - pd.Timedelta(days=730)
        initial_count = len(df)
        df = df[df['transaction_date'] >= two_years_ago]
        if initial_count - len(df) > 0:
            logger.warning(f"Removed {initial_count - len(df)} transactions older than 2 years")
        
        return df
    
    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add calculated fields for analysis.
        """
        # Extract date components
        df['transaction_year'] = df['transaction_date'].dt.year
        df['transaction_month'] = df['transaction_date'].dt.month
        df['transaction_day'] = df['transaction_date'].dt.day
        df['transaction_dayofweek'] = df['transaction_date'].dt.dayofweek
        df['transaction_hour'] = df['transaction_date'].dt.hour
        
        # Add amount categories
        df['amount_category'] = pd.cut(
            df['amount'],
            bins=[0, 50, 200, 500, 1000, float('inf')],
            labels=['small', 'medium', 'large', 'very_large', 'exceptional']
        )
        
        # Add risk score (simple example)
        df['risk_score'] = 0.0
        
        # High amount = higher risk
        df.loc[df['amount'] > 5000, 'risk_score'] += 30
        df.loc[df['amount'] > 10000, 'risk_score'] += 40
        
        # Failed status = higher risk
        df.loc[df['status'] == 'failed', 'risk_score'] += 50
        
        # Weekend transactions = slightly higher risk
        df.loc[df['transaction_dayofweek'].isin([5, 6]), 'risk_score'] += 10
        
        # Late night transactions = higher risk
        df.loc[df['transaction_hour'].isin(range(0, 6)), 'risk_score'] += 20
        
        # Add risk level
        df['risk_level'] = pd.cut(
            df['risk_score'],
            bins=[-1, 20, 50, 80, 100],
            labels=['low', 'medium', 'high', 'critical']
        )
        
        # Add processing timestamp
        df['processed_at'] = datetime.now()
        
        return df
    
    def _final_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final validation before loading.
        Ensure all required fields are present and valid.
        """
        required_fields = [
            'transaction_id', 'customer_id', 'transaction_date',
            'amount', 'merchant_id', 'category', 'status',
            'payment_method', 'risk_score', 'risk_level'
        ]
        
        # Check all required fields exist
        missing_fields = [f for f in required_fields if f not in df.columns]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Final check: no nulls in critical fields
        critical_nulls = df[required_fields].isnull().sum()
        if critical_nulls.sum() > 0:
            logger.error(f"Critical nulls found: {critical_nulls[critical_nulls > 0]}")
            raise ValueError("Critical fields contain null values")
        
        logger.info("Final validation passed ✓")
        return df
    
    def get_quality_report(self) -> Dict:
        """
        Return data quality report.
        """
        return self.quality_report
    
    def validate_against_customers(
        self, 
        transactions: pd.DataFrame, 
        customers: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Validate transactions against customer data.
        Remove transactions for non-existent customers.
        
        Args:
            transactions: Transaction DataFrame
            customers: Customer DataFrame
            
        Returns:
            Validated transactions
        """
        logger.info("Validating transactions against customer data...")
        
        initial_count = len(transactions)
        
        # Get valid customer IDs
        valid_customers = set(customers['customer_id'].unique())
        
        # Filter transactions
        transactions = transactions[
            transactions['customer_id'].isin(valid_customers)
        ]
        
        removed = initial_count - len(transactions)
        if removed > 0:
            logger.warning(f"Removed {removed} transactions for non-existent customers")
        
        return transactions


class DataQualityChecker:
    """
    Automated data quality checks.
    Demonstrates: Data-as-Code testing framework
    """
    
    def __init__(self, thresholds: Dict = None):
        self.thresholds = thresholds or {
            'max_null_percentage': 0.05,
            'min_row_count': 100,
            'max_duplicate_percentage': 0.01
        }
        self.quality_issues = []
    
    def run_quality_checks(self, df: pd.DataFrame, dataset_name: str = "dataset") -> bool:
        """
        Run comprehensive quality checks on dataset.
        
        Returns:
            True if all checks pass, False otherwise
        """
        logger.info(f"Running quality checks on {dataset_name}...")
        
        all_passed = True
        
        # Check 1: Minimum row count
        if not self._check_row_count(df, dataset_name):
            all_passed = False
        
        # Check 2: Null value percentage
        if not self._check_null_percentage(df, dataset_name):
            all_passed = False
        
        # Check 3: Duplicate percentage
        if not self._check_duplicates(df, dataset_name):
            all_passed = False
        
        # Check 4: Data types
        if not self._check_data_types(df, dataset_name):
            all_passed = False
        
        if all_passed:
            logger.info(f"✓ All quality checks passed for {dataset_name}")
        else:
            logger.error(f"✗ Quality checks failed for {dataset_name}")
            logger.error(f"Issues found: {self.quality_issues}")
        
        return all_passed
    
    def _check_row_count(self, df: pd.DataFrame, dataset_name: str) -> bool:
        """Check minimum row count"""
        if len(df) < self.thresholds['min_row_count']:
            issue = f"{dataset_name}: Row count {len(df)} below minimum {self.thresholds['min_row_count']}"
            self.quality_issues.append(issue)
            logger.warning(issue)
            return False
        return True
    
    def _check_null_percentage(self, df: pd.DataFrame, dataset_name: str) -> bool:
        """Check null value percentage"""
        null_pct = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
        
        if null_pct > self.thresholds['max_null_percentage'] * 100:
            issue = f"{dataset_name}: Null percentage {null_pct:.2f}% exceeds threshold {self.thresholds['max_null_percentage']*100}%"
            self.quality_issues.append(issue)
            logger.warning(issue)
            return False
        return True
    
    def _check_duplicates(self, df: pd.DataFrame, dataset_name: str) -> bool:
        """Check duplicate percentage"""
        if 'transaction_id' in df.columns:
            dup_pct = (df['transaction_id'].duplicated().sum() / len(df)) * 100
            
            if dup_pct > self.thresholds['max_duplicate_percentage'] * 100:
                issue = f"{dataset_name}: Duplicate percentage {dup_pct:.2f}% exceeds threshold"
                self.quality_issues.append(issue)
                logger.warning(issue)
                return False
        return True
    
    def _check_data_types(self, df: pd.DataFrame, dataset_name: str) -> bool:
        """Check expected data types"""
        # Add specific data type checks based on your schema
        return True
    
    def get_quality_report(self) -> Dict:
        """Return quality issues found"""
        return {
            'issues': self.quality_issues,
            'issue_count': len(self.quality_issues)
        }


if __name__ == "__main__":
    # Test the transformer
    from extract import DataExtractor
    from config import RAW_DATA_DIR
    
    # Extract sample data
    extractor = DataExtractor(RAW_DATA_DIR)
    raw_transactions = extractor.extract_transactions(1000)
    
    # Transform the data
    transformer = DataTransformer()
    clean_transactions = transformer.transform_transactions(raw_transactions)
    
    print("\n" + "="*80)
    print("TRANSFORMATION RESULTS")
    print("="*80)
    print(f"\nInitial records: {len(raw_transactions)}")
    print(f"Final records: {len(clean_transactions)}")
    print(f"\nSample of transformed data:")
    print(clean_transactions.head())
    
    print("\n" + "="*80)
    print("QUALITY REPORT")
    print("="*80)
    print(transformer.get_quality_report())
    
    # Run quality checks
    print("\n" + "="*80)
    print("QUALITY CHECKS")
    print("="*80)
    quality_checker = DataQualityChecker()
    passed = quality_checker.run_quality_checks(clean_transactions, "transactions")
    
    if not passed:
        print("\nQuality Issues:")
        print(quality_checker.get_quality_report())
