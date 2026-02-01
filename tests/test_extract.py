"""
Unit tests for data extraction
Tests data completeness, validity, and error handling
"""

import pytest
import pandas as pd
from pathlib import Path
from src.extract import DataExtractor
from src.config import RAW_DATA_DIR


class TestDataExtractor:
    """Test suite for DataExtractor class"""
    
    @pytest.fixture
    def extractor(self):
        """Create extractor instance for testing"""
        return DataExtractor(RAW_DATA_DIR)
    
    def test_extract_transactions_returns_dataframe(self, extractor):
        """Test that extract_transactions returns a DataFrame"""
        result = extractor.extract_transactions(num_records=100)
        assert isinstance(result, pd.DataFrame)
    
    def test_extract_transactions_correct_row_count(self, extractor):
        """Test that correct number of records are generated"""
        num_records = 100
        result = extractor.extract_transactions(num_records=num_records)
        assert len(result) == num_records
    
    def test_extract_transactions_has_required_columns(self, extractor):
        """Test that all required columns are present"""
        result = extractor.extract_transactions(num_records=100)
        
        required_columns = [
            'transaction_id', 'customer_id', 'transaction_date',
            'amount', 'merchant_id', 'category', 'status', 'payment_method'
        ]
        
        for col in required_columns:
            assert col in result.columns, f"Missing column: {col}"
    
    def test_transaction_ids_mostly_unique(self, extractor):
        """Test that transaction IDs are mostly unique (allowing for intentional duplicates)"""
        result = extractor.extract_transactions(num_records=1000)
        uniqueness = result['transaction_id'].nunique() / len(result)
        assert uniqueness > 0.95, "Transaction IDs should be >95% unique"
    
    def test_amounts_positive_when_not_null(self, extractor):
        """Test that non-null amounts are positive"""
        result = extractor.extract_transactions(num_records=100)
        non_null_amounts = result['amount'].dropna()
        assert (non_null_amounts > 0).all(), "All amounts should be positive"
    
    def test_status_valid_values(self, extractor):
        """Test that status contains only valid values"""
        result = extractor.extract_transactions(num_records=100)
        valid_statuses = ['completed', 'pending', 'failed']
        assert result['status'].isin(valid_statuses).all()
    
    def test_extract_customers_returns_dataframe(self, extractor):
        """Test that extract_customers returns a DataFrame"""
        result = extractor.extract_customers(num_records=100)
        assert isinstance(result, pd.DataFrame)
    
    def test_customer_ids_unique(self, extractor):
        """Test that customer IDs are unique"""
        result = extractor.extract_customers(num_records=100)
        assert result['customer_id'].is_unique


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
