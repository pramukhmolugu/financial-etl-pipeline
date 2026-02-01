"""
Unit tests for data transformation
Tests data cleaning, validation, and quality checks
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.transform import DataTransformer, DataQualityChecker


class TestDataTransformer:
    """Test suite for DataTransformer class"""
    
    @pytest.fixture
    def transformer(self):
        """Create transformer instance"""
        return DataTransformer()
    
    @pytest.fixture
    def sample_dirty_data(self):
        """Create sample data with quality issues"""
        return pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002', 'TXN002', 'TXN003', 'TXN004'],  # Has duplicate
            'customer_id': ['CUST001', 'CUST002', None, 'CUST004', 'CUST005'],     # Has null
            'transaction_date': [
                datetime.now() - timedelta(days=1),
                datetime.now() - timedelta(days=2),
                datetime.now() - timedelta(days=3),
                datetime.now() + timedelta(days=1),  # Future date (invalid)
                datetime.now() - timedelta(days=4)
            ],
            'amount': [100.50, 250.00, -50.00, 75.25, None],  # Has negative and null
            'merchant_id': ['MERCH001', 'MERCH002', 'MERCH003', 'MERCH004', 'MERCH005'],
            'category': ['groceries', 'restaurants', None, 'retail', 'utilities'],
            'status': ['completed', 'completed', 'completed', 'invalid_status', 'pending'],
            'payment_method': ['credit_card', 'debit_card', 'credit_card', 'cash', 'bank_transfer']
        })
    
    def test_remove_duplicates(self, transformer, sample_dirty_data):
        """Test duplicate removal"""
        result = transformer._remove_duplicates(sample_dirty_data)
        
        # Should remove one duplicate
        assert len(result) == 4
        assert result['transaction_id'].is_unique
    
    def test_handle_missing_values(self, transformer, sample_dirty_data):
        """Test missing value handling"""
        result = transformer._handle_missing_values(sample_dirty_data)
        
        # Should remove records with null critical fields
        assert result['transaction_id'].notna().all()
        assert result['customer_id'].notna().all()
        assert result['amount'].notna().all()
    
    def test_validate_data_types(self, transformer, sample_dirty_data):
        """Test data type validation"""
        result = transformer._validate_data_types(sample_dirty_data)
        
        # Check data types (pandas 3.0 uses 'us' instead of 'ns')
        assert str(result['transaction_date'].dtype).startswith('datetime64')
        assert pd.api.types.is_numeric_dtype(result['amount'])
        # Pandas 3.0 returns StringDtype which shows as 'str'
        assert result['status'].dtype == 'object' or 'str' in str(result['status'].dtype).lower()
    
    def test_apply_business_rules_removes_negative_amounts(self, transformer):
        """Test that negative amounts are removed"""
        df = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002'],
            'customer_id': ['CUST001', 'CUST002'],
            'transaction_date': [datetime.now(), datetime.now()],
            'amount': [100.0, -50.0],
            'status': ['completed', 'completed']
        })
        
        result = transformer._apply_business_rules(df)
        
        # Should only have positive amounts
        assert len(result) == 1
        assert (result['amount'] > 0).all()
    
    def test_apply_business_rules_removes_future_dates(self, transformer):
        """Test that future dates are removed"""
        df = pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002'],
            'customer_id': ['CUST001', 'CUST002'],
            'transaction_date': [
                datetime.now() - timedelta(days=1),
                datetime.now() + timedelta(days=1)
            ],
            'amount': [100.0, 150.0],
            'status': ['completed', 'completed']
        })
        
        result = transformer._apply_business_rules(df)
        
        # Should only have past/present dates
        assert len(result) == 1
        assert (result['transaction_date'] <= datetime.now()).all()
    
    def test_add_derived_fields(self, transformer):
        """Test derived field creation"""
        df = pd.DataFrame({
            'transaction_id': ['TXN001'],
            'customer_id': ['CUST001'],
            'transaction_date': [datetime(2024, 1, 15, 14, 30)],
            'amount': [250.0],
            'merchant_id': ['MERCH001'],
            'category': ['groceries'],
            'status': ['completed'],
            'payment_method': ['credit_card']
        })
        
        result = transformer._add_derived_fields(df)
        
        # Check derived fields exist
        assert 'transaction_year' in result.columns
        assert 'transaction_month' in result.columns
        assert 'transaction_day' in result.columns
        assert 'risk_score' in result.columns
        assert 'risk_level' in result.columns
        assert 'amount_category' in result.columns
        
        # Verify values
        assert result['transaction_year'].iloc[0] == 2024
        assert result['transaction_month'].iloc[0] == 1
        assert result['transaction_day'].iloc[0] == 15
    
    def test_complete_transformation_pipeline(self, transformer, sample_dirty_data):
        """Test complete transformation pipeline"""
        result = transformer.transform_transactions(sample_dirty_data)
        
        # Should return a DataFrame
        assert isinstance(result, pd.DataFrame)
        
        # Should have cleaned data
        assert len(result) > 0
        assert result['transaction_id'].is_unique
        assert result['amount'].notna().all()
        assert (result['amount'] > 0).all()
        
        # Should have derived fields
        assert 'risk_score' in result.columns
        assert 'risk_level' in result.columns
        
        # Quality report should be generated
        report = transformer.get_quality_report()
        assert 'transactions' in report
        assert 'initial_records' in report['transactions']
        assert 'final_records' in report['transactions']


class TestDataQualityChecker:
    """Test suite for DataQualityChecker class"""
    
    @pytest.fixture
    def quality_checker(self):
        """Create quality checker instance"""
        return DataQualityChecker()
    
    @pytest.fixture
    def good_data(self):
        """Create clean data that passes all checks"""
        return pd.DataFrame({
            'transaction_id': [f'TXN{i:03d}' for i in range(200)],
            'customer_id': [f'CUST{i:03d}' for i in range(200)],
            'amount': np.random.uniform(10, 1000, 200)
        })
    
    @pytest.fixture
    def bad_data_few_rows(self):
        """Create data with too few rows"""
        return pd.DataFrame({
            'transaction_id': ['TXN001', 'TXN002'],
            'customer_id': ['CUST001', 'CUST002'],
            'amount': [100.0, 200.0]
        })
    
    def test_quality_check_passes_good_data(self, quality_checker, good_data):
        """Test that good data passes all checks"""
        result = quality_checker.run_quality_checks(good_data, "test_dataset")
        assert result is True
        assert len(quality_checker.quality_issues) == 0
    
    def test_quality_check_fails_few_rows(self, quality_checker, bad_data_few_rows):
        """Test that data with too few rows fails"""
        result = quality_checker.run_quality_checks(bad_data_few_rows, "test_dataset")
        assert result is False
        assert len(quality_checker.quality_issues) > 0
    
    def test_quality_check_detects_high_nulls(self, quality_checker):
        """Test that high null percentage is detected"""
        data_with_nulls = pd.DataFrame({
            'transaction_id': [f'TXN{i:03d}' for i in range(200)],
            'customer_id': [None] * 100 + [f'CUST{i:03d}' for i in range(100)],  # 50% nulls
            'amount': [100.0] * 200
        })
        
        result = quality_checker.run_quality_checks(data_with_nulls, "test_dataset")
        assert result is False
        assert any('Null percentage' in issue for issue in quality_checker.quality_issues)


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
