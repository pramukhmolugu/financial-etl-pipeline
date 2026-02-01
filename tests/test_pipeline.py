"""
Integration tests for complete ETL pipeline
Tests end-to-end data flow
"""

import pytest
import pandas as pd
from src.pipeline import ETLPipeline
from src.config import DB_CONFIG
from src.load import DataLoader


class TestETLPipeline:
    """Integration tests for complete pipeline"""
    
    @pytest.fixture
    def pipeline(self):
        """Create pipeline instance"""
        return ETLPipeline()
    
    def test_pipeline_runs_successfully(self, pipeline):
        """Test that complete pipeline executes without errors"""
        # Run with small dataset
        result = pipeline.run(num_transactions=100, num_customers=50)
        assert result is True
    
    def test_data_loaded_to_database(self):
        """Test that data actually appears in database"""
        loader = DataLoader(DB_CONFIG)
        
        try:
            counts = loader.get_table_counts()
            
            # Should have data in both tables
            assert counts.get('dim_customers', 0) > 0
            assert counts.get('fact_transactions', 0) > 0
            
        finally:
            loader.close()
    
    def test_audit_log_created(self):
        """Test that audit log entries are created"""
        loader = DataLoader(DB_CONFIG)
        
        try:
            counts = loader.get_table_counts()
            assert counts.get('etl_audit_log', 0) > 0
            
        finally:
            loader.close()


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
