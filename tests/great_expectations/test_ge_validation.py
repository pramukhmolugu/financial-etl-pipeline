"""
Great Expectations Data Validation
Enterprise-grade data quality testing
"""

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GreatExpectationsValidator:
    """
    Data validation using Great Expectations.
    Demonstrates: Enterprise data quality framework
    """
    
    def __init__(self, context_root_dir: str = "tests/great_expectations"):
        """Initialize Great Expectations context"""
        self.context = gx.get_context(context_root_dir=context_root_dir)
        logger.info("Great Expectations context initialized")
    
    def create_transaction_expectations(self):
        """
        Create expectation suite for transaction data.
        Defines all data quality rules.
        """
        logger.info("Creating transaction expectations...")
        
        # Create expectation suite
        suite_name = "transaction_suite"
        suite = self.context.add_expectation_suite(
            expectation_suite_name=suite_name
        )
        
        # Add expectations
        expectations = [
            # 1. Column existence
            {
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "kwargs": {
                    "column_list": [
                        "transaction_id", "customer_id", "transaction_date",
                        "amount", "merchant_id", "category", "status",
                        "payment_method", "risk_score", "risk_level"
                    ]
                }
            },
            
            # 2. No null values in critical columns
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "transaction_id"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "customer_id"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "amount"}
            },
            
            # 3. Unique transaction IDs
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "transaction_id"}
            },
            
            # 4. Amount validations
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "amount",
                    "min_value": 0.01,
                    "max_value": 1000000.00
                }
            },
            
            # 5. Status values
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "status",
                    "value_set": ["completed", "pending", "failed"]
                }
            },
            
            # 6. Risk level values
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "risk_level",
                    "value_set": ["low", "medium", "high", "critical"]
                }
            },
            
            # 7. Risk score range
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "risk_score",
                    "min_value": 0.0,
                    "max_value": 100.0
                }
            },
            
            # 8. Date not in future
            {
                "expectation_type": "expect_column_max_to_be_between",
                "kwargs": {
                    "column": "transaction_date",
                    "min_value": None,
                    "max_value": "now"
                }
            },
            
            # 9. Minimum row count
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 100,
                    "max_value": None
                }
            }
        ]
        
        # Add all expectations to suite
        for exp in expectations:
            suite.add_expectation(**exp)
        
        # Save suite
        self.context.add_or_update_expectation_suite(expectation_suite=suite)
        
        logger.info(f"✓ Created expectation suite with {len(expectations)} expectations")
        return suite_name
    
    def validate_transactions(self, df: pd.DataFrame, suite_name: str = "transaction_suite") -> dict:
        """
        Validate transaction data against expectations.
        
        Args:
            df: Transaction DataFrame to validate
            suite_name: Name of expectation suite to use
            
        Returns:
            Validation results dictionary
        """
        logger.info(f"Validating {len(df)} transactions...")
        
        try:
            # Create batch request
            batch_request = RuntimeBatchRequest(
                datasource_name="transactions_datasource",
                data_connector_name="default_runtime_data_connector",
                data_asset_name="transactions",
                runtime_parameters={"batch_data": df},
                batch_identifiers={"default_identifier_name": "transaction_batch"}
            )
            
            # Get validator
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name
            )
            
            # Run validation
            results = validator.validate()
            
            # Log results
            success_count = results.statistics['successful_expectations']
            total_count = results.statistics['evaluated_expectations']
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0
            
            if results.success:
                logger.info(f"✓ Validation PASSED: {success_count}/{total_count} checks passed ({success_rate:.1f}%)")
            else:
                logger.error(f"✗ Validation FAILED: {success_count}/{total_count} checks passed ({success_rate:.1f}%)")
                
                # Log failed expectations
                for result in results.results:
                    if not result.success:
                        logger.error(f"  Failed: {result.expectation_config.expectation_type}")
            
            return {
                'success': results.success,
                'statistics': results.statistics,
                'results': results.results
            }
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")
            raise
    
    def create_checkpoint(self, suite_name: str = "transaction_suite"):
        """
        Create checkpoint for automated validation.
        """
        checkpoint_config = {
            "name": "transaction_checkpoint",
            "config_version": 1.0,
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "transactions_datasource",
                        "data_connector_name": "default_runtime_data_connector",
                        "data_asset_name": "transactions"
                    },
                    "expectation_suite_name": suite_name
                }
            ]
        }
        
        self.context.add_or_update_checkpoint(**checkpoint_config)
        logger.info("✓ Created validation checkpoint")


if __name__ == "__main__":
    # Test Great Expectations validation
    import sys
    from pathlib import Path
    
    # Add src to path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))
    
    from extract import DataExtractor
    from transform import DataTransformer
    from config import RAW_DATA_DIR
    
    # Generate test data
    extractor = DataExtractor(RAW_DATA_DIR)
    transformer = DataTransformer()
    
    raw_transactions = extractor.extract_transactions(1000)
    clean_transactions = transformer.transform_transactions(raw_transactions)
    
    # Initialize validator
    validator = GreatExpectationsValidator()
    
    # Create expectations
   suite_name = validator.create_transaction_expectations()
    
    # Validate data
    results = validator.validate_transactions(clean_transactions, suite_name)
    
    print("\n" + "="*80)
    print("GREAT EXPECTATIONS VALIDATION RESULTS")
    print("="*80)
    print(f"Success: {results['success']}")
    print(f"Statist

ics: {results['statistics']}")
    
    if not results['success']:
        print("\nFailed Expectations:")
        for result in results['results']:
            if not result.success:
                print(f"  - {result.expectation_config.expectation_type}")
