"""
ETL Pipeline Orchestrator
Coordinates Extract, Transform, Load operations
"""

import sys
from pathlib import Path
from datetime import datetime
import logging

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from extract import DataExtractor
from transform import DataTransformer, DataQualityChecker
from load import DataLoader
from config import DB_CONFIG, RAW_DATA_DIR, PROCESSED_DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Complete ETL Pipeline orchestrator.
    Demonstrates: End-to-end pipeline, error handling, monitoring
    """
    
    def __init__(self):
        self.extractor = DataExtractor(RAW_DATA_DIR)
        self.transformer = DataTransformer()
        self.quality_checker = DataQualityChecker()
        self.loader = DataLoader(DB_CONFIG)
        self.run_id = None
    
    def run(self, num_transactions: int = 10000, num_customers: int = 5000):
        """
        Execute complete ETL pipeline.
        
        Args:
            num_transactions: Number of transactions to process
            num_customers: Number of customers to process
        """
        logger.info("="*80)
        logger.info("STARTING ETL PIPELINE")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        try:
            # Start audit log
            self.run_id = self.loader.start_audit_log('financial_etl_pipeline')
            
            # EXTRACT
            logger.info("\n" + "="*80)
            logger.info("PHASE 1: EXTRACT")
            logger.info("="*80)
            raw_customers = self.extractor.extract_customers(num_customers)
            raw_transactions = self.extractor.extract_transactions(num_transactions)
            logger.info(f"✓ Extracted {len(raw_customers)} customers")
            logger.info(f"✓ Extracted {len(raw_transactions)} transactions")
            
            # TRANSFORM
            logger.info("\n" + "="*80)
            logger.info("PHASE 2: TRANSFORM")
            logger.info("="*80)
            clean_transactions = self.transformer.transform_transactions(raw_transactions)
            logger.info(f"✓ Transformed {len(clean_transactions)} transactions")
            
            # Validate against customers
            clean_transactions = self.transformer.validate_against_customers(
                clean_transactions,
                raw_customers
            )
            logger.info(f"✓ Validated against customer data")
            
            # Quality checks
            logger.info("\nRunning quality checks...")
            quality_passed = self.quality_checker.run_quality_checks(
                clean_transactions,
                "transactions"
            )
            
            if not quality_passed:
                logger.warning("⚠ Quality checks failed but continuing...")
            else:
                logger.info("✓ Quality checks passed")
            
            # LOAD
            logger.info("\n" + "="*80)
            logger.info("PHASE 3: LOAD")
            logger.info("="*80)
            customers_loaded = self.loader.load_customers(raw_customers)
            transactions_loaded = self.loader.load_transactions(clean_transactions)
            logger.info(f"✓ Loaded {customers_loaded} customers")
            logger.info(f"✓ Loaded {transactions_loaded} transactions")
            
            # Update audit log
            self.loader.update_audit_log(
                status='success',
                records_extracted=len(raw_transactions),
                records_transformed=len(clean_transactions),
                records_loaded=transactions_loaded,
                records_rejected=len(raw_transactions) - len(clean_transactions),
                quality_report=self.transformer.get_quality_report()
            )
            
            # Final summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("\n" + "="*80)
            logger.info("PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("="*80)
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Records extracted: {len(raw_transactions)}")
            logger.info(f"Records transformed: {len(clean_transactions)}")
            logger.info(f"Records loaded: {transactions_loaded}")
            logger.info(f"Records rejected: {len(raw_transactions) - len(clean_transactions)}")
            logger.info(f"Success rate: {(transactions_loaded/len(raw_transactions)*100):.2f}%")
            
            # Get final table counts
            logger.info("\nFinal database state:")
            counts = self.loader.get_table_counts()
            for table, count in counts.items():
                logger.info(f"  {table}: {count:,} records")
            
            return True
            
        except Exception as e:
            logger.error(f"\n{'='*80}")
            logger.error("PIPELINE FAILED")
            logger.error(f"{'='*80}")
            logger.error(f"Error: {str(e)}")
            
            # Update audit log with failure
            if self.run_id:
                self.loader.update_audit_log(
                    status='failed',
                    error_message=str(e)
                )
            
            raise
            
        finally:
            self.loader.close()


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Financial ETL Pipeline')
    parser.add_argument(
        '--transactions',
        type=int,
        default=10000,
        help='Number of transactions to process (default: 10000)'
    )
    parser.add_argument(
        '--customers',
        type=int,
        default=5000,
        help='Number of customers to process (default: 5000)'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = ETLPipeline()
    pipeline.run(
        num_transactions=args.transactions,
        num_customers=args.customers
    )


if __name__ == "__main__":
    main()
