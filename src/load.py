"""
Data Loading Module
Loads transformed data into PostgreSQL database
"""

import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, List, Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """
    Load transformed data into PostgreSQL database.
    Demonstrates: Database loading, transaction handling, error recovery
    """
    
    def __init__(self, db_config: Dict):
        """
        Initialize database connection.
        
        Args:
            db_config: Dictionary with database connection parameters
                      {host, port, database, user, password}
        """
        self.db_config = db_config
        self.engine = None
        self.run_id = None
        self._connect()
    
    def _connect(self):
        """Create database connection"""
        try:
            connection_string = (
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
                f"@{self.db_config['host']}:{self.db_config['port']}"
                f"/{self.db_config['database']}"
            )
            self.engine = create_engine(connection_string)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def create_schema(self, sql_file_path: str):
        """
        Execute SQL schema creation script.
        
        Args:
            sql_file_path: Path to SQL file with CREATE TABLE statements
        """
        logger.info(f"Creating database schema from {sql_file_path}")
        
        try:
            with open(sql_file_path, 'r') as f:
                sql_script = f.read()
            
            with self.engine.connect() as conn:
                # Execute the entire script
                conn.execute(text(sql_script))
                conn.commit()
            
            logger.info("Database schema created successfully")
        except Exception as e:
            logger.error(f"Failed to create schema: {str(e)}")
            raise
    
    def load_customers(self, customers_df: pd.DataFrame) -> int:
        """
        Load customer data into dim_customers table.
        
        Args:
            customers_df: DataFrame with customer data
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading {len(customers_df)} customers...")
        
        try:
            # Load data using pandas to_sql
            records_loaded = customers_df.to_sql(
                name='dim_customers',
                con=self.engine,
                if_exists='append',  # Append to existing table
                index=False,
                method='multi',  # Faster bulk insert
                chunksize=1000
            )
            
            logger.info(f"Successfully loaded {len(customers_df)} customers")
            return len(customers_df)
            
        except Exception as e:
            logger.error(f"Failed to load customers: {str(e)}")
            raise
    
    def load_transactions(self, transactions_df: pd.DataFrame) -> int:
        """
        Load transaction data into fact_transactions table.
        
        Args:
            transactions_df: DataFrame with transaction data
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading {len(transactions_df)} transactions...")
        
        try:
            # Prepare data for loading
            load_df = transactions_df.copy()
            
            # Ensure datetime columns are properly formatted
            if 'transaction_date' in load_df.columns:
                load_df['transaction_date'] = pd.to_datetime(load_df['transaction_date'])
            
            if 'processed_at' in load_df.columns:
                load_df['processed_at'] = pd.to_datetime(load_df['processed_at'])
            
            # Convert categorical columns to strings
            categorical_cols = ['amount_category', 'risk_level']
            for col in categorical_cols:
                if col in load_df.columns:
                    load_df[col] = load_df[col].astype(str)
            
            # Load data
            records_loaded = load_df.to_sql(
                name='fact_transactions',
                con=self.engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"Successfully loaded {len(transactions_df)} transactions")
            return len(transactions_df)
            
        except Exception as e:
            logger.error(f"Failed to load transactions: {str(e)}")
            raise
    
    def start_audit_log(self, pipeline_name: str) -> int:
        """
        Create audit log entry for pipeline run.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            run_id for this execution
        """
        logger.info(f"Starting audit log for {pipeline_name}")
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("""
                        INSERT INTO etl_audit_log 
                        (pipeline_name, start_time, status)
                        VALUES (:pipeline_name, :start_time, :status)
                        RETURNING run_id
                    """),
                    {
                        'pipeline_name': pipeline_name,
                        'start_time': datetime.now(),
                        'status': 'running'
                    }
                )
                conn.commit()
                self.run_id = result.fetchone()[0]
            
            logger.info(f"Created audit log entry with run_id: {self.run_id}")
            return self.run_id
            
        except Exception as e:
            logger.error(f"Failed to create audit log: {str(e)}")
            raise
    
    def update_audit_log(
        self,
        status: str,
        records_extracted: int = 0,
        records_transformed: int = 0,
        records_loaded: int = 0,
        records_rejected: int = 0,
        error_message: Optional[str] = None,
        quality_report: Optional[Dict] = None
    ):
        """
        Update audit log with pipeline execution results.
        
        Args:
            status: 'success' or 'failed'
            records_extracted: Number of records extracted
            records_transformed: Number of records transformed
            records_loaded: Number of records loaded
            records_rejected: Number of records rejected
            error_message: Error message if failed
            quality_report: Quality report dictionary
        """
        if self.run_id is None:
            logger.warning("No run_id found, skipping audit log update")
            return
        
        logger.info(f"Updating audit log (run_id: {self.run_id})")
        
        try:
            import json
            
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                        UPDATE etl_audit_log 
                        SET end_time = :end_time,
                            status = :status,
                            records_extracted = :records_extracted,
                            records_transformed = :records_transformed,
                            records_loaded = :records_loaded,
                            records_rejected = :records_rejected,
                            error_message = :error_message,
                            quality_report = :quality_report
                        WHERE run_id = :run_id
                    """),
                    {
                        'run_id': self.run_id,
                        'end_time': datetime.now(),
                        'status': status,
                        'records_extracted': records_extracted,
                        'records_transformed': records_transformed,
                        'records_loaded': records_loaded,
                        'records_rejected': records_rejected,
                        'error_message': error_message,
                        'quality_report': json.dumps(quality_report) if quality_report else None
                    }
                )
                conn.commit()
            
            logger.info("Audit log updated successfully")
            
        except Exception as e:
            logger.error(f"Failed to update audit log: {str(e)}")
            # Don't raise - audit failure shouldn't stop the pipeline
    
    def run_quality_checks(self, sql_file_path: str) -> pd.DataFrame:
        """
        Run data quality checks from SQL file.
        
        Args:
            sql_file_path: Path to SQL file with quality check queries
            
        Returns:
            DataFrame with quality check results
        """
        logger.info("Running data quality checks...")
        
        try:
            with open(sql_file_path, 'r') as f:
                sql_script = f.read()
            
            # Execute last query which has the summary
            queries = sql_script.split(';')
            summary_query = queries[-2]  # Second to last (last is empty)
            
            results = pd.read_sql_query(summary_query, self.engine)
            
            logger.info("Quality checks completed")
            logger.info(f"\n{results}")
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to run quality checks: {str(e)}")
            raise
    
    def get_table_counts(self) -> Dict[str, int]:
        """
        Get record counts for all tables.
        
        Returns:
            Dictionary with table names and counts
        """
        logger.info("Getting table counts...")
        
        try:
            with self.engine.connect() as conn:
                tables = ['dim_customers', 'fact_transactions', 'etl_audit_log']
                counts = {}
                
                for table in tables:
                    result = conn.execute(
                        text(f"SELECT COUNT(*) FROM {table}")
                    )
                    counts[table] = result.fetchone()[0]
            
            logger.info(f"Table counts: {counts}")
            return counts
            
        except Exception as e:
            logger.error(f"Failed to get table counts: {str(e)}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


if __name__ == "__main__":
    # Test the loader
    from config import DB_CONFIG, RAW_DATA_DIR
    from extract import DataExtractor
    from transform import DataTransformer
    import os
    
    # Get SQL file paths
    base_dir = os.path.dirname(os.path.dirname(__file__))
    schema_file = os.path.join(base_dir, 'sql', 'create_tables.sql')
    quality_file = os.path.join(base_dir, 'sql', 'quality_checks.sql')
    
    # Initialize loader
    loader = DataLoader(DB_CONFIG)
    
    # Create schema
    logger.info("Creating database schema...")
    loader.create_schema(schema_file)
    
    # Extract and transform sample data
    logger.info("\nExtracting and transforming data...")
    extractor = DataExtractor(RAW_DATA_DIR)
    transformer = DataTransformer()
    
    raw_customers = extractor.extract_customers(100)
    raw_transactions = extractor.extract_transactions(1000)
    
    clean_transactions = transformer.transform_transactions(raw_transactions)
    
    # Validate transactions against customers
    clean_transactions = transformer.validate_against_customers(
        clean_transactions, 
        raw_customers
    )
    
    # Start audit log
    run_id = loader.start_audit_log('test_pipeline')
    
    try:
        # Load data
        logger.info("\nLoading data to database...")
        customers_loaded = loader.load_customers(raw_customers)
        transactions_loaded = loader.load_transactions(clean_transactions)
        
        # Update audit log
        loader.update_audit_log(
            status='success',
            records_extracted=len(raw_transactions),
            records_transformed=len(clean_transactions),
            records_loaded=transactions_loaded,
            records_rejected=len(raw_transactions) - len(clean_transactions),
            quality_report=transformer.get_quality_report()
        )
        
        # Get table counts
        logger.info("\nTable counts after loading:")
        counts = loader.get_table_counts()
        for table, count in counts.items():
            print(f"{table}: {count} records")
        
        # Run quality checks
        logger.info("\nRunning quality checks...")
        quality_results = loader.run_quality_checks(quality_file)
        
    except Exception as e:
        loader.update_audit_log(
            status='failed',
            error_message=str(e)
        )
        raise
    finally:
        loader.close()
    
    print("\n" + "="*80)
    print("LOAD TEST COMPLETED SUCCESSFULLY!")
    print("="*80)
