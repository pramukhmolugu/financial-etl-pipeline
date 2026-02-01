"""
Apache Airflow DAG for Financial ETL Pipeline
Production-grade orchestration with monitoring and alerting
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# Import pipeline components
import sys
from pathlib import Path

# Add src to path
dag_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(dag_dir / 'src'))

from extract import DataExtractor
from transform import DataTransformer, DataQualityChecker
from load import DataLoader
from config import DB_CONFIG, RAW_DATA_DIR

logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'pramukh',
    'depends_on_past': False,
    'email': ['pramukhmolugu@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# DAG definition
dag = DAG(
    'financial_etl_pipeline',
    default_args=default_args,
    description='Production ETL pipeline for financial transactions',
    schedule_interval='0 2 * * *',  # Run daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'financial', 'production'],
)


def extract_data(**context):
    """Extract data from sources"""
    logger.info("Starting data extraction...")
    
    try:
        extractor = DataExtractor(RAW_DATA_DIR)
        
        # Extract customers
        customers_df = extractor.extract_customers(num_records=5000)
        logger.info(f"Extracted {len(customers_df)} customers")
        
        # Extract transactions
        transactions_df = extractor.extract_transactions(num_records=10000)
        logger.info(f"Extracted {len(transactions_df)} transactions")
        
        # Push to XCom for next task
        context['task_instance'].xcom_push(key='customers_count', value=len(customers_df))
        context['task_instance'].xcom_push(key='transactions_count', value=len(transactions_df))
        context['task_instance'].xcom_push(key='customers_data', value=customers_df.to_json())
        context['task_instance'].xcom_push(key='transactions_data', value=transactions_df.to_json())
        
        logger.info("✓ Extraction completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise


def transform_data(**context):
    """Transform and validate data"""
    logger.info("Starting data transformation...")
    
    try:
        # Pull data from XCom
        import pandas as pd
        transactions_json = context['task_instance'].xcom_pull(
            task_ids='extract_data',
            key='transactions_data'
        )
        customers_json = context['task_instance'].xcom_pull(
            task_ids='extract_data',
            key='customers_data'
        )
        
        transactions_df = pd.read_json(transactions_json)
        customers_df = pd.read_json(customers_json)
        
        # Transform
        transformer = DataTransformer()
        clean_transactions = transformer.transform_transactions(transactions_df)
        
        # Validate against customers
        clean_transactions = transformer.validate_against_customers(
            clean_transactions,
            customers_df
        )
        
        # Quality checks
        quality_checker = DataQualityChecker()
        quality_passed = quality_checker.run_quality_checks(
            clean_transactions,
            "transactions"
        )
        
        if not quality_passed:
            logger.warning("Quality checks failed")
        
        # Push to XCom
        context['task_instance'].xcom_push(
            key='clean_transactions',
            value=clean_transactions.to_json()
        )
        context['task_instance'].xcom_push(
            key='clean_customers',
            value=customers_df.to_json()
        )
        context['task_instance'].xcom_push(
            key='transformed_count',
            value=len(clean_transactions)
        )
        context['task_instance'].xcom_push(
            key='quality_report',
            value=transformer.get_quality_report()
        )
        
        logger.info(f"✓ Transformation completed: {len(clean_transactions)} records")
        return True
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        raise


def load_data(**context):
    """Load data to PostgreSQL"""
    logger.info("Starting data loading...")
    
    try:
        # Pull data from XCom
        import pandas as pd
        clean_transactions_json = context['task_instance'].xcom_pull(
            task_ids='transform_data',
            key='clean_transactions'
        )
        clean_customers_json = context['task_instance'].xcom_pull(
            task_ids='transform_data',
            key='clean_customers'
        )
        
        clean_transactions = pd.read_json(clean_transactions_json)
        clean_customers = pd.read_json(clean_customers_json)
        
        # Load to database
        loader = DataLoader(DB_CONFIG)
        
        # Start audit log
        run_id = loader.start_audit_log('airflow_etl_pipeline')
        
        # Load data
        customers_loaded = loader.load_customers(clean_customers)
        transactions_loaded = loader.load_transactions(clean_transactions)
        
        # Get metrics
        extracted_count = context['task_instance'].xcom_pull(
            task_ids='extract_data',
            key='transactions_count'
        )
        transformed_count = context['task_instance'].xcom_pull(
            task_ids='transform_data',
            key='transformed_count'
        )
        quality_report = context['task_instance'].xcom_pull(
            task_ids='transform_data',
            key='quality_report'
        )
        
        # Update audit log
        loader.update_audit_log(
            status='success',
            records_extracted=extracted_count,
            records_transformed=transformed_count,
            records_loaded=transactions_loaded,
            records_rejected=extracted_count - transformed_count,
            quality_report=quality_report
        )
        
        loader.close()
        
        logger.info(f"✓ Loading completed: {transactions_loaded} transactions loaded")
        return True
        
    except Exception as e:
        logger.error(f"Loading failed: {str(e)}")
        raise


def run_quality_checks(**context):
    """Run post-load quality checks"""
    logger.info("Running post-load quality checks...")
    
    try:
        loader = DataLoader(DB_CONFIG)
        
        # Get table counts
        counts = loader.get_table_counts()
        logger.info(f"Table counts: {counts}")
        
        # Run SQL quality checks
        import os
        sql_file = os.path.join(
            Path(__file__).parent.parent.parent,
            'sql',
            'quality_checks.sql'
        )
        results = loader.run_quality_checks(sql_file)
        
        loader.close()
        
        logger.info("✓ Quality checks completed")
        return True
        
    except Exception as e:
        logger.error(f"Quality checks failed: {str(e)}")
        raise


def send_success_notification(**context):
    """Send success notification"""
    logger.info("Sending success notification...")
    
    # Get metrics from XCom
    extracted = context['task_instance'].xcom_pull(
        task_ids='extract_data',
        key='transactions_count'
    )
    transformed = context['task_instance'].xcom_pull(
        task_ids='transform_data',
        key='transformed_count'
    )
    
    success_rate = (transformed / extracted * 100) if extracted > 0 else 0
    
    message = f"""
    ✓ ETL Pipeline Completed Successfully
    
    Execution Date: {context['execution_date']}
    
    Metrics:
    - Records Extracted: {extracted:,}
    - Records Transformed: {transformed:,}
    - Records Rejected: {extracted - transformed:,}
    - Success Rate: {success_rate:.2f}%
    
    Pipeline Duration: {context['dag_run'].duration if context['dag_run'].duration else 'N/A'}
    """
    
    logger.info(message)
    # Here you could send to Slack, email, etc.
    
    return True


# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id='quality_checks',
    python_callable=run_quality_checks,
    dag=dag,
)

notification_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task >> quality_check_task >> notification_task
