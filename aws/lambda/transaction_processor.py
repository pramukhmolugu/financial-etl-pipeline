"""
AWS Lambda Function for Transaction Processing
Serverless data processing
"""

import json
import boto3
import pandas as pd
from io import StringIO
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    """
    Lambda function to process transaction files from S3.
    
    Triggered when new file uploaded to S3 bucket.
    """
    logger.info("Lambda function started")
    
    try:
        # Get bucket and file info from event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"Processing file: s3://{bucket}/{key}")
        
        # Download file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Load data
        df = pd.read_csv(StringIO(file_content))
        logger.info(f"Loaded {len(df)} records")
        
        # Basic validation
        if len(df) == 0:
            logger.warning("Empty file received")
            return {
                'statusCode': 400,
                'body': json.dumps('Empty file')
            }
        
        # Process data
        processed_df = process_transactions(df)
        
        # Save processed data back to S3
        output_key = key.replace('raw/', 'processed/')
        csv_buffer = StringIO()
        processed_df.to_csv(csv_buffer, index=False)
        
        s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=csv_buffer.getvalue()
        )
        
        logger.info(f"Processed file saved: s3://{bucket}/{output_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processing successful',
                'records_processed': len(processed_df),
                'output_file': output_key
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


def process_transactions(df):
    """
    Process transaction data.
    Apply basic transformations.
    """
    # Remove duplicates
    df = df.drop_duplicates(subset=['transaction_id'])
    
    # Handle nulls
    df = df[df['amount'].notna()]
    
    # Filter valid amounts
    df = df[df['amount'] > 0]
    
    # Add processing timestamp
    df['processed_at'] = pd.Timestamp.now()
    
    return df
