# AWS Integration

This directory contains AWS resources for the ETL pipeline.

## Components

- **Lambda Function**: Serverless transaction processing
- **S3 Buckets**: Raw and processed data storage
- **Terraform**: Infrastructure as Code

## Setup

### Prerequisites
- AWS CLI configured
- Terraform installed
- AWS account with appropriate permissions

### Deploy Infrastructure

1. **Navigate to terraform directory**
```bash
cd aws/terraform
```

2. **Initialize Terraform**
```bash
terraform init
```

3. **Plan deployment**
```bash
terraform plan
```

4. **Apply infrastructure**
```bash
terraform apply
```

### Deploy Lambda Function

1. **Package Lambda function**
```bash
cd aws/lambda
pip install pandas -t .
zip -r transaction_processor.zip .
```

2. **Upload to AWS** (handled by Terraform)

## Usage

Upload CSV files to S3:
```bash
aws s3 cp transactions.csv s3://financial-etl-raw-data-dev/raw/
```

Lambda will automatically process and save to:
```
s3://financial-etl-processed-data-dev/processed/transactions.csv
```

## Cost Estimation

- **S3 Storage**: ~$0.023/GB/month
- **Lambda**: First 1M requests free, then $0.20/1M
- **Data Transfer**: First 100 GB/month free

Estimated monthly cost: **$5-20** for dev environment

## Architecture

```
┌─────────────┐
│   S3 Raw    │
│   Bucket    │
└──────┬──────┘
       │ (CSV upload triggers)
       ▼
┌─────────────┐
│   Lambda    │
│  Processor  │
└──────┬──────┘
       │ (processed data)
       ▼
┌─────────────┐
│ S3 Processed│
│   Bucket    │
└─────────────┘
```

## Cleanup

To destroy all AWS resources:
```bash
cd aws/terraform
terraform destroy
```
