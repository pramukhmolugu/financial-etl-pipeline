# Deployment Guide

Complete deployment instructions for the Financial ETL Pipeline.

## Quick Start (Local Development)

### Prerequisites
- Python 3.9+
- Docker Desktop
- Git

### 1. Clone Repository
```bash
git clone https://github.com/pramukhmolugu/financial-etl-pipeline.git
cd financial-etl-pipeline
```

### 2. Set Up Python Environment
```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Start PostgreSQL
```bash
docker-compose up -d

# Wait 10 seconds for database to initialize
```

### 4. Run Pipeline
```bash
python src/pipeline.py --transactions 5000 --customers 2500
```

### 5. Run Tests
```bash
# All tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=src --cov-report=html
```

---

## Apache Airflow Deployment

### Local Airflow Setup

1. **Install Airflow**
```bash
pip install apache-airflow==2.7.0
```

2. **Initialize Airflow Database**
```bash
airflow db init
```

3. **Create Admin User**
```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

4. **Set DAGs Folder**
```bash
# Edit airflow.cfg
dags_folder = /path/to/financial-etl-pipeline/airflow/dags
```

5. **Start Airflow**
```bash
# Terminal 1: Start webserver
airflow webserver --port 8080

# Terminal 2: Start scheduler
airflow scheduler
```

6. **Access UI**
- Open http://localhost:8080
- Login with admin/admin
- Enable `financial_etl_pipeline` DAG

### Production Airflow (Docker)

1. **Create docker-compose-airflow.yml**
```yaml
version: '3'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
  
  airflow:
    image: apache/airflow:2.7.0-python3.9
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
    ports:
      - "8080:8080"
    command: bash -c "airflow db init && airflow webserver"
```

2. **Deploy**
```bash
docker-compose -f docker-compose-airflow.yml up -d
```

---

## AWS Deployment

### Prerequisites
- AWS CLI configured
- Terraform installed
- AWS account with appropriate permissions

### 1. Configure AWS Credentials
```bash
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key
# Default region: us-east-1
# Default output format: json
```

### 2. Package Lambda Function
```bash
cd aws/lambda
pip install pandas -t .
zip -r transaction_processor.zip .
```

### 3. Deploy Infrastructure with Terraform
```bash
cd aws/terraform

# Initialize
terraform init

# Plan (review changes)
terraform plan

# Apply (deploy)
terraform apply

# Type 'yes' to confirm
```

### 4. Verify Deployment
```bash
# List S3 buckets
aws s3 ls | grep financial-etl

# Check Lambda function
aws lambda get-function --function-name financial-etl-transaction-processor
```

### 5. Test Lambda Function
```bash
# Upload test file
aws s3 cp test_data.csv s3://financial-etl-raw-data-dev/raw/

# Check logs
aws logs tail /aws/lambda/financial-etl-transaction-processor --follow

# Verify output
aws s3 ls s3://financial-etl-processed-data-dev/processed/
```

###6. Cleanup AWS Resources
```bash
cd aws/terraform
terraform destroy
```

---

## Production Deployment Checklist

### Pre-Deployment
- [ ] All tests passing (`pytest tests/ -v`)
- [ ] Code coverage > 85%
- [ ] Security scan completed
- [ ] Dependencies up to date
- [ ] Environment variables configured
- [ ] Database backups enabled

### Database
- [ ] PostgreSQL deployed (RDS for production)
- [ ] Connection pooling configured
- [ ] Indexes created
- [ ] Monitoring enabled
- [ ] Backup strategy defined

### Airflow
- [ ] DAGs tested locally
- [ ] Executor configured (Celery for production)
- [ ] Workers scaled appropriately
- [ ] Email alerts configured
- [ ] Logs centralized

### AWS
- [ ] Terraform state stored in S3
- [ ] IAM roles follow least privilege
- [ ] S3 versioning enabled
- [ ] CloudWatch alarms set
- [ ] Cost monitoring enabled

### Monitoring
- [ ] Application logging configured
- [ ] Metrics collection enabled
- [ ] Alerts configured
- [ ] Dashboard created
- [ ] On-call rotation defined

---

## Environment Variables

Create `.env` file:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
DB_USER=etl_user
DB_PASSWORD=your_password

# AWS (for Lambda)
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-1

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost/airflow
```

---

## Troubleshooting

### Pipeline Fails to Start
```bash
# Check Python version
python --version  # Should be 3.9+

# Check database connection
docker ps  # Verify postgres container running
docker logs <container_id>

# Check dependencies
pip install -r requirements.txt
```

### Airflow DAG Not Showing
```bash
# Refresh DAGs
airflow dags list

# Check for syntax errors
python airflow/dags/financial_etl_dag.py

# Check DAGs folder
airflow config get-value core dags_folder
```

### AWS Lambda Timeout
```bash
# Increase timeout in lambda.tf
timeout = 600  # 10 minutes

# Increase memory
memory_size = 1024  # 1GB

# Redeploy
terraform apply
```

### Database Connection Issues
```bash
# Test connection
psql -h localhost -U etl_user -d etl_db

# Check Docker network
docker network ls
docker network inspect financial-etl-pipeline_default
```

---

## Performance Tuning

### Database
- Add indexes for frequently queried columns
- Increase `shared_buffers` in PostgreSQL
- Use connection pooling (SQLAlchemy)
- Partition large tables

### Airflow
- Increase parallelism in airflow.cfg
- Use Celery executor for distributed processing
- Optimize DAG schedule intervals
- Clean old DAG runs regularly

### AWS Lambda
- Increase memory allocation
- Use Lambda layers for dependencies
- Implement caching where possible
- Optimize package size

---

## Security Best Practices

1. **Never commit secrets to Git**
2. **Use environment variables for credentials**
3. **Enable MFA for AWS accounts**
4. **Regularly rotate access keys**
5. **Use IAM roles instead of access keys where possible**
6. **Enable database encryption**
7. **Use VPC for network isolation**
8. **Implement least privilege access**

---

## Support

For issues or questions:
- Check logs: `docker logs <container_name>`
- Review documentation: `docs/ARCHITECTURE.md`
- GitHub Issues: https://github.com/pramukhmolugu/financial-etl-pipeline/issues
