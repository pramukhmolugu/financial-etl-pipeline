# Architecture Documentation

## System Overview

The Financial ETL Pipeline is a production-grade data processing system built with modern data engineering practices including Apache Airflow orchestration, AWS serverless components, and comprehensive monitoring.

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                               │
├──────────────────────────────────────────────────────────────────┤
│  • JSON APIs  • CSV Files  • Databases  • AWS S3                │
└───────────────────────────┬──────────────────────────────────────┘
                            │
                 ┌──────────▼──────────┐
                 │   EXTRACT LAYER     │
                 ├─────────────────────┤
                 │ • Multi-source ETL  │
                 │ • Error handling    │
                 │ • Data validation   │
                 └──────────┬──────────┘
                            │
                 ┌──────────▼──────────┐
                 │  TRANSFORM LAYER    │
                 ├─────────────────────┤
                 │ • Data cleaning     │
                 │ • Validation        │
                 │ • Enrichment        │
                 │ • Quality checks    │
                 └──────────┬──────────┘
                            │
                 ┌──────────▼──────────┐
                 │    LOAD LAYER       │
                 ├─────────────────────┤
                 │ • PostgreSQL DW     │
                 │ • Audit logging     │
                 │ • Quality reports   │
                 └──────────┬──────────┘
                            │
          ┌─────────────────┴─────────────────┐
          │                                   │
┌─────────▼─────────┐            ┌───────────▼────────┐
│  ORCHESTRATION    │            │   AWS SERVERLESS   │
├───────────────────┤            ├────────────────────┤
│ • Apache Airflow  │            │ • Lambda Functions │
│ • Scheduling      │            │ • S3 Storage       │
│ • Monitoring      │            │ • Event Triggers   │
└───────────────────┘            └────────────────────┘
```

## Component Details

### 1. Extract Layer
**Purpose**: Ingest data from multiple sources

**Technologies**: 
- Python 3.9+
- Requests library
- Pandas

**Key Features**:
- Multi-source support (API, CSV, Database)
- Error handling and retries
- Data validation at source
- Configurable record counts

**Performance**:
- Throughput: 10,000+ records/minute
- Error rate: < 1%

---

### 2. Transform Layer
**Purpose**: Clean and prepare data for analytics

**Technologies**:
- Python/Pandas for transformations
- Great Expectations for validation
- Custom business rules engine

**Key Features**:
- **Data Cleaning**:
  - Duplicate removal
  - Null value handling
  - Type standardization
  
- **Validation**:
  - 9 Great Expectations rules
  - Business rule enforcement
  - Foreign key validation
  
- **Enrichment**:
  - Risk scoring (0-100)
  - Date component extraction
  - Amount categorization

**Quality Metrics**:
- Success rate: 97%+
- Quality score: 95%+

---

### 3. Load Layer
**Purpose**: Store processed data in data warehouse

**Technologies**:
- PostgreSQL 15
- SQLAlchemy ORM
- Bulk insert optimization

**Schema Design**:
```
fact_transactions (10K+ rows)
├── transaction_id (PK)
├── customer_id (FK → dim_customers)
├── amount, risk_score, risk_level
└── 15+ indexed columns

dim_customers (5K+ rows)
├── customer_id (PK)
├── customer_name, tier
└── 5 indexed columns

etl_audit_log
├── run_id (PK)
├── records_extracted/transformed/loaded
└── quality_report (JSONB)
```

**Indexes**: 15+ indexes for query optimization

---

### 4. Orchestration Layer (Apache Airflow)
**Purpose**: Schedule and monitor pipeline execution

**DAG Structure**:
```python
extract_data 
    ↓
transform_data
    ↓
load_data
    ↓
quality_checks
    ↓
send_notification
```

**Features**:
- Daily schedule (2 AM)
- 2 retry attempts with 5-minute delay
- Email notifications on failure
- XCom data passing between tasks
- Execution timeout (1 hour)

---

### 5. AWS Serverless Layer
**Purpose**: Cloud-native event-driven processing

**Components**:

**S3 Buckets**:
- `financial-etl-raw-data` - Raw transaction files
- `financial-etl-processed-data` - Processed output

**Lambda Function**:
- Triggers on S3 file upload
- Processes transaction CSVs
- 512MB memory, 5-minute timeout
- Automatic scaling

**Infrastructure as Code**:
- Terraform for AWS resources
- Version-controlled infrastructure
- Multi-environment support

---

## Data Flow

### Standard Batch Flow
1. **Extraction** (daily 2 AM): Data pulled from sources
2. **Transformation** (15-30 seconds): Cleaned and validated
3. **Loading** (10-20 seconds): Written to PostgreSQL
4. **Quality Checks** (5-10 seconds): Automated validation
5. **Monitoring**: Metrics logged for analysis

### Event-Driven Flow (AWS)
1. CSV file uploaded to S3 raw bucket
2. Lambda triggered automatically
3. File processed (duplicates removed, nulls handled)
4. Output saved to S3 processed bucket
5. CloudWatch logs capture execution

---

## Performance Characteristics

| Metric | Value |
|--------|-------|
| **Throughput** | 10,000+ records/minute |
| **End-to-end Latency** | < 60 seconds |
| **Success Rate** | 97%+ |
| **Availability** | 99.9% target |
| **Data Quality Score** | 95%+ |

---

## Security

### Data Protection
- Database credentials in environment variables
- No hardcoded secrets
- Audit logs for compliance

### AWS Security
- IAM roles with least privilege
- S3 bucket versioning enabled
- CloudWatch logging for all Lambda invocations

### Network Security
- PostgreSQL in private subnet (when on AWS)
- VPC security groups
- Encrypted data in transit and at rest

---

## Scalability

### Horizontal Scaling
- Add more Airflow workers
- Increase Lambda concurrency
- Add read replicas for PostgreSQL

### Vertical Scaling
- Increase database instance size
- Increase Lambda memory allocation
- Optimize batch sizes

### Cloud Elasticity
- Auto-scaling with AWS
- Serverless Lambda handles load spikes
- S3 unlimited storage

---

## Monitoring & Alerting

### Metrics Tracked
- Records extracted/transformed/loaded
- Processing duration
- Error rates and types
- Data quality scores
- Infrastructure costs

### Alerting Channels
- Email notifications (Airflow)
- CloudWatch alarms (AWS)
- Audit log entries (PostgreSQL)

### Dashboards
- Airflow UI for DAG monitoring
- AWS CloudWatch for Lambda metrics
- PostgreSQL audit log queries

---

## Technology Stack Summary

| Layer | Technologies |
|-------|-------------|
| **Data Processing** | Python 3.9, Pandas, NumPy |
| **Database** | PostgreSQL 15, SQLAlchemy |
| **Orchestration** | Apache Airflow 2.x |
| **Cloud** | AWS Lambda, S3 |
| **IaC** | Terraform |
| **Testing** | PyTest, Great Expectations |
| **CI/CD** | GitHub Actions |
| **Containerization** | Docker, Docker Compose |

---

## Future Enhancements

- **Real-time Processing**: Kafka + Spark Streaming
- **ML Integration**: Fraud detection models
- **Advanced Analytics**: dbt for transformations
- **Multi-cloud**: Azure/GCP deployment options
- **Kubernetes**: Container orchestration for scaling
