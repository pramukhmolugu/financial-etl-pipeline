# Financial ETL Pipeline

[![CI/CD Pipeline](https://github.com/pramukhmolugu/financial-etl-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/pramukhmolugu/financial-etl-pipeline/actions)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-ready ETL (Extract, Transform, Load) pipeline for processing financial transaction data with comprehensive automated testing, data quality validation, and monitoring capabilities.

## 🎯 Project Overview

This project demonstrates enterprise-level data engineering practices including:

- ✅ **Multi-source data extraction** with error handling
- ✅ **Data transformation** with business rule validation
- ✅ **PostgreSQL data warehouse** with optimized schema
- ✅ **Automated testing** with PyTest (21+ tests)
- ✅ **Data quality validation** with Great Expectations
- ✅ **CI/CD pipeline** with GitHub Actions
- ✅ **Audit logging** for compliance and monitoring
- ✅ **Performance optimization** with proper indexing

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Extract   │ ──> │  Transform   │ ──> │    Load     │
│             │     │              │     │             │
│ • Synthetic │     │ • Cleaning   │     │ • Postgres  │
│ • CSV Files │     │ • Validation │     │ • Audit Log │
│ • Databases │     │ • Enrichment │     │ • Quality   │
└─────────────┘     └──────────────┘     └─────────────┘
       │                    │                    │
       └────────────────────┴────────────────────┘
                            │
                    ┌───────▼────────┐
                    │  Orchestration │
                    │   & Monitoring │
                    └────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Docker & Docker Compose (optional, for PostgreSQL)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/pramukhmolugu/financial-etl-pipeline.git
cd financial-etl-pipeline
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Run tests** (no Docker needed)
```bash
pytest tests/test_extract.py tests/test_transform.py -v
```

4. **(Optional) Start PostgreSQL and run complete pipeline**
```bash
docker-compose up -d
python src/pipeline.py --transactions 5000 --customers 2500
```

## 📊 Features

### Extract Module
- Synthetic data generation for testing
- Configurable record counts
- Error handling and validation
- CSV output for inspection

### Transform Module
- **Data Cleaning**: Remove duplicates, handle nulls
- **Validation**: Type checking, format standardization
- **Business Rules**: Amount limits, date validation, status checks
- **Enrichment**: Risk scoring, date components, categorization
- **Quality Reports**: Automated data quality metrics

### Load Module
- PostgreSQL data warehouse
- Fact and dimension tables
- Optimized indexes for performance
- Audit logging for compliance
- Transaction management

### Testing Framework
- **Unit Tests**: 18 tests for extract + transform
- **Integration Tests**: 3 tests for end-to-end pipeline
- **Great Expectations**: Enterprise data quality validation
- **Code Coverage**: 85%+ coverage target
- **CI/CD**: Automated testing on every commit

## 📁 Project Structure

```
financial-etl-pipeline/
├── src/
│   ├── extract.py          # Data extraction (10K+ records)
│   ├── transform.py        # Data cleaning & validation
│   ├── load.py            # PostgreSQL loading
│   ├── pipeline.py        # ETL orchestration
│   └── config.py          # Configuration
├── tests/
│   ├── test_extract.py    # 8 extraction tests
│   ├── test_transform.py  # 10 transformation tests
│   ├── test_pipeline.py   # 3 integration tests
│   └── great_expectations/ # Enterprise validation
├── sql/
│   ├── create_tables.sql  # Database schema
│   └── quality_checks.sql # Quality validation
├── .github/workflows/
│   └── ci.yml             # GitHub Actions CI/CD
└── docker-compose.yml      # PostgreSQL setup
```

## 🧪 Running Tests

```bash
# Run all unit tests
pytest tests/test_extract.py tests/test_transform.py -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Test extraction module
python src/extract.py

# Test transformation module
python src/transform.py

# Run Great Expectations validation (requires great-expectations package)
python tests/great_expectations/test_ge_validation.py
```

## 📈 Performance

-  Throughput: 10,000+ records/minute
- **Success Rate**: 97%+ after transformation
- **Quality Score**: 95%+ data quality checks passed
- **Latency**: < 15 seconds for 10K records

## 🛠️ Technologies

| Category | Technologies |
|----------|-------------|
| **Languages** | Python 3.9+, SQL |
| **Databases** | PostgreSQL 15 |
| **Testing** | PyTest, Great Expectations |
| **CI/CD** | GitHub Actions |
| **Data Processing** | Pandas, NumPy, SQLAlchemy |
| **Containerization** | Docker, Docker Compose |

## 📊 Database Schema

### **Fact Table**: `fact_transactions`
- Primary Key: `transaction_id`
- Foreign Key: `customer_id` → `dim_customers`
- Fields: amount, risk_score, risk_level, transaction_date, status, etc.
- Indexes: 8 indexes for optimized queries

### **Dimension Table**: `dim_customers`
- Primary Key: `customer_id`
- Fields: customer_name, tier, email, registration_date
- Indexes: tier, is_active

### **Audit Table**: `etl_audit_log`
- Primary Key: `run_id`
- Tracks: pipeline execution, record counts, quality reports
- Storage: Quality reports stored as JSONB

## 🔍 Data Quality Checks

The pipeline includes automated quality validation:

1. **Completeness**: No nulls in critical fields
2. **Uniqueness**: No duplicate transaction IDs
3. **Validity**: Amounts positive, dates not in future
4. **Accuracy**: Customer references exist
5. **Consistency**: Status values in valid set
6. **Timeliness**: Data within expected date ranges

## 🚀 Usage Examples

### Basic Pipeline Execution (no Docker needed)
```bash
# Extract data
python src/extract.py

# Transform data
python src/transform.py
```

### Full Pipeline (requires Docker)
```bash
# Start PostgreSQL
docker-compose up -d

# Run complete ETL pipeline
python src/pipeline.py --transactions 10000 --customers 5000
```

### Run Quality Checks
```python
from src.load import DataLoader
from src.config import DB_CONFIG

loader = DataLoader(DB_CONFIG)
results = loader.run_quality_checks('sql/quality_checks.sql')
print(results)
```

## 👤 Author

**Pramukh Molugu**  
Data Engineer | 8+ Years Experience

- 📧 Email: pramukhmolugu@gmail.com
- 🔗 [GitHub](https://github.com/pramukhmolugu)

## 🙏 Acknowledgments

- Built as a portfolio project demonstrating production-grade ETL development
- Showcases best practices in data engineering, testing, and automation
- Designed for interview demonstrations and technical discussions

---

⭐ **Star this repository if you find it helpful!**

📫 **Questions?** Feel free to open an issue or reach out directly.
