-- ================================================================
-- Financial Transaction ETL Database Schema
-- Author: Pramukh Molugu
-- Date: January 31, 2026
-- ================================================================

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS fact_transactions CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS etl_audit_log CASCADE;

-- ================================================================
-- DIMENSION TABLE: Customers
-- ================================================================
CREATE TABLE dim_customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    customer_name VARCHAR(100),
    registration_date TIMESTAMP,
    customer_tier VARCHAR(20),
    email VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster lookups
CREATE INDEX idx_customers_tier ON dim_customers(customer_tier);
CREATE INDEX idx_customers_active ON dim_customers(is_active);

-- ================================================================
-- FACT TABLE: Transactions
-- ================================================================
CREATE TABLE fact_transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    merchant_id VARCHAR(20),
    category VARCHAR(50),
    status VARCHAR(20) NOT NULL CHECK (status IN ('completed', 'pending', 'failed')),
    payment_method VARCHAR(50),
    
    -- Derived fields
    transaction_year INTEGER,
    transaction_month INTEGER,
    transaction_day INTEGER,
    transaction_dayofweek INTEGER,
    transaction_hour INTEGER,
    amount_category VARCHAR(20),
    
    -- Risk fields
    risk_score NUMERIC(5, 2) DEFAULT 0.00,
    risk_level VARCHAR(20),
    
    -- Metadata
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);

-- Create indexes for performance
CREATE INDEX idx_transactions_customer ON fact_transactions(customer_id);
CREATE INDEX idx_transactions_date ON fact_transactions(transaction_date DESC);
CREATE INDEX idx_transactions_amount ON fact_transactions(amount);
CREATE INDEX idx_transactions_status ON fact_transactions(status);
CREATE INDEX idx_transactions_risk ON fact_transactions(risk_level);
CREATE INDEX idx_transactions_category ON fact_transactions(category);

-- Composite indexes for common queries
CREATE INDEX idx_transactions_customer_date ON fact_transactions(customer_id, transaction_date DESC);
CREATE INDEX idx_transactions_date_status ON fact_transactions(transaction_date DESC, status);

-- ================================================================
-- AUDIT TABLE: ETL Execution Log
-- ================================================================
CREATE TABLE etl_audit_log (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('running', 'success', 'failed')),
    records_extracted INTEGER DEFAULT 0,
    records_transformed INTEGER DEFAULT 0,
    records_loaded INTEGER DEFAULT 0,
    records_rejected INTEGER DEFAULT 0,
    error_message TEXT,
    quality_report JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_pipeline ON etl_audit_log(pipeline_name);
CREATE INDEX idx_audit_start_time ON etl_audit_log(start_time DESC);
CREATE INDEX idx_audit_status ON etl_audit_log(status);

-- ================================================================
-- VIEWS FOR ANALYSIS
-- ================================================================

-- View: Transaction Summary by Customer
CREATE OR REPLACE VIEW vw_customer_transaction_summary AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_tier,
    COUNT(t.transaction_id) as transaction_count,
    SUM(t.amount) as total_amount,
    AVG(t.amount) as avg_amount,
    MAX(t.transaction_date) as last_transaction_date,
    SUM(CASE WHEN t.risk_level = 'critical' THEN 1 ELSE 0 END) as critical_risk_count,
    SUM(CASE WHEN t.risk_level = 'high' THEN 1 ELSE 0 END) as high_risk_count
FROM dim_customers c
LEFT JOIN fact_transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_tier;

-- View: Daily Transaction Summary
CREATE OR REPLACE VIEW vw_daily_transaction_summary AS
SELECT 
    DATE(transaction_date) as transaction_day,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_count,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_count,
    SUM(CASE WHEN risk_level IN ('high', 'critical') THEN 1 ELSE 0 END) as high_risk_count
FROM fact_transactions
GROUP BY DATE(transaction_date)
ORDER BY transaction_day DESC;

-- View: High Risk Transactions
CREATE OR REPLACE VIEW vw_high_risk_transactions AS
SELECT 
    t.*,
    c.customer_name,
    c.customer_tier
FROM fact_transactions t
JOIN dim_customers c ON t.customer_id = c.customer_id
WHERE t.risk_level IN ('high', 'critical')
ORDER BY t.risk_score DESC, t.transaction_date DESC;

COMMENT ON TABLE fact_transactions IS 'Main fact table storing all processed transactions';
COMMENT ON TABLE dim_customers IS 'Dimension table with customer master data';
COMMENT ON TABLE etl_audit_log IS 'Audit log tracking ETL pipeline execution';
