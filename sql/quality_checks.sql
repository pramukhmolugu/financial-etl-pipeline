-- ================================================================
-- Data Quality Check Queries
-- Run these after ETL to validate data integrity
-- ================================================================

-- Check 1: Count records loaded today
SELECT 
    'Records loaded today' as check_name,
    COUNT(*) as record_count,
    CASE 
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM fact_transactions
WHERE DATE(created_at) = CURRENT_DATE;

-- Check 2: Verify no null amounts
SELECT 
    'No null amounts' as check_name,
    COUNT(*) as null_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM fact_transactions
WHERE amount IS NULL;

-- Check 3: Verify all amounts positive
SELECT 
    'All amounts positive' as check_name,
    COUNT(*) as negative_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM fact_transactions
WHERE amount <= 0;

-- Check 4: Verify no duplicate transaction IDs
SELECT 
    'No duplicate transaction IDs' as check_name,
    COUNT(*) - COUNT(DISTINCT transaction_id) as duplicate_count,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT transaction_id) THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM fact_transactions;

-- Check 5: Verify all customers exist in dim table
SELECT 
    'All customers exist in dimension' as check_name,
    COUNT(*) as orphan_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM fact_transactions t
LEFT JOIN dim_customers c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Check 6: Verify no future dates
SELECT 
    'No future transaction dates' as check_name,
    COUNT(*) as future_date_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM fact_transactions
WHERE transaction_date > CURRENT_TIMESTAMP;

-- Check 7: Verify valid status values
SELECT 
    'All valid status values' as check_name,
    COUNT(*) as invalid_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM fact_transactions
WHERE status NOT IN ('completed', 'pending', 'failed');

-- Check 8: Check for reasonable amounts
SELECT 
    'Amounts within reasonable range' as check_name,
    COUNT(*) as unreasonable_count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM fact_transactions
WHERE amount > 1000000 OR amount < 0.01;

-- Summary: All quality checks
WITH quality_checks AS (
    SELECT 'Records loaded today' as check_name, 
           CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END as status
    FROM fact_transactions WHERE DATE(created_at) = CURRENT_DATE
    
    UNION ALL
    
    SELECT 'No null amounts',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM fact_transactions WHERE amount IS NULL
    
    UNION ALL
    
    SELECT 'All amounts positive',
           CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM fact_transactions WHERE amount <= 0
    
    UNION ALL
    
    SELECT 'No duplicate transaction IDs',
           CASE WHEN COUNT(*) = COUNT(DISTINCT transaction_id) THEN 'PASS' ELSE 'FAIL' END
    FROM fact_transactions
)
SELECT 
    COUNT(*) as total_checks,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 2) as pass_percentage
FROM quality_checks;
