-- Manual check of the ABC logic with synthetic data.
--
-- Why: the real data has only 4 products with almost equal sales, so it
-- cannot show the edge cases. This script creates six test products with
-- chosen totals, runs the real ABC view (mart.v_product_abc) on them, and
-- compares the result with the expected answers. It tests:
--   * a product at exactly 50% cumulative  -> must be A (rule: <= 50)
--   * a product at exactly 70% cumulative  -> must be B (rule: <= 70)
--   * products with equal totals           -> each gets its own cumulative %
--   * a product without sales              -> C
-- The totals add up to 100, so the expected percentages are easy to check.
--
-- Safe to run: everything happens inside one transaction that ends with
-- ROLLBACK, so no real data is changed. (One small side effect: Postgres does
-- not roll back ID counters, so the next new real product skips a few IDs.)
--
-- WARNING: always run the WHOLE file, never a selected part.
-- The protection comes from BEGIN at the top and ROLLBACK at the bottom.
-- In DataGrip's auto-commit mode, a single statement run on its own is saved
-- immediately: TRUNCATE would really empty the aggregation table, and the
-- INSERTs would really add test products.
-- If that happens: run ROLLBACK, then check mart.agg_sales_daily_store_product
-- and core.dim_product.
-- To repair: DROP SCHEMA staging, core, mart CASCADE; then run the DAG once.
--
-- Before running: run the DAG once, so dim_date and dim_store have rows.
--
-- How to run:
--   DataGrip: open this file and run the whole script (Execute Script).
--   psql:     psql -h localhost -p 5433 -U <DWH_USER> -d retail_dwh -f sql/manual_checks/check_abc_logic.sql
--
-- Expected result: every row shows "OK", and the summary says
-- "12 of 12 checks OK".

BEGIN;

-- Stop early with a clear message if the DAG has not run yet.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM core.dim_date) OR NOT EXISTS (SELECT 1 FROM core.dim_store) THEN
        RAISE EXCEPTION 'Run the DAG once before this check (dim_date and dim_store are needed).';
    END IF;
END $$;

-- Remove all real daily sales, only inside this transaction.
-- The real products stay in dim_product, but without sales they get 0 and
-- do not change the totals.
TRUNCATE TABLE mart.agg_sales_daily_store_product;

-- Six test products.
INSERT INTO core.dim_product (product_code, product_name) VALUES
    ('X1', 'Test product 1'),
    ('X2', 'Test product 2'),
    ('X3', 'Test product 3'),
    ('X4', 'Test product 4'),
    ('X5', 'Test product 5'),
    ('X6', 'Test product 6 (no sales)');

-- One row of sales per test product. Totals: amount 100, quantity 100.
--   amount:   X1 50, X2 20, X3 10, X4 10, X5 10
--   quantity: X5 50, X4 20, X1 10, X2 10, X3 10
WITH test_sales (product_code, amount, quantity) AS (
    VALUES
        ('X1', 50, 10),
        ('X2', 20, 10),
        ('X3', 10, 10),
        ('X4', 10, 20),
        ('X5', 10, 50)
)
INSERT INTO mart.agg_sales_daily_store_product (
    date_id, store_id, product_id, total_revenue, total_gross_amount,
    total_quantity, transaction_count, unique_customer_count, weighted_discount_pct
)
SELECT
    (SELECT MIN(date_id) FROM core.dim_date),
    (SELECT MIN(store_id) FROM core.dim_store),
    products.product_id,
    test_sales.amount,
    test_sales.amount,
    test_sales.quantity,
    1,
    1,
    0
FROM test_sales
JOIN core.dim_product AS products ON products.product_code = test_sales.product_code;

-- Compare the view with the expected answers.
-- Products with the same total are ordered by product_code.
CREATE TEMPORARY TABLE abc_check_result ON COMMIT DROP AS
WITH expected (product_code, measure, expected_cumulative_pct, expected_class, reason) AS (
    VALUES
        ('X1', 'amount',   50,  'A', 'exactly 50% -> A'),
        ('X2', 'amount',   70,  'B', 'exactly 70% -> B'),
        ('X3', 'amount',   80,  'C', 'equal total with X4, X5 -> own cumulative %'),
        ('X4', 'amount',   90,  'C', 'equal total with X3, X5 -> own cumulative %'),
        ('X5', 'amount',   100, 'C', 'equal total with X3, X4 -> own cumulative %'),
        ('X6', 'amount',   100, 'C', 'no sales -> C'),
        ('X5', 'quantity', 50,  'A', 'exactly 50% -> A'),
        ('X4', 'quantity', 70,  'B', 'exactly 70% -> B'),
        ('X1', 'quantity', 80,  'C', 'equal total with X2, X3 -> own cumulative %'),
        ('X2', 'quantity', 90,  'C', 'equal total with X1, X3 -> own cumulative %'),
        ('X3', 'quantity', 100, 'C', 'equal total with X1, X2 -> own cumulative %'),
        ('X6', 'quantity', 100, 'C', 'no sales -> C')
),
actual AS (
    SELECT product_code, 'amount' AS measure, amount_cumulative_pct AS cumulative_pct, abc_amount AS abc_class
    FROM mart.v_product_abc
    UNION ALL
    SELECT product_code, 'quantity', quantity_cumulative_pct, abc_quantity
    FROM mart.v_product_abc
)
SELECT
    expected.product_code,
    expected.measure,
    expected.reason,
    expected.expected_cumulative_pct,
    actual.cumulative_pct AS actual_cumulative_pct,
    expected.expected_class,
    actual.abc_class AS actual_class,
    CASE
        WHEN actual.cumulative_pct = expected.expected_cumulative_pct
            AND actual.abc_class = expected.expected_class THEN 'OK'
        ELSE 'WRONG'
    END AS check_result
FROM expected
LEFT JOIN actual
    ON actual.product_code = expected.product_code
    AND actual.measure = expected.measure;

-- Result 1: one row per test case.
SELECT * FROM abc_check_result ORDER BY measure, expected_cumulative_pct, product_code;

-- Result 2: summary.
SELECT
    COUNT(*) FILTER (WHERE check_result = 'OK') || ' of ' || COUNT(*) || ' checks OK' AS summary
FROM abc_check_result;

ROLLBACK;
