-- Manual check of the LFL logic with synthetic data.
--
-- Why: the real data cannot show an LFL store. It starts on 29 April 2023,
-- so no store has a full month in the previous year, and every status is 0.
-- This script creates two years of made-up sales for six test stores, runs
-- the real LFL view (core.v_store_lfl) on them, and compares the result with
-- the expected answers.
--
-- Safe to run: everything happens inside one transaction that ends with
-- ROLLBACK, so no real data is changed. (One small side effect: Postgres does
-- not roll back ID counters, so the next new real store skips a few IDs.)
--
-- Before running: run the DAG once, so dim_date covers 2023 and 2024 and the
-- product, category and payment method tables have rows.
--
-- WARNING: always run the WHOLE file, never a selected part.
-- The protection comes from BEGIN at the top and ROLLBACK at the bottom.
-- In DataGrip's auto-commit mode, a single statement run on its own is saved
-- immediately: TRUNCATE would really empty core.fact_sales, and the INSERTs
-- would really add test stores and test sales.
-- If that happens: run ROLLBACK, then check core.fact_sales and core.dim_store.
-- To repair: DROP SCHEMA staging, core, mart CASCADE; then run the DAG once.
--
-- How to run:
--   DataGrip: open this file and run the whole script (Execute Script).
--   psql:     psql -h localhost -p 5433 -U <DWH_USER> -d retail_dwh -f sql/manual_checks/check_lfl_logic.sql
--
-- Expected result: every row shows "OK", and the summary says
-- "18 of 18 checks OK".

BEGIN;

-- Stop early with a clear message if the DAG has not run yet.
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM core.dim_date WHERE full_date BETWEEN '2023-01-01' AND '2024-03-31') < 456
       OR NOT EXISTS (SELECT 1 FROM core.dim_product)
       OR NOT EXISTS (SELECT 1 FROM core.dim_product_category)
       OR NOT EXISTS (SELECT 1 FROM core.dim_payment_method) THEN
        RAISE EXCEPTION 'Run the DAG once before this check (dim_date and lookup tables are needed).';
    END IF;
END $$;

-- Remove all real sales, only inside this transaction.
-- The LFL view uses the last sale date, so real sales would change the result.
TRUNCATE TABLE core.fact_sales;

-- Six test stores.
INSERT INTO core.dim_store (store_code, store_name) VALUES
    ('T1', 'Test: every day in both years'),
    ('T2', 'Test: missed 10 Feb 2023'),
    ('T3', 'Test: missed 5 Mar 2024'),
    ('T4', 'Test: new store, no sales in 2023'),
    ('T5', 'Test: no sales at all'),
    ('T6', 'Test: missed 29 Feb 2024 (leap day)');

-- Synthetic sales: one sale per store and day.
-- The last sale date is 15 March 2024, so:
--   current year = 2024, months = January, February, March,
--   March is the current month: 15 days required this year, 31 last year.
WITH open_periods (store_code, first_day, last_day) AS (
    VALUES
        ('T1', DATE '2023-01-01', DATE '2023-03-31'), ('T1', DATE '2024-01-01', DATE '2024-03-15'),
        ('T2', DATE '2023-01-01', DATE '2023-03-31'), ('T2', DATE '2024-01-01', DATE '2024-03-15'),
        ('T3', DATE '2023-01-01', DATE '2023-03-31'), ('T3', DATE '2024-01-01', DATE '2024-03-15'),
        ('T4', DATE '2024-01-01', DATE '2024-03-15'),
        ('T6', DATE '2023-01-01', DATE '2023-03-31'), ('T6', DATE '2024-01-01', DATE '2024-03-15')
),
closed_days (store_code, closed_day) AS (
    VALUES
        ('T2', DATE '2023-02-10'),
        ('T3', DATE '2024-03-05'),
        ('T6', DATE '2024-02-29')
),
sale_days AS (
    SELECT periods.store_code, calendar.day::DATE AS sale_day
    FROM open_periods AS periods
    CROSS JOIN LATERAL GENERATE_SERIES(periods.first_day, periods.last_day, INTERVAL '1 day') AS calendar (day)
    EXCEPT
    SELECT store_code, closed_day FROM closed_days
)
INSERT INTO core.fact_sales (
    source_row_hash, sale_ts, date_id, store_id, product_id, product_category_id,
    payment_method_id, customer_id, quantity, unit_price, discount_pct,
    gross_amount, net_amount, batch_id
)
SELECT
    MD5('lfl-check-' || sale_days.store_code || sale_days.sale_day),
    sale_days.sale_day + TIME '12:00',
    TO_CHAR(sale_days.sale_day, 'YYYYMMDD')::INTEGER,
    stores.store_id,
    (SELECT MIN(product_id) FROM core.dim_product),
    (SELECT MIN(product_category_id) FROM core.dim_product_category),
    (SELECT MIN(payment_method_id) FROM core.dim_payment_method),
    1, 1, 10, 0, 10, 10,
    'lfl-check'
FROM sale_days
JOIN core.dim_store AS stores ON stores.store_code = sale_days.store_code;

-- Compare the view with the expected answers.
CREATE TEMPORARY TABLE lfl_check_result ON COMMIT DROP AS
WITH expected (store_code, month, expected_is_lfl, reason) AS (
    VALUES
        ('T1', 1, TRUE,  'worked every day'),
        ('T1', 2, TRUE,  'worked every day, incl. 29 Feb 2024'),
        ('T1', 3, TRUE,  'current month: 15 of 15 days so far'),
        ('T2', 1, TRUE,  'worked every day'),
        ('T2', 2, FALSE, 'missed 10 Feb 2023 (previous year)'),
        ('T2', 3, TRUE,  'worked every day'),
        ('T3', 1, TRUE,  'worked every day'),
        ('T3', 2, TRUE,  'worked every day'),
        ('T3', 3, FALSE, 'missed 5 Mar 2024 (current month)'),
        ('T4', 1, FALSE, 'no sales in 2023'),
        ('T4', 2, FALSE, 'no sales in 2023'),
        ('T4', 3, FALSE, 'no sales in 2023'),
        ('T5', 1, FALSE, 'no sales at all'),
        ('T5', 2, FALSE, 'no sales at all'),
        ('T5', 3, FALSE, 'no sales at all'),
        ('T6', 1, TRUE,  'worked every day'),
        ('T6', 2, FALSE, 'missed the leap day: 28 of 29 days'),
        ('T6', 3, TRUE,  'worked every day')
)
SELECT
    expected.store_code,
    expected.month,
    expected.reason,
    expected.expected_is_lfl,
    lfl.is_lfl AS actual_is_lfl,
    lfl.days_worked_current || ' / ' || lfl.days_required_current AS days_current,
    lfl.days_worked_prior || ' / ' || lfl.days_required_prior AS days_prior,
    CASE WHEN lfl.is_lfl IS NOT DISTINCT FROM expected.expected_is_lfl THEN 'OK' ELSE 'WRONG' END AS check_result
FROM expected
JOIN core.dim_store AS stores ON stores.store_code = expected.store_code
LEFT JOIN core.v_store_lfl AS lfl
    ON lfl.store_id = stores.store_id
    AND lfl.year = 2024
    AND lfl.month = expected.month;

-- Result 1: one row per test case.
SELECT * FROM lfl_check_result ORDER BY store_code, month;

-- Result 2: summary.
SELECT
    COUNT(*) FILTER (WHERE check_result = 'OK') || ' of ' || COUNT(*) || ' checks OK' AS summary
FROM lfl_check_result;

ROLLBACK;
