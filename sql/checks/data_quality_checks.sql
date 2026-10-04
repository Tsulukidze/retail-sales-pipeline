-- Data quality checks, run as the last task of the pipeline.
--
-- Each check returns one row: check_name, passed (true/false), details.
-- The DAG task prints all results and fails if any check did not pass.
--
-- What is NOT checked here: things the database already enforces.
-- Foreign keys, NOT NULL and unique keys already guarantee that every sale
-- has a store, product, date, ... and that no ID appears twice. Repeating
-- those checks would add nothing. These checks look at what constraints
-- cannot see: totals across layers, business rules, and whether the LFL
-- and ABC results are complete and up to date.
--
-- If a value is NULL (for example, an empty table), "passed" is NULL, and
-- the runner treats NULL as "failed".

WITH
staging AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT source_row_hash) AS distinct_rows,
        SUM(total_amount) AS revenue
    FROM staging.stg_retail_transactions
),
fact AS (
    SELECT
        COUNT(*) AS row_count,
        SUM(net_amount) AS revenue,
        SUM(quantity) AS quantity
    FROM core.fact_sales
),
mart AS (
    SELECT
        COUNT(*) AS row_count,
        SUM(transaction_count) AS transactions,
        SUM(total_quantity) AS quantity,
        SUM(total_revenue) AS revenue
    FROM mart.agg_sales_daily_store_product
),
staging_rows_missing_in_fact AS (
    SELECT COUNT(*) AS row_count
    FROM staging.stg_retail_transactions AS staging_rows
    WHERE NOT EXISTS (
        SELECT 1 FROM core.fact_sales AS sales
        WHERE sales.source_row_hash = staging_rows.source_row_hash
    )
),
fact_rows_not_in_staging AS (
    -- Sales that are in the fact table but not in the current source file:
    -- the source deleted or changed them. The fact load only adds rows, so
    -- this check is how such a change gets noticed.
    SELECT COUNT(*) AS row_count
    FROM core.fact_sales AS sales
    WHERE NOT EXISTS (
        SELECT 1 FROM staging.stg_retail_transactions AS staging_rows
        WHERE staging_rows.source_row_hash = sales.source_row_hash
    )
),
invalid_amounts AS (
    -- Business rules for every sale: amounts are not negative, the discount
    -- does not raise the price, and net = gross * (1 - discount / 100).
    -- 0.01 allows for rounding.
    SELECT COUNT(*) AS row_count
    FROM core.fact_sales
    WHERE net_amount < 0
        OR net_amount > gross_amount
        OR ABS(net_amount - gross_amount * (1 - discount_pct / 100)) > 0.01
),
lfl AS (
    SELECT
        COUNT(*) AS row_count,
        MIN(year) AS min_year,
        MAX(year) AS max_year,
        COUNT(*) FILTER (
            WHERE is_lfl IS DISTINCT FROM (
                days_worked_current = days_required_current
                AND days_worked_prior = days_required_prior
            )
            OR days_worked_current > days_required_current
            OR days_worked_prior > days_required_prior
        ) AS inconsistent_rows
    FROM core.dim_store_lfl
),
expected_lfl AS (
    -- Every store, for every month from January to the month of the last sale.
    SELECT
        (SELECT COUNT(*) FROM core.dim_store)
            * EXTRACT(MONTH FROM MAX(sale_ts))::INTEGER AS row_count,
        EXTRACT(YEAR FROM MAX(sale_ts))::INTEGER AS current_year
    FROM core.fact_sales
),
abc AS (
    SELECT
        COUNT(*) FILTER (
            WHERE products.abc_quantity IS NULL OR products.abc_amount IS NULL
        ) AS missing,
        COUNT(*) FILTER (
            WHERE products.abc_quantity IS DISTINCT FROM abc_view.abc_quantity
                OR products.abc_amount IS DISTINCT FROM abc_view.abc_amount
        ) AS out_of_date
    FROM core.dim_product AS products
    JOIN mart.v_product_abc AS abc_view USING (product_id)
)

-- 1. Staging
SELECT
    'staging_has_rows' AS check_name,
    staging.row_count > 0 AS passed,
    format('%s rows in staging', staging.row_count) AS details
FROM staging

UNION ALL
SELECT
    'staging_has_no_duplicate_rows',
    staging.row_count = staging.distinct_rows,
    format('%s duplicate rows (identical rows are loaded into the fact table once)',
        staging.row_count - staging.distinct_rows)
FROM staging

-- 2. Fact table vs staging
UNION ALL
SELECT
    'every_staging_row_is_in_fact',
    missing.row_count = 0,
    format('%s staging rows are missing in the fact table', missing.row_count)
FROM staging_rows_missing_in_fact AS missing

UNION ALL
SELECT
    'every_fact_row_is_in_staging',
    extra.row_count = 0,
    format('%s fact rows are not in the current source file (deleted or changed in the source)',
        extra.row_count)
FROM fact_rows_not_in_staging AS extra

UNION ALL
SELECT
    'fact_revenue_equals_staging',
    fact.revenue IS NOT DISTINCT FROM staging.revenue,
    format('fact %s, staging %s', fact.revenue, staging.revenue)
FROM fact, staging

UNION ALL
SELECT
    'fact_amounts_follow_business_rules',
    invalid.row_count = 0,
    format('%s sales break an amount rule', invalid.row_count)
FROM invalid_amounts AS invalid

-- 3. Aggregation table vs fact table
UNION ALL
SELECT
    'mart_transactions_equal_fact_rows',
    mart.transactions IS NOT DISTINCT FROM fact.row_count,
    format('mart %s, fact %s', mart.transactions, fact.row_count)
FROM mart, fact

UNION ALL
SELECT
    'mart_quantity_equals_fact',
    mart.quantity IS NOT DISTINCT FROM fact.quantity,
    format('mart %s, fact %s', mart.quantity, fact.quantity)
FROM mart, fact

UNION ALL
SELECT
    'mart_revenue_equals_fact_within_rounding',
    -- The mart rounds each row to 2 decimals, so each row can be off by at
    -- most 0.005. That is the largest allowed difference in total.
    ABS(mart.revenue - fact.revenue) <= 0.005 * mart.row_count,
    format('difference %s, allowed up to %s',
        ROUND(ABS(mart.revenue - fact.revenue), 4), 0.005 * mart.row_count)
FROM mart, fact

-- 4. LFL
UNION ALL
SELECT
    'lfl_covers_every_store_and_month',
    lfl.row_count = expected_lfl.row_count
        AND lfl.min_year = expected_lfl.current_year
        AND lfl.max_year = expected_lfl.current_year,
    format('%s rows for year %s, expected %s rows for year %s',
        lfl.row_count, lfl.max_year, expected_lfl.row_count, expected_lfl.current_year)
FROM lfl, expected_lfl

UNION ALL
SELECT
    'lfl_status_matches_day_counts',
    lfl.inconsistent_rows = 0,
    format('%s rows where the status does not match the day counts', lfl.inconsistent_rows)
FROM lfl

-- 5. ABC
UNION ALL
SELECT
    'abc_classes_complete_and_current',
    abc.missing = 0 AND abc.out_of_date = 0,
    format('%s products without a class, %s products with an old class',
        abc.missing, abc.out_of_date)
FROM abc;
