-- Add new sales from staging to the fact table.
--
-- How it works:
-- 1. Each staging row is joined to the dimensions to get its keys.
-- 2. Only rows that are not in the fact table yet (by source_row_hash) are
--    inserted. So running this again adds nothing, and no row appears twice.
-- 3. The result is the number of inserted rows. Airflow shows it in the log.
--
-- Why LEFT JOIN and not JOIN:
-- With JOIN, a row whose store (or product, ...) is missing in the dimension
-- would disappear without any error, and all totals would be too low.
-- With LEFT JOIN, the key becomes NULL, the NOT NULL rule on the fact table
-- rejects it, and the task fails with a clear error. I prefer a failed run
-- over wrong numbers that nobody notices.
--
-- Known limitation: rows are only added, never changed or deleted. If the
-- source corrected or removed a sale, the old row would stay here. The data
-- quality checks compare the fact table with staging, so this would be noticed.

WITH inserted AS (
    INSERT INTO core.fact_sales (
        source_row_hash,
        sale_ts,
        date_id,
        store_id,
        product_id,
        product_category_id,
        payment_method_id,
        customer_id,
        quantity,
        unit_price,
        discount_pct,
        gross_amount,
        net_amount,
        batch_id
    )
    SELECT
        staging_rows.source_row_hash,
        staging_rows.transaction_ts,
        TO_CHAR(staging_rows.transaction_ts, 'YYYYMMDD')::INTEGER,  -- must exist in dim_date (foreign key)
        stores.store_id,
        products.product_id,
        categories.product_category_id,
        payment_methods.payment_method_id,
        staging_rows.customer_id,
        staging_rows.quantity,
        staging_rows.unit_price,
        staging_rows.discount_pct,
        staging_rows.unit_price * staging_rows.quantity,  -- gross_amount: before discount
        staging_rows.total_amount,                         -- net_amount: after discount
        staging_rows.batch_id
    FROM staging.stg_retail_transactions AS staging_rows
    LEFT JOIN core.dim_store AS stores
        ON stores.store_code = staging_rows.store_code
    LEFT JOIN core.dim_product AS products
        ON products.product_code = staging_rows.product_code
    LEFT JOIN core.dim_product_category AS categories
        ON categories.product_category_name = staging_rows.product_category
    LEFT JOIN core.dim_payment_method AS payment_methods
        ON payment_methods.payment_method_name = staging_rows.payment_method
    -- Only new rows. This also stops Postgres from using up an ID number for
    -- every existing row on every run.
    WHERE NOT EXISTS (
        SELECT 1
        FROM core.fact_sales AS existing
        WHERE existing.source_row_hash = staging_rows.source_row_hash
    )
    -- New IDs follow the sale time, so sales_id order = time order.
    ORDER BY staging_rows.transaction_ts, staging_rows.source_row_hash
    -- If the file had two identical rows, they get the same hash, and only
    -- the first one is kept. The source has no duplicates (see README).
    ON CONFLICT (source_row_hash) DO NOTHING
    RETURNING 1
)
SELECT COUNT(*) AS inserted_rows
FROM inserted;
