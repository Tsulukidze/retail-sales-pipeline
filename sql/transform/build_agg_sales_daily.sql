-- Rebuild the daily summary per day, store and product from the fact table.
--
-- I rebuild the whole table on every run instead of updating only new days.
-- With 100,000 sales this takes about a second, and the table can never get out of sync with the fact table
-- All statements run in one transaction: if the INSERT fails, the TRUNCATE is undone and the old data stays.
-- For much bigger data, I would rebuild only the days that changed.

TRUNCATE TABLE mart.agg_sales_daily_store_product;

INSERT INTO mart.agg_sales_daily_store_product (
    date_id,
    store_id,
    product_id,
    total_revenue,
    total_gross_amount,
    total_quantity,
    transaction_count,
    unique_customer_count,
    weighted_discount_pct
)
SELECT
    sales.date_id,
    sales.store_id,
    sales.product_id,
    SUM(sales.net_amount) AS total_revenue,
    SUM(sales.gross_amount) AS total_gross_amount,
    SUM(sales.quantity) AS total_quantity,
    COUNT(*) AS transaction_count,                        -- one row in the fact table = one transaction
    COUNT(DISTINCT sales.customer_id) AS unique_customer_count,
    -- Weighted discount: each sale counts as much as its value before discount.
    -- Example: 10% off a 1,000 sale and 20% off a 100 sale gives about 10.9%,
    -- not the simple average of 15%.
    -- COALESCE gives 0 if all sales of the group have a price of 0, so there
    -- is nothing to divide by.
    COALESCE(
        SUM(sales.gross_amount * sales.discount_pct) / NULLIF(SUM(sales.gross_amount), 0),
        0
    ) AS weighted_discount_pct
FROM core.fact_sales AS sales
GROUP BY sales.date_id, sales.store_id, sales.product_id;

-- This result is shown in the Airflow log.
SELECT COUNT(*) AS aggregated_rows
FROM mart.agg_sales_daily_store_product;
