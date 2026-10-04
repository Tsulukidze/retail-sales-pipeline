-- ABC analysis of products, by quantity and by amount (separately).
--
-- The rule lives only here. The pipeline copies the classes into
-- core.dim_product, and the manual check (sql/manual_checks/) uses the same
-- view. The view also shows the shares and cumulative percentages, so anyone
-- can see why a product got its class.
--
-- The rule from the task, done once for quantity and once for amount:
--   1. Total per product, from the daily aggregation table.
--   2. Sort products from the biggest total to the smallest.
--   3. Share % = product total / total of all products * 100.
--   4. Cumulative % = share of this product + shares of all products above it.
--   5. Class: A if cumulative % <= 50, B if <= 70, C above 70.
--
-- Details:
--   * Totals are converted to NUMERIC before dividing. Quantity is an integer,
--     and integer division in SQL would turn 25 / 100 into 0.
--   * If two products have the same total, product_code decides the order.
--     So the result is always the same, and each product gets its own
--     cumulative %. Without this tie-breaker, the window function would treat
--     products with the same total as one group and give all of them the
--     same (highest) cumulative %. "ROWS" makes the "one by one" running
--     total explicit; with the tie-breaker it gives the same result as the
--     default, but the intention is clearer to the reader.
--   * Products without sales get 0, so they end up at the bottom as C.
--   * If there are no sales at all, there is nothing to divide by, and the
--     classes are NULL.
CREATE OR REPLACE VIEW mart.v_product_abc AS
WITH product_totals AS (
    SELECT
        products.product_id,
        products.product_code,
        COALESCE(SUM(daily.total_quantity), 0)::NUMERIC AS total_quantity,
        COALESCE(SUM(daily.total_revenue), 0)::NUMERIC AS total_amount
    FROM core.dim_product AS products
    LEFT JOIN mart.agg_sales_daily_store_product AS daily
        ON daily.product_id = products.product_id
    GROUP BY products.product_id, products.product_code
),

percentages AS (
    SELECT
        product_id,
        product_code,
        total_quantity,
        100 * total_quantity
            / NULLIF(SUM(total_quantity) OVER (), 0) AS quantity_share_pct,
        100 * SUM(total_quantity) OVER (
                ORDER BY total_quantity DESC, product_code
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            / NULLIF(SUM(total_quantity) OVER (), 0) AS quantity_cumulative_pct,
        total_amount,
        100 * total_amount
            / NULLIF(SUM(total_amount) OVER (), 0) AS amount_share_pct,
        100 * SUM(total_amount) OVER (
                ORDER BY total_amount DESC, product_code
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
            / NULLIF(SUM(total_amount) OVER (), 0) AS amount_cumulative_pct
    FROM product_totals
)

SELECT
    product_id,
    product_code,
    total_quantity,
    ROUND(quantity_share_pct, 4) AS quantity_share_pct,
    ROUND(quantity_cumulative_pct, 4) AS quantity_cumulative_pct,
    CASE
        WHEN quantity_cumulative_pct IS NULL THEN NULL
        WHEN quantity_cumulative_pct <= 50 THEN 'A'
        WHEN quantity_cumulative_pct <= 70 THEN 'B'
        ELSE 'C'
    END AS abc_quantity,
    total_amount,
    ROUND(amount_share_pct, 4) AS amount_share_pct,
    ROUND(amount_cumulative_pct, 4) AS amount_cumulative_pct,
    CASE
        WHEN amount_cumulative_pct IS NULL THEN NULL
        WHEN amount_cumulative_pct <= 50 THEN 'A'
        WHEN amount_cumulative_pct <= 70 THEN 'B'
        ELSE 'C'
    END AS abc_amount
FROM percentages;

COMMENT ON VIEW mart.v_product_abc IS
    'ABC analysis per product by quantity and by amount, with shares and cumulative percentages.';
