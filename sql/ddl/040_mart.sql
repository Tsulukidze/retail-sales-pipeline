-- Daily sales for each store and product. Built again on every run.
CREATE TABLE IF NOT EXISTS mart.agg_sales_daily_store_product (
    date_id                INTEGER         NOT NULL REFERENCES core.dim_date (date_id),
    store_id               INTEGER         NOT NULL REFERENCES core.dim_store (store_id),
    product_id             INTEGER         NOT NULL REFERENCES core.dim_product (product_id),
    total_revenue          NUMERIC(18, 2)  NOT NULL,  -- sum of net_amount
    total_gross_amount     NUMERIC(18, 2)  NOT NULL,  -- sum of gross_amount; needed to recalculate the discount
    total_quantity         INTEGER         NOT NULL,
    transaction_count      INTEGER         NOT NULL,
    unique_customer_count  INTEGER         NOT NULL,  -- do not sum this across days, stores or products
    weighted_discount_pct  NUMERIC(7, 4)   NOT NULL,  -- discount weighted by sales value
    calculated_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    PRIMARY KEY (date_id, store_id, product_id)
);

COMMENT ON TABLE mart.agg_sales_daily_store_product IS
    'Daily revenue, quantity, transactions, unique customers and weighted discount for each store and product.';
