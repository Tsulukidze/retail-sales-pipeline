-- One row for each row in the source CSV file.
-- This table is emptied and loaded again on every run.
CREATE TABLE IF NOT EXISTS staging.stg_retail_transactions (
    customer_id       BIGINT         NOT NULL,
    product_code      VARCHAR(50)    NOT NULL,
    quantity          INTEGER        NOT NULL,
    unit_price        NUMERIC(18, 6) NOT NULL,
    transaction_ts    TIMESTAMP      NOT NULL,
    payment_method    VARCHAR(50)    NOT NULL,
    store_location    TEXT           NOT NULL,
    store_code        VARCHAR(10)    NOT NULL,  -- state code taken from store_location (e.g. 'MO')
    product_category  VARCHAR(100)   NOT NULL,
    discount_pct      NUMERIC(9, 6)  NOT NULL,
    total_amount      NUMERIC(18, 6) NOT NULL,
    source_row_hash   CHAR(32)       NOT NULL,  -- md5 of the original row; used to find each row again
    batch_id          VARCHAR(100)   NOT NULL,  -- which pipeline run loaded this row
    source_file       TEXT           NOT NULL,  -- which file the row came from
    loaded_at         TIMESTAMPTZ    NOT NULL DEFAULT now()
);

COMMENT ON TABLE staging.stg_retail_transactions IS
    'Raw Kaggle sales rows with correct data types and load information. Loaded again on every run.';
