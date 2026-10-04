-- Sales: one row per sale (one row from the source file).
CREATE TABLE IF NOT EXISTS core.fact_sales (
    sales_id             BIGINT          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_row_hash      CHAR(32)        NOT NULL UNIQUE,  -- prevents loading the same row twice
    sale_ts              TIMESTAMP       NOT NULL,
    date_id              INTEGER         NOT NULL REFERENCES core.dim_date (date_id),
    store_id             INTEGER         NOT NULL REFERENCES core.dim_store (store_id),
    product_id           INTEGER         NOT NULL REFERENCES core.dim_product (product_id),
    product_category_id  SMALLINT        NOT NULL REFERENCES core.dim_product_category (product_category_id),
    payment_method_id    SMALLINT        NOT NULL REFERENCES core.dim_payment_method (payment_method_id),
    customer_id          BIGINT          NOT NULL,  -- customer ID from the source; no customer table, because the source has only the ID
    quantity             INTEGER         NOT NULL CHECK (quantity > 0),
    unit_price           NUMERIC(18, 6)  NOT NULL CHECK (unit_price >= 0),
    discount_pct         NUMERIC(9, 6)   NOT NULL CHECK (discount_pct BETWEEN 0 AND 100),
    gross_amount         NUMERIC(18, 6)  NOT NULL,  -- price * quantity, before discount
    net_amount           NUMERIC(18, 6)  NOT NULL,  -- amount after discount (TotalAmount in the source)
    batch_id             VARCHAR(100)    NOT NULL,  -- which pipeline run loaded this row
    loaded_at            TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Indexes on the columns we join and filter on most often.
CREATE INDEX IF NOT EXISTS ix_fact_sales_date_id ON core.fact_sales (date_id);
CREATE INDEX IF NOT EXISTS ix_fact_sales_store_id ON core.fact_sales (store_id);
CREATE INDEX IF NOT EXISTS ix_fact_sales_product_id ON core.fact_sales (product_id);

COMMENT ON TABLE core.fact_sales IS 'All sales at the most detailed level. One row per row in the source file.';
