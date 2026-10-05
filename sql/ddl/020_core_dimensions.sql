-- Calendar table: one row per day.
-- Used for date keys and to know how many days each month has (for LFL).
CREATE TABLE IF NOT EXISTS core.dim_date (
    date_id        INTEGER     PRIMARY KEY,  -- date as a number, e.g. 20240131
    full_date      DATE        NOT NULL UNIQUE,
    year           SMALLINT    NOT NULL,
    quarter        SMALLINT    NOT NULL,
    month          SMALLINT    NOT NULL,
    month_name     VARCHAR(9)  NOT NULL,
    day_of_month   SMALLINT    NOT NULL,
    day_of_week    SMALLINT    NOT NULL,     -- 1 = Monday ... 7 = Sunday
    day_name       VARCHAR(9)  NOT NULL,
    is_weekend     BOOLEAN     NOT NULL,
    days_in_month  SMALLINT    NOT NULL
);

-- Stores. The source file has no store ID, so I use the state code from
-- the store address as the store (see README, "Store derivation").
CREATE TABLE IF NOT EXISTS core.dim_store (
    store_id     INTEGER       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_code   VARCHAR(10)   NOT NULL UNIQUE,  -- business key from the source (state code)
    store_name   VARCHAR(100)  NOT NULL,         -- random name; created once and never changed
    created_at   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dim_product (
    product_id         INTEGER       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_code       VARCHAR(50)   NOT NULL UNIQUE,  -- business key from the source (e.g. 'A')
    product_name       VARCHAR(100)  NOT NULL,         -- random name; created once and never changed
    abc_quantity       CHAR(1)       CHECK (abc_quantity IN ('A', 'B', 'C')),
    abc_amount         CHAR(1)       CHECK (abc_amount IN ('A', 'B', 'C')),
    abc_calculated_at  TIMESTAMPTZ,
    created_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Product category belongs to the sale, not to the product:
-- in the source data, every product is sold in every category.
CREATE TABLE IF NOT EXISTS core.dim_product_category (
    product_category_id    SMALLINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_category_name  VARCHAR(100)  NOT NULL UNIQUE,
    created_at             TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.dim_payment_method (
    payment_method_id    SMALLINT     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_method_name  VARCHAR(50)  NOT NULL UNIQUE,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- LFL status for each store and each month of the current year.
-- The day columns show why a store got its status,
-- e.g. "30 days were needed last year, but the store worked only 2".
CREATE TABLE IF NOT EXISTS core.dim_store_lfl (
    year                   SMALLINT     NOT NULL,
    month                  SMALLINT     NOT NULL CHECK (month BETWEEN 1 AND 12),
    store_id               INTEGER      NOT NULL REFERENCES core.dim_store (store_id),
    is_lfl                 BOOLEAN      NOT NULL,
    days_worked_current    SMALLINT     NOT NULL,  -- days with sales this year
    days_required_current  SMALLINT     NOT NULL,  -- days needed this year
    days_worked_prior      SMALLINT     NOT NULL,  -- days with sales last year
    days_required_prior    SMALLINT     NOT NULL,  -- days needed last year
    calculated_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (year, month, store_id)
);

COMMENT ON TABLE core.dim_store IS 'Stores found in the sales data. One row per state code.';
COMMENT ON TABLE core.dim_product IS 'Products found in the sales data, with ABC class by quantity and by amount.';
COMMENT ON TABLE core.dim_store_lfl IS 'Like-for-like (LFL) status for each store and month of the current year.';
