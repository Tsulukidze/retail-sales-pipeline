-- Add product categories and payment methods that are not in the tables yet.
-- WHERE NOT EXISTS sends only new values to the INSERT. Without it, every run
-- would use up ID numbers for values that already exist, and the IDs would
-- get gaps. ORDER BY gives IDs in alphabetical order on a new database.
-- ON CONFLICT is an extra safety net if two runs overlap.

INSERT INTO core.dim_product_category (product_category_name)
SELECT DISTINCT staging_rows.product_category
FROM staging.stg_retail_transactions AS staging_rows
WHERE NOT EXISTS (
    SELECT 1
    FROM core.dim_product_category AS existing
    WHERE existing.product_category_name = staging_rows.product_category
)
ORDER BY staging_rows.product_category
ON CONFLICT (product_category_name) DO NOTHING;

INSERT INTO core.dim_payment_method (payment_method_name)
SELECT DISTINCT staging_rows.payment_method
FROM staging.stg_retail_transactions AS staging_rows
WHERE NOT EXISTS (
    SELECT 1
    FROM core.dim_payment_method AS existing
    WHERE existing.payment_method_name = staging_rows.payment_method
)
ORDER BY staging_rows.payment_method
ON CONFLICT (payment_method_name) DO NOTHING;
