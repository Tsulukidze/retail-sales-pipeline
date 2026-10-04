-- Copy the ABC classes from the ABC view (sql/ddl/060_mart_views.sql)
-- into the product dimension. The rule itself is explained in the view.
-- The classes are calculated again on every run, because new sales can
-- change them.

UPDATE core.dim_product AS products
SET
    abc_quantity = abc.abc_quantity,
    abc_amount = abc.abc_amount,
    abc_calculated_at = now(),
    updated_at = now()
FROM mart.v_product_abc AS abc
WHERE abc.product_id = products.product_id;

-- This result is shown in the Airflow log: code, class by quantity, class by amount.
SELECT product_code, abc_quantity, abc_amount
FROM core.dim_product
ORDER BY product_code;
