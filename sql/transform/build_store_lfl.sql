-- Rebuild the LFL table from the LFL view (sql/ddl/050_core_views.sql).
-- The rule itself is explained in the view. Both statements run in one
-- transaction: if the INSERT fails, the old data stays.

TRUNCATE TABLE core.dim_store_lfl;

INSERT INTO core.dim_store_lfl (
    year,
    month,
    store_id,
    is_lfl,
    days_worked_current,
    days_required_current,
    days_worked_prior,
    days_required_prior
)
SELECT
    year,
    month,
    store_id,
    is_lfl,
    days_worked_current,
    days_required_current,
    days_worked_prior,
    days_required_prior
FROM core.v_store_lfl;

-- This result is shown in the Airflow log.
SELECT
    COUNT(*) AS lfl_rows,
    COUNT(*) FILTER (WHERE is_lfl) AS lfl_stores_months
FROM core.dim_store_lfl;
