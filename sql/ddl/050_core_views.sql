-- LFL (like-for-like) calculation.
--
-- The rule lives only here. The pipeline copies the result of this view into
-- core.dim_store_lfl, and the manual check (sql/manual_checks/) uses the same
-- view on synthetic data. So the logic exists once, and the check tests the
-- real logic, not a copy.
--
-- The rule from the task:
-- A store is LFL in a month if it had sales on every required day, in this
-- year AND in the same month of the previous year.
--   * Current year = the year of the last sale in the data.
--   * Months = January up to the month of the last sale.
--   * Days required this year = all days of the month. For the month of the
--     last sale (the current month): only the days up to that date.
--   * Days required last year = all days of the same month.
--   * Every store gets a row for every month, also stores with no sales.
--
-- The days required are calculated from the calendar, not from dim_date.
-- If the previous year were missing from dim_date, a count from dim_date
-- would give 0 required days, and every store would wrongly become LFL.
CREATE OR REPLACE VIEW core.v_store_lfl AS
WITH last_sale AS (
    SELECT
        MAX(sale_ts)::DATE AS last_sale_date,
        EXTRACT(YEAR FROM MAX(sale_ts))::INTEGER AS current_year,
        EXTRACT(MONTH FROM MAX(sale_ts))::INTEGER AS last_month
    FROM core.fact_sales
),

months AS (
    -- One row per month of the current year, with the days a store must work.
    SELECT
        last_sale.current_year AS year,
        month_numbers.month,
        CASE
            WHEN month_numbers.month = last_sale.last_month
                THEN EXTRACT(DAY FROM last_sale.last_sale_date)::INTEGER  -- days so far
            ELSE EXTRACT(
                DAY FROM MAKE_DATE(last_sale.current_year, month_numbers.month, 1)
                    + INTERVAL '1 month - 1 day'
            )::INTEGER                                                       -- full month
        END AS days_required_current,
        EXTRACT(
            DAY FROM MAKE_DATE(last_sale.current_year - 1, month_numbers.month, 1)
                + INTERVAL '1 month - 1 day'
        )::INTEGER AS days_required_prior                                    -- full month
    FROM last_sale
    CROSS JOIN LATERAL GENERATE_SERIES(1, last_sale.last_month) AS month_numbers (month)
),

days_worked AS (
    -- Number of different days with at least one sale, per store and month.
    -- Only the current and the previous year are needed.
    SELECT
        sales.store_id,
        dates.year,
        dates.month,
        COUNT(DISTINCT dates.full_date) AS days_worked
    FROM core.fact_sales AS sales
    JOIN core.dim_date AS dates
        ON dates.date_id = sales.date_id
    WHERE dates.year >= (SELECT current_year - 1 FROM last_sale)
    GROUP BY sales.store_id, dates.year, dates.month
)

SELECT
    months.year,
    months.month,
    stores.store_id,
    -- A store cannot work more days than required, so "=" means "every day".
    (
        COALESCE(current_days.days_worked, 0) = months.days_required_current
        AND COALESCE(prior_days.days_worked, 0) = months.days_required_prior
    ) AS is_lfl,
    COALESCE(current_days.days_worked, 0)::SMALLINT AS days_worked_current,
    months.days_required_current::SMALLINT AS days_required_current,
    COALESCE(prior_days.days_worked, 0)::SMALLINT AS days_worked_prior,
    months.days_required_prior::SMALLINT AS days_required_prior
FROM months
CROSS JOIN core.dim_store AS stores          -- every store, also without sales
LEFT JOIN days_worked AS current_days
    ON current_days.store_id = stores.store_id
    AND current_days.year = months.year
    AND current_days.month = months.month
LEFT JOIN days_worked AS prior_days
    ON prior_days.store_id = stores.store_id
    AND prior_days.year = months.year - 1
    AND prior_days.month = months.month;

COMMENT ON VIEW core.v_store_lfl IS
    'LFL calculation for each store and month of the current year. Source for core.dim_store_lfl.';
