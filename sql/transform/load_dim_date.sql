-- Add every day of every year that has sales, from 1 January of the first
-- year to 31 December of the last year. Full years are needed for LFL,
-- which also looks at months of the previous year.
-- Days that already exist are kept, so this is safe to run every day.
INSERT INTO core.dim_date (
    date_id,
    full_date,
    year,
    quarter,
    month,
    month_name,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend,
    days_in_month
)
SELECT
    TO_CHAR(calendar.day, 'YYYYMMDD')::INTEGER,
    calendar.day::DATE,
    EXTRACT(YEAR FROM calendar.day),
    EXTRACT(QUARTER FROM calendar.day),
    EXTRACT(MONTH FROM calendar.day),
    TO_CHAR(calendar.day, 'FMMonth'),           -- FM removes the extra spaces
    EXTRACT(DAY FROM calendar.day),
    EXTRACT(ISODOW FROM calendar.day),          -- 1 = Monday ... 7 = Sunday
    TO_CHAR(calendar.day, 'FMDay'),
    EXTRACT(ISODOW FROM calendar.day) IN (6, 7),
    EXTRACT(DAY FROM DATE_TRUNC('month', calendar.day) + INTERVAL '1 month - 1 day')
FROM (
    SELECT
        DATE_TRUNC('year', MIN(transaction_ts)) AS first_day,
        DATE_TRUNC('year', MAX(transaction_ts)) + INTERVAL '1 year - 1 day' AS last_day
    FROM staging.stg_retail_transactions
) AS bounds
CROSS JOIN LATERAL GENERATE_SERIES(bounds.first_day, bounds.last_day, INTERVAL '1 day') AS calendar (day)
ON CONFLICT (date_id) DO NOTHING;
