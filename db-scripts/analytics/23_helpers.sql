-- Helper functions and dim_date population.

-- Open-status mapping. Resolved decision in plan: open ≡ status != 'COMPLETED'.
CREATE OR REPLACE FUNCTION marts.is_open_status(status TEXT)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
    SELECT status IS DISTINCT FROM 'COMPLETED';
$$;

-- Populate dim_date from 2020-01-01 through 2030-12-31.
INSERT INTO marts.dim_date (
    date_key, full_date, year, quarter, month, month_name,
    day, day_of_week, day_name, is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER       AS date_key,
    d                                     AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT     AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT     AS quarter,
    EXTRACT(MONTH   FROM d)::SMALLINT     AS month,
    TO_CHAR(d, 'Month')                   AS month_name,
    EXTRACT(DAY     FROM d)::SMALLINT     AS day,
    EXTRACT(DOW     FROM d)::SMALLINT     AS day_of_week,
    TO_CHAR(d, 'Day')                     AS day_name,
    EXTRACT(DOW FROM d) IN (0, 6)         AS is_weekend
FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;
