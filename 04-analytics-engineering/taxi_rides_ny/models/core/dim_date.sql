WITH date_series AS (
    SELECT 
        CAST(DATE('2019-01-01') + INTERVAL n DAY AS DATE) AS date_day
    FROM UNNEST(GENERATE_ARRAY(0, 365 * 10)) AS n -- 10 years of dates
)

SELECT 
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    FORMAT_DATE('%Y/Q%Q', date_day) AS year_quarter,
    FORMAT_DATE('%Y-%m', date_day) AS year_month
FROM date_series;
