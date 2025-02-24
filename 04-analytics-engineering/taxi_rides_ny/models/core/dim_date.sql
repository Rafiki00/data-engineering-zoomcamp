{{ config(materialized='table') }}

WITH date_series AS (
    -- Generate a list of dates from the earliest to the latest pickup date
    SELECT 
        DATE_ADD(
            (SELECT MIN(DATE(pickup_datetime)) FROM {{ ref('fact_trips') }}), 
            INTERVAL n DAY
        ) AS date_actual
    FROM UNNEST(GENERATE_ARRAY(0, 
        DATE_DIFF(
            (SELECT MAX(DATE(pickup_datetime)) FROM {{ ref('fact_trips') }}), 
            (SELECT MIN(DATE(pickup_datetime)) FROM {{ ref('fact_trips') }}),
            DAY
        )
    )) AS n
)

SELECT 
    date_actual,
    EXTRACT(YEAR FROM date_actual) AS year,
    EXTRACT(QUARTER FROM date_actual) AS quarter,
    EXTRACT(MONTH FROM date_actual) AS month,
    CONCAT(EXTRACT(YEAR FROM date_actual), '/Q', EXTRACT(QUARTER FROM date_actual)) AS year_quarter,
    CONCAT(EXTRACT(YEAR FROM date_actual), '-', LPAD(EXTRACT(MONTH FROM date_actual), 2, '0')) AS year_month,
    EXTRACT(WEEK FROM date_actual) AS week_of_year,
    EXTRACT(DAY FROM date_actual) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM date_actual) AS day_of_week
FROM date_series
