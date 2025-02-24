{{
    config(
        materialized='table'
    )
}}

WITH trips AS (
    SELECT 
        tripid,
        service_type,
        total_amount,
        DATE(trips_unioned.pickup_datetime) AS trip_date
    FROM {{ ref('fact_trips') }}
),
trip_with_quarter AS (
    SELECT 
        t.tripid,
        t.service_type,
        t.total_amount,
        d.year_quarter
    FROM trips t
    INNER JOIN {{ ref('dim_date') }} d
        ON t.trip_date = d.date_day
),
quarterly_revenue AS (
    SELECT
        year_quarter,
        service_type,
        SUM(total_amount) AS total_revenue
    FROM trip_with_quarter
    GROUP BY year_quarter, service_type
)

SELECT * FROM quarterly_revenue;
