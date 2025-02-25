{{ config(materialized='table') }}

WITH fhv_trips AS (
    SELECT 
        *,
        -- Calculate trip duration in seconds
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
),

valid_trips AS (
    SELECT *
    FROM fhv_trips
    WHERE trip_duration > 0  -- Filter out invalid trip durations
),

-- Calculate p90 by grouping dimensions
p90_travel_times AS (
    SELECT
        year,
        month,
        pickup_location_id,
        pickup_borough,
        pickup_zone,
        dropoff_location_id,
        dropoff_borough,
        dropoff_zone,
        -- Compute p90 continuous for trip duration
        PERCENTILE_CONT(trip_duration, 0.9) OVER (
            PARTITION BY year, month, pickup_location_id, dropoff_location_id
        ) AS p90_trip_duration
    FROM valid_trips
)

-- Get distinct records (since window function creates duplicates)
SELECT DISTINCT
    year,
    month,
    pickup_location_id,
    pickup_borough,
    pickup_zone,
    dropoff_location_id,
    dropoff_borough,
    dropoff_zone,
    p90_trip_duration
FROM p90_travel_times
ORDER BY 
    year, 
    month, 
    pickup_location_id, 
    dropoff_location_id