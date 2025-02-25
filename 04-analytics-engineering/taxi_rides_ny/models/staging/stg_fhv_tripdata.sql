{{ config(materialized='view') }}

SELECT
    -- identifiers
    dispatching_base_num,
    PUlocationID as pickup_location_id,
    DOlocationID as dropoff_location_id,
    SR_Flag as sr_flag,
    Affiliated_base_number as affiliated_base_number,
    
    -- timestamps
    CAST(pickup_datetime AS TIMESTAMP) as pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) as dropoff_datetime
FROM {{ source('staging', 'fhv_tripdata') }}
WHERE dispatching_base_num IS NOT NULL