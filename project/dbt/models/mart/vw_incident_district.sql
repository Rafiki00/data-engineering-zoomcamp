{{ config(
    materialized='view'
) }}

SELECT
    f.incident_offense_key,
    f.incident_number,
    f.offense_description,    
    f.occurred_on_datetime,
    f.offense_code,
    f.offense_code_group,
    f.district_code,
    d.neighborhood,
    f.shooting,
    f.year,
    f.month,
    f.day_of_week,
    f.hour,
    f.street,
    f.lat,
    f.long
FROM {{ ref('incident_fact') }} f
LEFT JOIN {{ ref('district_dim') }} d
    ON f.district_code = d.district_code
