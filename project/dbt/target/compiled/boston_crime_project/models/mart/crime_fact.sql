

WITH casted AS (
    SELECT
        CAST(INCIDENT_NUMBER AS STRING) AS incident_number,
        CAST(OFFENSE_CODE AS INT64) AS offense_code,
        OFFENSE_CODE_GROUP AS offense_code_group,
        OFFENSE_DESCRIPTION AS offense_description,
        DISTRICT AS district,
        REPORTING_AREA AS reporting_area,
        SHOOTING AS shooting,
        CAST(OCCURRED_ON_DATE AS TIMESTAMP) AS occurred_on_date,
        CAST(YEAR AS INT64) AS year,
        CAST(MONTH AS INT64) AS month,
        DAY_OF_WEEK AS day_of_week,
        CAST(HOUR AS INT64) AS hour,
        UCR_PART AS ucr_part,
        STREET AS street,
        CAST(Lat AS FLOAT64) AS lat,
        CAST(Long AS FLOAT64) AS long
    FROM `galvanic-flame-447801-n5`.`boston_crime`.`crime_incidents_stg`
)

SELECT * FROM casted