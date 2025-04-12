

WITH casted AS (
    SELECT DISTINCT 
        NULLIF(TRIM(INCIDENT_NUMBER), '') AS incident_number,
        CAST(OFFENSE_CODE AS INT64) AS offense_code,
        NULLIF(TRIM(OFFENSE_CODE_GROUP), '') AS offense_code_group,
        NULLIF(TRIM(OFFENSE_DESCRIPTION), '') AS offense_description,
        CASE 
            WHEN UPPER(TRIM(DISTRICT)) = 'OUTSIDE OF' THEN 'External'
            ELSE NULLIF(TRIM(DISTRICT), '')
        END AS district_code,
        NULLIF(TRIM(REPORTING_AREA), '') AS reporting_area,
        CASE
            WHEN UPPER(TRIM(SHOOTING)) IN ('Y','1') THEN 1
            WHEN UPPER(TRIM(SHOOTING)) IN ('N','0') THEN 0
            ELSE NULL
        END AS shooting,
        SAFE_CAST(TRIM(REPLACE(OCCURRED_ON_DATE, '+00', '')) AS DATETIME)
            AS occurred_on_datetime,
        CAST(YEAR AS INT64) AS year,
        CAST(MONTH AS INT64) AS month,
        NULLIF(TRIM(DAY_OF_WEEK), '') AS day_of_week,
        CAST(HOUR AS INT64) AS hour,
        NULLIF(TRIM(UCR_PART), '') AS ucr_part,
        NULLIF(TRIM(STREET), '') AS street,
        CAST(Lat AS FLOAT64) AS lat,
        CAST(Long AS FLOAT64) AS long
    FROM `galvanic-flame-447801-n5`.`boston_crime`.`crime_incidents_stg`
), deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY incident_number, offense_code ORDER BY occurred_on_datetime DESC) AS row_num
    FROM casted
    WHERE incident_number IS NOT NULL
      AND offense_code IS NOT NULL
)


SELECT CONCAT(incident_number, offense_code)AS incident_offense_key,
        incident_number,
        offense_code,
        offense_code_group,
        offense_description,
        district_code,
        reporting_area,
        shooting,
        occurred_on_datetime,
        year,
        month,
        day_of_week,
        hour,
        street,
        lat,
        long
FROM deduped 
WHERE row_num = 1