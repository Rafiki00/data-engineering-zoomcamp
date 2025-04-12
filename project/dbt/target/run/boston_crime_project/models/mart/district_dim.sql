
  
    

    create or replace table `galvanic-flame-447801-n5`.`boston_crime`.`district_dim`
    
    
    OPTIONS()
    as (
      

SELECT
  NULLIF(TRIM(district), '') AS district_code,
  CASE WHEN NULLIF(TRIM(district), '') = 'A1' THEN 'Downtown'
       WHEN NULLIF(TRIM(district), '') = 'D4' THEN 'South End'
       ELSE NULLIF(TRIM(neighborhood), '') END AS neighborhood
FROM `galvanic-flame-447801-n5`.`boston_crime`.`police_districts_stg`
WHERE district IS NOT NULL
  AND neighborhood IS NOT NULL
    );
  