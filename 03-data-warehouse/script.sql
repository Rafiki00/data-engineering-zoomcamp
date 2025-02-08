CREATE OR REPLACE EXTERNAL TABLE `project_id.dataset_name.yellow_taxi_2024_external`
OPTIONS (
 format = 'PARQUET',
 uris = ['gs://bucket_name/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE `project_id.dataset_name.mat_yellow_taxi`
AS
SELECT *
FROM `project_id.dataset_name.yellow_taxi_2024_external`;

SELECT COUNT(*)
FROM `project_id.dataset_name.mat_yellow_taxi`;

SELECT COUNT(DISTINCT PULocationID)
FROM `project_id.dataset_name.yellow_taxi_2024_external`;

SELECT COUNT(DISTINCT PULocationID)
FROM `project_id.dataset_name.mat_yellow_taxi`;

SELECT PULocationID
FROM `project_id.dataset_name.mat_yellow_taxi`;

SELECT PULocationID, DOLocationID
FROM `project_id.dataset_name.mat_yellow_taxi`;

SELECT COUNT(*)
FROM `project_id.dataset_name.mat_yellow_taxi`
WHERE fare_amount = 0;

CREATE OR REPLACE TABLE `project_id.dataset_name.mat_yellow_taxi_monthly_part`
PARTITION BY DATE_TRUNC(tpep_dropoff_datetime, MONTH)
CLUSTER BY VendorID
AS SELECT * FROM `project_id.dataset_name.mat_yellow_taxi`;

CREATE OR REPLACE TABLE `project_id.dataset_name.mat_yellow_taxi_daily_part`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS SELECT * FROM `project_id.dataset_name.mat_yellow_taxi`;

SELECT 
 partition_id,
 total_rows,
 total_logical_bytes/1024/1024/1024 as logical_gb,
 total_billable_bytes/1024/1024/1024 as billable_gb
FROM `project_id.dataset_name.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'mat_yellow_taxi_daily_part'
ORDER BY logical_gb DESC;

SELECT 
 partition_id,
 total_rows,
 total_logical_bytes/1024/1024/1024 as logical_gb,
 total_billable_bytes/1024/1024/1024 as billable_gb
FROM `project_id.dataset_name.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'mat_yellow_taxi_monthly_part'
ORDER BY logical_gb DESC;

SELECT DISTINCT VendorID
FROM `project_id.dataset_name.mat_yellow_taxi`
WHERE tpep_dropoff_datetime >= '2024-03-01'
     AND tpep_dropoff_datetime <= '2024-03-15';

SELECT DISTINCT VendorID
FROM `project_id.dataset_name.mat_yellow_taxi_daily_part`
WHERE tpep_dropoff_datetime >= '2024-03-01'
     AND tpep_dropoff_datetime <= '2024-03-15';

SELECT DISTINCT VendorID
FROM `project_id.dataset_name.mat_yellow_taxi_monthly_part`
WHERE tpep_dropoff_datetime >= '2024-03-01'
     AND tpep_dropoff_datetime <= '2024-03-15';

SELECT COUNT(*)
FROM `project_id.dataset_name.mat_yellow_taxi`;