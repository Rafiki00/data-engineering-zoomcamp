import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = "galvanic-flame-447801-n5"
DATASET_NAME = "boston_crime"
TABLE_NAME = "police_districts_stg"
GCS_BUCKET_NAME = "boston-crime"

default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2023, 1, 1),
}

with DAG(
    dag_id="load_police_districts",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    load_police_districts_task = GCSToBigQueryOperator(
        task_id="load_police_districts_task",
        bucket=GCS_BUCKET_NAME,
        source_objects=["raw/reference/boston_police_stations_bpd_only.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        source_format="CSV",
        skip_leading_rows=1, 
        schema_fields=[
            {"name": "BLDG_ID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "BID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ADDRESS", "type": "STRING", "mode": "NULLABLE"},
            {"name": "POINT_X", "type": "STRING", "mode": "NULLABLE"},
            {"name": "POINT_Y", "type": "STRING", "mode": "NULLABLE"},
            {"name": "NAME", "type": "STRING", "mode": "NULLABLE"},
            {"name": "NEIGHBORHOOD", "type": "STRING", "mode": "NULLABLE"},
            {"name": "CITY", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ZIP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "FT_SQFT", "type": "STRING", "mode": "NULLABLE"},
            {"name": "STORY_HT", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PARCEL_ID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "District", "type": "STRING", "mode": "NULLABLE"},
            {"name": "shape_wkt", "type": "STRING", "mode": "NULLABLE"}
        ],
        write_disposition="WRITE_TRUNCATE",  
        allow_quoted_newlines=True,
        ignore_unknown_values=True,  
    )

    load_police_districts_task
