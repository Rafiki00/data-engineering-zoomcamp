import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = "galvanic-flame-447801-n5"
DATASET_NAME = "boston_crime"
TABLE_NAME = "crime_incidents_stg"
GCS_BUCKET_NAME = "boston-crime"

default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2023, 1, 1),
}

with DAG(
    dag_id="load_incidents",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
) as dag:

    load_incidents_task = GCSToBigQueryOperator(
        task_id="load_incidents_task",
        bucket=GCS_BUCKET_NAME,
        source_objects=["raw/incidents/*.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "INCIDENT_NUMBER", "type": "STRING", "mode": "NULLABLE"},
            {"name": "OFFENSE_CODE", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "OFFENSE_CODE_GROUP", "type": "STRING", "mode": "NULLABLE"},
            {"name": "OFFENSE_DESCRIPTION", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DISTRICT", "type": "STRING", "mode": "NULLABLE"},
            {"name": "REPORTING_AREA", "type": "STRING", "mode": "NULLABLE"},
            {"name": "SHOOTING", "type": "STRING", "mode": "NULLABLE"},
            {"name": "OCCURRED_ON_DATE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "YEAR", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "MONTH", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "DAY_OF_WEEK", "type": "STRING", "mode": "NULLABLE"},
            {"name": "HOUR", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "UCR_PART", "type": "STRING", "mode": "NULLABLE"},
            {"name": "STREET", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Lat", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "Long", "type": "FLOAT", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
    )

    load_incidents_task
