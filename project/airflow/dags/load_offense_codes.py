import os
import datetime
import pandas as pd
import openpyxl

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Constants
GCS_BUCKET = "boston-crime"
XLSX_GCS_PATH = "raw/reference/rmsoffensecodes.xlsx"   
CSV_LOCAL_PATH = "/tmp/rmsoffensecodes.csv" 
CSV_GCS_PATH = "raw/reference/rmsoffensecodes.csv"  
PROJECT_ID = "galvanic-flame-447801-n5"
DATASET_NAME = "boston_crime"
TABLE_NAME = "offense_codes_stg"

default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2025, 1, 1),
    "retries": 0,
}

def convert_xlsx_to_csv(**kwargs):
    """
    1) Download the XLSX from GCS to local
    2) Convert to CSV with pandas
    3) Upload CSV back to GCS
    """
    # 1) Download XLSX from GCS
    gcs_hook = GCSHook()
    gcs_hook.download(
        bucket_name=GCS_BUCKET,
        object_name=XLSX_GCS_PATH,
        filename="/tmp/rmsoffensecodes.xlsx"
    )

    # 2) Convert to CSV locally
    df = pd.read_excel("/tmp/rmsoffensecodes.xlsx", engine="openpyxl") 

    df.to_csv(CSV_LOCAL_PATH, index=False)

    # 3) Upload the CSV back to GCS
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=CSV_GCS_PATH,
        filename=CSV_LOCAL_PATH
    )

    # Clean up local files if desired
    os.remove("/tmp/rmsoffensecodes.xlsx")
    os.remove(CSV_LOCAL_PATH)

with DAG(
    dag_id="load_offense_codes",
    default_args=default_args,
    schedule_interval=None,  # Run on demand
    catchup=False
) as dag:

    # Task 1: Convert XLSX → CSV, store in GCS
    convert_xlsx = PythonOperator(
        task_id="convert_xlsx_to_csv",
        python_callable=convert_xlsx_to_csv,
        provide_context=True,
    )

    # Task 2: Load CSV from GCS into BigQuery
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",
        bucket=GCS_BUCKET,
        source_objects=[CSV_GCS_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "CODE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "NAME", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    convert_xlsx >> load_csv_to_bq
