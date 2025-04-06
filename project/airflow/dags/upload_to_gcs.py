import os
import datetime
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

GCS_BUCKET_NAME = "boston-crime"


def upload_links_to_gcs(links_file, gcs_subfolder, **context):
    """
    1) Reads links from links_file
    2) Downloads each file
    3) Uploads each file to GCS under gcs_subfolder/
    """
    with open(links_file, "r") as f:
        links = [line.strip() for line in f if line.strip()]

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)

    for link in links:
        print(f"Processing link: {link}")
        filename = link.split("/")[-1] or "tempfile.csv"
        local_path = f"/tmp/{filename}"

        # Download
        response = requests.get(link)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(response.content)

        # Upload to GCS
        blob = bucket.blob(f"{gcs_subfolder}/{filename}")
        blob.upload_from_filename(local_path)
        print(f"Uploaded {filename} to gs://{GCS_BUCKET_NAME}/{gcs_subfolder}/{filename}")

        # Remove local
        os.remove(local_path)


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="upload_links_to_gcs",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    upload_incidents = PythonOperator(
        task_id="upload_incidents",
        python_callable=upload_links_to_gcs,
        op_kwargs={
            "links_file": "/opt/airflow/dags/incidents_links.txt",
            "gcs_subfolder": "raw/incidents"
        },
    )

    upload_reference = PythonOperator(
        task_id="upload_reference",
        python_callable=upload_links_to_gcs,
        op_kwargs={
            "links_file": "/opt/airflow/dags/reference_data_links.txt",
            "gcs_subfolder": "raw/reference"
        },
    )

    [upload_incidents, upload_reference]
