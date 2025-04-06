import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2025, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='dbt_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    dbt_run_fact = BashOperator(
        task_id='dbt_run_fact',
        bash_command='cd /opt/dbt && dbt run --profiles-dir . --select crime_fact'
    )

    dbt_run_fact
