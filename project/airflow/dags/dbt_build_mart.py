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

    # Task 1: Build the fact table (incident_fact)
    dbt_run_fact = BashOperator(
        task_id='dbt_run_fact',
        bash_command='cd /opt/dbt && dbt run --profiles-dir . --select incident_fact'
    )

    # Task 2: Build the dimension table (district_dim)
    dbt_run_dim = BashOperator(
        task_id='dbt_run_dim',
        bash_command='cd /opt/dbt && dbt run --profiles-dir . --select district_dim'
    )

    # Task 3: Build the view (vw_incident_district)
    dbt_run_view = BashOperator(
        task_id='dbt_run_view',
        bash_command='cd /opt/dbt && dbt run --profiles-dir . --select vw_incident_district'
    )

    dbt_run_fact >> dbt_run_dim >> dbt_run_view
