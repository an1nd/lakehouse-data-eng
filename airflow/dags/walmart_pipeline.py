import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient

import os
import yaml
from pathlib import Path

# Job ID is not a secret and will not trigger GitHub push protection
DATABRICKS_JOB_ID = os.getenv("DATABRICKS_JOB_ID", "YOUR_NUMERIC_JOB_ID")

# Path to profiles.yml inside the Airflow Docker container
PROFILES_FILE = Path("/opt/airflow/walmart_project/profiles.yml")

def load_databricks_credentials(profile_path: Path):
    if not profile_path.exists():
        # Fallback to local host environment if running outside Docker
        profile_path = Path.home() / ".dbt" / "profiles.yml"
        if not profile_path.exists():
            return None, None

    with open(profile_path, "r") as f:
        data = yaml.safe_load(f)

    # Navigates: walmart_project -> outputs -> dev
    dev_config = (
        data.get("walmart_project", {})
        .get("outputs", {})
        .get("dev", {})
    )

    host = dev_config.get("host")
    token = dev_config.get("token")

    # Ensure the host string has the https:// protocol prefix for Databricks SDK
    if host and not host.startswith("http"):
        host = f"https://{host}"

    return host, token

DATABRICKS_HOST, DATABRICKS_TOKEN = load_databricks_credentials(PROFILES_FILE)

def trigger_and_wait_for_databricks():
    w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
    print(f"Triggering Databricks ingestion job {DATABRICKS_JOB_ID}...")
    run_response = w.jobs.run_now(job_id=DATABRICKS_JOB_ID)
    run_id = run_response.bind()['run_id']
    print(f"Databricks Run ID: {run_id}. Polling status...")

    while True:
        run_state = w.jobs.get_run(run_id=run_id).state
        life_cycle = run_state.life_cycle_state.value
        print(f"Current State: {life_cycle}")

        if life_cycle in ['TERMINATED', 'SUCCESS']:
            result = run_state.result_state.value if run_state.result_state else 'SUCCESS'
            if result != 'SUCCESS':
                raise Exception(f"Databricks Job Failed with state: {result}")
            print("Databricks ingestion finished successfully.")
            break
        elif life_cycle in ['INTERNAL_ERROR', 'SKIPPED', 'FAILED']:
            raise Exception(f"Databricks Job Aborted: {run_state.state_message}")

        time.sleep(5)

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='walmart_lakehouse_orchestration',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 11 * * *',
    catchup=False,
) as dag:

    # 1. Trigger Databricks JDBC Ingestion Notebook
    ingest_bronze = PythonOperator(
        task_id='trigger_databricks_bronze_ingest',
        python_callable=trigger_and_wait_for_databricks
    )

    # 2. Clean DBT build targets to avoid residual caching issues
    clean_dbt = BashOperator(
        task_id='clean_dbt_target',
        bash_command='rm -rf target/',
        cwd='/opt/airflow/walmart_project'
    )

    # 3. Silver Technical: Run & Test
    run_silver_t = BashOperator(
        task_id='dbt_run_silver_technical',
        bash_command='dbt run --select silver_t --profiles-dir .',
        cwd='/opt/airflow/walmart_project'
    )

    test_silver_t = BashOperator(
        task_id='dbt_test_silver_technical',
        bash_command='dbt test --select silver_t --profiles-dir .',
        cwd='/opt/airflow/walmart_project'
    )

    # 4. Silver Business: OBT
    run_silver_b = BashOperator(
        task_id='dbt_run_silver_business',
        bash_command='dbt run --select silver_b --profiles-dir .',
        cwd='/opt/airflow/walmart_project'
    )

    # 5. Gold: SCD Type 2 Snapshots & Fact Table
    run_gold_snapshot = BashOperator(
        task_id='dbt_snapshot_gold',
        bash_command='dbt snapshot --profiles-dir .',
        cwd='/opt/airflow/walmart_project'
    )

    run_gold_fact = BashOperator(
        task_id='dbt_run_gold_fact',
        bash_command='dbt run --select gold --profiles-dir .',
        cwd='/opt/airflow/walmart_project'
    )

    # Set dependency chain using bitshift operator
    ingest_bronze >> clean_dbt >> run_silver_t >> test_silver_t >> run_silver_b >> run_gold_snapshot >> run_gold_fact