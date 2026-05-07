from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

NOTEBOOK_ROOT = "/Users/agrawalshruti167@gmail.com/ipl"
DATABRICKS_CONN_ID = "databricks_default"

default_args = {
    "owner": "ipl_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def run_notebook(notebook_name):
    import requests
    import time
    from airflow.hooks.base import BaseHook
    conn = BaseHook.get_connection(DATABRICKS_CONN_ID)
    token = conn.password
    host = conn.host.rstrip('/')
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "run_name": f"airflow_{notebook_name}",
        "new_cluster": {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "Standard_DS3_v2",
            "num_workers": 1
        },
        "notebook_task": {
            "notebook_path": f"{NOTEBOOK_ROOT}/{notebook_name}"
        }
    }
    response = requests.post(
        f"{host}/api/2.0/jobs/runs/submit",
        headers=headers,
        json=payload
    )
    run_id = response.json()["run_id"]
    print(f"Started run_id: {run_id}")
    while True:
        status = requests.get(
            f"{host}/api/2.0/jobs/runs/get?run_id={run_id}",
            headers=headers
        ).json()
        state = status["state"]["life_cycle_state"]
        print(f"Status: {state}")
        if state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
            result = status["state"]["result_state"]
            if result != "SUCCESS":
                raise Exception(f"Notebook failed: {result}")
            break
        time.sleep(30)

def run_bronze():
    run_notebook("01_bronze")

def run_silver():
    run_notebook("02_silver")

def run_gold():
    run_notebook("03_gold")

def run_duckdb_loader():
    result = subprocess.run(
        ["python", "/opt/airflow/scripts/load_duckdb.py"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"DuckDB loader failed:\n{result.stderr}")

with DAG(
    dag_id="ipl_etl_pipeline",
    default_args=default_args,
    description="IPL ETL: Databricks Bronze Silver Gold + DuckDB",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["ipl", "pyspark", "portfolio"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze,
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver,
    )

    gold = PythonOperator(
        task_id="gold_kpis",
        python_callable=run_gold,
    )

    load_duckdb = PythonOperator(
        task_id="load_duckdb",
        python_callable=run_duckdb_loader,
    )

    bronze >> silver >> gold >> load_duckdb