from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello():
    print("Hello, Airflow is alive!")

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2025, 8, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    description="Minimal sanity DAG",
) as dag:
    PythonOperator(task_id="hello_task", python_callable=say_hello)
