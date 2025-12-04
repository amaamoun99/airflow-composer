from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def my_task():
    try:
        x = 1 / 0   # Error
    except Exception as e:
        logging.error(f"Custom Error Occurred: {str(e)}")
        raise       # important: re-raise so Airflow marks task as failed

with DAG(
    "error_logging_demo",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="test_error_logging",
        python_callable=my_task,
    )
