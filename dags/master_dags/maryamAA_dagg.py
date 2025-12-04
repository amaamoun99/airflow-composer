from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def task_with_error_handling():
    try:
        logging.info("Task started by Maryam...")

        # ❌ Example: This will fail to test logging
        result = 10 / 0

        logging.info(f"Result is: {result}")

    except Exception as e:
        logging.error("❌ An error occurred!")
        logging.error(str(e))
        raise  # Re-raise so Airflow marks the task as FAILED

with DAG(
    dag_id="maryam_error_logging_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["maryam", "error_logging"]
) as dag:

    error_task = PythonOperator(
        task_id="task_log_errors",
        python_callable=task_with_error_handling
    )
