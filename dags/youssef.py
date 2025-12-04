from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os

# Make sure logs folder exists
os.makedirs("/opt/airflow/logs", exist_ok=True)
error_log_file = "/opt/airflow/logs/errors.log"

def task_that_may_fail():
    # Example error
    raise ValueError("Something went wrong in this task!")

def log_error(context):
    """This function logs the error when a task fails."""
    task_id = context.get('task_instance').task_id
    exception = context.get('exception')

    message = f"[{datetime.now()}] Task failed: {task_id} | Error: {exception}\n"

    # Write to the log file
    with open(error_log_file, "a") as f:
        f.write(message)

    # Also print in Airflow logs
    logging.error(message)

with DAG(
    dag_id="youssef_logging_flow",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    risky_task = PythonOperator(
        task_id="risky_task",
        python_callable=task_that_may_fail,)
