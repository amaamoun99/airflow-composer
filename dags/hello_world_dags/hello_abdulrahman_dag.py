from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def force_failure_function():
    logging.info("Task started. Preparing to fail...")
    
    # This creates a log entry
    logging.error("An error condition was detected!")
    
    # CRITICAL: This line actually tells Airflow the task FAILED
    raise ValueError("Stopping execution: This task is designed to fail.")

with DAG(
    dag_id="abdulrahman_error_dag",
    schedule=None,
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=["test", "error"],
) as dag:
    
    fail_task = PythonOperator(
        task_id="force_fail_task",
        python_callable=force_failure_function,
    )

    fail_task