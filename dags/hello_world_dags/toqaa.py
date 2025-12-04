from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Create logger
logger = logging.getLogger("airflow.task")


def my_task():
    logger.info("Task started...")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    
    # Custom log data
    for i in range(5):
        logger.info(f"Processing item {i}")

    logger.info("Task finished successfully.")


# Define the DAG
with DAG(
    dag_id="toqa_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
):
    log_task = PythonOperator(
        task_id="log_task",
        python_callable=my_task
    )
