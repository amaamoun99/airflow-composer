from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def error_task_jumana():
    print("hello_world")
    logging.error("This is an error message from Jumana's error task.")
    logging.info("This is an info message from Jumana's error task.")
    logging.warning("This is a warning message from Jumana's error task.")
    logging.debug("This is a debug message from Jumana's error task.")
    logging.critical("This is a critical message from Jumana's error task.")

with DAG(
    dag_id="hello_dag_jumana",
    schedule=None,
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=["example", "jumana"],
) as dag:
    
    jumana_error_task = PythonOperator(
        task_id="jumana_error_task",
        python_callable=error_task_jumana,
    )

    jumana_error_task
