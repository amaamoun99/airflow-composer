from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

def safe_function():
    try:
        logging.info("Task started. Trying to do something dangerous...")
        
        # 1. We deliberately cause an error here
        raise ValueError("Something went wrong with the calculation!")
        
        # This line will never be reached
        logging.info("This will never print.")

    except ValueError as e:
        # 2. We CATCH the error here instead of letting it crash
        logging.error(f"Caught an error: {e}")
        logging.info("Handling the error gracefully... keeping the task Green.")

with DAG(
    dag_id="safe_handled_dag",
    schedule=None,
    start_date=datetime(2025, 5, 15),
    catchup=False,
    tags=["test", "handled"],
) as dag:
    
    safe_task = PythonOperator(
        task_id="handled_error_task",
        python_callable=safe_function,
    )

    safe_task