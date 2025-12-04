import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Update the function to accept a parameter (name)
def log_hello_function(name):
    logging.info(f"Hello! This is Nora.")

with DAG(
    dag_id='hello_nora_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['tutorial']
) as dag:

    # 2. Pass the name using op_kwargs
    say_hello = PythonOperator(
        task_id='log_hello_task',
        python_callable=log_hello_function,
        # This dictionary maps to the arguments in your function above
        op_kwargs={'name': 'Nora'} 
    )