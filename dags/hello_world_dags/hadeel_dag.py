from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello World! This is Hadeel’s DAG.")

with DAG(
    dag_id="hadeel_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # Run manually
    catchup=False
):
    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello
    )
