from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello World from Airflow ✅")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # Run manually
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
    )
