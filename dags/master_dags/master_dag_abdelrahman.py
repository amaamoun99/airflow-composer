from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow import DAG

default_args = {
    "owner": "abdelrahman",
    "depends_on_past": False,
    "start_date": datetime(2025, 5, 22),
    "bigquery_conn_id": "bigquery_default",
    "max_active_runs": 1,
    "retries": 0,
}

ext_date = "{{ execution_date }}"

# split into priority groups
priority_1_dags = ["orders_db_transfer_abdelrahman"]
priority_2_dags = ["example_airflow_dag_abdelrahman"]

dag = DAG(
    dag_id="master_dag_abdelrahman",
    description="All transfer DAGs with priorities",
    schedule_interval=None,
    concurrency=6,
    max_active_runs=1,
    default_args=default_args,
    tags=["master_transfer"],
    catchup=False,
)

with dag:
    # ----------------- Priority 1 Group -----------------
    with TaskGroup("priority_1_group", tooltip="Priority 1 DAGs") as priority_1_group:
       
        for dag_id in priority_1_dags:
            t = TriggerDagRunOperator(
                task_id=f"{dag_id}_abdelrahman",
                trigger_dag_id=f"{dag_id}",
                execution_date=ext_date,
                wait_for_completion=True,
                poke_interval=30,
                deferrable=True,
                trigger_rule="all_done",
            )
          

    # ----------------- Priority 2 Group -----------------
    with TaskGroup("priority_2_group", tooltip="Priority 2 DAGs") as priority_2_group:
       
        for dag_id in priority_2_dags:
            t = TriggerDagRunOperator(
                task_id=f"{dag_id}_abdelrahman",
                trigger_dag_id=f"{dag_id}",
                execution_date=ext_date,
                wait_for_completion=True,
                poke_interval=30,
                deferrable=True,
                trigger_rule="all_done",
            )
         

    # ----------------- Set Dependency -----------------
    priority_1_group >> priority_2_group
