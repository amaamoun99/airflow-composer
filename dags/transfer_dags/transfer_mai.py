from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

with DAG(
    dag_id="postgres_to_bigquery",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
):

    export_from_postgres = PostgresToGCSOperator(
        task_id="export_from_postgres",
        postgres_conn_id="applai_postgres_db",          # your conn_id
        sql="SELECT * FROM 'Order'",      # or custom query
        bucket="db_to_datalake",                     # your bucket
        filename="pg_export/your_table.json",         # path in bucket
        export_format="json",
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="db_to_datalake",
        destination_project_dataset_table="applai-dwh.airflow_landing",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",           # or WRITE_APPEND
    )

    export_from_postgres >> load_to_bigquery
