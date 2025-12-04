from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

BQ = "applai-dwh.airflow_landing"
GCS_file="rinad_Order"
BQ_table="Order"
with DAG(
    dag_id="postgres_to_bigquery_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # run manually
    catchup=False,
    
) as dag:

    # 1️⃣ Export from PostgreSQL → GCS
    export_postgres = PostgresToGCSOperator(
        task_id="export_postgres_to_gcs",
        sql="SELECT * FROM Order ;",     # your query
        bucket="db-to-datalake",
        filename="my_table.json",
        postgres_conn_id="applai_postgres_db",   # your Airflow connection
        export_format="json",   # BigQuery prefers JSON
    )

    # 2️⃣ Load from GCS → BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="db-to-datalake",
        source_objects=GCS_file,
        destination_project_dataset_table=f"{BQ}",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",   # overwrite table
        autodetect=True,
    )

    export_postgres >> load_to_bigquery