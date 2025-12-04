from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

POSTGRES_CONN_ID = "applai_postgress_db"
GCS_BUCKET = "db_to_datalake"
GCS_FILE = "maryam_order"
BQ_PROJECT = "applai-dwh"
BQ_DATASET = "applai-dwh.airflow_landing"
BQ_TABLE = "Order"

with DAG(
    dag_id="maryam_postgres_to_bigquery",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["maryam", "postgres", "bigquery"]
):

    # Step 1: Extract from Postgres → Upload to GCS as JSON
    extract_to_gcs = PostgresToGCSOperator(
        task_id="extract_postgres_to_gcs",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="SELECT * FROM Order;",  # your SQL
        bucket=GCS_BUCKET,
        export_format="json",
    )

    # Step 2: Load GCS data → BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_gcs_to_bq",
        bucket=GCS_BUCKET,
        source_objects=[GCS_FILE],
        destination_project_dataset_table=f"{BQ_DATASET}",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
    )

    extract_to_gcs >> load_to_bq
