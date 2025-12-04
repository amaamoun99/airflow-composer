from airflow import DAG
from airflow.providers.postgres.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

PROJECT_ID = "applai-postgres DB-id"
BUCKET_NAME = "db-to-datalake"
DATASET = "applai-dwh.airflow_landing"
TABLE = "Order"

with DAG(
    dag_id="postgres_to_bigquery_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "bigquery", "etl"],
) as dag:

    # 1️⃣ Postgres → GCS
    postgres_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        postgres_conn_id="postgres_default",
        sql=f"SELECT * FROM {TABLE}",
        bucket=BUCKET_NAME,
        filename="postgres_export/{{ ds }}/data.json",
        export_format="json",
    )

    # 2️⃣ GCS → BigQuery
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["postgres_export/{{ ds }}/data.json"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.{TABLE}",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",  # or WRITE_APPEND
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="google_cloud_default",
    )

    postgres_to_gcs >> gcs_to_bigquery
