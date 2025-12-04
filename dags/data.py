from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

POSTGRES_CONN_ID = "applai_postgres_db"
GCS_BUCKET = "db-to-datalake"
BIGQUERY_DATASET = "applai-dwh.airflow_landing"

TABLE_NAME = "Order"   # 🔥 Change this table name

with DAG(
    dag_id="postgres_to_bigquery_operator_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "gcs", "bigquery"],
):

    # 1️⃣ Extract data from Postgres → Store in GCS (JSON)
    extract_to_gcs = PostgresToGCSOperator(
        task_id="extract_postgres_to_gcs",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT * FROM 'Order';",
        bucket=GCS_BUCKET,
        filename=f"landing/{TABLE_NAME}.json",
        export_format="json",  # BigQuery supports NDJSON
    )

    # 2️⃣ Load from GCS → BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[f"landing/{TABLE_NAME}.json"],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.{TABLE_NAME}",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",   # Replace table on each load
    )

    extract_to_gcs >> load_to_bigquery
