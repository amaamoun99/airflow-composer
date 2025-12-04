from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

POSTGRES_CONN_ID = "applai_postgres_db"
GCS_BUCKET = "db-to-datalake"
BIGQUERY_DATASET = "applai-dwh.airflow_landing"

TABLE_NAME = "your_table_name"   # 🔥 change this
GCS_PATH = f"landing/{TABLE_NAME}.json"


with DAG(
    dag_id="pg_to_bq_operator_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "gcs", "bigquery"],
):

    # 1️⃣ Extract Postgres → GCS (JSON)
    postgres_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"SELECT * FROM 'Order';",
        bucket=GCS_BUCKET,
        filename=GCS_PATH,
        export_format="json",
        gzip=False,
    )

    # 2️⃣ Load GCS → BigQuery
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[GCS_PATH],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.{TABLE_NAME}",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
    )

    postgres_to_gcs >> gcs_to_bigquery
