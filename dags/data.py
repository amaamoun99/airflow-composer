from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Constants
POSTGRES_CONN_ID = "applai_postgres_db"
GCS_BUCKET = "db-to-datalake"
BIGQUERY_DATASET = "applai-dwh.airflow_landing"
FILENAME = "postgres_export.csv"
LOCAL_PATH = f"/tmp/{FILENAME}"
GCS_PATH = f"landing/{FILENAME}"


def extract_from_postgres():
    """Extract data from Postgres and save it as CSV."""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = "SELECT * FROM 'Order'"   # 🔥 Change table name here

    df = pg_hook.get_pandas_df(sql)
    df.to_csv(LOCAL_PATH, index=False)


# -----------------------------------------
# DAG Definition
# -----------------------------------------
with DAG(
    dag_id="pg_to_bq_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["postgres", "bigquery", "gcs"],
) as dag:

    # Step 1: Extract from Postgres → local CSV
    extract_task = PythonOperator(
        task_id="extract_postgres",
        python_callable=extract_from_postgres
    )

    # Step 2: Upload CSV to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=LOCAL_PATH,
        dst=GCS_PATH,
        bucket=GCS_BUCKET,
        mime_type="text/csv"
    )

    # Step 3: Load GCS → BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[GCS_PATH],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.your_table_name",  # 🔥 Change table name
        autodetect=True,
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",   # Replace entire table
        skip_leading_rows=1
    )

    extract_task >> upload_to_gcs >> load_to_bigquery
