from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
import pandas as pd

POSTGRES_CONN = "applai-postgress_db"
GCP_CONN = "gcp_conn"

GCS_BUCKET = "db-to-datalake"
GCS_FILE = "postgres/data.csv"

BQ_PROJECT = "my_project"
BQ_DATASET = "my_dapplai-dwh.airflow_landing"
BQ_TABLE = "Order"


def extract_from_postgres():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    df = hook.get_pandas_df("SELECT * FROM source_table;")
    df.to_csv("/tmp/data.csv", index=False)


with DAG(
    "postgres_to_bigquery_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
):

    extract_task = PythonOperator(
        task_id="extract_postgres",
        python_callable=extract_from_postgres
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/tmp/data.csv",
        dst=GCS_FILE,
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONN
    )

    load_to_bq = BigQueryInsertJobOperator(
        task_id="load_to_bigquery",
        gcp_conn_id=GCP_CONN,
        configuration={
            "load": {
                "sourceUris": [f"gs://{GCS_BUCKET}/{GCS_FILE}"],
                "destinationTable": {
                    "projectId": BQ_PROJECT,
                    "datasetId": BQ_DATASET,
                    "tableId": BQ_TABLE,
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    extract_task >> upload_to_gcs >> load_to_bq
