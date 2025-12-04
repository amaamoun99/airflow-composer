from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from datetime import datetime

# -------------------------
# CONFIGURATION
# -------------------------
POSTGRES_CONN_ID = "applai_postgres_db"
GCS_BUCKET = "db-to-datalake"
GCS_FILENAME = "order_table_export.json"
BIGQUERY_DATASET = "applai-dwh.airflow_landing"
BIGQUERY_TABLE = "order_table"

default_args = {
    "retries": 1,
}

# -------------------------
# DAG DEFINITION
# -------------------------
with DAG(
    dag_id="order_table_transfer_dag",
    description="Transfer 'Order' table from PostgreSQL to GCS, then load into BigQuery",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    # ------------------------------------
    # 1. Extract from PostgreSQL → Upload to GCS
    # ------------------------------------
    export_order_to_gcs = PostgresToGCSOperator(
        task_id="export_order_table_to_gcs",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='SELECT * FROM "Order";',
        bucket=GCS_BUCKET,
        filename=GCS_FILENAME,
        export_format="json",
    )

    # ------------------------------------
    # 2. Load JSON from GCS → BigQuery
    # ------------------------------------
    load_order_to_bigquery = GCSToBigQueryOperator(
        task_id="load_order_table_into_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[GCS_FILENAME],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.{BIGQUERY_TABLE}",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )

    # Task dependency
    export_order_to_gcs >> load_order_to_bigquery
