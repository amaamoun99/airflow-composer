from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# --- CONFIGURATION ---
POSTGRES_CONN_ID = 'applai_postgres_db'
GCS_BUCKET = 'db-to-datalake'
FILENAME = 'order_export.json'

# Exact destination path requested
# Format: ProjectID.DatasetID.TableID
DESTINATION_TABLE = 'applai-dwh.airflow_landing.Order_nora' 

with DAG(
    dag_id='move_order_to_airflow_landing',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['practice', 'order_transfer']
) as dag:

    # Task 1: Export 'Order' table to GCS
    # We use "Order" (with double quotes) because Order is a reserved SQL keyword
    extract_to_gcs = PostgresToGCSOperator(
        task_id='extract_order_to_gcs',
        postgres_conn_id=POSTGRES_CONN_ID,
        gcp_conn_id='google_cloud_default',
        sql='SELECT * FROM public."Order";', 
        bucket=GCS_BUCKET,
        filename=FILENAME,
        export_format='json',
        write_on_empty=False
    )

    # Task 2: Load into BigQuery
    # Target: applai-dwh.airflow_landing.Order
    load_to_bq = GCSToBigQueryOperator(
        task_id='load_order_to_bq',
        gcp_conn_id='google_cloud_default',
        bucket=GCS_BUCKET,
        source_objects=[FILENAME],
        destination_project_dataset_table=DESTINATION_TABLE,
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect=True
    )

    extract_to_gcs >> load_to_bq