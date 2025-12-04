from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# --- CONFIGURATION FROM YOUR IMAGE ---
# The connection ID you created in Airflow Admin > Connections
POSTGRES_CONN_ID = "applai_postgres_db"

# The GCS Bucket name from the image
GCS_BUCKET = "db-to-datalake"

# The BigQuery destination. 
# "applai-dwh" is the Project ID and "airflow_landing" is the Dataset.
# I added '.transferred_data' as the table name (you can change this).
BQ_DESTINATION_TABLE = "applai-dwh.airflow_landing.transferred_data_abdulrahman"

# The file name to use temporarily in GCS
FILENAME = "postgres_export.csv"

# The SQL query you want to run on Postgres (You need to change this!)
SQL_QUERY = "SELECT * FROM \"Order\";"

with DAG(
    dag_id="practice2_db_transfer_job",
    schedule=None, 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["practice", "postgres", "bigquery"],
) as dag:

    # Step 1: Export data from Postgres to GCS (CSV format)
    extract_to_gcs = PostgresToGCSOperator(
        task_id="extract_postgres_to_gcs",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_QUERY,
        bucket=GCS_BUCKET,
        filename=FILENAME,
        export_format="csv",
        gzip=False,  # Set to True if you want to compress data
    )

    # Step 2: Load data from GCS to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=[FILENAME],
        destination_project_dataset_table=BQ_DESTINATION_TABLE,
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE", # Options: WRITE_TRUNCATE (overwrite), WRITE_APPEND
        autodetect=True, # Automatically figure out the schema (Integer, String, etc.)
        skip_leading_rows=0, # Use 1 if your CSV export includes a header row
    )

    # Set dependency: Extract must finish before Load starts
    extract_to_gcs >> load_to_bigquery