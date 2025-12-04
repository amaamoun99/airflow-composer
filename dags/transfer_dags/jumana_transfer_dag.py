from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="postgres_to_bq_order",
    start_date=days_ago(1),
    schedule_interval="@daily"
):

    export_from_postgres = PostgresToGCSOperator(
        task_id="export_postgres_to_gcs",
        postgres_conn_id="applai_postgres_db",
        sql="SELECT * FROM public.order;",
        bucket="db-to-datalake",
        filename="order/order_{{ ds }}.json",
        export_format="json"
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="db-to-datalake",
        source_objects=["order/order_{{ ds }}.json"],
        destination_project_dataset_table="applai_dwh.airflow_landing.Order",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND"
    )

    export_from_postgres >> load_to_bigquery
