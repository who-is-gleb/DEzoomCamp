import datetime
import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2019, 1, 1),
    "depends_on_past": False,
    "retries": 4,
}


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    gcs_2_gcs_task = GCSToGCSOperator(
        task_id="gcs_2_gcs_task",
        source_bucket=BUCKET,
        source_object='raw/fhv_tripdata*.parquet',
        destination_bucket=BUCKET,
        destination_object='fhv/fhv_tripdata',
        move_object=True,
    )

    gcs_2_bq_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table_fhv_tripdata",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/fhv/*"],
            },
        },
    )

    CREATE_PARTITION_TABLE_QUERY = f"""
        CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.fhv_tripdata_partitoned AS
        SELECT * FROM {BIGQUERY_DATASET}.external_table_fhv_tripdata;
    """
    # dispatching_base_num
    bq_external_table_to_partitioned = BigQueryInsertJobOperator(
        task_id='bq_external_table_to_partitioned',
        configuration={
            "query": {
                "query": CREATE_PARTITION_TABLE_QUERY,
                "useLegacySql": False,
            }
        },
    )

    gcs_2_gcs_task >> gcs_2_bq_external_table_task >> bq_external_table_to_partitioned