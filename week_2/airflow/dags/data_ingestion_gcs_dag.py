from fileinput import filename
import os
import pyarrow.csv as pv
import pyarrow.parquet as pq

from datetime import datetime, timedelta

from pendulum import time
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage

# DATASET_FILE_NAME = "yellow_tripdata_2021-01.csv"
# URL = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{DATASET_FILE_NAME}"


DATASET_FILE_NAME = "yellow_tripdata_2021-01.csv"
DATASET_PARQUET_NAME = DATASET_FILE_NAME.replace('csv', 'parquet')
URL = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{DATASET_FILE_NAME}"

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
PATH_TO_AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
BIG_QUERY_TABLE_NAME = 'yellow_taxi_trips'

def convert_to_parquet(file_path):
    """
    Convert the provided csv file to parquet using PyArrow
    """
    if not file_path.endswith('.csv'):
        raise ValueError('The passed file is not in csv format.')
    table = pv.read_csv(file_path)
    pq.write_table(table, f'{PATH_TO_AIRFLOW_HOME}/{DATASET_PARQUET_NAME}')


def upload_to_gcs(file_path, bucket_name, blob_name):
    """
    Upload the downloaded file to GCS
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)


default_args = {
    'owner' : 'airflow',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
} 

with DAG(
    dag_id='data_ingestion_gcs',
    default_args=default_args,
    description="Insert data into Google Cloud Storage",
    schedule_interval="@daily",
    start_date=datetime(2022,1,26),
    catchup=False,
    tags=['dtc-de-dags']
) as dag:

    download_csv_task=BashOperator(
        task_id='download_csv',
        bash_command=f'curl -sS {URL} > {PATH_TO_AIRFLOW_HOME}/{DATASET_FILE_NAME}'
    )

    convert_to_parquet_task=PythonOperator(
        task_id='covert_to_parquet',
        python_callable=convert_to_parquet,
        op_kwargs={
            'file_path':f'{PATH_TO_AIRFLOW_HOME}/{DATASET_FILE_NAME}'
            }
    )

    upload_to_gcs_task=PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'file_path' : f'{PATH_TO_AIRFLOW_HOME}/{DATASET_PARQUET_NAME}',
            'bucket_name' : GCP_GCS_BUCKET,
            'blob_name' : f'raw/{DATASET_PARQUET_NAME}'
        }
    )

    create_external_table_task = BigQueryCreateExternalTableOperator(
    task_id="create_external_table",
    table_resource={
        "tableReference": {
            "projectId": GCP_PROJECT_ID,
            "datasetId": BIGQUERY_DATASET,
            "tableId": BIG_QUERY_TABLE_NAME,
        },
        "externalDataConfiguration": {
            "sourceFormat": "PARQUET",
            "sourceUris": [f'gs://{GCP_GCS_BUCKET}/raw/{DATASET_PARQUET_NAME}'],
        },
    },
    )

    download_csv_task >> convert_to_parquet_task >> upload_to_gcs_task >> create_external_table_task