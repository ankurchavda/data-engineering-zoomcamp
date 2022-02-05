import os
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage


AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
URL = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'

CSV_FILENAME = 'zones.csv'
CSV_OUTFILE = f'{AIRFLOW_HOME}/zones/{CSV_FILENAME}'

PARQUET_FILENAME = 'zones.parquet'
PARQUET_OUTFILE = f'{AIRFLOW_HOME}/zones/{PARQUET_FILENAME}'

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')


def convert_to_parquet(csv_file, parquet_file):
    if not csv_file.endswith('csv'):
        raise ValueError('The input file is not in csv format')
    
    # Path(f'{AIRFLOW_HOME}/fhv_tripdata/parquet').mkdir(parents=True, exist_ok=True) 
    
    table=pv.read_csv(csv_file)
    pq.write_table(table, parquet_file)


def upload_to_gcs(file_path, bucket_name, blob_name):
    """
    Upload the downloaded file to GCS
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)


default_args = {
    "owner" : "airflow"
}

with DAG(
    'ingest_zones',
    default_args=default_args,
    description='Ingest zones data to GCS',
    schedule_interval='@once',
    start_date=datetime(2021,1,1),
    catchup=True
) as dag:

    download_csv_file_task=BashOperator(
        task_id='download_csv_file',
        # bash_command=f'curl -sSLf {URL} > {CSV_OUTFILE}'
        bash_command=f'echo {URL} > {CSV_OUTFILE}'
    )

    convert_to_parquet_task=PythonOperator(
        task_id='convert_to_parquet',
        python_callable=convert_to_parquet,
        op_kwargs={
            'csv_file' : CSV_OUTFILE,
            'parquet_file' : PARQUET_OUTFILE
        }
    )

    upload_to_gcs_task=PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'file_path' : CSV_OUTFILE,
            'bucket_name' : GCP_GCS_BUCKET,
            'blob_name' : f'parquet/zones/{PARQUET_FILENAME}'
        }
    )

    remove_files_from_local_task=BashOperator(
        task_id='remove_files_from_local',
        #cd into the directory and then find and delete all files with csv or parquet extension
        bash_command=f'cd {AIRFLOW_HOME}/zones/ &&  rm {CSV_FILENAME} {PARQUET_FILENAME}'
    )

    download_csv_file_task >> convert_to_parquet_task >> upload_to_gcs_task >> remove_files_from_local_task
