import logging
import os
from datetime import datetime
import pyarrow.csv as pv
import pyarrow.parquet as pq
from google.cloud import storage
from airflow import DAG, macros
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

year = '{{ macros.datetime.strptime(ds, "%Y-%M-%d").strftime("%Y") }}'
month = '{{ macros.datetime.strptime(ds, "%Y-%M-%d").strftime("%M") }}'
dataset_file = f"fhv_tripdata_{year}-{month}.csv"
dataset_url = f"https://nyc-tlc.s3.amazonaws.com/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv','.parquet')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Only CSV files accepted")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv','.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "retries":1,
    "depends_on_past": False,
    "start_date" : datetime(2019,1,1),
    "end_date":datetime(2019,12,1)
}


with DAG(
    dag_id="for_hire_vehicles",
    schedule_interval="0 0 1 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=3
) as dag:

    download_dataset_task = BashOperator(
        task_id="downloading_dataset_task",
        bash_command = f"curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}"
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name" : f"for_hire_vehicles/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )
    

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task 
