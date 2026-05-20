from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3

BUCKET = "airflow-bucket"

def upload_file():

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    s3.put_object(
        Bucket=BUCKET,
        Key="hello.txt",
        Body="Hello from Airflow + MinIO"
    )

    print("Upload successful")

with DAG(
    dag_id="test_minio",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):

    upload = PythonOperator(
        task_id="upload_file",
        python_callable=upload_file,
    )