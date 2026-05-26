import shlex
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
PROJECT_DIR = "/opt/airflow/project"
raw_source_url = "https://data.cityofnewyork.us/resource/4b4i-vvec.json"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def get_variable(name, default=None, required=False):
    value = Variable.get(name, default_var=default)

    if required and (value is None or value == ""):
        raise ValueError(f"Airflow Variable '{name}' is required.")

    return value


def run_command(command):
    print("Running command:")
    print(" ".join(shlex.quote(part) for part in command))
    subprocess.run(command, check=True)


def raw_input_path(pipeline_run_id):
    return (
        f"s3a://{get_variable('minio_bucket', 'airflow-bucket')}/"
        f"{get_variable('raw_zone_prefix', 'raw')}/"
        f"{get_variable('dataset_name', 'api_csv')}/"
        f"ingestion_date={pipeline_run_id}/run_id={pipeline_run_id}"
    )


def raw_ingest_task(pipeline_run_id):
    command = [
        "spark-submit",
        "--packages",
        SPARK_PACKAGES,
        f"{PROJECT_DIR}/batch_pipeline.py",
        "--source-url",
        get_variable("raw_source_url", required=True),
        "--dataset-name",
        get_variable("dataset_name", "api_csv"),
        "--bucket",
        get_variable("minio_bucket", "airflow-bucket"),
        "--raw-prefix",
        get_variable("raw_zone_prefix", "raw"),
        "--minio-endpoint",
        get_variable("minio_endpoint", "http://minio:9000"),
        "--write-mode",
        get_variable("raw_write_mode", "overwrite"),
        "--run-id",
        pipeline_run_id,
    ]

    run_command(command)


def transform_task(pipeline_run_id):
    command = [
        "spark-submit",
        "--packages",
        SPARK_PACKAGES,
        f"{PROJECT_DIR}/transform_pipeline.py",
        "--dataset-name",
        get_variable("dataset_name", "api_csv"),
        "--raw-input-path",
        raw_input_path(pipeline_run_id),
        "--lookup-path",
        get_variable("lookup_path", required=True),
        "--lookup-format",
        get_variable("lookup_format", "parquet"),
        "--dedupe-keys",
        get_variable("dedupe_keys", ""),
        "--join-left-keys",
        get_variable("join_left_keys", required=True),
        "--join-right-keys",
        get_variable(
            "join_right_keys",
            get_variable("join_left_keys", required=True),
        ),
        "--join-type",
        get_variable("join_type", "left"),
        "--event-date-col",
        get_variable("event_date_col", required=True),
        "--metric-columns",
        get_variable("metric_columns", required=True),
        "--group-by-columns",
        get_variable("group_by_columns", ""),
        "--rolling-window-days",
        get_variable("rolling_window_days", "7"),
        "--bucket",
        get_variable("minio_bucket", "airflow-bucket"),
        "--raw-prefix",
        get_variable("raw_zone_prefix", "raw"),
        "--clean-prefix",
        get_variable("clean_zone_prefix", "clean"),
        "--minio-endpoint",
        get_variable("minio_endpoint", "http://minio:9000"),
        "--write-mode",
        get_variable("clean_write_mode", "overwrite"),
        "--run-id",
        pipeline_run_id,
    ]

    transform_cast_args = get_variable("transform_cast_args", "")
    lookup_cast_args = get_variable("lookup_cast_args", "")

    if transform_cast_args:
        command.extend(shlex.split(transform_cast_args))

    if lookup_cast_args:
        command.extend(shlex.split(lookup_cast_args))

    run_command(command)


def quality_check_task(pipeline_run_id):
    command = [
        "spark-submit",
        "--packages",
        SPARK_PACKAGES,
        f"{PROJECT_DIR}/quality_pipeline.py",
        "--dataset-name",
        get_variable("dataset_name", "api_csv"),
        "--run-id",
        pipeline_run_id,
        "--key-columns",
        get_variable(
            "quality_key_columns",
            get_variable("join_left_keys", required=True),
        ),
        "--date-column",
        get_variable("quality_date_column", "_event_date"),
        "--min-rows",
        get_variable("quality_min_rows", "1"),
        "--max-rows",
        get_variable("quality_max_rows", "1000000000"),
        "--bucket",
        get_variable("minio_bucket", "airflow-bucket"),
        "--clean-prefix",
        get_variable("clean_zone_prefix", "clean"),
        "--validated-folder",
        get_variable("validated_folder", "validated_records"),
        "--minio-endpoint",
        get_variable("minio_endpoint", "http://minio:9000"),
        "--write-mode",
        get_variable("validated_write_mode", "overwrite"),
    ]

    run_command(command)


with DAG(
    dag_id="raw_to_clean_minio_pipeline_python_operator",
    default_args=default_args,
    description="Daily raw CSV ingestion, transformation, and validation using PythonOperator.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["pyspark", "minio", "python-operator"],
) as dag:
    raw_ingest = PythonOperator(
        task_id="raw_ingest",
        python_callable=raw_ingest_task,
        op_kwargs={"pipeline_run_id": "{{ ds_nodash }}"},
        retries=2,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        op_kwargs={"pipeline_run_id": "{{ ds_nodash }}"},
        retries=2,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check_task,
        op_kwargs={"pipeline_run_id": "{{ ds_nodash }}"},
        retries=2,
    )

    raw_ingest >> transform >> quality_check
