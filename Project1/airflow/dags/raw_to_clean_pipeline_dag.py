from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)
PROJECT_DIR = "/opt/airflow/project"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="raw_to_clean_minio_pipeline",
    default_args=default_args,
    description="Daily raw CSV ingestion, transformation, and validation pipeline.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["pyspark", "minio"],
) as dag:
    raw_ingest = BashOperator(
        task_id="raw_ingest",
        retries=2,
        bash_command=f"""
        set -euo pipefail

        spark-submit \\
          --packages {SPARK_PACKAGES} \\
          {PROJECT_DIR}/batch_pipeline.py \\
          --source-url "{{{{ var.value.raw_source_url }}}}" \\
          --dataset-name "{{{{ var.value.get('dataset_name', 'api_csv') }}}}" \\
          --bucket "{{{{ var.value.get('minio_bucket', 'airflow-bucket') }}}}" \\
          --raw-prefix "{{{{ var.value.get('raw_zone_prefix', 'raw') }}}}" \\
          --minio-endpoint "{{{{ var.value.get('minio_endpoint', 'http://minio:9000') }}}}" \\
          --write-mode "{{{{ var.value.get('raw_write_mode', 'overwrite') }}}}" \\
          --run-id "{{{{ ds_nodash }}}}"
        """,
    )

    transform = BashOperator(
        task_id="transform",
        retries=2,
        bash_command=f"""
        set -euo pipefail

        spark-submit \\
          --packages {SPARK_PACKAGES} \\
          {PROJECT_DIR}/transform_pipeline.py \\
          --dataset-name "{{{{ var.value.get('dataset_name', 'api_csv') }}}}" \\
          --raw-input-path "s3a://{{{{ var.value.get('minio_bucket', 'airflow-bucket') }}}}/{{{{ var.value.get('raw_zone_prefix', 'raw') }}}}/{{{{ var.value.get('dataset_name', 'api_csv') }}}}/ingestion_date={{{{ ds_nodash }}}}/run_id={{{{ ds_nodash }}}}" \\
          --lookup-path "{{{{ var.value.lookup_path }}}}" \\
          --lookup-format "{{{{ var.value.get('lookup_format', 'parquet') }}}}" \\
          --dedupe-keys "{{{{ var.value.get('dedupe_keys', '') }}}}" \\
          {{{{ var.value.get('transform_cast_args', '') }}}} \\
          {{{{ var.value.get('lookup_cast_args', '') }}}} \\
          --join-left-keys "{{{{ var.value.join_left_keys }}}}" \\
          --join-right-keys "{{{{ var.value.get('join_right_keys', var.value.join_left_keys) }}}}" \\
          --join-type "{{{{ var.value.get('join_type', 'left') }}}}" \\
          --event-date-col "{{{{ var.value.event_date_col }}}}" \\
          --metric-columns "{{{{ var.value.metric_columns }}}}" \\
          --group-by-columns "{{{{ var.value.get('group_by_columns', '') }}}}" \\
          --rolling-window-days "{{{{ var.value.get('rolling_window_days', '7') }}}}" \\
          --bucket "{{{{ var.value.get('minio_bucket', 'airflow-bucket') }}}}" \\
          --raw-prefix "{{{{ var.value.get('raw_zone_prefix', 'raw') }}}}" \\
          --clean-prefix "{{{{ var.value.get('clean_zone_prefix', 'clean') }}}}" \\
          --minio-endpoint "{{{{ var.value.get('minio_endpoint', 'http://minio:9000') }}}}" \\
          --write-mode "{{{{ var.value.get('clean_write_mode', 'overwrite') }}}}" \\
          --run-id "{{{{ ds_nodash }}}}"
        """,
    )

    quality_check = BashOperator(
        task_id="quality_check",
        retries=2,
        bash_command=f"""
        set -euo pipefail

        spark-submit \\
          --packages {SPARK_PACKAGES} \\
          {PROJECT_DIR}/quality_pipeline.py \\
          --dataset-name "{{{{ var.value.get('dataset_name', 'api_csv') }}}}" \\
          --run-id "{{{{ ds_nodash }}}}" \\
          --key-columns "{{{{ var.value.get('quality_key_columns', var.value.join_left_keys) }}}}" \\
          --date-column "{{{{ var.value.get('quality_date_column', '_event_date') }}}}" \\
          --min-rows "{{{{ var.value.get('quality_min_rows', '1') }}}}" \\
          --max-rows "{{{{ var.value.get('quality_max_rows', '1000000000') }}}}" \\
          --bucket "{{{{ var.value.get('minio_bucket', 'airflow-bucket') }}}}" \\
          --clean-prefix "{{{{ var.value.get('clean_zone_prefix', 'clean') }}}}" \\
          --validated-folder "{{{{ var.value.get('validated_folder', 'validated_records') }}}}" \\
          --minio-endpoint "{{{{ var.value.get('minio_endpoint', 'http://minio:9000') }}}}" \\
          --write-mode "{{{{ var.value.get('validated_write_mode', 'overwrite') }}}}"
        """,
    )

    raw_ingest >> transform >> quality_check
