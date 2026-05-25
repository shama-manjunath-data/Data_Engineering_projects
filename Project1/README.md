# Batch Data Pipeline

This project runs a daily Airflow pipeline that ingests raw CSV data from an API,
stores it in MinIO, transforms it with PySpark, validates it with Great
Expectations, and publishes validated Parquet.

## Architecture

```text
                   Docker Compose
+--------------------------------------------------------------+
|                                                              |
|  +------------+        +------------------+                  |
|  | Airflow UI | <----> | Airflow Postgres |                  |
|  +-----+------+        +------------------+                  |
|        |                                                     |
|        v                                                     |
|  +--------------------------------------------------------+  |
|  | Airflow Scheduler                                      |  |
|  | raw_ingest >> transform >> quality_check               |  |
|  +-----+------------------+------------------+-------------+  |
|        |                  |                  |                |
|        v                  v                  v                |
|  +-----------+      +-------------+      +------------------+ |
|  | API CSV   | ---> | PySpark Jobs| ---> | Great Expectations| |
|  +-----------+      +------+------+      +---------+--------+ |
|                            |                       |          |
|                            v                       v          |
|                     +-----------------------------------+     |
|                     | MinIO S3-compatible lake           |     |
|                     | raw / clean / validated_records    |     |
|                     +-----------------------------------+     |
|                                                              |
+--------------------------------------------------------------+
```

Airflow orchestrates the pipeline. Postgres stores Airflow metadata. MinIO acts
as the local S3 data lake. PySpark performs ingestion and transformations, and
Great Expectations blocks bad data before it is published to the validated zone.

## Pipeline

```text
raw_ingest >> transform >> quality_check
```

The DAGs are in `airflow/dags`:

- `raw_to_clean_pipeline_dag.py`: uses `BashOperator`
- `raw_to_clean_python_operator_dag.py`: uses `PythonOperator`

Both DAGs run daily and set `retries=2` on each task.

## Run Inside Docker

Start from the project directory:

```powershell
cd "C:\<project-path>\Data_Engineering_projects\Project1"
```

Create local Airflow folders if they do not exist:

```powershell
New-Item -ItemType Directory -Force airflow\dags,airflow\logs,airflow\plugins,airflow\config
```

Initialize Airflow:

```powershell
docker compose up airflow-init
```

Start the services:

```powershell
docker compose up -d postgres minio mc airflow-webserver airflow-scheduler
```

Open the UIs:

```text
Airflow: http://localhost:8080
Username: admin
Password: admin

MinIO Console: http://localhost:9001
Username: minioadmin
Password: minioadmin
```

Set the required Airflow Variables from inside Docker:

```powershell
docker compose exec airflow-webserver airflow variables set raw_source_url "https://example.com/data.csv"
docker compose exec airflow-webserver airflow variables set dataset_name "orders"
docker compose exec airflow-webserver airflow variables set lookup_path "s3a://airflow-bucket/lookups/customers.parquet"
docker compose exec airflow-webserver airflow variables set join_left_keys "customer_id"
docker compose exec airflow-webserver airflow variables set event_date_col "order_date"
docker compose exec airflow-webserver airflow variables set metric_columns "amount"
docker compose exec airflow-webserver airflow variables set quality_key_columns "order_id,customer_id"
docker compose exec airflow-webserver airflow variables set transform_cast_args "--cast order_id:string --cast customer_id:string --cast order_date:date --cast amount:double"
```

Optional variables:

```powershell
docker compose exec airflow-webserver airflow variables set dedupe_keys "order_id"
docker compose exec airflow-webserver airflow variables set group_by_columns "region"
docker compose exec airflow-webserver airflow variables set rolling_window_days "7"
docker compose exec airflow-webserver airflow variables set quality_min_rows "1"
docker compose exec airflow-webserver airflow variables set quality_max_rows "1000000"
```

Trigger the BashOperator DAG:

```powershell
docker compose exec airflow-webserver airflow dags trigger raw_to_clean_minio_pipeline
```

Or trigger the PythonOperator DAG:

```powershell
docker compose exec airflow-webserver airflow dags trigger raw_to_clean_minio_pipeline_python_operator
```

Watch logs:

```powershell
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

Stop the stack:

```powershell
docker compose down
```

Note: the DAG tasks run `spark-submit` inside the Airflow container. The current
compose file installs Python packages from `requirements.txt`, including
`pyspark` and `great_expectations`, but the container also needs Java available
for Spark. If `spark-submit` fails with a Java error, build a custom Airflow
image with a JRE/JDK installed before running the DAG.

## MinIO Layout

Default bucket: `airflow-bucket`

```text
raw/<dataset_name>/ingestion_date=YYYYMMDD/run_id=YYYYMMDD/
clean/<dataset_name>/records/run_id=YYYYMMDD/
clean/<dataset_name>/daily_totals/run_id=YYYYMMDD/
clean/<dataset_name>/rolling_averages/window_days=N/run_id=YYYYMMDD/
clean/<dataset_name>/validated_records/run_id=YYYYMMDD/year=YYYY/month=M/
```

## Raw Schema

The raw ingestion job reads the API CSV with all source columns as strings.
It adds these metadata columns:

| Column | Type | Description |
| --- | --- | --- |
| `_ingested_at` | timestamp | Time the raw file was processed by Spark |
| `_source_url` | string | API endpoint used for ingestion |
| `_source_file` | string | Local CSV file path read by Spark |
| `_run_id` | string | Pipeline run id |
| `_raw_zone` | string | Raw zone prefix, usually `raw` |

## Clean Records Schema

The transform job deduplicates, casts configured columns, joins a lookup table,
and writes clean records.

Source columns are controlled by Airflow Variables:

| Variable | Purpose |
| --- | --- |
| `dedupe_keys` | Comma-separated columns used to remove duplicates |
| `transform_cast_args` | Cast rules, for example `--cast order_date:date --cast amount:double` |
| `lookup_path` | Lookup table path in MinIO |
| `lookup_format` | `parquet`, `csv`, or `json` |
| `lookup_cast_args` | Optional cast rules for lookup columns |
| `join_left_keys` | Source join keys |
| `join_right_keys` | Lookup join keys; defaults to `join_left_keys` |

Clean records add these metadata columns:

| Column | Type | Description |
| --- | --- | --- |
| `_event_date` | date | Date used for aggregation and partition logic |
| `_processed_at` | timestamp | Time the transform completed |
| `_transform_run_id` | string | Transform run id |
| `_clean_zone` | string | Clean zone prefix, usually `clean` |

## Aggregate Schemas

Daily totals are written to `clean/<dataset_name>/daily_totals`.

| Column | Type | Description |
| --- | --- | --- |
| `_event_date` | date | Aggregation date |
| `<group_by_columns>` | configured | Optional dimensions such as region or product |
| `<metric>_daily_total` | numeric | Daily total for each configured metric |

Rolling averages are written to `clean/<dataset_name>/rolling_averages`.

| Column | Type | Description |
| --- | --- | --- |
| `_event_date` | date | Aggregation date |
| `<group_by_columns>` | configured | Optional dimensions |
| `<metric>_daily_total` | numeric | Daily total for each metric |
| `<metric>_rolling_avg_<N>d` | double | Rolling average over the configured window |

## Data Quality Checks

The `quality_check` task uses Great Expectations and fails the Airflow pipeline
if any check fails.

Checks:

- key columns contain no nulls
- row count is between `quality_min_rows` and `quality_max_rows`
- date column contains no nulls
- date column has no future dates

Required or commonly used Airflow Variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `quality_key_columns` | `join_left_keys` | Comma-separated columns that must be non-null |
| `quality_date_column` | `_event_date` | Date column checked for nulls and future dates |
| `quality_min_rows` | `1` | Minimum expected record count |
| `quality_max_rows` | `1000000000` | Maximum expected record count |

## Validated Records Schema

After Great Expectations passes, validated records are written as partitioned
Parquet under:

```text
clean/<dataset_name>/validated_records/run_id=YYYYMMDD/year=YYYY/month=M/
```

Validated records contain all clean record columns plus:

| Column | Type | Description |
| --- | --- | --- |
| `_validated_at` | timestamp | Time validation succeeded |
| `_quality_run_id` | string | Quality check run id |
| `year` | int | Partition year derived from `quality_date_column` |
| `month` | int | Partition month derived from `quality_date_column` |

## Required Airflow Variables

At minimum, set:

```text
raw_source_url
lookup_path
join_left_keys
event_date_col
metric_columns
quality_key_columns
```

Useful optional variables:

```text
dataset_name
dedupe_keys
group_by_columns
transform_cast_args
lookup_cast_args
rolling_window_days
quality_min_rows
quality_max_rows
```
