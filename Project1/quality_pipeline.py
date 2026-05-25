import argparse
import json
import os
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, month, to_date, year

try:
    from great_expectations.dataset import SparkDFDataset
except ImportError as exc:
    raise ImportError(
        "Great Expectations is required for data quality checks. "
        "Install it with the project requirements."
    ) from exc


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate clean records with Great Expectations and publish validated Parquet."
    )
    parser.add_argument(
        "--dataset-name",
        required=True,
        help="Dataset folder name used under the clean zone.",
    )
    parser.add_argument(
        "--clean-records-path",
        default=None,
        help="Optional full clean records path. Defaults to clean/dataset/records/run_id=run-id.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Pipeline run id. Defaults to current UTC timestamp.",
    )
    parser.add_argument(
        "--key-columns",
        required=True,
        help="Comma-separated key columns that must not contain nulls.",
    )
    parser.add_argument(
        "--date-column",
        default="_event_date",
        help="Date column used to reject future-dated records.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=1,
        help="Minimum expected row count.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=1000000000,
        help="Maximum expected row count.",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("MINIO_BUCKET", "airflow-bucket"),
        help="MinIO bucket name.",
    )
    parser.add_argument(
        "--clean-prefix",
        default=os.getenv("CLEAN_ZONE_PREFIX", "clean"),
        help="Clean zone prefix inside the bucket.",
    )
    parser.add_argument(
        "--validated-folder",
        default="validated_records",
        help="Folder under clean/dataset where validated records are written.",
    )
    parser.add_argument(
        "--minio-endpoint",
        default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        help="MinIO S3 endpoint. Use http://minio:9000 when running inside Docker.",
    )
    parser.add_argument(
        "--access-key",
        default=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        help="MinIO access key.",
    )
    parser.add_argument(
        "--secret-key",
        default=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        help="MinIO secret key.",
    )
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        choices=["append", "overwrite", "error", "ignore"],
        help="Spark write mode for validated records.",
    )
    return parser.parse_args()


def create_spark_session(args):
    ssl_enabled = str(args.minio_endpoint.startswith("https://")).lower()

    return (
        SparkSession.builder.appName("clean-data-quality-validation")
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled)
        .getOrCreate()
    )


def parse_csv_list(value):
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def zone_path(bucket, prefix, dataset_name):
    path_parts = [part.strip("/") for part in [prefix, dataset_name] if part.strip("/")]
    return f"s3a://{bucket}/{'/'.join(path_parts)}"


def build_clean_records_path(args, run_id):
    if args.clean_records_path:
        return args.clean_records_path.rstrip("/")

    clean_base_path = zone_path(args.bucket, args.clean_prefix, args.dataset_name)
    return f"{clean_base_path}/records/run_id={run_id}"


def build_validated_records_path(args, run_id):
    clean_base_path = zone_path(args.bucket, args.clean_prefix, args.dataset_name)
    return f"{clean_base_path}/{args.validated_folder}/run_id={run_id}"


def assert_columns_exist(df, columns, label):
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"Missing {label}: {', '.join(missing)}")


def result_to_dict(result):
    if hasattr(result, "to_json_dict"):
        return result.to_json_dict()
    return dict(result)


def validate_with_great_expectations(df, key_columns, date_column, min_rows, max_rows):
    if min_rows < 0:
        raise ValueError("--min-rows must be zero or greater.")

    if max_rows < min_rows:
        raise ValueError("--max-rows must be greater than or equal to --min-rows.")

    assert_columns_exist(df, key_columns + [date_column], "quality check columns")

    ge_df = SparkDFDataset(df)
    results = [
        ge_df.expect_table_row_count_to_be_between(
            min_value=min_rows,
            max_value=max_rows,
        )
    ]

    for key_column in key_columns:
        results.append(ge_df.expect_column_values_to_not_be_null(key_column))

    results.append(ge_df.expect_column_values_to_not_be_null(date_column))
    results.append(
        ge_df.expect_column_values_to_be_between(
            date_column,
            max_value=date.today().isoformat(),
            parse_strings_as_datetimes=True,
        )
    )

    failed_results = [
        result_to_dict(result)
        for result in results
        if not result_to_dict(result).get("success", False)
    ]

    if failed_results:
        print(json.dumps(failed_results, indent=2, default=str))
        raise ValueError("Great Expectations data quality checks failed.")

    print("Great Expectations data quality checks passed.")


def add_validation_partitions(df, date_column, run_id):
    event_date = to_date(col(date_column))

    return (
        df.withColumn("_validated_at", current_timestamp())
        .withColumn("_quality_run_id", lit(run_id))
        .withColumn("year", year(event_date))
        .withColumn("month", month(event_date))
    )


def main():
    args = parse_args()
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key_columns = parse_csv_list(args.key_columns)

    if not key_columns:
        raise ValueError("--key-columns must contain at least one column.")

    spark = create_spark_session(args)

    clean_records_path = build_clean_records_path(args, run_id)
    validated_records_path = build_validated_records_path(args, run_id)

    clean_df = spark.read.parquet(clean_records_path)

    validate_with_great_expectations(
        clean_df,
        key_columns,
        args.date_column,
        args.min_rows,
        args.max_rows,
    )

    validated_df = add_validation_partitions(clean_df, args.date_column, run_id)
    validated_df.write.mode(args.write_mode).partitionBy("year", "month").parquet(
        validated_records_path
    )

    print(f"Wrote validated records to {validated_records_path}")

    spark.stop()


if __name__ == "__main__":
    main()
