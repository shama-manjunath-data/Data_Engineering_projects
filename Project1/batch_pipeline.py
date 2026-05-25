import argparse
import os
import tempfile
from datetime import datetime, timezone
from urllib.request import urlretrieve

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read a raw CSV from an API and write it as Parquet to MinIO."
    )
    parser.add_argument(
        "--source-url",
        required=True,
        help="API endpoint that returns raw CSV data.",
    )
    parser.add_argument(
        "--dataset-name",
        default="api_csv",
        help="Dataset folder name under the raw zone.",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("MINIO_BUCKET", "airflow-bucket"),
        help="MinIO bucket name.",
    )
    parser.add_argument(
        "--raw-prefix",
        default=os.getenv("RAW_ZONE_PREFIX", "raw"),
        help="Raw zone prefix inside the bucket.",
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
        "--header",
        default="true",
        choices=["true", "false"],
        help="Whether the CSV has a header row.",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter.",
    )
    parser.add_argument(
        "--write-mode",
        default="overwrite",
        choices=["append", "overwrite", "error", "ignore"],
        help="Spark write mode for the Parquet output.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Pipeline run id. Defaults to current UTC timestamp.",
    )
    return parser.parse_args()


def create_spark_session(args):
    ssl_enabled = str(args.minio_endpoint.startswith("https://")).lower()

    return (
        SparkSession.builder.appName("api-csv-to-minio-raw")
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


def download_csv(source_url, run_id):
    local_path = os.path.join(tempfile.gettempdir(), f"api_raw_{run_id}.csv")
    urlretrieve(source_url, local_path)
    return local_path


def main():
    args = parse_args()
    spark = create_spark_session(args)

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    local_csv_path = download_csv(args.source_url, run_id)
    output_path = (
        f"s3a://{args.bucket}/{args.raw_prefix}/{args.dataset_name}/"
        f"ingestion_date={run_id[:8]}/run_id={run_id}"
    )

    df = (
        spark.read.option("header", args.header)
        .option("sep", args.delimiter)
        .option("inferSchema", "false")
        .csv(local_csv_path)
    )

    enriched_df = (
        df.withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_url", lit(args.source_url))
        .withColumn("_source_file", input_file_name())
        .withColumn("_run_id", lit(run_id))
        .withColumn("_raw_zone", lit(args.raw_prefix))
    )

    enriched_df.write.mode(args.write_mode).parquet(output_path)

    print(f"Wrote raw parquet to {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
