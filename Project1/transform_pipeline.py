import argparse
import os
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    lit,
    sum as spark_sum,
    to_date,
    to_timestamp,
)


TYPE_ALIASES = {
    "bool": "boolean",
    "datetime": "timestamp",
    "integer": "int",
    "long": "bigint",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Transform raw MinIO parquet into clean records and aggregates."
    )
    parser.add_argument(
        "--dataset-name",
        required=True,
        help="Dataset folder name used under the raw and clean zones.",
    )
    parser.add_argument(
        "--raw-input-path",
        default=None,
        help="Optional full raw input path. Defaults to s3a://bucket/raw-prefix/dataset.",
    )
    parser.add_argument(
        "--lookup-path",
        required=True,
        help="Lookup table path, for example s3a://airflow-bucket/lookups/customers.parquet.",
    )
    parser.add_argument(
        "--lookup-format",
        default="parquet",
        choices=["parquet", "csv", "json"],
        help="Lookup table file format.",
    )
    parser.add_argument(
        "--lookup-header",
        default="true",
        choices=["true", "false"],
        help="Whether CSV lookup files have a header row.",
    )
    parser.add_argument(
        "--lookup-delimiter",
        default=",",
        help="CSV delimiter for lookup files.",
    )
    parser.add_argument(
        "--dedupe-keys",
        default="",
        help="Comma-separated columns to use for deduplication. Defaults to all columns.",
    )
    parser.add_argument(
        "--cast",
        action="append",
        default=[],
        help="Column cast in column:type format. Repeat for multiple columns.",
    )
    parser.add_argument(
        "--lookup-cast",
        action="append",
        default=[],
        help="Lookup column cast in column:type format. Repeat for multiple columns.",
    )
    parser.add_argument(
        "--date-format",
        default=None,
        help="Optional Spark date/timestamp pattern, for example yyyy-MM-dd.",
    )
    parser.add_argument(
        "--join-left-keys",
        required=True,
        help="Comma-separated columns from the raw dataset.",
    )
    parser.add_argument(
        "--join-right-keys",
        default="",
        help="Comma-separated columns from the lookup table. Defaults to join-left-keys.",
    )
    parser.add_argument(
        "--join-type",
        default="left",
        choices=["inner", "left", "right", "full"],
        help="Join type to use with the lookup table.",
    )
    parser.add_argument(
        "--lookup-prefix",
        default="lookup_",
        help="Prefix for lookup columns that would otherwise collide with source columns.",
    )
    parser.add_argument(
        "--event-date-col",
        required=True,
        help="Column used for daily aggregations and rolling averages.",
    )
    parser.add_argument(
        "--metric-columns",
        required=True,
        help="Comma-separated numeric columns to aggregate.",
    )
    parser.add_argument(
        "--group-by-columns",
        default="",
        help="Comma-separated dimensions for aggregations, such as region or product_id.",
    )
    parser.add_argument(
        "--rolling-window-days",
        type=int,
        default=7,
        help="Calendar-day window used for rolling averages.",
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
        "--clean-prefix",
        default=os.getenv("CLEAN_ZONE_PREFIX", "clean"),
        help="Clean zone prefix inside the bucket.",
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
        help="Spark write mode for clean-zone outputs.",
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
        SparkSession.builder.appName("raw-to-clean-transform")
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


def parse_cast_specs(specs):
    cast_specs = {}

    for spec in specs:
        if ":" not in spec:
            raise ValueError(f"Invalid cast spec '{spec}'. Use column:type.")

        column_name, target_type = spec.split(":", 1)
        column_name = column_name.strip()
        target_type = target_type.strip()

        if not column_name or not target_type:
            raise ValueError(f"Invalid cast spec '{spec}'. Use column:type.")

        cast_specs[column_name] = target_type

    return cast_specs


def assert_columns_exist(df, columns, label):
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"Missing {label}: {', '.join(missing)}")


def zone_path(bucket, prefix, dataset_name):
    path_parts = [part.strip("/") for part in [prefix, dataset_name] if part.strip("/")]
    return f"s3a://{bucket}/{'/'.join(path_parts)}"


def build_raw_input_path(args):
    if args.raw_input_path:
        return args.raw_input_path.rstrip("/")
    return zone_path(args.bucket, args.raw_prefix, args.dataset_name)


def build_clean_base_path(args):
    return zone_path(args.bucket, args.clean_prefix, args.dataset_name)


def read_lookup_table(spark, args):
    if args.lookup_format == "parquet":
        return spark.read.parquet(args.lookup_path)

    if args.lookup_format == "json":
        return spark.read.json(args.lookup_path)

    return (
        spark.read.option("header", args.lookup_header)
        .option("sep", args.lookup_delimiter)
        .option("inferSchema", "false")
        .csv(args.lookup_path)
    )


def apply_casts(df, cast_specs, date_format):
    assert_columns_exist(df, cast_specs.keys(), "cast columns")

    for column_name, target_type in cast_specs.items():
        normalized_type = TYPE_ALIASES.get(target_type.lower(), target_type)

        if normalized_type.lower() == "date":
            parsed_col = (
                to_date(col(column_name), date_format)
                if date_format
                else to_date(col(column_name))
            )
            df = df.withColumn(column_name, parsed_col)
        elif normalized_type.lower() == "timestamp":
            parsed_col = (
                to_timestamp(col(column_name), date_format)
                if date_format
                else to_timestamp(col(column_name))
            )
            df = df.withColumn(column_name, parsed_col)
        else:
            df = df.withColumn(column_name, col(column_name).cast(normalized_type))

    return df


def deduplicate(df, dedupe_keys):
    if not dedupe_keys:
        return df.dropDuplicates()

    assert_columns_exist(df, dedupe_keys, "dedupe keys")
    return df.dropDuplicates(dedupe_keys)


def rename_colliding_lookup_columns(source_df, lookup_df, right_keys, prefix):
    source_columns = set(source_df.columns)

    for lookup_column in lookup_df.columns:
        if lookup_column in right_keys:
            continue

        if lookup_column in source_columns:
            lookup_df = lookup_df.withColumnRenamed(
                lookup_column, f"{prefix}{lookup_column}"
            )

    return lookup_df


def join_lookup(source_df, lookup_df, left_keys, right_keys, join_type, lookup_prefix):
    assert_columns_exist(source_df, left_keys, "left join keys")
    assert_columns_exist(lookup_df, right_keys, "right join keys")

    lookup_df = rename_colliding_lookup_columns(
        source_df, lookup_df, right_keys, lookup_prefix
    )

    if left_keys == right_keys:
        return source_df.join(lookup_df, on=left_keys, how=join_type)

    source = source_df.alias("source")
    lookup = lookup_df.alias("lookup")
    conditions = [
        source[left_key] == lookup[right_key]
        for left_key, right_key in zip(left_keys, right_keys)
    ]
    joined_df = source.join(lookup, reduce(lambda left, right: left & right, conditions), join_type)

    for right_key in right_keys:
        if right_key not in left_keys:
            joined_df = joined_df.drop(lookup[right_key])

    return joined_df


def add_clean_metadata(df, args, run_id):
    event_date = (
        to_date(col(args.event_date_col), args.date_format)
        if args.date_format
        else to_date(col(args.event_date_col))
    )

    return (
        df.withColumn("_event_date", event_date)
        .withColumn("_processed_at", current_timestamp())
        .withColumn("_transform_run_id", lit(run_id))
        .withColumn("_clean_zone", lit(args.clean_prefix))
    )


def compute_daily_totals(df, group_by_columns, metric_columns):
    assert_columns_exist(df, ["_event_date"] + group_by_columns + metric_columns, "aggregation columns")

    aggregate_expressions = [
        spark_sum(col(metric_column)).alias(f"{metric_column}_daily_total")
        for metric_column in metric_columns
    ]

    return (
        df.where(col("_event_date").isNotNull())
        .groupBy(*(["_event_date"] + group_by_columns))
        .agg(*aggregate_expressions)
    )


def compute_rolling_averages(daily_totals_df, group_by_columns, metric_columns, window_days):
    if window_days <= 0:
        raise ValueError("--rolling-window-days must be greater than zero.")

    seconds_per_day = 86400
    range_start = -(window_days - 1) * seconds_per_day
    window_base = (
        Window.partitionBy(*group_by_columns) if group_by_columns else Window
    )
    window_spec = window_base.orderBy(col("_event_date_epoch")).rangeBetween(
        range_start, 0
    )

    rolling_df = daily_totals_df.withColumn(
        "_event_date_epoch", col("_event_date").cast("timestamp").cast("long")
    )

    for metric_column in metric_columns:
        daily_total_column = f"{metric_column}_daily_total"
        rolling_df = rolling_df.withColumn(
            f"{metric_column}_rolling_avg_{window_days}d",
            avg(col(daily_total_column)).over(window_spec),
        )

    return rolling_df.drop("_event_date_epoch")


def write_parquet(df, path, mode, partition_columns=None):
    writer = df.write.mode(mode)

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.parquet(path)


def main():
    args = parse_args()
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    dedupe_keys = parse_csv_list(args.dedupe_keys)
    left_keys = parse_csv_list(args.join_left_keys)
    right_keys = parse_csv_list(args.join_right_keys) or left_keys
    group_by_columns = parse_csv_list(args.group_by_columns)
    metric_columns = parse_csv_list(args.metric_columns)

    if not left_keys:
        raise ValueError("--join-left-keys must contain at least one column.")

    if not metric_columns:
        raise ValueError("--metric-columns must contain at least one column.")

    if len(left_keys) != len(right_keys):
        raise ValueError("--join-left-keys and --join-right-keys must have the same length.")

    spark = create_spark_session(args)

    raw_df = spark.read.parquet(build_raw_input_path(args))
    lookup_df = read_lookup_table(spark, args)

    deduped_df = deduplicate(raw_df, dedupe_keys)
    typed_df = apply_casts(deduped_df, parse_cast_specs(args.cast), args.date_format)
    typed_lookup_df = apply_casts(
        lookup_df, parse_cast_specs(args.lookup_cast), args.date_format
    )

    joined_df = join_lookup(
        typed_df,
        typed_lookup_df,
        left_keys,
        right_keys,
        args.join_type,
        args.lookup_prefix,
    )
    clean_records_df = add_clean_metadata(joined_df, args, run_id)

    daily_totals_df = compute_daily_totals(
        clean_records_df, group_by_columns, metric_columns
    )
    rolling_averages_df = compute_rolling_averages(
        daily_totals_df, group_by_columns, metric_columns, args.rolling_window_days
    )

    clean_base_path = build_clean_base_path(args)
    records_path = f"{clean_base_path}/records/run_id={run_id}"
    daily_totals_path = f"{clean_base_path}/daily_totals/run_id={run_id}"
    rolling_averages_path = (
        f"{clean_base_path}/rolling_averages/"
        f"window_days={args.rolling_window_days}/run_id={run_id}"
    )

    write_parquet(clean_records_df, records_path, args.write_mode, ["_event_date"])
    write_parquet(daily_totals_df, daily_totals_path, args.write_mode, ["_event_date"])
    write_parquet(
        rolling_averages_df, rolling_averages_path, args.write_mode, ["_event_date"]
    )

    print(f"Wrote clean records to {records_path}")
    print(f"Wrote daily totals to {daily_totals_path}")
    print(f"Wrote rolling averages to {rolling_averages_path}")

    spark.stop()


if __name__ == "__main__":
    main()
