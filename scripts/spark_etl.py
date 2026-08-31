#!/usr/bin/env python3
"""PySpark S3 ETL POC for LocalStack/AWS Glue-style replay."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Settings:
    bucket: str
    input_prefix: str
    lookup_key: str
    output_prefix: str


def env_value(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def normalize_prefix(value: str) -> str:
    return value.strip("/")


def build_settings() -> Settings:
    parser = argparse.ArgumentParser(description="Run the PySpark S3 ETL POC.")
    parser.add_argument("--bucket", default=env_value("S3_BUCKET", "glue-etl-poc"))
    parser.add_argument("--input-prefix", default=env_value("S3_INPUT_PREFIX", "input/json"))
    parser.add_argument("--lookup-key", default=env_value("S3_LOOKUP_KEY", "input/lookup/customers.csv"))
    parser.add_argument(
        "--output-prefix",
        default=env_value("SPARK_S3_OUTPUT_PREFIX", f"{env_value('S3_OUTPUT_PREFIX', 'output/enriched')}/spark"),
    )
    args = parser.parse_args()

    return Settings(
        bucket=args.bucket,
        input_prefix=normalize_prefix(args.input_prefix),
        lookup_key=normalize_prefix(args.lookup_key),
        output_prefix=normalize_prefix(args.output_prefix),
    )


def s3a_uri(bucket: str, key: str) -> str:
    return f"s3a://{bucket}/{normalize_prefix(key)}"


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("glue-etl-thread-execution-spark-poc")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def run(settings: Settings) -> int:
    spark = spark_session()
    spark.sparkContext.setLogLevel(env_value("SPARK_LOG_LEVEL", "WARN") or "WARN")

    input_uri = s3a_uri(settings.bucket, settings.input_prefix)
    lookup_uri = s3a_uri(settings.bucket, settings.lookup_key)
    orders_output_uri = s3a_uri(settings.bucket, f"{settings.output_prefix}/orders")
    manifest_output_uri = s3a_uri(settings.bucket, f"{settings.output_prefix}/manifest")

    orders = spark.read.json(input_uri)
    customers = spark.read.option("header", True).csv(lookup_uri)

    enriched = (
        orders.join(customers, on="customer_id", how="left")
        .withColumnRenamed("segment", "customer_segment")
        .withColumnRenamed("country", "customer_country")
        .withColumn("matched_customer", F.col("customer_name").isNotNull())
        .withColumn("source_key", F.input_file_name())
    )

    enriched.write.mode("overwrite").json(orders_output_uri)

    record_count = enriched.count()
    manifest = {
        "input_prefix": settings.input_prefix,
        "lookup_key": settings.lookup_key,
        "record_count": record_count,
        "output_prefix": f"{settings.output_prefix}/orders",
    }
    spark.createDataFrame([(json.dumps(manifest, sort_keys=True),)], ["value"]).coalesce(1).write.mode(
        "overwrite"
    ).text(manifest_output_uri)

    print(f"Read JSON objects from s3://{settings.bucket}/{settings.input_prefix}/")
    print(f"Loaded customer rows from s3://{settings.bucket}/{settings.lookup_key}")
    print(f"Wrote {record_count} enriched row(s) to s3://{settings.bucket}/{settings.output_prefix}/orders/")

    spark.stop()
    return 0


def main() -> int:
    return run(build_settings())


if __name__ == "__main__":
    raise SystemExit(main())
