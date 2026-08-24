#!/usr/bin/env python3
"""Async S3 I/O POC ETL for LocalStack/AWS Glue-style replay."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from io import StringIO
from pathlib import PurePosixPath
from typing import Any

import boto3
from botocore.config import Config


@dataclass(frozen=True)
class Settings:
    """Runtime options resolved from CLI flags and environment variables."""

    bucket: str
    input_prefix: str
    lookup_key: str
    output_prefix: str
    endpoint_url: str | None
    region_name: str
    max_concurrency: int


@dataclass(frozen=True)
class ProcessResult:
    """Result of processing one source object."""

    source_key: str
    output_key: str | None = None
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.error is None


def env_value(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def normalize_prefix(value: str) -> str:
    """Store prefixes without leading/trailing slashes to avoid malformed S3 keys."""
    return value.strip("/")


def build_settings() -> Settings:
    parser = argparse.ArgumentParser(description="Run the pure Python async S3 ETL POC.")
    parser.add_argument("--bucket", default=env_value("S3_BUCKET", "glue-etl-poc"))
    parser.add_argument("--input-prefix", default=env_value("S3_INPUT_PREFIX", "input/json"))
    parser.add_argument("--lookup-key", default=env_value("S3_LOOKUP_KEY", "input/lookup/customers.csv"))
    parser.add_argument(
        "--output-prefix",
        default=env_value("PYTHON_S3_OUTPUT_PREFIX", f"{env_value('S3_OUTPUT_PREFIX', 'output/enriched')}/python"),
    )
    parser.add_argument("--endpoint-url", default=env_value("AWS_ENDPOINT_URL_S3", env_value("AWS_ENDPOINT_URL")))
    parser.add_argument("--region-name", default=env_value("AWS_DEFAULT_REGION", "us-east-1"))
    parser.add_argument("--max-concurrency", type=int, default=int(env_value("MAX_S3_CONCURRENCY", "16") or "16"))
    args = parser.parse_args()

    return Settings(
        bucket=args.bucket,
        input_prefix=normalize_prefix(args.input_prefix),
        lookup_key=normalize_prefix(args.lookup_key),
        output_prefix=normalize_prefix(args.output_prefix),
        endpoint_url=args.endpoint_url,
        region_name=args.region_name,
        max_concurrency=max(args.max_concurrency, 1),
    )


def s3_client(settings: Settings):
    """Build an S3 client that works with both AWS S3 and LocalStack S3."""
    return boto3.client(
        "s3",
        endpoint_url=settings.endpoint_url,
        region_name=settings.region_name,
        aws_access_key_id=env_value("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=env_value("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(
            connect_timeout=5,
            # Match the HTTP connection pool to the thread pool; otherwise botocore
            # can serialize requests behind its default, smaller connection pool.
            max_pool_connections=settings.max_concurrency,
            read_timeout=10,
            retries={"max_attempts": 2, "mode": "standard"},
            s3={"addressing_style": "path"},
        ),
    )


async def call_s3(executor: ThreadPoolExecutor, function, /, **kwargs):
    """Run blocking boto3 calls in worker threads so asyncio can coordinate them."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, lambda: function(**kwargs))


async def list_json_keys(s3, executor: ThreadPoolExecutor, bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/")

    def collect_keys() -> list[str]:
        # Pagination is blocking because boto3 is synchronous, so it runs in the
        # same executor used by object reads and writes.
        return [
            item["Key"]
            for page in pages
            for item in page.get("Contents", [])
            if item["Key"].endswith(".json")
        ]

    loop = asyncio.get_running_loop()
    keys = await loop.run_in_executor(executor, collect_keys)
    return sorted(keys)


async def read_text_object(s3, executor: ThreadPoolExecutor, bucket: str, key: str) -> str:
    response = await call_s3(executor, s3.get_object, Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


async def put_json_object(s3, executor: ThreadPoolExecutor, bucket: str, key: str, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    await call_s3(
        executor,
        s3.put_object,
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


async def put_text_object(s3, executor: ThreadPoolExecutor, bucket: str, key: str, body: str) -> None:
    await call_s3(
        executor,
        s3.put_object,
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="text/plain",
    )


def parse_customers(csv_text: str) -> dict[str, dict[str, str]]:
    """Index lookup rows by customer_id for constant-time enrichment."""
    reader = csv.DictReader(StringIO(csv_text))
    customers: dict[str, dict[str, str]] = {}
    for row in reader:
        customer_id = row.get("customer_id")
        if customer_id:
            customers[customer_id] = row
    return customers


def enrich_order(order: dict[str, Any], source_key: str, customers: dict[str, dict[str, str]]) -> dict[str, Any]:
    """Apply the POC join rule: enrich each order with its customer lookup row."""
    customer = customers.get(str(order.get("customer_id", "")), {})
    return {
        **order,
        "customer_name": customer.get("customer_name"),
        "customer_segment": customer.get("segment"),
        "customer_country": customer.get("country"),
        "matched_customer": bool(customer),
        "source_key": source_key,
    }


def output_key(output_prefix: str, order: dict[str, Any], source_key: str) -> str:
    """Keep output deterministic and stable across repeated local runs."""
    order_id = str(order.get("order_id") or PurePosixPath(source_key).stem)
    return f"{output_prefix}/orders/{order_id}.json"


async def process_record(
    s3,
    executor: ThreadPoolExecutor,
    semaphore: asyncio.Semaphore,
    settings: Settings,
    key: str,
    customers: dict[str, dict[str, str]],
) -> ProcessResult:
    """Read, enrich, and write one order object without failing the whole ETL."""
    async with semaphore:
        try:
            text = await read_text_object(s3, executor, settings.bucket, key)
            order = json.loads(text)
            record = enrich_order(order, key, customers)
            destination_key = output_key(settings.output_prefix, record, key)
            await put_json_object(s3, executor, settings.bucket, destination_key, record)
        except Exception as exc:
            return ProcessResult(source_key=key, error=f"{type(exc).__name__}: {exc}")

        return ProcessResult(source_key=key, output_key=destination_key)


async def run(settings: Settings) -> int:
    s3 = s3_client(settings)
    with ThreadPoolExecutor(max_workers=settings.max_concurrency) as executor:
        # The lookup table is small in this POC, so load it once and reuse it for
        # all input objects instead of reading it per record.
        customer_csv = await read_text_object(s3, executor, settings.bucket, settings.lookup_key)
        customers = parse_customers(customer_csv)

        source_keys = await list_json_keys(s3, executor, settings.bucket, settings.input_prefix)
        if not source_keys:
            raise RuntimeError(f"No JSON input objects found at s3://{settings.bucket}/{settings.input_prefix}/")

        semaphore = asyncio.Semaphore(settings.max_concurrency)
        results = await asyncio.gather(
            *(
                process_record(s3, executor, semaphore, settings, key, customers)
                for key in source_keys
            )
        )
        written_keys = sorted(result.output_key for result in results if result.output_key)
        failed_records = [
            {"source_key": result.source_key, "error": result.error}
            for result in results
            if not result.succeeded
        ]
        # The manifest makes local validation simple and gives downstream scripts
        # one small object to inspect for record counts and generated keys.
        success_body = json.dumps(
            {
                "failed_count": len(failed_records),
                "failed_records": failed_records,
                "input_prefix": settings.input_prefix,
                "lookup_key": settings.lookup_key,
                "record_count": len(written_keys),
                "source_count": len(source_keys),
                "status": "completed" if not failed_records else "completed_with_errors",
                "written_keys": written_keys,
            },
            indent=2,
            sort_keys=True,
        )
        await put_text_object(s3, executor, settings.bucket, f"{settings.output_prefix}/_SUCCESS", success_body)

    print(f"Read {len(source_keys)} JSON object(s) from s3://{settings.bucket}/{settings.input_prefix}/")
    print(f"Loaded {len(customers)} customer row(s) from s3://{settings.bucket}/{settings.lookup_key}")
    print(f"Wrote {len(written_keys)} enriched object(s) to s3://{settings.bucket}/{settings.output_prefix}/orders/")
    if failed_records:
        print(f"Failed {len(failed_records)} object(s); see s3://{settings.bucket}/{settings.output_prefix}/_SUCCESS")
    return 0


def main() -> int:
    return asyncio.run(run(build_settings()))


if __name__ == "__main__":
    raise SystemExit(main())
