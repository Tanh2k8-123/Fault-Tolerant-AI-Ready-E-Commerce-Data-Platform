"""Process one UTC arrival date for a single immutable generator batch."""

import json
import logging
import os
import re
import subprocess
from datetime import date, timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, get_current_context


def run_inputs() -> tuple[str, str]:
    context = get_current_context()
    override = context["params"].get("partition_date")
    day = date.fromisoformat(override) if override else context["data_interval_start"].in_timezone("UTC").date()
    batch = os.environ["PIPELINE_BATCH_ID"]
    if not re.fullmatch(r"[A-Za-z0-9_-]+", batch):
        raise ValueError("Invalid PIPELINE_BATCH_ID")
    return day.isoformat(), batch


def check_raw_partition() -> None:
    """At least one table must have a shard; dimension arrivals can be sparse."""
    import boto3
    from botocore.config import Config

    day, batch = run_inputs()
    bucket = os.environ["MINIO_BUCKET"]
    filename = re.compile(rf"part-{re.escape(batch)}-[0-9]{{5}}\.parquet")
    client = boto3.client(
        "s3", endpoint_url=os.environ["MINIO_ENDPOINT_URL"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}),
    )
    try:
        for table in ("orders", "customers", "products", "reviews"):
            prefix = f"raw/{table}/dt={day}/"
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=f"{prefix}part-{batch}-"
            ):
                for item in page.get("Contents", []):
                    if item["Size"] and filename.fullmatch(item["Key"][len(prefix):]):
                        logging.info("Found partition for %s in s3://%s/%s", batch, bucket, prefix)
                        return
    finally:
        client.close()
    raise FileNotFoundError(f"No raw Parquet shard for batch {batch} on {day} in {bucket}")


def run_dbt(commands: list[list[str]]) -> None:
    day, batch = run_inputs()
    variables = json.dumps({"partition_date": day, "batch_id": batch})
    for command in commands:
        logging.info("Running dbt %s for batch %s on %s", " ".join(command), batch, day)
        subprocess.run(
            ["/home/airflow/dbt-venv/bin/dbt", *command, "--vars", variables],
            cwd=os.environ["DBT_PROJECT_DIR"], check=True,
        )


with DAG(
    dag_id="ecommerce_daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={"partition_date": Param(None, type=["null", "string"], format="date")},
) as dag:
    previous = PythonOperator(task_id="check_raw_partition", python_callable=check_raw_partition)
    for task_id, commands in (
        # Bronze views must exist before the very first snapshot in a fresh warehouse.
        ("dbt_snapshot", [["run", "--select", "path:models/bronze"], ["snapshot"]]),
        ("dbt_run_silver", [["run", "--select", "path:models/silver"]]),
        ("dbt_run_gold", [["run", "--select", "path:models/gold"]]),
        ("dbt_test", [["test"]]),
    ):
        current = PythonOperator(
            task_id=task_id, python_callable=run_dbt, op_kwargs={"commands": commands}
        )
        previous >> current
        previous = current
