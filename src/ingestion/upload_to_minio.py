"""Publish local raw batches to MinIO with conditional S3 writes."""

import argparse
import base64
import hashlib
import logging
from collections import Counter
from contextlib import closing
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

if __package__:
    from .config import MinioConfig
else:
    from config import MinioConfig


def ensure_bucket(client: Any, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
    try:
        client.create_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
            raise


def upload_file(client: Any, bucket: str, key: str, path: Path) -> str:
    """Compare server-verified SHA-256, streaming older objects lacking a checksum."""
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").digest()
        checksum = base64.b64encode(digest).decode("ascii")
        try:
            head = client.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
        except ClientError as exc:
            if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            stream.seek(0)
            try:
                client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=stream,
                    IfNoneMatch="*",
                    ChecksumSHA256=checksum,
                )
                return "uploaded"
            except ClientError as race:
                if race.response["ResponseMetadata"]["HTTPStatusCode"] not in (
                    409,
                    412,
                ):
                    raise
                head = client.head_object(
                    Bucket=bucket, Key=key, ChecksumMode="ENABLED"
                )
        remote = head.get("ChecksumSHA256")
        if not remote or head.get("ChecksumType") == "COMPOSITE":
            obj = client.get_object(Bucket=bucket, Key=key, IfMatch=head["ETag"])
            with closing(obj["Body"]) as body:
                remote_hash = hashlib.sha256()
                for chunk in body.iter_chunks(chunk_size=1024 * 1024):
                    remote_hash.update(chunk)
                remote = base64.b64encode(remote_hash.digest()).decode("ascii")
        if remote != checksum:
            raise FileExistsError(f"Checksum conflict: s3://{bucket}/{key}")
        return "skipped"


def upload(source: Path, config: MinioConfig) -> Counter[str]:
    """Preserve raw/ as the top-level prefix for both accepted source paths."""
    raw = source if source.name == "raw" else source / "raw"
    files = sorted(raw.glob("*/dt=*/*.parquet"))
    if not files:
        raise ValueError(f"No partitioned Parquet files under {raw}")
    counts: Counter[str] = Counter()
    client = boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        region_name="us-east-1",
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        config=Config(s3={"addressing_style": "path"}),
    )
    with closing(client):
        ensure_bucket(client, config.bucket)
        for path in [*sorted((raw / ".batches").glob("*.json")), *files]:
            key = path.relative_to(raw.parent).as_posix()
            action = upload_file(client, config.bucket, key, path)
            counts[action] += 1
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    counts = upload(args.source_dir, MinioConfig.from_env())
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logging.info("uploaded=%d skipped=%d", counts["uploaded"], counts["skipped"])


if __name__ == "__main__":
    main()
