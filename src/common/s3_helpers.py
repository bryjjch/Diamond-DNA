"""S3 Parquet read/write helpers for pipelines."""

from __future__ import annotations

import io
import logging
from typing import Any, Literal, Optional, Sequence

import boto3
import pandas as pd
import pyarrow.parquet as pq
from botocore.config import Config

logger = logging.getLogger(__name__)

_s3_client: Optional[Any] = None

# The shared client is used from thread pools (e.g. bronze daily reads run with up to
# 16 workers); keep the connection pool at least that large to avoid discards.
_MAX_POOL_CONNECTIONS = 32


def get_s3_client() -> Any:
    """Return a shared Boto3 S3 client (credentials from env / IAM role)."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3", config=Config(max_pool_connections=_MAX_POOL_CONNECTIONS)
        )
    return _s3_client


def read_parquet_from_s3(
    bucket: str,
    key: str,
    *,
    client=None,
    log_read: bool = True,
    missing_key_log: Literal["none", "info", "warning"] = "info",
    columns: Optional[Sequence[str]] = None,
) -> Optional[pd.DataFrame]:
    """
    Read a Parquet object from S3 as a DataFrame.

    When ``columns`` is given, only those columns are parsed (any not present in the
    file are skipped), cutting parse time and memory for wide files.

    Returns None if the object does not exist (NoSuchKey). Other errors are logged
    and re-raised.
    """
    if client is None:
        client = get_s3_client()
    try:
        if log_read:
            logger.info("Reading s3://%s/%s", bucket, key)
        obj = client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        buf = io.BytesIO(body)
        if columns is not None:
            available = set(pq.ParquetFile(buf).schema_arrow.names)
            use = [c for c in columns if c in available]
            buf.seek(0)
            return pd.read_parquet(buf, columns=use)
        return pd.read_parquet(buf)
    except client.exceptions.NoSuchKey:
        if missing_key_log == "info":
            logger.info("No existing object at s3://%s/%s (treat as empty)", bucket, key)
        elif missing_key_log == "warning":
            logger.warning("No such parquet at s3://%s/%s", bucket, key)
        return None
    except Exception as exc:
        logger.error("Error reading s3://%s/%s: %s", bucket, key, exc)
        raise


def write_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    *,
    client=None,
    log_write: bool = True,
) -> None:
    """Write a DataFrame to S3 as Snappy-compressed Parquet."""
    if client is None:
        client = get_s3_client()
    if log_write:
        logger.info("Writing %d rows to s3://%s/%s", len(df), bucket, key)
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    buf.seek(0)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/x-parquet",
    )
