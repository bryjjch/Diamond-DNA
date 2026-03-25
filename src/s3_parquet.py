"""Shared S3 Parquet read/write helpers for data pipelines."""

from __future__ import annotations

import io
import logging
from typing import Any, Literal, Optional

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

_s3_client: Optional[Any] = None


def get_s3_client() -> Any:
    """Return a shared Boto3 S3 client (credentials from env / IAM role)."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def read_parquet_from_s3(
    bucket: str,
    key: str,
    *,
    client=None,
    log_read: bool = True,
    missing_key_log: Literal["none", "info", "warning"] = "info",
) -> Optional[pd.DataFrame]:
    """
    Read a Parquet object from S3 as a DataFrame.

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
        return pd.read_parquet(io.BytesIO(body))
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
