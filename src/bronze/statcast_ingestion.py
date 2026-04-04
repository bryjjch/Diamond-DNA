#!/usr/bin/env python3
"""
Statcast Pitch Data Ingestion

Fetches pitch-level Statcast data from pybaseball for a date range
and uploads each day to S3 as Parquet at {s3_prefix}/year=Y/date=D/statcast_pitches.parquet.

Use for both daily (start_date = end_date = yesterday) and backfill (larger range).
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from pybaseball import statcast

from .ingest_common import retry_with_backoff
from ..pipeline.lake_paths import raw_statcast_day_key
from ..pipeline.s3_parquet import write_parquet_to_s3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_statcast_data_for_date(date_str: str, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """
    Fetch Statcast pitch data for a single calendar day (YYYY-MM-DD) with retry logic.
    """

    def _fetch() -> pd.DataFrame:
        df = statcast(start_dt=date_str, end_dt=date_str)
        if df is not None and not df.empty:
            logger.info("Fetched %d records for %s", len(df), date_str)
            return df
        logger.warning("No data returned for %s", date_str)
        return pd.DataFrame()

    return retry_with_backoff(
        f"Statcast pitch data for {date_str}",
        _fetch,
        max_retries=max_retries,
    )


def fetch_pitch_data_for_date(date_str: str, s3_bucket: str, s3_prefix: str) -> dict:
    """
    Fetch one day of Statcast pitch data and upload to S3.

    Args:
        date_str: Date to ingest (YYYY-MM-DD).
        s3_bucket: S3 bucket name.
        s3_prefix: S3 key prefix (e.g. bronze/statcast).

    Returns:
        Dict with keys: status ("ok" | "no_data" | "error"), message, and optionally records.
    """
    try:
        ingest_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        return {"status": "error", "message": f"Invalid date '{date_str}'; use YYYY-MM-DD"}

    today_utc = datetime.now(timezone.utc).date()
    if ingest_date > today_utc:
        return {
            "status": "error",
            "message": f"Date {ingest_date} is in the future (today UTC: {today_utc})",
        }

    logger.info(f"Ingesting Statcast data for {date_str}")

    df = fetch_statcast_data_for_date(date_str)
    if df is None:
        return {"status": "error", "message": "Fetch failed after retries"}
    if df.empty:
        logger.warning(f"No Statcast data for {date_str}; skipping upload")
        return {"status": "no_data", "message": f"No Statcast data for {date_str}", "records": 0}

    s3_key = raw_statcast_day_key(s3_prefix, ingest_date)
    logger.info(f"Uploading {len(df)} records to s3://{s3_bucket}/{s3_key}")
    write_parquet_to_s3(df, s3_bucket, s3_key, log_write=False)
    logger.info(f"Successfully uploaded to s3://{s3_bucket}/{s3_key}")
    return {"status": "ok", "message": "OK", "records": len(df)}


def ingest_date_range(start_date_str: str, end_date_str: str, s3_bucket: str, s3_prefix: str) -> dict:
    """
    Ingest Statcast data for every day in [start_date_str, end_date_str] (inclusive).
    Fetches each day separately and uploads one Parquet file per day.

    Returns:
        Dict with status ("ok" | "partial" | "error"), total_records, days_ok, days_no_data,
        days_error, and errors (list of messages for failed days).
    """
    try:
        start_dt = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_dt = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError as e:
        return {
            "status": "error",
            "message": str(e),
            "total_records": 0,
            "days_ok": 0,
            "days_no_data": 0,
            "days_error": 0,
            "errors": [],
        }

    if start_dt > end_dt:
        return {
            "status": "error",
            "message": f"start_date ({start_date_str}) must be <= end_date ({end_date_str})",
            "total_records": 0,
            "days_ok": 0,
            "days_no_data": 0,
            "days_error": 0,
            "errors": [],
        }

    total_records = 0
    days_ok = 0
    days_no_data = 0
    days_error = 0
    errors = []

    current = start_dt
    while current <= end_dt:
        date_str = current.strftime('%Y-%m-%d')
        result = fetch_pitch_data_for_date(date_str, s3_bucket, s3_prefix)

        if result["status"] == "ok":
            total_records += result.get("records", 0)
            days_ok += 1
        elif result["status"] == "no_data":
            days_no_data += 1
        else:
            days_error += 1
            errors.append(f"{date_str}: {result['message']}")

        current += timedelta(days=1)

    if days_error:
        status = "partial" if (days_ok or days_no_data) else "error"
    else:
        status = "ok"

    return {
        "status": status,
        "message": f"Processed {start_date_str} to {end_date_str}: {days_ok} ok, {days_no_data} no data, {days_error} errors",
        "total_records": total_records,
        "days_ok": days_ok,
        "days_no_data": days_no_data,
        "days_error": days_error,
        "errors": errors,
    }


def main() -> None:
    from ..pipeline.cli import run_statcast_ingestion_main

    run_statcast_ingestion_main()


def handler(event: dict, context) -> dict:
    from ..pipeline.handlers import statcast_ingestion_handler

    return statcast_ingestion_handler(event, context)


if __name__ == "__main__":
    main()
