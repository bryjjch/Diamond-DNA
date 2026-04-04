#!/usr/bin/env python3
"""
Statcast Pitch Data Ingestion

Fetches pitch-level Statcast data from pybaseball for a date range
and uploads each day to S3 as Parquet at {s3_prefix}/year=Y/date=D/statcast_pitches.parquet.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pandas as pd
from pybaseball import statcast

from .ingest_common import retry_with_backoff
from ..pipeline.s3_interaction import raw_statcast_day_key
from ..pipeline.runtime import event_or_env_str, yesterday_utc_date_str
from ..pipeline.s3_interaction import write_parquet_to_s3
from ..pipeline.settings import PipelineSettings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_pitch_data_for_date(
    date_str: str,
    s3_bucket: str,
    s3_prefix: str,
    *,
    max_retries: int = 3,
) -> dict:
    """
    Fetch one day of Statcast pitch data (with retries) and upload to S3.

    Args:
        date_str: Date to ingest (YYYY-MM-DD).
        s3_bucket: S3 bucket name.
        s3_prefix: S3 key prefix (e.g. bronze/statcast).
        max_retries: Attempts for the Statcast API call before giving up.

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

    def _fetch() -> pd.DataFrame:
        df = statcast(start_dt=date_str, end_dt=date_str)
        if df is not None and not df.empty:
            logger.info("Fetched %d records for %s", len(df), date_str)
            return df
        logger.warning("No data returned for %s", date_str)
        return pd.DataFrame()

    df = retry_with_backoff(
        f"Statcast pitch data for {date_str}",
        _fetch,
        max_retries=max_retries,
    )
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
    cfg = PipelineSettings.from_environ()
    yesterday = yesterday_utc_date_str()
    parser = argparse.ArgumentParser(
        description="Ingest Statcast pitch data from pybaseball to S3 (date range; one file per day)"
    )
    parser.add_argument("--start-date", type=str, default=yesterday)
    parser.add_argument("--end-date", type=str, default=yesterday)
    parser.add_argument("--s3-bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default=cfg.raw_statcast_prefix,
        help="S3 prefix for bronze Statcast pitches (env: S3_PREFIX or RAW_PREFIX)",
    )
    args = parser.parse_args()

    result = ingest_date_range(args.start_date, args.end_date, args.s3_bucket, args.s3_prefix)

    if result["status"] == "error":
        logger.error(result["message"])
        if result.get("errors"):
            for err in result["errors"]:
                logger.error(err)
        sys.exit(1)
    if result["status"] == "partial":
        logger.warning(result["message"])
        for err in result.get("errors", []):
            logger.warning(err)
        sys.exit(1)
    logger.info(result["message"])
    sys.exit(0)


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    y = yesterday_utc_date_str()
    cfg = PipelineSettings.from_environ()
    start_date = event_or_env_str(event, "start_date", "START_DATE", y)
    end_date = event_or_env_str(event, "end_date", "END_DATE", y)
    s3_bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    s3_prefix = event_or_env_str(
        event, "s3_prefix", "S3_PREFIX", cfg.raw_statcast_prefix
    )

    result = ingest_date_range(start_date, end_date, s3_bucket, s3_prefix)

    if result["status"] == "error":
        return {
            "statusCode": 400,
            "body": result["message"],
            "errors": result.get("errors", []),
        }
    if result["status"] == "partial":
        return {
            "statusCode": 207,
            "body": result["message"],
            "total_records": result["total_records"],
            "days_ok": result["days_ok"],
            "days_no_data": result["days_no_data"],
            "days_error": result["days_error"],
            "errors": result.get("errors", []),
        }
    return {
        "statusCode": 200,
        "body": result["message"],
        "total_records": result["total_records"],
        "days_ok": result["days_ok"],
        "days_no_data": result["days_no_data"],
    }


if __name__ == "__main__":
    main()
