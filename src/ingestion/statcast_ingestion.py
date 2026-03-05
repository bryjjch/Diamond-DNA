#!/usr/bin/env python3
"""
Statcast Pitch Data Ingestion

Fetches pitch-level Statcast data from pybaseball for a date range (one API call per day)
and uploads each day to S3 as Parquet at {s3_prefix}/year=Y/date=D/statcast_pitches.parquet.

Use for both daily (start_date = end_date = yesterday) and backfill (larger range).
"""

import io
import os
import time
import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
import pandas as pd
from pybaseball import statcast

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# S3 client (credentials from IAM role in Batch/Lambda or env)
s3_client = boto3.client('s3')


def fetch_statcast_data(start_date: datetime, end_date: datetime, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """
    Fetch Statcast data for a date range with retry logic.
    """
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching Statcast data for {start_str} to {end_str} (attempt {attempt + 1})")
            df = statcast(start_dt=start_str, end_dt=end_str)

            if df is not None and not df.empty:
                logger.info(f"Fetched {len(df)} records for {start_str} to {end_str}")
                return df
            else:
                logger.warning(f"No data returned for {start_str} to {end_str}")
                return pd.DataFrame()

        except Exception as e:
            logger.error(f"Error fetching data for {start_str} to {end_str} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) ** 2
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to fetch data for {start_str} to {end_str} after {max_retries} attempts")
                return None

    return None


def upload_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Upload DataFrame to S3 as Parquet file."""
    logger.info(f"Uploading {len(df)} records to s3://{bucket}/{key}")
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False, compression='snappy')
    parquet_buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue(),
        ContentType='application/x-parquet'
    )
    logger.info(f"Successfully uploaded to s3://{bucket}/{key}")


def fetch_pitch_data_for_date(date_str: str, s3_bucket: str, s3_prefix: str) -> dict:
    """
    Fetch one day of Statcast pitch data and upload to S3.

    Args:
        date_str: Date to ingest (YYYY-MM-DD).
        s3_bucket: S3 bucket name.
        s3_prefix: S3 key prefix (e.g. raw-data/statcast).

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
    start_dt = datetime(ingest_date.year, ingest_date.month, ingest_date.day)
    end_dt = start_dt

    df = fetch_statcast_data(start_dt, end_dt)
    if df is None:
        return {"status": "error", "message": "Fetch failed after retries"}
    if df.empty:
        logger.warning(f"No Statcast data for {date_str}; skipping upload")
        return {"status": "no_data", "message": f"No Statcast data for {date_str}", "records": 0}

    year = ingest_date.year
    s3_key = f"{s3_prefix}/year={year}/date={date_str}/statcast_pitches.parquet"
    upload_to_s3(df, s3_bucket, s3_key)
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
    """CLI entrypoint: parse --start-date and --end-date (and S3 options), run ingest_date_range, then exit."""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
    parser = argparse.ArgumentParser(
        description='Ingest Statcast pitch data from pybaseball to S3 (date range; one file per day)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=yesterday,
        help=f'Start date (YYYY-MM-DD). Default: yesterday UTC ({yesterday})'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=yesterday,
        help=f'End date (YYYY-MM-DD). Default: yesterday UTC ({yesterday})'
    )
    parser.add_argument(
        '--s3-bucket',
        type=str,
        default='diamond-dna',
        help='S3 bucket name (default: diamond-dna)'
    )
    parser.add_argument(
        '--s3-prefix',
        type=str,
        default='raw-data/statcast',
        help='S3 prefix/path (default: raw-data/statcast)'
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


def handler(event: dict, context) -> dict:
    """
    Lambda entrypoint: read start_date, end_date, bucket, prefix from event (and env), run ingest_date_range.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (
        event.get("start_date")
        or os.environ.get("START_DATE")
        or yesterday
    )
    end_date = (
        event.get("end_date")
        or os.environ.get("END_DATE")
        or yesterday
    )
    s3_bucket = event.get("s3_bucket") or os.environ.get("S3_BUCKET", "diamond-dna")
    s3_prefix = event.get("s3_prefix") or os.environ.get("S3_PREFIX", "raw-data/statcast")

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


if __name__ == '__main__':
    main()
