#!/usr/bin/env python3
"""
Daily Statcast Pitch Data Ingestion Script

Fetches one day of pitch-level Statcast data from pybaseball (default: yesterday UTC)
and uploads to S3 as Parquet at {s3_prefix}/year=Y/date=D/statcast_pitches.parquet.
"""

import io
import time
import argparse
import logging
import sys
from datetime import datetime, timedelta
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

# S3 client (credentials from IAM role in Batch or env)
s3_client = boto3.client('s3')


def fetch_statcast_data(start_date: datetime, end_date: datetime, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """
    Fetch Statcast data for a date with retry logic.
    Same behavior as statcast_backfill.
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


def upload_to_s3(df: pd.DataFrame, bucket: str, key: str):
    """Upload DataFrame to S3 as Parquet file. Same as statcast_backfill."""
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


def main():
    parser = argparse.ArgumentParser(
        description='Ingest one day of Statcast pitch data from pybaseball to S3'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=None,
        help='Date to ingest (YYYY-MM-DD). Default: yesterday UTC'
    )
    parser.add_argument(
        '--s3-bucket',
        type=str,
        default='diamond-dna-raw-data',
        help='S3 bucket name (default: diamond-dna-raw-data)'
    )
    parser.add_argument(
        '--s3-prefix',
        type=str,
        default='raw-data/statcast',
        help='S3 prefix/path (default: raw-data/statcast)'
    )
    args = parser.parse_args()

    # Resolve ingest date: default yesterday UTC
    if args.date is None:
        ingest_date = (datetime.utcnow() - timedelta(days=1)).date()
    else:
        try:
            ingest_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid --date '{args.date}'; use YYYY-MM-DD")
            sys.exit(1)

    # Reject future dates
    today_utc = datetime.utcnow().date()
    if ingest_date > today_utc:
        logger.error(f"Date {ingest_date} is in the future (today UTC: {today_utc})")
        sys.exit(1)

    date_str = ingest_date.strftime('%Y-%m-%d')
    year = ingest_date.year
    logger.info(f"Ingesting Statcast data for {date_str}")

    start_dt = datetime(ingest_date.year, ingest_date.month, ingest_date.day)
    end_dt = start_dt

    df = fetch_statcast_data(start_dt, end_dt)
    if df is None:
        logger.error("Fetch failed after retries")
        sys.exit(1)
    if df.empty:
        logger.warning(f"No Statcast data for {date_str}; skipping upload")
        sys.exit(0)

    s3_key = f"{args.s3_prefix}/year={year}/date={date_str}/statcast_pitches.parquet"
    upload_to_s3(df, args.s3_bucket, s3_key)
    logger.info("Daily ingestion completed successfully")


if __name__ == '__main__':
    main()
