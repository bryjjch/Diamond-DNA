#!/usr/bin/env python3
"""
Statcast Pitch Data Backfill Script

Downloads pitch-level Statcast data from pybaseball in 5-day chunks
(maximum allowed to keep requests together) and uploads to S3 as Parquet files,
partitioned by year and date.
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

# S3 client (credentials from IAM role in Batch)
s3_client = boto3.client('s3')


def date_range(start_date: datetime, end_date: datetime, period_days: int = 5):
    """
    Generate date ranges in chunks of period_days.
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        period_days: Number of days per chunk (default 5 for pybaseball max)
    
    Returns:
        Tuple of (chunk_start, chunk_end) datetime objects
    """
    current = start_date
    chunks = []
    while current <= end_date:
        chunk_end = min(current + timedelta(days=period_days - 1), end_date)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def fetch_statcast_data(start_date: datetime, end_date: datetime, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """
    Fetch Statcast data for a date range with retry logic.
    
    Args:
        start_date: Start date
        end_date: End date
        max_retries: Maximum number of retry attempts (default 3)
    
    Returns:
        DataFrame with Statcast data or None if failed
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
                wait_time = (attempt + 1) ** 2  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to fetch data for {start_str} to {end_str} after {max_retries} attempts")
                return None


def upload_to_s3(df: pd.DataFrame, bucket: str, key: str):
    """
    Upload DataFrame to S3 as Parquet file.
    
    Args:
        df: DataFrame to upload
        bucket: S3 bucket name
        key: S3 object key (path)
    """
    
    logger.info(f"Uploading {len(df)} records to s3://{bucket}/{key}")
    
    # Convert DataFrame to Parquet in memory
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False, compression='snappy')
    parquet_buffer.seek(0)
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=parquet_buffer.getvalue(),
        ContentType='application/x-parquet'
    )
    
    logger.info(f"Successfully uploaded to s3://{bucket}/{key}")


def process_year_range(
    start_year: int,
    end_year: int,
    s3_bucket: str,
    s3_prefix: str,
    chunk_days: int = 5
):
    """
    Process Statcast data for a year range, fetching in chunks and writing one Parquet file per day.
    
    Args:
        start_year: Start year (inclusive)
        end_year: End year (inclusive)
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix/path
        chunk_days: Number of days per chunk (default 5 for pybaseball)
    """
    # Process each year
    for year in range(start_year, end_year + 1):
        logger.info(f"Processing year {year}")
        
        # Define year boundaries
        year_start = datetime(year, 1, 1)
        year_end = datetime(year, 12, 31)
        
        # Process in 5-day chunks
        chunk_count = 0
        for chunk_start, chunk_end in date_range(year_start, year_end, chunk_days):
            chunk_count += 1
            logger.info(f"Processing chunk {chunk_count} for year {year}: {chunk_start.date()} to {chunk_end.date()}")
            
            # Fetch data for this chunk
            df = fetch_statcast_data(chunk_start, chunk_end)
            
            if df is None:
                # fetch failure
                logger.error(f"Failed to fetch data for chunk {chunk_start.date()} to {chunk_end.date()}")
            elif df.empty:
                # no data available for this chunk (empty dataframe)
                logger.info(f"No statcast data for chunk {chunk_start.date()} to {chunk_end.date()}")
            else:
                if "game_date" not in df.columns:
                    logger.error(
                        "Statcast response missing required column 'game_date'. "
                        f"Columns: {list(df.columns)}"
                    )
                    continue

                game_dates = pd.to_datetime(df["game_date"], errors="coerce")
                if game_dates.isna().all():
                    logger.error(
                        "Unable to parse any values in 'game_date' to datetimes. "
                        "Skipping chunk upload."
                    )
                    continue

                df = df.copy()
                df["_partition_date"] = game_dates.dt.strftime("%Y-%m-%d")

                uploaded_days = 0
                for date_str, day_df in df.groupby("_partition_date", dropna=True):
                    if not isinstance(date_str, str) or not date_str:
                        continue

                    # Defensive: derive year from the partition date, not the loop variable.
                    try:
                        date_year = datetime.strptime(date_str, "%Y-%m-%d").year
                    except ValueError:
                        logger.warning(f"Skipping unparseable partition date '{date_str}'")
                        continue

                    day_df = day_df.drop(columns=["_partition_date"])
                    s3_key = f"{s3_prefix}/year={date_year}/date={date_str}/statcast_pitches.parquet"
                    upload_to_s3(day_df, s3_bucket, s3_key)
                    uploaded_days += 1

                logger.info(
                    f"Uploaded {uploaded_days} day partition(s) for chunk "
                    f"{chunk_start.date()} to {chunk_end.date()}"
                )
        
        logger.info(f"Completed processing year {year}")


def main():
    parser = argparse.ArgumentParser(
        description='Backfill Statcast pitch data from pybaseball to S3'
    )
    parser.add_argument(
        '--start-year',
        type=int,
        required=True,
        help='Start year (inclusive)'
    )
    parser.add_argument(
        '--end-year',
        type=int,
        required=True,
        help='End year (inclusive)'
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
    parser.add_argument(
        '--chunk-days',
        type=int,
        default=5,
        help='Number of days per chunk (default: 5, pybaseball maximum)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.start_year > args.end_year:
        logger.error("start-year must be <= end-year")
        sys.exit(1)
    
    if args.chunk_days > 5:
        logger.warning(f"chunk-days is {args.chunk_days}, but pybaseball maximum is 5. Setting to 5.")
        args.chunk_days = 5
    
    logger.info(f"Starting Statcast backfill: {args.start_year} to {args.end_year}")
    logger.info(f"S3 destination: s3://{args.s3_bucket}/{args.s3_prefix}")
    logger.info(f"Processing in {args.chunk_days}-day chunks")
    
    try:
        process_year_range(
            start_year=args.start_year,
            end_year=args.end_year,
            s3_bucket=args.s3_bucket,
            s3_prefix=args.s3_prefix,
            chunk_days=args.chunk_days
        )
        logger.info("Backfill completed successfully")
    except Exception as e:
        logger.error(f"Backfill failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
