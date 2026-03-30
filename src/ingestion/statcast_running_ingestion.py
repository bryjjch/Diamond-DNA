#!/usr/bin/env python3
"""
Statcast Running Data Ingestion (Sprint Speed)

Fetches Statcast running leaderboard data from pybaseball and uploads to S3 as Parquet:
  {s3_prefix}/year=YYYY/statcast_sprint_speed.parquet

This is separate from pitch-level Statcast ingestion because sprint speed is published as a
leaderboard (player-season) rather than pitch-level rows.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

try:
    from pybaseball import statcast_sprint_speed
except Exception:  # pragma: no cover
    statcast_sprint_speed = None  # type: ignore[assignment]

from ..s3_parquet import write_parquet_to_s3

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_sprint_speed_for_year(year: int, *, min_opp: int, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """
    Fetch Statcast sprint speed leaderboard for one season year (YYYY).
    """
    if statcast_sprint_speed is None:
        raise ImportError("pybaseball is required for sprint speed ingestion. Install `pybaseball` in this environment.")

    for attempt in range(max_retries):
        try:
            logger.info("Fetching Statcast sprint speed for year=%d (attempt %d)", year, attempt + 1)
            df = statcast_sprint_speed(year=year, min_opp=min_opp)
            if df is None:
                return None
            if df.empty:
                logger.warning("No sprint speed rows returned for year=%d", year)
            return df
        except Exception as exc:
            logger.error("Error fetching sprint speed for year=%d (attempt %d): %s", year, attempt + 1, exc)
            if attempt < max_retries - 1:
                wait_s = (attempt + 1) ** 2
                logger.info("Retrying in %d seconds...", wait_s)
                time.sleep(wait_s)
            else:
                return None

    return None


def fetch_running_data_for_year(year: int, s3_bucket: str, s3_prefix: str, *, min_opp: int) -> dict:
    """
    Fetch sprint speed leaderboard for `year` and upload to S3.
    """
    current_year = datetime.now(timezone.utc).year
    if year > current_year + 1:
        return {"status": "error", "message": f"Year {year} is too far in the future (current UTC year: {current_year})."}

    df = fetch_sprint_speed_for_year(year, min_opp=min_opp)
    if df is None:
        return {"status": "error", "message": f"Fetch failed for year {year}"}

    out_key = f"{s3_prefix}/year={year}/statcast_sprint_speed.parquet"
    logger.info("Uploading %d sprint speed rows to s3://%s/%s", len(df), s3_bucket, out_key)
    write_parquet_to_s3(df, s3_bucket, out_key, log_write=False)
    return {"status": "ok", "message": "OK", "records": int(len(df))}


def ingest_year_range(start_year: int, end_year: int, s3_bucket: str, s3_prefix: str, *, min_opp: int) -> dict:
    """
    Ingest sprint speed for each year in [start_year, end_year].
    """
    if start_year > end_year:
        return {"status": "error", "message": "start_year must be <= end_year"}

    years_ok = 0
    years_error = 0
    total_records = 0
    errors = []

    for year in range(start_year, end_year + 1):
        result = fetch_running_data_for_year(year, s3_bucket, s3_prefix, min_opp=min_opp)
        if result["status"] == "ok":
            years_ok += 1
            total_records += int(result.get("records", 0))
        else:
            years_error += 1
            errors.append(f"{year}: {result.get('message')}")

    status = "ok" if years_error == 0 else ("partial" if years_ok else "error")
    return {
        "status": status,
        "message": f"Processed years {start_year}..{end_year}: {years_ok} ok, {years_error} errors",
        "total_records": total_records,
        "years_ok": years_ok,
        "years_error": years_error,
        "errors": errors,
    }


def main() -> None:
    current_year = datetime.now(timezone.utc).year
    parser = argparse.ArgumentParser(description="Ingest Statcast sprint speed leaderboard to S3 (year range).")
    parser.add_argument("--start-year", type=int, default=current_year - 3)
    parser.add_argument("--end-year", type=int, default=current_year)
    parser.add_argument("--min-opp", type=int, default=10, help="Minimum sprint opportunities for leaderboard inclusion.")
    parser.add_argument("--s3-bucket", type=str, default=os.environ.get("S3_BUCKET", "diamond-dna"))
    parser.add_argument("--s3-prefix", type=str, default=os.environ.get("S3_PREFIX", "raw-data/statcast_running"))
    args = parser.parse_args()

    result = ingest_year_range(args.start_year, args.end_year, args.s3_bucket, args.s3_prefix, min_opp=args.min_opp)

    if result["status"] == "error":
        logger.error(result["message"])
        for err in result.get("errors", []):
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
    Lambda entrypoint. Reads start_year/end_year/min_opp and S3 info from event/env.
    """
    current_year = datetime.now(timezone.utc).year
    start_year = int(event.get("start_year") or os.environ.get("START_YEAR") or (current_year - 3))
    end_year = int(event.get("end_year") or os.environ.get("END_YEAR") or current_year)
    min_opp = int(event.get("min_opp") or os.environ.get("MIN_OPP") or 10)
    s3_bucket = event.get("s3_bucket") or os.environ.get("S3_BUCKET", "diamond-dna")
    s3_prefix = event.get("s3_prefix") or os.environ.get("S3_PREFIX", "raw-data/statcast_running")

    result = ingest_year_range(start_year, end_year, s3_bucket, s3_prefix, min_opp=min_opp)
    status_code = 200 if result["status"] == "ok" else (207 if result["status"] == "partial" else 400)
    return {"statusCode": status_code, **result}


if __name__ == "__main__":
    main()

