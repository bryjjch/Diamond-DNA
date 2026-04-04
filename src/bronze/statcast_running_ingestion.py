#!/usr/bin/env python3
"""
Statcast Running Data Ingestion (Sprint Speed)

Fetches Statcast running leaderboard data from pybaseball and uploads to S3 as Parquet:
  {s3_prefix}/year=YYYY/statcast_sprint_speed.parquet.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd

try:
    from pybaseball import statcast_sprint_speed
except Exception:
    statcast_sprint_speed = None

from .ingest_common import retry_with_backoff
from ..pipeline.lake_paths import raw_sprint_speed_key
from ..pipeline.runtime import current_utc_year, event_or_env_int, event_or_env_str
from ..pipeline.s3_parquet import write_parquet_to_s3
from ..pipeline.settings import PipelineSettings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def fetch_sprint_speed_for_year(
    year: int,
    s3_bucket: str,
    s3_prefix: str,
    *,
    min_opp: int,
    max_retries: int = 3,
) -> dict:
    """
    Fetch Statcast sprint speed leaderboard for one season year (YYYY), then upload to S3.
    """
    if statcast_sprint_speed is None:
        raise ImportError("pybaseball is required for sprint speed ingestion. Install `pybaseball` in this environment.")

    current_year = datetime.now(timezone.utc).year
    if year > current_year + 1:
        return {"status": "error", "message": f"Year {year} is too far in the future (current UTC year: {current_year})."}

    def _fetch() -> pd.DataFrame:
        df = statcast_sprint_speed(year=year, min_opp=min_opp)
        if df is None:
            raise RuntimeError("statcast_sprint_speed returned None")
        if df.empty:
            logger.warning("No sprint speed rows returned for year=%d", year)
        return df

    df = retry_with_backoff(
        f"Statcast sprint speed year={year}",
        _fetch,
        max_retries=max_retries,
    )
    if df is None:
        return {"status": "error", "message": f"Fetch failed for year {year}"}

    out_key = raw_sprint_speed_key(s3_prefix, year)
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
        result = fetch_sprint_speed_for_year(year, s3_bucket, s3_prefix, min_opp=min_opp)
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
    cfg = PipelineSettings.from_environ()
    cy = current_utc_year()
    parser = argparse.ArgumentParser(
        description="Ingest Statcast sprint speed leaderboard to S3 (year range)."
    )
    parser.add_argument("--start-year", type=int, default=cy - 3)
    parser.add_argument("--end-year", type=int, default=cy)
    parser.add_argument("--min-opp", type=int, default=10)
    parser.add_argument("--s3-bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--s3-prefix", type=str, default=cfg.raw_running_prefix)
    args = parser.parse_args()

    result = ingest_year_range(
        args.start_year, args.end_year, args.s3_bucket, args.s3_prefix, min_opp=args.min_opp
    )

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


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    cy = current_utc_year()
    cfg = PipelineSettings.from_environ()
    start_year = event_or_env_int(event, "start_year", "START_YEAR", cy - 3)
    end_year = event_or_env_int(event, "end_year", "END_YEAR", cy)
    min_opp = event_or_env_int(event, "min_opp", "MIN_OPP", 10)
    s3_bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    s3_prefix = event_or_env_str(event, "s3_prefix", "S3_PREFIX", cfg.raw_running_prefix)

    result = ingest_year_range(start_year, end_year, s3_bucket, s3_prefix, min_opp=min_opp)
    status_code = 200 if result["status"] == "ok" else (207 if result["status"] == "partial" else 400)
    return {"statusCode": status_code, **result}


if __name__ == "__main__":
    main()

