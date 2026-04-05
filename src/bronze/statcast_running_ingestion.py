#!/usr/bin/env python3
"""
Statcast Running Data Ingestion (Sprint Speed)

Fetches Statcast running leaderboard data from pybaseball and uploads to S3 as Parquet:
  {s3_prefix}/year=YYYY/statcast_sprint_speed.parquet.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd

try:
    from pybaseball import statcast_sprint_speed
except Exception:
    statcast_sprint_speed = None

from .ingest_common import retry_with_backoff
from ..pipeline.s3_interaction import raw_sprint_speed_key, write_parquet_to_s3

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
    from ..pipeline.cli import run_statcast_running_ingestion_main

    run_statcast_running_ingestion_main()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..pipeline.handlers import statcast_running_ingestion_handler

    return statcast_running_ingestion_handler(event, context)


if __name__ == "__main__":
    main()

