#!/usr/bin/env python3
"""
Build a by-player processed layer from raw Statcast data.

Reads daily raw Parquet files from S3 at:
  {raw_prefix}/year=Y/date=YYYY-MM-DD/statcast_pitches.parquet

and writes/updates per-player Parquet files at:
  {processed_prefix}/{role}_id=<id>/year=Y/data.parquet

where role is either "pitcher" or "batter".
"""

import argparse
import io
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, Literal, Optional

import boto3
import pandas as pd

Role = Literal["pitcher", "batter"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

s3_client = boto3.client("s3")


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _build_raw_key(prefix: str, d: date) -> str:
    return f"{prefix}/year={d.year}/date={d.strftime('%Y-%m-%d')}/statcast_pitches.parquet"


def _build_processed_key(prefix: str, role: Role, player_id: int, year: int) -> str:
    return f"{prefix}/{role}_id={player_id}/year={year}/statcast_pitches.parquet"


def _read_parquet_from_s3(bucket: str, key: str) -> Optional[pd.DataFrame]:
    try:
        logger.info("Reading s3://%s/%s", bucket, key)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return pd.read_parquet(io.BytesIO(body))
    except s3_client.exceptions.NoSuchKey:
        logger.info("No existing object at s3://%s/%s (treat as empty)", bucket, key)
        return None
    except Exception as exc:
        logger.error("Error reading s3://%s/%s: %s", bucket, key, exc)
        raise


def _write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    logger.info("Writing %d rows to s3://%s/%s", len(df), bucket, key)
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    buf.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/x-parquet",
    )


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def build_by_player_layer(
    start_date_str: str,
    end_date_str: str,
    *,
    s3_bucket: str,
    raw_prefix: str,
    processed_prefix: str,
    role: Role = "pitcher",
) -> Dict[str, object]:
    """
    Build / update the by-player processed layer for all days in [start_date_str, end_date_str].

    Returns a summary dict with counts for logging / Lambda responses.
    """
    try:
        start = _parse_date(start_date_str)
        end = _parse_date(end_date_str)
    except ValueError as exc:
        return {
            "status": "error",
            "message": str(exc),
            "days_processed": 0,
            "players_updated": 0,
            "rows_written": 0,
        }

    if start > end:
        return {
            "status": "error",
            "message": f"start_date ({start_date_str}) must be <= end_date ({end_date_str})",
            "days_processed": 0,
            "players_updated": 0,
            "rows_written": 0,
        }

    logger.info(
        "Building by-player layer from %s to %s (role=%s, raw_prefix=%s, processed_prefix=%s)",
        start_date_str,
        end_date_str,
        role,
        raw_prefix,
        processed_prefix,
    )

    combined_raw: list[pd.DataFrame] = []
    days_with_data = 0

    for d in _date_range(start, end):
        key = _build_raw_key(raw_prefix, d)
        try:
            df = _read_parquet_from_s3(s3_bucket, key)
        except s3_client.exceptions.NoSuchKey:
            logger.warning("Raw file missing for %s at %s", d, key)
            continue
        if df is None or df.empty:
            logger.info("No rows for %s (empty DataFrame)", d)
            continue
        if "game_date" not in df.columns and "date" in df.columns:
            df = df.rename(columns={"date": "game_date"})
        combined_raw.append(df)
        days_with_data += 1

    if not combined_raw:
        msg = f"No raw data found between {start_date_str} and {end_date_str}"
        logger.warning(msg)
        return {
            "status": "no_data",
            "message": msg,
            "days_processed": 0,
            "players_updated": 0,
            "rows_written": 0,
        }

    full_raw = pd.concat(combined_raw, ignore_index=True)

    if role not in full_raw.columns:
        msg = f"Column '{role}' not found in raw data"
        logger.error(msg)
        return {
            "status": "error",
            "message": msg,
            "days_processed": days_with_data,
            "players_updated": 0,
            "rows_written": 0,
        }

    if "game_date" not in full_raw.columns:
        msg = "Column 'game_date' not found in raw data (needed for yearly partitioning)"
        logger.error(msg)
        return {
            "status": "error",
            "message": msg,
            "days_processed": days_with_data,
            "players_updated": 0,
            "rows_written": 0,
        }

    players_updated = 0
    rows_written = 0

    for player_id, player_df in full_raw.groupby(role):
        if pd.isna(player_id):
            continue
        players_updated += 1

        player_df = player_df.copy()
        player_df["year"] = pd.to_datetime(player_df["game_date"]).dt.year
        
        for year, df_year in player_df.groupby("year"):
            target_key = _build_processed_key(processed_prefix, role, int(player_id), int(year))
            existing_df = _read_parquet_from_s3(s3_bucket, target_key)

            if existing_df is not None and not existing_df.empty:
                combined = pd.concat([existing_df, df_year], ignore_index=True)
                dedup_cols = []
                for col in ("game_pk", "pitch_number", "at_bat_number"):
                    if col in combined.columns:
                        dedup_cols.append(col)
                if dedup_cols:
                    combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
                df_to_write = combined
            else:
                df_to_write = df_year

            _write_parquet_to_s3(df_to_write, s3_bucket, target_key)
            rows_written += len(df_to_write)

    status = "ok"
    message = (
        f"Built by-player layer for {start_date_str} to {end_date_str}: "
        f"{days_with_data} days with data, {players_updated} players updated, "
        f"{rows_written} rows written"
    )

    logger.info(message)
    return {
        "status": status,
        "message": message,
        "days_processed": days_with_data,
        "players_updated": players_updated,
        "rows_written": rows_written,
    }


def main() -> None:
    """
    CLI entrypoint for by-player build.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(
        description=(
            "Build by-player Statcast layer from raw daily files. "
            "Reads raw-data/statcast by default and writes processed/statcast."
        )
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=yesterday,
        help=f"Start date (YYYY-MM-DD). Default: yesterday UTC ({yesterday})",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=yesterday,
        help=f"End date (YYYY-MM-DD). Default: yesterday UTC ({yesterday})",
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        default=os.environ.get("S3_BUCKET", "diamond-dna"),
        help="S3 bucket name.",
    )
    parser.add_argument(
        "--raw-prefix",
        type=str,
        default=os.environ.get("RAW_PREFIX", "raw-data/statcast"),
        help="Raw S3 prefix/path (default: raw-data/statcast).",
    )
    parser.add_argument(
        "--processed-prefix",
        type=str,
        default=os.environ.get("PROCESSED_PREFIX", "processed/statcast"),
        help="Processed S3 prefix/path (default: processed/statcast).",
    )
    parser.add_argument(
        "--role",
        type=str,
        choices=["pitcher", "batter"],
        default=os.environ.get("ROLE", "pitcher"),
        help="Which player role to build (pitcher or batter). Default: pitcher.",
    )

    args = parser.parse_args()

    result = build_by_player_layer(
        args.start_date,
        args.end_date,
        s3_bucket=args.s3_bucket,
        raw_prefix=args.raw_prefix,
        processed_prefix=args.processed_prefix,
        role=args.role,  # type: ignore[arg-type]
    )

    if result["status"] != "ok":
        logger.error(result["message"])
        raise SystemExit(1)


def handler(event: Dict[str, object], context) -> Dict[str, object]:
    """
    Lambda entrypoint for by-player build.
    """
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    start_date = (
        event.get("start_date")
        if isinstance(event, dict)
        else None
    ) or os.environ.get("START_DATE") or yesterday

    end_date = (
        event.get("end_date")
        if isinstance(event, dict)
        else None
    ) or os.environ.get("END_DATE") or yesterday

    bucket = (
        event.get("s3_bucket")
        if isinstance(event, dict)
        else None
    ) or os.environ.get("S3_BUCKET", "diamond-dna")

    raw_prefix = (
        event.get("raw_prefix")
        if isinstance(event, dict)
        else None
    ) or os.environ.get("RAW_PREFIX", "raw-data/statcast")

    processed_prefix = (
        event.get("processed_prefix")
        if isinstance(event, dict)
        else None
    ) or os.environ.get("PROCESSED_PREFIX", "processed/statcast")

    role_value = (
        event.get("role")
        if isinstance(event, dict)
        else None
    ) or os.environ.get("ROLE", "pitcher")

    if role_value not in ("pitcher", "batter"):
        role_value = "pitcher"

    result = build_by_player_layer(
        str(start_date),
        str(end_date),
        s3_bucket=str(bucket),
        raw_prefix=str(raw_prefix),
        processed_prefix=str(processed_prefix),
        role=role_value,  # type: ignore[arg-type]
    )

    status_code = 200 if result.get("status") == "ok" else 400
    return {
        "statusCode": status_code,
        "body": result.get("message", ""),
        "details": result,
    }


if __name__ == "__main__":
    main()

