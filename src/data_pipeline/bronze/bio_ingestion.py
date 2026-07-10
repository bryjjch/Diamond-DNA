#!/usr/bin/env python3
"""
Player bio ingestion (all MLB players rostered in a season).

Fetches player bios (birth date/place, height, weight, handedness, position) from
the MLB Stats API season-players endpoint and uploads Parquet under:

  {s3_prefix}/year=YYYY/mlb_player_bios.parquet

Default s3_prefix: bronze/bio
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
import requests

from .ingest_common import retry_with_backoff
from ...common.s3_interaction import raw_player_bio_key, write_parquet_to_s3

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

MLB_SEASON_PLAYERS_URL = "https://statsapi.mlb.com/api/v1/sports/1/players?season={year}"

_HEIGHT_RE = re.compile(r"(\d+)\s*'\s*(\d+)")


def parse_height_inches(height: object) -> float:
    """Convert an MLB Stats API height string like ``6' 2"`` to total inches."""
    m = _HEIGHT_RE.search(str(height or ""))
    if not m:
        return float("nan")
    return float(int(m.group(1)) * 12 + int(m.group(2)))


def person_to_bio_row(person: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten one MLB Stats API person record into a bronze bio row."""
    return {
        "player_id": person.get("id"),
        "full_name": person.get("fullName"),
        "birth_date": person.get("birthDate"),
        "birth_city": person.get("birthCity"),
        "birth_state_province": person.get("birthStateProvince"),
        "birth_country": person.get("birthCountry"),
        "height": person.get("height"),
        "height_in": parse_height_inches(person.get("height")),
        "weight_lb": person.get("weight"),
        "bat_side": (person.get("batSide") or {}).get("code"),
        "pitch_hand": (person.get("pitchHand") or {}).get("code"),
        "primary_position": (person.get("primaryPosition") or {}).get("abbreviation"),
        "mlb_debut_date": person.get("mlbDebutDate"),
        "draft_year": person.get("draftYear"),
        "active": person.get("active"),
        "current_team_id": (person.get("currentTeam") or {}).get("id"),
        "current_team": (person.get("currentTeam") or {}).get("name"),
    }


def fetch_mlb_player_bios(year: int) -> pd.DataFrame:
    """
    Season player list (MLB Stats API): one bio row per player rostered in ``year``.
    """
    res = requests.get(MLB_SEASON_PLAYERS_URL.format(year=year), timeout=120)
    res.raise_for_status()
    people: List[Dict[str, Any]] = res.json().get("people", [])
    df = pd.DataFrame([person_to_bio_row(p) for p in people])
    if df.empty:
        logger.warning("No player bios returned for year=%d", year)
        return df
    return df.loc[df["player_id"].notna()].reset_index(drop=True)


def ingest_bio_year(
    year: int,
    s3_bucket: str,
    s3_prefix: str,
    *,
    max_retries: int = 3,
) -> dict:
    """
    Fetch player bios for one season year (YYYY), then upload to S3.
    """
    current_year = datetime.now(timezone.utc).year
    if year > current_year + 1:
        return {"status": "error", "message": f"Year {year} is too far in the future (current UTC year: {current_year})."}

    df = retry_with_backoff(
        f"MLB player bios year={year}",
        lambda: fetch_mlb_player_bios(year),
        max_retries=max_retries,
    )
    if df is None:
        return {"status": "error", "message": f"Fetch failed for year {year}"}

    out_key = raw_player_bio_key(s3_prefix, year)
    logger.info("Uploading %d player bio rows to s3://%s/%s", len(df), s3_bucket, out_key)
    write_parquet_to_s3(df, s3_bucket, out_key, log_write=False)
    return {"status": "ok", "message": "OK", "records": int(len(df))}


def ingest_year_range(start_year: int, end_year: int, s3_bucket: str, s3_prefix: str) -> dict:
    """
    Ingest player bios for each year in [start_year, end_year].
    """
    if start_year > end_year:
        return {"status": "error", "message": "start_year must be <= end_year"}

    years_ok = 0
    years_error = 0
    total_records = 0
    errors = []

    for year in range(start_year, end_year + 1):
        result = ingest_bio_year(year, s3_bucket, s3_prefix)
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
    from ...common.cli import run_bio_ingestion_main

    run_bio_ingestion_main()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ...common.handlers import bio_ingestion_handler

    return bio_ingestion_handler(event, context)


if __name__ == "__main__":
    main()
