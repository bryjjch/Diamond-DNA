#!/usr/bin/env python3
"""
Standard season stat-line ingestion (batters + pitchers).

Fetches conventional season totals from the MLB Stats API season-stats endpoint and
uploads Parquet under:

  {s3_prefix}/year=YYYY/mlb_standard_batting.parquet
  {s3_prefix}/year=YYYY/mlb_standard_pitching.parquet

Default s3_prefix: bronze/standard_stats

The MLB Stats API is used (rather than a FanGraphs leaderboard) so rows carry native
MLBAM player ids—matching the rest of the pipeline, which keys on ``player_id``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Tuple

import pandas as pd
import requests

from .ingest_common import retry_with_backoff
from ...common.s3_interaction import raw_standard_stats_key, write_parquet_to_s3

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# season stats, regular season, one split per player who logged the group's stats.
MLB_SEASON_STATS_URL = (
    "https://statsapi.mlb.com/api/v1/stats"
    "?stats=season&season={year}&group={group}&gameType=R&limit=2000"
)

# (bronze_col, statsapi_stat_key) pairs. Explicit so the bronze schema stays stable
# regardless of the API's field ordering or additions.
_BATTING_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("g", "gamesPlayed"),
    ("pa", "plateAppearances"),
    ("ab", "atBats"),
    ("r", "runs"),
    ("h", "hits"),
    ("doubles", "doubles"),
    ("triples", "triples"),
    ("hr", "homeRuns"),
    ("rbi", "rbi"),
    ("bb", "baseOnBalls"),
    ("ibb", "intentionalWalks"),
    ("so", "strikeOuts"),
    ("sb", "stolenBases"),
    ("cs", "caughtStealing"),
    ("hbp", "hitByPitch"),
    ("sf", "sacFlies"),
    ("sh", "sacBunts"),
    ("gidp", "groundIntoDoublePlay"),
    ("tb", "totalBases"),
    ("avg", "avg"),
    ("obp", "obp"),
    ("slg", "slg"),
    ("ops", "ops"),
)
_PITCHING_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("g", "gamesPlayed"),
    ("gs", "gamesStarted"),
    ("ip", "inningsPitched"),
    ("w", "wins"),
    ("l", "losses"),
    ("sv", "saves"),
    ("hld", "holds"),
    ("bf", "battersFaced"),
    ("h", "hits"),
    ("r", "runs"),
    ("er", "earnedRuns"),
    ("hr", "homeRuns"),
    ("bb", "baseOnBalls"),
    ("ibb", "intentionalWalks"),
    ("so", "strikeOuts"),
    ("hbp", "hitByPitch"),
    ("era", "era"),
    ("whip", "whip"),
)

# group name -> (statsapi group param, field spec).
_GROUPS: Dict[str, Tuple[str, Tuple[Tuple[str, str], ...]]] = {
    "batting": ("hitting", _BATTING_FIELDS),
    "pitching": ("pitching", _PITCHING_FIELDS),
}
ALL_GROUPS: Tuple[str, ...] = ("batting", "pitching")


def _split_to_row(split: Dict[str, Any], fields: Sequence[Tuple[str, str]]) -> Dict[str, Any]:
    """Flatten one statsapi 'split' (player + stat block) into a bronze row."""
    player = split.get("player") or {}
    stat = split.get("stat") or {}
    row: Dict[str, Any] = {
        "player_id": player.get("id"),
        "full_name": player.get("fullName"),
        "team_id": (split.get("team") or {}).get("id"),
        "team_name": (split.get("team") or {}).get("name"),
    }
    for col, key in fields:
        row[col] = stat.get(key)
    return row


def fetch_standard_stats(year: int, group: str) -> pd.DataFrame:
    """
    Season stat lines (MLB Stats API): one row per player for ``group``
    ('batting'|'pitching') in ``year``.
    """
    api_group, fields = _GROUPS[group]
    url = MLB_SEASON_STATS_URL.format(year=year, group=api_group)
    res = requests.get(url, timeout=120)
    res.raise_for_status()
    stats_blocks = res.json().get("stats") or []
    splits: List[Dict[str, Any]] = stats_blocks[0].get("splits", []) if stats_blocks else []
    df = pd.DataFrame([_split_to_row(s, fields) for s in splits])
    if df.empty:
        logger.warning("No %s stat lines returned for year=%d", group, year)
        return df
    return df.loc[df["player_id"].notna()].reset_index(drop=True)


def ingest_standard_stats_year(
    year: int,
    s3_bucket: str,
    s3_prefix: str,
    *,
    max_retries: int = 3,
) -> dict:
    """
    Fetch batting + pitching stat lines for one season year (YYYY), then upload to S3.
    """
    current_year = datetime.now(timezone.utc).year
    if year > current_year + 1:
        return {"status": "error", "message": f"Year {year} is too far in the future (current UTC year: {current_year})."}

    records = 0
    for group in ALL_GROUPS:
        df = retry_with_backoff(
            f"MLB standard {group} year={year}",
            lambda g=group: fetch_standard_stats(year, g),
            max_retries=max_retries,
        )
        if df is None:
            return {"status": "error", "message": f"Fetch failed for {group} year {year}"}
        out_key = raw_standard_stats_key(s3_prefix, year, group)
        logger.info("Uploading %d %s stat lines to s3://%s/%s", len(df), group, s3_bucket, out_key)
        write_parquet_to_s3(df, s3_bucket, out_key, log_write=False)
        records += int(len(df))

    return {"status": "ok", "message": "OK", "records": records}


def ingest_year_range(start_year: int, end_year: int, s3_bucket: str, s3_prefix: str) -> dict:
    """
    Ingest standard stat lines for each year in [start_year, end_year].
    """
    if start_year > end_year:
        return {"status": "error", "message": "start_year must be <= end_year"}

    years_ok = 0
    years_error = 0
    total_records = 0
    errors = []

    for year in range(start_year, end_year + 1):
        result = ingest_standard_stats_year(year, s3_bucket, s3_prefix)
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
    from ...common.cli import run_standard_stats_ingestion_main

    run_standard_stats_ingestion_main()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ...common.handlers import standard_stats_ingestion_handler

    return standard_stats_ingestion_handler(event, context)


if __name__ == "__main__":
    main()
