#!/usr/bin/env python3
"""
Build a by-player processed layer from raw Statcast data.

Reads daily raw Parquet files from S3 at:
  {raw_prefix}/year=Y/date=YYYY-MM-DD/statcast_pitches.parquet

and writes/updates per-player Parquet files at:
  {processed_prefix}/{role}/{role}_id=<id>/year=Y/statcast_pitches.parquet

where role is either "pitcher" or "batter".

Before splitting by player, the pipeline enriches each row with player name columns
via pybaseball's playerid_reverse_lookup (MLBAM IDs): batter_name, pitcher_name,
fielder_2_name, fielder_3_name, ... fielder_9_name. Requires pybaseball; if not
installed, name columns are skipped with a warning.
"""

import logging
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Literal, Optional, Set

import pandas as pd
from pybaseball import playerid_reverse_lookup

from ..pipeline.lake_paths import processed_statcast_player_year_key, raw_statcast_day_key
from ..s3_parquet import read_parquet_from_s3, write_parquet_to_s3

Role = Literal["pitcher", "batter"]

# Statcast ID columns for batter, pitcher, and fielders (positions 2–9). Name columns will be *_name.
PLAYER_ID_COLUMNS = [
    "batter",
    "pitcher",
    "fielder_2",
    "fielder_3",
    "fielder_4",
    "fielder_5",
    "fielder_6",
    "fielder_7",
    "fielder_8",
    "fielder_9",
]
LOOKUP_BATCH_SIZE = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _collect_unique_player_ids(df: pd.DataFrame) -> Set[int]:
    """Collect all unique non-null player IDs from batter, pitcher, and fielder columns."""
    ids: Set[int] = set()
    for col in PLAYER_ID_COLUMNS:
        if col not in df.columns:
            continue
        for val in df[col].dropna().unique():
            try:
                ids.add(int(val))
            except (ValueError, TypeError):
                pass
    return ids


def _lookup_player_names(player_ids: List[int], batch_size: int = LOOKUP_BATCH_SIZE) -> Dict[int, str]:
    """
    Resolve MLBAM player IDs to display names (Last, First) via pybaseball.
    Returns a dict mapping player_id -> "Last, First". Missing IDs are omitted.
    """
    if not player_ids:
        return {}
    id_to_name: Dict[int, str] = {}
    for i in range(0, len(player_ids), batch_size):
        batch = player_ids[i : i + batch_size]
        try:
            lookup_df = playerid_reverse_lookup(batch, key_type="mlbam")
        except Exception as exc:
            logger.warning("playerid_reverse_lookup failed for batch: %s", exc)
            continue
        if lookup_df is None or lookup_df.empty:
            continue
        for _, row in lookup_df.iterrows():
            raw_id = row.get("key_mlbam")
            if raw_id is None or pd.isna(raw_id):
                continue
            try:
                pid = int(raw_id)
            except (ValueError, TypeError):
                continue
            first = row.get("name_first") or ""
            last = row.get("name_last") or ""
            name = f"{last}, {first}".strip(", ") if (first or last) else ""
            if name:
                id_to_name[pid] = name
    return id_to_name


def _add_player_name_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add batter_name, pitcher_name, fielder_2_name, ... fielder_9_name using
    pybaseball playerid_reverse_lookup. Only adds columns for ID columns present.
    """
    present_id_cols = [c for c in PLAYER_ID_COLUMNS if c in df.columns]
    if not present_id_cols:
        return df
    unique_ids = _collect_unique_player_ids(df)
    if not unique_ids:
        return df
    logger.info("Resolving names for %d unique player IDs", len(unique_ids))
    id_to_name = _lookup_player_names(list(unique_ids))
    logger.info("Resolved %d player names", len(id_to_name))
    def safe_lookup(x: object) -> Optional[str]:
        if pd.isna(x):
            return None
        try:
            return id_to_name.get(int(x), None)
        except (ValueError, TypeError):
            return None

    out = df.copy()
    for id_col in present_id_cols:
        name_col = f"{id_col}_name"
        out[name_col] = out[id_col].apply(safe_lookup)
    return out


def build_by_player_layer(
    start_date_str: str,
    end_date_str: str,
    *,
    s3_bucket: str,
    raw_prefix: str,
    processed_prefix: str,
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
        "Building by-player layer from %s to %s (roles=pitcher,batter, raw_prefix=%s, processed_prefix=%s)",
        start_date_str,
        end_date_str,
        raw_prefix,
        processed_prefix,
    )

    combined_raw: list[pd.DataFrame] = []
    days_with_data = 0

    for d in _date_range(start, end):
        key = raw_statcast_day_key(raw_prefix, d)
        df = read_parquet_from_s3(s3_bucket, key)
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

    missing_roles = [r for r in ("pitcher", "batter") if r not in full_raw.columns]
    if missing_roles:
        msg = f"Column(s) not found in raw data: {', '.join(missing_roles)}"
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

    full_raw = _add_player_name_columns(full_raw)

    players_updated = 0
    rows_written = 0
    players_updated_by_role: Dict[str, int] = {"pitcher": 0, "batter": 0}
    rows_written_by_role: Dict[str, int] = {"pitcher": 0, "batter": 0}

    for role in ("pitcher", "batter"):
        for player_id, player_df in full_raw.groupby(role):
            if pd.isna(player_id):
                continue
            players_updated += 1
            players_updated_by_role[role] += 1

            player_df = player_df.copy()
            player_df["year"] = pd.to_datetime(player_df["game_date"]).dt.year

            for year, df_year in player_df.groupby("year"):
                target_key = processed_statcast_player_year_key(
                    processed_prefix, role, int(player_id), int(year)
                )
                existing_df = read_parquet_from_s3(s3_bucket, target_key)

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

                write_parquet_to_s3(df_to_write, s3_bucket, target_key)
                rows_written += len(df_to_write)
                rows_written_by_role[role] += len(df_to_write)

    status = "ok"
    message = (
        f"Built by-player layer for {start_date_str} to {end_date_str}: "
        f"{days_with_data} days with data, {players_updated} players updated "
        f"(pitcher={players_updated_by_role['pitcher']}, batter={players_updated_by_role['batter']}), "
        f"{rows_written} rows written "
        f"(pitcher={rows_written_by_role['pitcher']}, batter={rows_written_by_role['batter']})"
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
    from ..pipeline.cli import run_processing_statcast_by_player_main

    run_processing_statcast_by_player_main()


def handler(event: Dict[str, object], context) -> Dict[str, object]:
    from ..pipeline.handlers import statcast_by_player_handler

    return statcast_by_player_handler(event, context)


if __name__ == "__main__":
    main()

