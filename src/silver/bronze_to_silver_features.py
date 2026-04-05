#!/usr/bin/env python3
"""
Bronze → silver: build player-year archetype feature tables from bronze data.

Reads bronze data from the S3 bucket and writes silver data to the S3 bucket.

For scheduled runs that only pass ``end_date`` (e.g. yesterday), use ``year_to_date=True`` so
bronze is loaded from Jan 1 through ``end_date`` for each affected calendar year—matching
season-to-date aggregates.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from ..pipeline.s3_interaction import (
    feature_player_year_output_key,
    raw_statcast_day_key,
    read_parquet_from_s3,
    write_parquet_to_s3,
)
from .silver_build_player_year_archetype_rows import (
    _validate_feature_row,
    player_year_features_from_df,
)
from .silver_defence_player_year import (
    fangraphs_to_mlbam_map,
    load_defence_metrics_by_player_year,
    merge_defence_into_row,
)
from .silver_player_names import (
    build_mlbam_statcast_style_name_map,
    resolve_mlbam_display_name,
)
from .silver_sprint_helper import build_sprint_speed_lookups_by_year

logger = logging.getLogger(__name__)

def _parse_date(value: str) -> date:
    """Parse a date string and return a date object."""
    return datetime.strptime(value, "%Y-%m-%d").date()


def _date_range(start: date, end: date) -> Iterable[date]:
    """Generate a range of dates between start and end."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _effective_bronze_window(
    start_date: date,
    end_date: date,
    *,
    year_to_date: bool,
) -> Tuple[date, date]:
    """Return (bronze_start, bronze_end) for loading daily files."""
    if not year_to_date:
        return start_date, end_date
    years = sorted({d.year for d in _date_range(start_date, end_date)})
    bronze_start = date(years[0], 1, 1)
    bronze_end = end_date
    return bronze_start, bronze_end


def normalize_statcast_bronze_df(df: pd.DataFrame) -> pd.DataFrame:
    """Align bronze schema with feature builder expectations."""
    out = df.copy()
    if "game_date" not in out.columns and "date" in out.columns:
        out = out.rename(columns={"date": "game_date"})
    for col in ("pitcher", "batter"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _dedupe_pitches(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate pitches by game_pk, pitch_number, and at_bat_number."""
    dedup_cols = [c for c in ("game_pk", "pitch_number", "at_bat_number") if c in df.columns]
    if not dedup_cols:
        return df
    return df.drop_duplicates(subset=dedup_cols, keep="last")


def load_bronze_statcast_range(
    bucket: str,
    bronze_prefix: str,
    start_date: date,
    end_date: date,
) -> Optional[pd.DataFrame]:
    """Load and concatenate bronze daily files in [start_date, end_date]."""
    frames: List[pd.DataFrame] = []
    for d in _date_range(start_date, end_date):
        key = raw_statcast_day_key(bronze_prefix, d)
        df = read_parquet_from_s3(bucket, key)
        if df is None or df.empty:
            continue
        frames.append(normalize_statcast_bronze_df(df))

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def build_bronze_to_silver_features(
    *,
    bucket: str,
    bronze_statcast_prefix: str,
    silver_prefix: str,
    start_date_str: str,
    end_date_str: str,
    year_to_date: bool = True,
    min_pitches_pitcher: int = 500,
    min_pitches_batter: int = 500,
    min_batted_ball_batter: int = 200,
    hard_hit_speed_mph: float = 95.0,
    min_pitches_per_pitch_type: int = 15,
    raw_running_prefix: str,
    sprint_speed_min_opp: int = 10,
    raw_defence_prefix: str,
) -> Dict[str, object]:
    """
    Build silver player-year feature tables from bronze Statcast dailies.

    When ``year_to_date`` is True, expands the bronze read to ``date(Y,1,1) .. end_date``
    for every calendar year Y present in the inclusive ``[start_date_str, end_date_str]`` window,
    so a single-day scheduled job still aggregates season-to-date.
    """
    try:
        start_user = _parse_date(start_date_str)
        end_user = _parse_date(end_date_str)
    except ValueError as exc:
        return {
            "status": "error",
            "message": str(exc),
            "years_written": [],
            "rows_written": 0,
        }

    if start_user > end_user:
        return {
            "status": "error",
            "message": f"start_date ({start_date_str}) must be <= end_date ({end_date_str})",
            "years_written": [],
            "rows_written": 0,
        }

    bronze_start, bronze_end = _effective_bronze_window(start_user, end_user, year_to_date=year_to_date)
    logger.info(
        "Loading bronze Statcast %s .. %s (user window %s .. %s, year_to_date=%s)",
        bronze_start,
        bronze_end,
        start_user,
        end_user,
        year_to_date,
    )

    # Load the bronze Statcast data.
    full_raw = load_bronze_statcast_range(bucket, bronze_statcast_prefix, bronze_start, bronze_end)
    if full_raw is None or full_raw.empty:
        msg = f"No bronze Statcast data between {bronze_start} and {bronze_end}"
        logger.warning(msg)
        return {
            "status": "no_data",
            "message": msg,
            "years_written": [],
            "rows_written": 0,
        }

    missing_roles = [r for r in ("pitcher", "batter") if r not in full_raw.columns]
    if missing_roles:
        msg = f"Column(s) not found in bronze data: {', '.join(missing_roles)}"
        logger.error(msg)
        return {"status": "error", "message": msg, "years_written": [], "rows_written": 0}

    if "game_date" not in full_raw.columns:
        msg = "Column 'game_date' not found in bronze data (needed for yearly grouping)"
        logger.error(msg)
        return {"status": "error", "message": msg, "years_written": [], "rows_written": 0}

    full_raw = full_raw.copy()
    full_raw["year"] = pd.to_datetime(full_raw["game_date"]).dt.year

    year_lo = int(full_raw["year"].min())
    year_hi = int(full_raw["year"].max())

    chadwick_df: Optional[pd.DataFrame] = None
    name_by_mlbam: Dict[int, str] = {}
    try:
        from pybaseball import chadwick_register as _chadwick_register

        chadwick_df = _chadwick_register()
        name_by_mlbam = build_mlbam_statcast_style_name_map(chadwick_df)
    except Exception as exc:
        logger.warning(
            "Could not load Chadwick register (%s); pitcher names may be blank and FanGraphs id map falls back to a separate load.",
            exc,
        )

    years_written: List[int] = []
    rows_written = 0

    for role in ("batter", "pitcher"):
        # Build the sprint speed lookup by year.
        sprint_lookup_by_year: Dict[int, Dict[int, float]] = {}
        if role == "batter":
            sprint_lookup_by_year = build_sprint_speed_lookups_by_year(
                bucket,
                raw_running_prefix,
                year_lo,
                year_hi,
                sprint_speed_min_opp,
            )

        # Build the defence metrics by year.
        defence_by_year: Dict[int, Dict[int, Dict[str, float]]] = {}
        if role == "batter":
            fg_map = fangraphs_to_mlbam_map(chadwick_df) if chadwick_df is not None else fangraphs_to_mlbam_map()
            for y in range(year_lo, year_hi + 1):
                defence_by_year[y] = load_defence_metrics_by_player_year(
                    bucket,
                    raw_defence_prefix,
                    y,
                    fg_id_map=fg_map,
                )

        feature_rows: List[Dict[str, object]] = []
        # Group the full raw data by role and drop missing values.
        grouped = full_raw.groupby(role, dropna=True)
        n_players = grouped.ngroups
        for idx, (player_id, player_df) in enumerate(grouped):
            if pd.isna(player_id):
                continue
            pid = int(player_id)
            if (idx % 100) == 0:
                logger.info("Role %s: processing player %d / %d (player_id=%s)", role, idx + 1, n_players, pid)

            # Group the player data by year and deduplicate pitches.
            for year, df_year in player_df.groupby("year"):
                y = int(year)
                df_work = _dedupe_pitches(df_year.copy())

                display_name = resolve_mlbam_display_name(pid, name_by_mlbam)

                # Build the player-year features from the deduplicated data (sprint speed lookup included for batters).
                row = player_year_features_from_df(
                    df=df_work,
                    role=role,
                    player_id=pid,
                    year=y,
                    player_name=display_name,
                    min_pitches_pitcher=min_pitches_pitcher,
                    min_pitches_batter=min_pitches_batter,
                    min_batted_ball_batter=min_batted_ball_batter,
                    hard_hit_speed_mph=hard_hit_speed_mph,
                    min_pitches_per_pitch_type=min_pitches_per_pitch_type,
                    sprint_speed_lookup=sprint_lookup_by_year.get(y) if role == "batter" else None,
                )
                if row is None:
                    continue

                # Merge the defence metrics into the row.
                if role == "batter":
                    merge_defence_into_row(row, defence_by_year.get(y, {}))

                # Validate the feature row.
                _validate_feature_row(row, role=role)
                feature_rows.append(row)

        if not feature_rows:
            logger.warning("No feature rows computed for role=%s.", role)
            continue

        features_df = pd.DataFrame(feature_rows)
        # Write the feature rows to the S3 bucket.
        for y in range(year_lo, year_hi + 1):
            df_year = features_df[features_df["year"] == y]
            if df_year.empty:
                continue
            out_key = feature_player_year_output_key(silver_prefix, role, y)
            logger.info(
                "Writing %d %s feature rows to s3://%s/%s",
                len(df_year),
                role,
                bucket,
                out_key,
            )
            write_parquet_to_s3(df_year, bucket, out_key, log_write=False)
            rows_written += len(df_year)
            if y not in years_written:
                years_written.append(y)

    years_written.sort()
    message = (
        f"Bronze→silver features: bronze window {bronze_start}..{bronze_end}, "
        f"wrote {rows_written} rows across years {years_written}"
    )
    logger.info(message)
    return {
        "status": "ok",
        "message": message,
        "years_written": years_written,
        "rows_written": rows_written,
        "bronze_start": bronze_start.isoformat(),
        "bronze_end": bronze_end.isoformat(),
    }


def main() -> None:
    from ..pipeline.cli import run_bronze_to_silver_features_main

    run_bronze_to_silver_features_main()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    from ..pipeline.handlers import bronze_to_silver_features_handler

    return bronze_to_silver_features_handler(event, context)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
