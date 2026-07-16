#!/usr/bin/env python3
"""
Bronze → silver: build player-year archetype feature tables from bronze data.

Reads bronze data from the S3 bucket and writes silver data to the S3 bucket.
Feature aggregation lives in ``player_year_feature_table.compute_player_year_features``.

For scheduled runs that only pass ``end_date`` (e.g. yesterday), use ``year_to_date=True`` so
bronze is loaded from Jan 1 through ``end_date`` for each affected calendar year—matching
season-to-date aggregates.
"""

from __future__ import annotations

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from ....common.runtime_helpers import event_or_env_str, yesterday_utc_date_str
from ....common.s3_helpers import get_s3_client, read_parquet_from_s3, write_parquet_to_s3
from ....common.settings import PipelineSettings
from ....common.timing import log_phase
from .bio_player_year_helper import load_player_bios_by_year, merge_bio_features
from .defence_player_year_helper import (
    load_defence_metrics_by_player_year,
    load_primary_positions_by_player_year,
    merge_defence_features,
    merge_primary_position_features,
)
from .player_names_helper import build_mlbam_statcast_style_name_map
from .player_year_feature_table import compute_player_year_features
from .sprint_helper import apply_sprint_speed_lookup, build_sprint_speed_lookups_by_year

logger = logging.getLogger(__name__)

# Columns the feature builder actually uses. Bronze Statcast files carry ~90 columns,
# so projecting the read to these cuts parse time and memory several-fold. Columns
# missing from older files are skipped at read time rather than erroring.
BRONZE_STATCAST_COLUMNS: Tuple[str, ...] = (
    # identity / grouping / dedupe
    "game_date",
    "date",
    "batter",
    "pitcher",
    "game_pk",
    "at_bat_number",
    "pitch_number",
    # location & swing/zone flags
    "description",
    "type",
    "zone",
    "plate_x",
    "plate_z",
    "sz_top",
    "sz_bot",
    # pitch physics (pitcher features)
    "pitch_type",
    "release_speed",
    "release_spin_rate",
    "release_extension",
    "pfx_x",
    "pfx_z",
    "delta_run_exp",
    # batted-ball outcomes / platoon splits (batter features)
    "stand",
    "events",
    "bb_type",
    "hc_x",
    "hc_y",
    "launch_speed",
    "launch_angle",
    "iso_value",
    "estimated_slg_using_speedangle",
    "estimated_woba_using_speedangle",
    "woba_value",
    "sprint_speed",
)

# S3 GETs are I/O-bound; concurrent day reads speed up the load phase near-linearly.
DEFAULT_S3_READ_WORKERS = 16


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
    # Bronze parquet stores numerics as pandas nullable dtypes (Float64/Int64), whose
    # pd.NA breaks numpy ops in the feature builders; convert to float64 with np.nan.
    nullable_numeric = {
        col: "float64"
        for col, dtype in out.dtypes.items()
        if isinstance(dtype, pd.api.extensions.ExtensionDtype)
        and pd.api.types.is_numeric_dtype(dtype)
    }
    if nullable_numeric:
        out = out.astype(nullable_numeric)
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
    *,
    columns: Optional[Sequence[str]] = BRONZE_STATCAST_COLUMNS,
    max_workers: int = DEFAULT_S3_READ_WORKERS,
) -> Optional[pd.DataFrame]:
    """
    Load and concatenate bronze daily files in [start_date, end_date].

    Days are fetched concurrently and each read is projected to ``columns``
    (pass ``columns=None`` to load every column).
    """
    keys = [
        (
            f"{bronze_prefix.strip('/')}/year={d.year}"
            f"/date={d.strftime('%Y-%m-%d')}/statcast_pitches.parquet"
        )
        for d in _date_range(start_date, end_date)
    ]
    if not keys:
        return None

    client = get_s3_client()

    def _read_day(key: str) -> Optional[pd.DataFrame]:
        return read_parquet_from_s3(
            bucket,
            key,
            client=client,
            log_read=False,
            missing_key_log="none",
            columns=columns,
        )

    frames: List[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(keys))) as pool:
        for df in pool.map(_read_day, keys):
            if df is None or df.empty:
                continue
            frames.append(normalize_statcast_bronze_df(df))

    logger.info(
        "Loaded %d/%d bronze daily files (%d rows) for %s..%s",
        len(frames),
        len(keys),
        sum(len(f) for f in frames),
        start_date,
        end_date,
    )
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _validate_feature_frame(df: pd.DataFrame, *, role: str) -> None:
    """Checks to catch broken derived flags."""
    # Required rates for batter features.
    required_rates = ["swing_rate", "zone_swing_rate", "chase_rate", "whiff_rate"]
    # Required rates for pitcher features.
    pitcher_rates = ["batter_swing_rate", "batter_zone_swing_rate", "batter_chase_rate", "batter_whiff_rate"]

    # If the role is a batter, add the required rates for batter features.
    if role == "batter":
        rates_to_check = required_rates + [
            "walk_rate",
            "barrel_rate",
            "hard_hit_rate",
            "pull_percent",
            "opposite_field_percent",
            "gb_percent",
            "ld_percent",
            "fb_percent",
            "iffb_percent",
            "sweet_spot_percent",
            "def_actual_fielding_success_rate_mean",
            "def_adj_estimated_fielding_success_rate_mean",
            "def_outfield_catch_completion_rate",
        ]
    # If the role is a pitcher, add the required rates for pitcher features.
    else:
        rates_to_check = pitcher_rates + [
            "in_zone_rate",
            "edge_percent",
            "meatball_percent",
            "first_pitch_strike_rate",
            "gb_percent_allowed",
            "ld_percent_allowed",
            "fb_percent_allowed",
            "iffb_percent_allowed",
        ]

    cols = [c for c in rates_to_check if c in df.columns]
    if not cols:
        return
    vals = df[cols].apply(pd.to_numeric, errors="coerce")
    bad = vals.lt(0.0) | vals.gt(1.0)
    if bad.any().any():
        flat = bad.stack()
        idx, col = flat[flat].index[0]
        raise ValueError(
            f"Sanity check failed: {col}={vals.at[idx, col]} out of [0,1] for role={role}, "
            f"player={df.at[idx, 'player_id']}, year={df.at[idx, 'year']}"
        )


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
    raw_running_prefix: str = "bronze/statcast_running",
    sprint_speed_min_opp: int = 10,
    raw_defence_prefix: str = "bronze/defence",
    raw_bio_prefix: str = "bronze/bio"
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

    chadwick_df: Optional[pd.DataFrame] = None
    name_by_mlbam: Dict[int, str] = {}
    try:
        from pybaseball import chadwick_register as _chadwick_register

        with log_phase(logger, "load Chadwick register"):
            chadwick_df = _chadwick_register()
        name_by_mlbam = build_mlbam_statcast_style_name_map(chadwick_df)
    except Exception as exc:
        logger.warning(
            "Could not load Chadwick register (%s); pitcher names may be blank.",
            exc,
        )

    # Bronze is loaded and processed one calendar year at a time so peak memory is
    # bounded by a single season regardless of the requested range. Feature tables are
    # tiny compared to pitch data, so accumulating them across years is cheap.
    feature_frames_by_role: Dict[str, List[pd.DataFrame]] = {"batter": [], "pitcher": []}
    loaded_any = False

    for batch_year in range(bronze_start.year, bronze_end.year + 1):
        batch_start = max(bronze_start, date(batch_year, 1, 1))
        batch_end = min(bronze_end, date(batch_year, 12, 31))

        with log_phase(logger, f"year={batch_year}: load bronze {batch_start}..{batch_end}"):
            raw = load_bronze_statcast_range(bucket, bronze_statcast_prefix, batch_start, batch_end)
        if raw is None or raw.empty:
            logger.warning("No bronze Statcast data between %s and %s", batch_start, batch_end)
            continue
        loaded_any = True

        missing_roles = [r for r in ("pitcher", "batter") if r not in raw.columns]
        if missing_roles:
            msg = f"Column(s) not found in bronze data: {', '.join(missing_roles)}"
            logger.error(msg)
            return {"status": "error", "message": msg, "years_written": [], "rows_written": 0}

        if "game_date" not in raw.columns:
            msg = "Column 'game_date' not found in bronze data (needed for yearly grouping)"
            logger.error(msg)
            return {"status": "error", "message": msg, "years_written": [], "rows_written": 0}

        raw["year"] = pd.to_datetime(raw["game_date"]).dt.year
        # (game_pk, at_bat_number, pitch_number) uniquely identify a pitch, so one
        # batch-wide dedupe is equivalent to the old per-player-year dedupe.
        raw = _dedupe_pitches(raw)

        # Normally just batch_year, but derived from game_date to stay faithful to the data.
        year_lo = int(raw["year"].min())
        year_hi = int(raw["year"].max())

        with log_phase(logger, f"year={batch_year}: load bio/sprint/defence lookups"):
            # Player bios apply to both roles; load once per year.
            bios_by_year: Dict[int, Dict[int, Dict[str, object]]] = {
                y: load_player_bios_by_year(bucket, raw_bio_prefix, y)
                for y in range(year_lo, year_hi + 1)
            }
            # Sprint speed, defence metrics, and primary positions apply to batters only.
            sprint_lookup_by_year: Dict[int, Dict[int, float]] = build_sprint_speed_lookups_by_year(
                bucket,
                raw_running_prefix,
                year_lo,
                year_hi,
                sprint_speed_min_opp,
            )
            defence_by_year: Dict[int, Dict[int, Dict[str, float]]] = {}
            positions_by_year: Dict[int, Dict[int, str]] = {}
            for y in range(year_lo, year_hi + 1):
                defence_by_year[y] = load_defence_metrics_by_player_year(
                    bucket,
                    raw_defence_prefix,
                    y,
                )
                positions_by_year[y] = load_primary_positions_by_player_year(
                    bucket,
                    raw_defence_prefix,
                    y,
                )

        for role in ("batter", "pitcher"):
            with log_phase(logger, f"year={batch_year}: compute {role} features"):
                # Aggregate every (player, year) group in one vectorized pass.
                feats = compute_player_year_features(
                    raw,
                    role=role,
                    min_pitches_pitcher=min_pitches_pitcher,
                    min_pitches_batter=min_pitches_batter,
                    min_batted_ball_batter=min_batted_ball_batter,
                    hard_hit_speed_mph=hard_hit_speed_mph,
                    min_pitches_per_pitch_type=min_pitches_per_pitch_type,
                )
                if feats.empty:
                    logger.warning("No %s feature rows for year batch %d.", role, batch_year)
                    continue

                feats.insert(
                    3, "player_name", feats["player_id"].map(name_by_mlbam).fillna("")
                )

                # Enrichment side tables join on (player_id, year).
                if role == "batter":
                    feats = apply_sprint_speed_lookup(feats, sprint_lookup_by_year)
                    feats = merge_defence_features(feats, defence_by_year)
                    feats = merge_primary_position_features(feats, positions_by_year)
                feats = merge_bio_features(feats, bios_by_year)

                _validate_feature_frame(feats, role=role)
                feature_frames_by_role[role].append(feats)

        # Release this year's pitch data before loading the next.
        del raw

    if not loaded_any:
        msg = f"No bronze Statcast data between {bronze_start} and {bronze_end}"
        logger.warning(msg)
        return {
            "status": "no_data",
            "message": msg,
            "years_written": [],
            "rows_written": 0,
        }

    years_written: List[int] = []
    rows_written = 0

    with log_phase(logger, "write silver feature tables"):
        for role in ("batter", "pitcher"):
            feature_frames = feature_frames_by_role[role]
            if not feature_frames:
                logger.warning("No feature rows computed for role=%s.", role)
                continue

            features_df = pd.concat(feature_frames, ignore_index=True, sort=False)
            # Write the feature rows to the S3 bucket.
            for y in sorted(features_df["year"].unique()):
                y = int(y)
                df_year = features_df[features_df["year"] == y]
                if df_year.empty:
                    continue
                out_key = f"{silver_prefix.strip('/')}/{role}/year={y}/player_year_features.parquet"
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
    cfg = PipelineSettings.from_environ()
    yesterday = yesterday_utc_date_str()
    parser = argparse.ArgumentParser(
        description=(
            "Build silver player-year feature tables from bronze Statcast dailies. "
            "By default loads year-to-date through --end-date for each affected season."
        )
    )
    parser.add_argument("--start-date", type=str, default=yesterday)
    parser.add_argument("--end-date", type=str, default=yesterday)
    parser.add_argument(
        "--no-year-to-date",
        action="store_true",
        help="Only load bronze for [start-date, end-date] exactly (no Jan 1 expansion).",
    )
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--bronze-prefix", type=str, default=cfg.raw_statcast_prefix)
    parser.add_argument("--silver-prefix", type=str, default=cfg.feature_prefix)
    parser.add_argument("--min-pitches-pitcher", type=int, default=500)
    parser.add_argument("--min-pitches-batter", type=int, default=500)
    parser.add_argument("--min-batted-ball-batter", type=int, default=200)
    parser.add_argument("--hard-hit-speed-mph", type=float, default=95.0)
    parser.add_argument("--min-pitches-per-pitch-type", type=int, default=15)
    parser.add_argument("--raw-running-prefix", type=str, default=cfg.raw_running_prefix)
    parser.add_argument("--sprint-speed-min-opp", type=int, default=10)
    parser.add_argument("--raw-defence-prefix", type=str, default=cfg.raw_defence_prefix)
    parser.add_argument("--raw-bio-prefix", type=str, default=cfg.raw_bio_prefix)
    args = parser.parse_args()

    result = build_bronze_to_silver_features(
        bucket=args.bucket,
        bronze_statcast_prefix=args.bronze_prefix,
        silver_prefix=args.silver_prefix,
        start_date_str=args.start_date,
        end_date_str=args.end_date,
        year_to_date=not args.no_year_to_date,
        min_pitches_pitcher=args.min_pitches_pitcher,
        min_pitches_batter=args.min_pitches_batter,
        min_batted_ball_batter=args.min_batted_ball_batter,
        hard_hit_speed_mph=args.hard_hit_speed_mph,
        min_pitches_per_pitch_type=args.min_pitches_per_pitch_type,
        raw_running_prefix=args.raw_running_prefix,
        sprint_speed_min_opp=args.sprint_speed_min_opp,
        raw_defence_prefix=args.raw_defence_prefix,
        raw_bio_prefix=args.raw_bio_prefix,
    )

    if result["status"] == "error":
        logger.error(result["message"])
        raise SystemExit(1)
    if result["status"] == "no_data":
        logger.warning(result["message"])
    else:
        logger.info(result["message"])


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    y = yesterday_utc_date_str()
    cfg = PipelineSettings.from_environ()
    start_date = event_or_env_str(event, "start_date", "START_DATE", y)
    end_date = event_or_env_str(event, "end_date", "END_DATE", y)
    bucket = event_or_env_str(event, "s3_bucket", "S3_BUCKET", cfg.s3_bucket)
    bronze_prefix = event_or_env_str(event, "bronze_prefix", "RAW_PREFIX", cfg.raw_statcast_prefix)
    silver_prefix = event_or_env_str(event, "silver_prefix", "FEATURE_PREFIX", cfg.feature_prefix)
    raw_running = event_or_env_str(
        event, "raw_running_prefix", "RAW_RUNNING_PREFIX", cfg.raw_running_prefix
    )
    raw_defence = event_or_env_str(
        event, "raw_defence_prefix", "RAW_DEFENCE_PREFIX", cfg.raw_defence_prefix
    )
    raw_bio = event_or_env_str(event, "raw_bio_prefix", "RAW_BIO_PREFIX", cfg.raw_bio_prefix)
    yt_raw = event_or_env_str(event, "year_to_date", "YEAR_TO_DATE", "true")
    year_to_date = str(yt_raw).strip().lower() not in ("0", "false", "no")

    result = build_bronze_to_silver_features(
        bucket=bucket,
        bronze_statcast_prefix=bronze_prefix,
        silver_prefix=silver_prefix,
        start_date_str=start_date,
        end_date_str=end_date,
        year_to_date=year_to_date,
        raw_running_prefix=raw_running,
        raw_defence_prefix=raw_defence,
        raw_bio_prefix=raw_bio,
    )

    status_code = 200 if result.get("status") in ("ok", "no_data") else 400
    return {
        "statusCode": status_code,
        "body": result.get("message", ""),
        "details": result,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
