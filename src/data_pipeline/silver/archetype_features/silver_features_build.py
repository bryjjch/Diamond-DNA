#!/usr/bin/env python3
"""
Bronze → silver: build player-year archetype feature tables from bronze data.

Reads bronze data from the S3 bucket and writes silver data to the S3 bucket.
Core row logic lives in ``player_year_features_from_df``.

For scheduled runs that only pass ``end_date`` (e.g. yesterday), use ``year_to_date=True`` so
bronze is loaded from Jan 1 through ``end_date`` for each affected calendar year—matching
season-to-date aggregates.
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from ....common.runtime_helpers import event_or_env_str, yesterday_utc_date_str
from ....common.s3_helpers import read_parquet_from_s3, write_parquet_to_s3
from ....common.settings import PipelineSettings
from .archetype_feature_defs import (
    DEFAULT_BARREL_DEF,
    batted_ball_type_rates,
    compute_barrel_flag,
    compute_in_zone,
    compute_swing_flag,
    fastball_offspeed_velo_means_and_diff,
    first_pitch_strike_rate,
    iqr_mean_summary,
    pitch_type_physical_means,
    pitch_type_shares_and_entropy,
    platoon_estimated_woba_means,
    pull_oppo_center_rates,
    sweet_spot_rate,
    zone_edge_and_meatball_rates,
)
from .bio_player_year_helper import load_player_bios_by_year, merge_bio_into_row
from .defence_player_year_helper import (
    load_defence_metrics_by_player_year,
    load_primary_positions_by_player_year,
    merge_defence_into_row,
    merge_primary_position_into_row,
)
from .player_names_helper import (
    build_mlbam_statcast_style_name_map,
    resolve_mlbam_display_name,
)
from .sprint_helper import build_sprint_speed_lookups_by_year

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
        key = (
            f"{bronze_prefix.strip('/')}/year={d.year}"
            f"/date={d.strftime('%Y-%m-%d')}/statcast_pitches.parquet"
        )
        df = read_parquet_from_s3(bucket, key)
        if df is None or df.empty:
            continue
        frames.append(normalize_statcast_bronze_df(df))

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _nan_mean(series: pd.Series) -> float:
    """Calculate the mean of a series, ignoring NaN values."""
    x = pd.to_numeric(series, errors="coerce")
    return float(x.mean(skipna=True))


def _nan_std(series: pd.Series) -> float:
    """Calculate the standard deviation of a series, ignoring NaN values."""
    x = pd.to_numeric(series, errors="coerce")
    return float(x.std(skipna=True, ddof=0))


def player_year_features_from_df(
    *,
    df: pd.DataFrame,
    role: str,
    player_id: int,
    year: int,
    player_name: str = "",
    min_pitches_pitcher: int,
    min_pitches_batter: int,
    min_batted_ball_batter: int,
    hard_hit_speed_mph: float,
    min_pitches_per_pitch_type: int,
    sprint_speed_lookup: Optional[Dict[int, float]] = None,
) -> Optional[Dict[str, object]]:
    """Calculate player-year pitch-derived archetype-friendly features for each player-year."""
    # Compute the in-zone flag.
    in_zone = compute_in_zone(df)
    # Compute the swing flag.
    swing_flag = compute_swing_flag(df)

    base: Dict[str, object] = {
        "role": role,
        "player_id": int(player_id),
        "year": int(year),
        "player_name": str(player_name) if player_name is not None else "",
    }

    # Get the total number of pitches.
    total_pitches = len(df)
    # Set the total number of pitches for the player-year.
    base["n_pitches_total"] = int(total_pitches)

    # Shared swing metrics (interpretation differs by role, but names remain consistent).
    swing_rate = float(swing_flag.mean()) if total_pitches else float("nan")
    zone_swing_rate = float((swing_flag & in_zone).mean()) if total_pitches else float("nan")
    chase_rate = float((swing_flag & ~in_zone).mean()) if total_pitches else float("nan")

    # Compute whiffs using description heuristics.
    desc = df["description"].fillna("").astype(str).str.lower()
    whiff_flag = swing_flag & desc.str.contains("swinging_strike", na=False)

    # Compute the whiff rate.
    whiff_rate = float(whiff_flag.mean()) if total_pitches else float("nan")

    if role == "pitcher":
        # Minimum sample thresholds
        if total_pitches < min_pitches_pitcher:
            return None

        # Required columns for pitcher features.
        required = [
            "pitch_type",
            "release_speed",
            "release_spin_rate",
            "release_extension",
            "pfx_x",
            "pfx_z",
            "zone",
            "description",
            "plate_x",
            "plate_z",
        ]
        # Get the missing columns.
        missing = [c for c in required if c not in df.columns]
        # If there are missing columns, raise an error.
        if missing:
            raise ValueError(f"Pitcher parquet missing required columns: {missing}")

        # Get the delta run expected column.
        delta_col = "delta_run_exp"
        # If the delta run expected column is not in the dataframe, set the delta mean to NaN.
        if delta_col not in df.columns:
            logger.warning("Missing `%s`; setting run value stats to NA.", delta_col)
            delta_mean = float("nan")
        else:
            delta_mean = _nan_mean(df[delta_col])

        # Get the release speed column.
        rs = pd.to_numeric(df["release_speed"], errors="coerce")
        # Get the maximum release speed for the player-year.
        release_speed_max = float(rs.max()) if rs.notna().any() else float("nan")

        # Get the fastball velocity mean, offspeed velocity mean, and velocity differential.
        fb_v, off_v, velo_diff = fastball_offspeed_velo_means_and_diff(df)
        # Get the edge percentage and meatball percentage.
        edge_pct, meat_pct = zone_edge_and_meatball_rates(df)
        fps = first_pitch_strike_rate(df)
        # Get the platoon estimated woba means for the player-year.
        xw_l, xw_r, xw_plat = platoon_estimated_woba_means(df, bip_only=True)
        # Get the batted ball type rates for the player-year.
        bb_p = batted_ball_type_rates(df)

        # Set the output dictionary.
        out = dict(base)
        # Update the output dictionary with the pitcher features.
        out.update(
            {
                "batter_swing_rate": swing_rate,
                "batter_zone_swing_rate": zone_swing_rate,
                "batter_chase_rate": chase_rate,
                "batter_whiff_rate": whiff_rate,
                "in_zone_rate": float(in_zone.mean()),
                "release_speed_max": release_speed_max,
                "fastball_velo_mean": fb_v,
                "offspeed_velo_mean": off_v,
                "velo_differential": velo_diff,
                "release_speed_iqr": iqr_mean_summary(df["release_speed"])[1],
                "release_spin_rate_iqr": iqr_mean_summary(df["release_spin_rate"])[1],
                "pfx_x_mean": float(pd.to_numeric(df["pfx_x"], errors="coerce").mean(skipna=True)),
                "pfx_x_iqr": iqr_mean_summary(df["pfx_x"])[1],
                "release_extension_mean": float(pd.to_numeric(df["release_extension"], errors="coerce").mean(skipna=True)),
                "release_extension_iqr": iqr_mean_summary(df["release_extension"])[1],
                "pfx_z_mean": float(pd.to_numeric(df["pfx_z"], errors="coerce").mean(skipna=True)),
                "pfx_z_iqr": iqr_mean_summary(df["pfx_z"])[1],
                "plate_x_mean": _nan_mean(df["plate_x"]),
                "plate_x_sd": _nan_std(df["plate_x"]),
                "plate_z_mean": _nan_mean(df["plate_z"]),
                "plate_z_sd": _nan_std(df["plate_z"]),
                "edge_percent": edge_pct,
                "meatball_percent": meat_pct,
                "first_pitch_strike_rate": fps,
                "xwoba_allowed_lhb_mean": xw_l,
                "xwoba_allowed_rhb_mean": xw_r,
                "platoon_xwoba_allowed_diff": xw_plat,
                "gb_percent_allowed": bb_p["gb_percent"],
                "ld_percent_allowed": bb_p["ld_percent"],
                "fb_percent_allowed": bb_p["fb_percent"],
                "iffb_percent_allowed": bb_p["iffb_percent"],
                "delta_run_exp_mean": delta_mean,
            }
        )

        out.update(pitch_type_physical_means(df, min_pitches_per_type=min_pitches_per_pitch_type))
        # Get the pitch type shares and entropy.
        shares = pitch_type_shares_and_entropy(df, pitch_type_col="pitch_type")
        # Update the output dictionary with the pitch type shares and entropy.
        out.update(shares)
        return out

    if role == "batter":
        if total_pitches < min_pitches_batter:
            return None

        # Required columns for batter features.
        required = [
            "zone",
            "description",
            "launch_speed",
            "launch_angle",
            "iso_value",
            "estimated_slg_using_speedangle",
            "woba_value",
            "estimated_woba_using_speedangle",
        ]
        missing = [c for c in required if c not in df.columns]
        # If there are missing columns, raise an error.
        if missing:
            raise ValueError(f"Batter parquet missing required columns: {missing}")

        # Compute the barrel flag.
        barrel_flag = compute_barrel_flag(df, barrel_def=DEFAULT_BARREL_DEF)

        # Get the launch speed and launch angle columns.
        launch_speed = pd.to_numeric(df["launch_speed"], errors="coerce")
        launch_angle = pd.to_numeric(df["launch_angle"], errors="coerce")
        has_launch = launch_speed.notna() & launch_angle.notna()

        # Get the number of pitches with valid launch speed and launch angle.
        denom = int(has_launch.sum())
        # If the number of pitches with valid launch speed and launch angle is less than the minimum number of pitches, return None.
        if denom < min_batted_ball_batter:
            return None

        # Get the barrel rate.
        barrel_rate = float(barrel_flag[has_launch].mean())

        # Compute the hard hit flag.
        hard_hit_flag = (launch_speed >= hard_hit_speed_mph) & has_launch
        # Get the hard hit rate.
        hard_hit_rate = float(hard_hit_flag[has_launch].mean()) if denom else float("nan")

        # Get the pull percentage, opposite field percentage, and center percentage.
        pull_p, oppo_p, _center_p = pull_oppo_center_rates(df)
        # Get the batted ball type rates for the player-year.
        bb_b = batted_ball_type_rates(df)
        # Get the sweet spot rate.
        sweet_spot = sweet_spot_rate(df["launch_angle"])
        # Walk rate (BB%) = walks / plate appearances. The `events` column is only
        # populated on the final pitch of each PA, so non-null events count PAs and
        # `walk` / `intent_walk` events count walks.
        if "events" in df.columns:
            events = df["events"].astype("object")
            pa_mask = events.notna() & (events.astype(str).str.strip() != "")
            pa_count = int(pa_mask.sum())
            walk_count = int(events.isin(["walk", "intent_walk"]).sum())
            walk_rate = float(walk_count / pa_count) if pa_count > 0 else float("nan")
        else:
            walk_rate = float("nan")

        # Set the output dictionary.
        out = dict(base)
        # Update the output dictionary with the batter features.
        out.update(
            {
                "swing_rate": swing_rate,
                "zone_swing_rate": zone_swing_rate,
                "chase_rate": chase_rate,
                "whiff_rate": whiff_rate,
                "walk_rate": walk_rate,
                "barrel_rate": barrel_rate,
                "hard_hit_rate": hard_hit_rate,
                "pull_percent": pull_p,
                "opposite_field_percent": oppo_p,
                "gb_percent": bb_b["gb_percent"],
                "ld_percent": bb_b["ld_percent"],
                "fb_percent": bb_b["fb_percent"],
                "iffb_percent": bb_b["iffb_percent"],
                "sweet_spot_percent": sweet_spot,
                "launch_speed_mean": _nan_mean(df["launch_speed"]),
                "launch_speed_iqr": iqr_mean_summary(df["launch_speed"])[1],
                "launch_angle_mean": _nan_mean(df["launch_angle"]),
                "launch_angle_iqr": iqr_mean_summary(df["launch_angle"])[1],
                "iso_value_mean": _nan_mean(df["iso_value"]),
                "estimated_slg_using_speedangle_mean": _nan_mean(df["estimated_slg_using_speedangle"]),
                "woba_value_mean": _nan_mean(df["woba_value"]),
                "estimated_woba_using_speedangle_mean": _nan_mean(df["estimated_woba_using_speedangle"]),
            }
        )
        # Get the sprint speed mean.
        if sprint_speed_lookup is not None:
            out["sprint_speed_mean"] = float(sprint_speed_lookup.get(int(player_id), float("nan")))
        elif "sprint_speed" in df.columns:
            ss = pd.to_numeric(df["sprint_speed"], errors="coerce")
            out["sprint_speed_mean"] = float(ss.mean(skipna=True)) if ss.notna().any() else float("nan")
        else:
            out["sprint_speed_mean"] = float("nan")
        return out

    raise ValueError(f"Unknown role: {role}")


def _validate_feature_row(row: Dict[str, object], *, role: str) -> None:
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

    for k in rates_to_check:
        if k not in row:
            continue
        v = row[k]
        if v is None:
            continue
        try:
            fv = float(v)  # type: ignore[arg-type]
        except Exception:
            continue
        if not np.isnan(fv) and (fv < 0.0 or fv > 1.0):
            raise ValueError(f"Sanity check failed: {k}={fv} out of [0,1] for role={role}, player={row.get('player_id')}, year={row.get('year')}")


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
            "Could not load Chadwick register (%s); pitcher names may be blank.",
            exc,
        )

    # Player bios apply to both roles; load once per year.
    bios_by_year: Dict[int, Dict[int, Dict[str, object]]] = {
        y: load_player_bios_by_year(bucket, raw_bio_prefix, y)
        for y in range(year_lo, year_hi + 1)
    }

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
        positions_by_year: Dict[int, Dict[int, str]] = {}
        if role == "batter":
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

                # Merge the defence metrics and primary position into the row.
                if role == "batter":
                    merge_defence_into_row(row, defence_by_year.get(y, {}))
                    merge_primary_position_into_row(row, positions_by_year.get(y, {}))

                # Merge player bio (age, height, weight, birth info) into the row.
                merge_bio_into_row(row, bios_by_year.get(y, {}))

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
        raise SystemExit(1)


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
