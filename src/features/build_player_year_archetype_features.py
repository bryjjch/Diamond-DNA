#!/usr/bin/env python3
"""
Build player-year pitch-derived archetype-friendly features.

Reads from processed by-player Statcast parquet for each role:
  {processed_prefix}/{role}/{role}_id=<id>/year=<year>/statcast_pitches.parquet

Writes one parquet per role and year:
  {feature_prefix}/{role}/year={year}/player_year_features.parquet

A single run processes batters first, then pitchers.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from ..s3_parquet import get_s3_client, read_parquet_from_s3, write_parquet_to_s3

from .archetype_feature_defs import (
    DEFAULT_BARREL_DEF,
    compute_barrel_flag,
    compute_in_zone,
    compute_swing_flag,
    iqr_mean_summary,
    pitch_type_shares_and_entropy,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _list_player_year_keys(
    *,
    bucket: str,
    processed_prefix: str,
    role: str,
    start_year: int,
    end_year: int,
) -> List[Tuple[int, int, str]]:
    """
    List all statcast player-year parquet objects for a role/year range.

    Returns list of (player_id, year, key).
    """
    # Keys are created by processing script as:
    # {processed_prefix}/{role}/{role}_id=<id>/year=<year>/statcast_pitches.parquet
    list_prefix = f"{processed_prefix}/{role}/{role}_id="

    out: List[Tuple[int, int, str]] = []
    paginator = get_s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key")
            if not key:
                continue
            player_m = re.search(rf"{re.escape(role)}_id=(\d+)", key)
            year_m = re.search(r"year=(\d+)", key)
            if not player_m or not year_m:
                continue
            player_id = int(player_m.group(1))
            year = int(year_m.group(1))
            if year < start_year or year > end_year:
                continue
            out.append((player_id, year, key))
    out.sort(key=lambda x: (x[1], x[0]))
    return out


def _nan_mean(series: pd.Series) -> float:
    x = pd.to_numeric(series, errors="coerce")
    return float(x.mean(skipna=True))


def _nan_std(series: pd.Series) -> float:
    x = pd.to_numeric(series, errors="coerce")
    return float(x.std(skipna=True, ddof=0))


def _player_year_features_from_df(
    *,
    df: pd.DataFrame,
    role: str,
    player_id: int,
    year: int,
    min_pitches_pitcher: int,
    min_pitches_batter: int,
    min_batted_ball_batter: int,
    hard_hit_speed_mph: float,
) -> Optional[Dict[str, object]]:
    # Derived flags used across both roles.
    in_zone = compute_in_zone(df)
    swing_flag = compute_swing_flag(df)

    base: Dict[str, object] = {
        "role": role,
        "player_id": int(player_id),
        "year": int(year)
    }

    total_pitches = len(df)
    base["n_pitches_total"] = int(total_pitches)

    # Shared swing metrics (interpretation differs by role, but names remain consistent).
    swing_rate = float(swing_flag.mean()) if total_pitches else float("nan")
    zone_swing_rate = float((swing_flag & in_zone).mean()) if total_pitches else float("nan")
    chase_rate = float((swing_flag & ~in_zone).mean()) if total_pitches else float("nan")

    # Compute whiffs using description heuristics.
    desc = df["description"].fillna("").astype(str).str.lower()
    whiff_flag = swing_flag & desc.str.contains("swinging_strike", na=False)
    contact_flag = swing_flag & ~whiff_flag

    contact_rate = float(contact_flag.mean()) if total_pitches else float("nan")
    whiff_rate = float(whiff_flag.mean()) if total_pitches else float("nan")

    if role == "pitcher":
        # Minimum sample thresholds
        if total_pitches < min_pitches_pitcher:
            return None

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
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Pitcher parquet missing required columns: {missing}")

        delta_col = "delta_run_exp"
        if delta_col not in df.columns:
            logger.warning("Missing `%s`; setting run value stats to NA.", delta_col)
            delta_mean = float("nan")
        else:
            delta_mean = _nan_mean(df[delta_col])

        out: Dict[str, object] = dict(base)
        out.update(
            {
                "batter_swing_rate": swing_rate,
                "batter_zone_swing_rate": zone_swing_rate,
                "batter_chase_rate": chase_rate,
                "batter_contact_rate": contact_rate,
                "batter_whiff_rate": whiff_rate,
                "in_zone_rate": float(in_zone.mean()),
                "release_speed_mean": float(pd.to_numeric(df["release_speed"], errors="coerce").mean(skipna=True)),
                "release_speed_iqr": iqr_mean_summary(df["release_speed"])[1],
                "release_spin_rate_mean": float(pd.to_numeric(df["release_spin_rate"], errors="coerce").mean(skipna=True)),
                "release_spin_rate_iqr": iqr_mean_summary(df["release_spin_rate"])[1],
                "release_extension_mean": float(pd.to_numeric(df["release_extension"], errors="coerce").mean(skipna=True)),
                "release_extension_iqr": iqr_mean_summary(df["release_extension"])[1],
                "pfx_x_mean": float(pd.to_numeric(df["pfx_x"], errors="coerce").mean(skipna=True)),
                "pfx_x_iqr": iqr_mean_summary(df["pfx_x"])[1],
                "pfx_z_mean": float(pd.to_numeric(df["pfx_z"], errors="coerce").mean(skipna=True)),
                "pfx_z_iqr": iqr_mean_summary(df["pfx_z"])[1],
                "plate_x_mean": _nan_mean(df["plate_x"]),
                "plate_x_sd": _nan_std(df["plate_x"]),
                "plate_z_mean": _nan_mean(df["plate_z"]),
                "plate_z_sd": _nan_std(df["plate_z"]),
                "delta_run_exp_mean": delta_mean,
            }
        )

        shares = pitch_type_shares_and_entropy(df, pitch_type_col="pitch_type")
        out.update(shares)
        return out

    if role == "batter":
        if total_pitches < min_pitches_batter:
            return None

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
        if missing:
            raise ValueError(f"Batter parquet missing required columns: {missing}")

        barrel_flag = compute_barrel_flag(df, barrel_def=DEFAULT_BARREL_DEF)

        launch_speed = pd.to_numeric(df["launch_speed"], errors="coerce")
        launch_angle = pd.to_numeric(df["launch_angle"], errors="coerce")
        has_launch = launch_speed.notna() & launch_angle.notna()

        denom = int(has_launch.sum())
        if denom < min_batted_ball_batter:
            return None

        barrel_rate = float(barrel_flag[has_launch].mean())

        hard_hit_flag = (launch_speed >= hard_hit_speed_mph) & has_launch
        hard_hit_rate = float(hard_hit_flag[has_launch].mean()) if denom else float("nan")

        out = dict(base)
        out.update(
            {
                "swing_rate": swing_rate,
                "zone_swing_rate": zone_swing_rate,
                "chase_rate": chase_rate,
                "contact_rate": contact_rate,
                "whiff_rate": whiff_rate,
                "barrel_rate": barrel_rate,
                "hard_hit_rate": hard_hit_rate,
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
        return out

    raise ValueError(f"Unknown role: {role}")


def _validate_feature_row(row: Dict[str, object], *, role: str) -> None:
    """Checks to catch broken derived flags."""
    required_rates = ["swing_rate", "zone_swing_rate", "chase_rate", "contact_rate", "whiff_rate"]
    pitcher_rates = ["batter_swing_rate", "batter_zone_swing_rate", "batter_chase_rate", "batter_contact_rate", "batter_whiff_rate"]

    if role == "batter":
        rates_to_check = required_rates + ["barrel_rate", "hard_hit_rate"]
    else:
        rates_to_check = pitcher_rates + ["in_zone_rate"]

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


def build_features(
    *,
    bucket: str,
    processed_prefix: str,
    role: str,
    feature_prefix: str,
    start_year: int,
    end_year: int,
    min_pitches_pitcher: int,
    min_pitches_batter: int,
    min_batted_ball_batter: int,
    hard_hit_speed_mph: float,
) -> None:
    keys = _list_player_year_keys(
        bucket=bucket,
        processed_prefix=processed_prefix,
        role=role,
        start_year=start_year,
        end_year=end_year,
    )
    if not keys:
        logger.warning("No parquet objects found for role=%s in years [%d..%d]", role, start_year, end_year)
        return

    feature_rows: List[Dict[str, object]] = []
    for i, (player_id, year, key) in enumerate(keys):
        if (i % 50) == 0:
            logger.info("Processing %d/%d: player_id=%d year=%d", i + 1, len(keys), player_id, year)

        df = read_parquet_from_s3(bucket, key, log_read=False, missing_key_log="warning")
        if df is None or df.empty:
            logger.warning("Empty parquet for player_id=%d year=%d (%s)", player_id, year, key)
            continue

        row = _player_year_features_from_df(
            df=df,
            role=role,
            player_id=player_id,
            year=year,
            min_pitches_pitcher=min_pitches_pitcher,
            min_pitches_batter=min_pitches_batter,
            min_batted_ball_batter=min_batted_ball_batter,
            hard_hit_speed_mph=hard_hit_speed_mph,
        )
        if row is None:
            continue

        _validate_feature_row(row, role=role)
        feature_rows.append(row)

    if not feature_rows:
        logger.warning("No feature rows computed for role=%s.", role)
        return

    features_df = pd.DataFrame(feature_rows)
    # Write per year.
    for year in range(start_year, end_year + 1):
        df_year = features_df[features_df["year"] == year]
        if df_year.empty:
            continue
        out_key = f"{feature_prefix}/{role}/year={year}/player_year_features.parquet"
        logger.info("Writing %d feature rows to s3://%s/%s", len(df_year), bucket, out_key)
        write_parquet_to_s3(df_year, bucket, out_key, log_write=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build pitch-derived player-year archetype features from processed statcast parquet.")
    parser.add_argument("--bucket", type=str, default=os.environ.get("S3_BUCKET", "diamond-dna"))
    parser.add_argument("--processed-prefix", type=str, default=os.environ.get("PROCESSED_PREFIX", "processed/statcast"))
    parser.add_argument("--feature-prefix", type=str, default=os.environ.get("FEATURE_PREFIX", "features/statcast"))
    parser.add_argument("--start-year", type=int, default=2022)
    parser.add_argument("--end-year", type=int, default=2025)

    parser.add_argument("--min-pitches-pitcher", type=int, default=500)
    parser.add_argument("--min-pitches-batter", type=int, default=500)
    parser.add_argument("--min-batted-ball-batter", type=int, default=200)
    parser.add_argument("--hard-hit-speed-mph", type=float, default=95.0)

    args = parser.parse_args()

    for role in ("batter", "pitcher"):
        logger.info("Building features for role=%s", role)
        build_features(
            bucket=args.bucket,
            processed_prefix=args.processed_prefix,
            role=role,
            feature_prefix=args.feature_prefix,
            start_year=args.start_year,
            end_year=args.end_year,
            min_pitches_pitcher=args.min_pitches_pitcher,
            min_pitches_batter=args.min_pitches_batter,
            min_batted_ball_batter=args.min_batted_ball_batter,
            hard_hit_speed_mph=args.hard_hit_speed_mph,
        )


if __name__ == "__main__":
    main()

