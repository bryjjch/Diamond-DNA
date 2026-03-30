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
    min_pitches_per_pitch_type: int,
    sprint_speed_lookup: Optional[Dict[int, float]] = None,
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

        rs = pd.to_numeric(df["release_speed"], errors="coerce")
        release_speed_max = float(rs.max()) if rs.notna().any() else float("nan")

        fb_v, off_v, velo_diff = fastball_offspeed_velo_means_and_diff(df)
        edge_pct, meat_pct = zone_edge_and_meatball_rates(df)
        fps = first_pitch_strike_rate(df)
        xw_l, xw_r, xw_plat = platoon_estimated_woba_means(df, bip_only=True)
        bb_p = batted_ball_type_rates(df)

        out = dict(base)
        out.update(
            {
                "batter_swing_rate": swing_rate,
                "batter_zone_swing_rate": zone_swing_rate,
                "batter_chase_rate": chase_rate,
                "batter_contact_rate": contact_rate,
                "batter_whiff_rate": whiff_rate,
                "in_zone_rate": float(in_zone.mean()),
                "release_speed_max": release_speed_max,
                "fastball_velo_mean": fb_v,
                "offspeed_velo_mean": off_v,
                "velo_differential": velo_diff,
                "release_speed_iqr": iqr_mean_summary(df["release_speed"])[1],
                "release_spin_rate_iqr": iqr_mean_summary(df["release_spin_rate"])[1],
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

        pull_p, oppo_p, _center_p = pull_oppo_center_rates(df)
        bb_b = batted_ball_type_rates(df)
        sweet_spot = sweet_spot_rate(df["launch_angle"])

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
        # Sprint speed is not present in pitch-level Statcast pulls. Prefer a lookup from the
        # Statcast running leaderboard layer (player-year), if provided.
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
    required_rates = ["swing_rate", "zone_swing_rate", "chase_rate", "contact_rate", "whiff_rate"]
    pitcher_rates = ["batter_swing_rate", "batter_zone_swing_rate", "batter_chase_rate", "batter_contact_rate", "batter_whiff_rate"]

    if role == "batter":
        rates_to_check = required_rates + [
            "barrel_rate",
            "hard_hit_rate",
            "pull_percent",
            "opposite_field_percent",
            "gb_percent",
            "ld_percent",
            "fb_percent",
            "iffb_percent",
            "sweet_spot_percent",
        ]
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
    min_pitches_per_pitch_type: int,
    raw_running_prefix: str,
    sprint_speed_min_opp: int,
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

    sprint_lookup_by_year: Dict[int, Dict[int, float]] = {}
    if role == "batter":
        for y in range(start_year, end_year + 1):
            key = f"{raw_running_prefix}/year={y}/statcast_sprint_speed.parquet"
            running_df = read_parquet_from_s3(bucket, key, log_read=False, missing_key_log="none")
            if running_df is None or running_df.empty:
                continue

            cols_lower = {c.lower(): c for c in running_df.columns}
            id_col = (
                cols_lower.get("player_id")
                or cols_lower.get("mlbam_id")
                or cols_lower.get("mlbamid")
                or cols_lower.get("id")
            )
            ss_col = cols_lower.get("sprint_speed") or cols_lower.get("sprint_speed_ft_per_sec")
            opp_col = cols_lower.get("opportunities") or cols_lower.get("opp") or cols_lower.get("attempts")

            if not id_col or not ss_col:
                logger.warning("Sprint speed parquet missing expected id/sprint_speed columns: s3://%s/%s", bucket, key)
                continue

            tmp = running_df.copy()
            tmp[id_col] = pd.to_numeric(tmp[id_col], errors="coerce")
            tmp[ss_col] = pd.to_numeric(tmp[ss_col], errors="coerce")
            tmp = tmp[tmp[id_col].notna() & tmp[ss_col].notna()]
            if opp_col and opp_col in tmp.columns:
                tmp[opp_col] = pd.to_numeric(tmp[opp_col], errors="coerce")
                tmp = tmp[(tmp[opp_col].isna()) | (tmp[opp_col] >= sprint_speed_min_opp)]

            sprint_lookup_by_year[y] = {int(pid): float(ss) for pid, ss in zip(tmp[id_col], tmp[ss_col])}

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
            min_pitches_per_pitch_type=min_pitches_per_pitch_type,
            sprint_speed_lookup=sprint_lookup_by_year.get(year) if role == "batter" else None,
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
    parser.add_argument(
        "--min-pitches-per-pitch-type",
        type=int,
        default=15,
        help="Minimum pitches of a type to emit pt_<TYPE>_release_speed_mean (and spin, pfx_x) for pitchers.",
    )
    parser.add_argument(
        "--raw-running-prefix",
        type=str,
        default=os.environ.get("RAW_RUNNING_PREFIX", "raw-data/statcast_running"),
        help="S3 prefix for Statcast running leaderboard parquet (sprint speed).",
    )
    parser.add_argument(
        "--sprint-speed-min-opp",
        type=int,
        default=10,
        help="Minimum sprint opportunities to include from the sprint speed leaderboard parquet.",
    )

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
            min_pitches_per_pitch_type=args.min_pitches_per_pitch_type,
            raw_running_prefix=args.raw_running_prefix,
            sprint_speed_min_opp=args.sprint_speed_min_opp,
        )


if __name__ == "__main__":
    main()

