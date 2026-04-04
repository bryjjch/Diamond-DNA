#!/usr/bin/env python3
"""
Build player-year pitch-derived archetype-friendly features.

Core row logic lives in ``player_year_features_from_df``. The production pipeline
loads bronze dailies and writes silver tables via ``bronze_to_silver_features``.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

from .silver_archetype_feature_defs import (
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


def _nan_mean(series: pd.Series) -> float:
    x = pd.to_numeric(series, errors="coerce")
    return float(x.mean(skipna=True))


def _nan_std(series: pd.Series) -> float:
    x = pd.to_numeric(series, errors="coerce")
    return float(x.std(skipna=True, ddof=0))


def player_year_features_from_df(
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
            "def_actual_fielding_success_rate_mean",
            "def_adj_estimated_fielding_success_rate_mean",
            "def_outfield_catch_completion_rate",
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


def main() -> None:
    from ..pipeline.cli import run_build_player_year_archetype_features_main

    run_build_player_year_archetype_features_main()


if __name__ == "__main__":
    main()

