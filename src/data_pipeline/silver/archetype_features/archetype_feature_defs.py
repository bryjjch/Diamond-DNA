"""
Shared definitions for pitch-derived archetype-friendly features.

Per-pitch flag/value functions used by ``player_year_feature_table``; each operates on
the whole pitch frame and returns a row-aligned Series.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Set, Tuple

import numpy as np
import pandas as pd

# Statcast field coordinates (feet): back tip of home plate to hit location.
_HC_HOME_X = 125.42
_HC_HOME_Y = 198.27

# Fastball pitch types.
FASTBALL_PITCH_TYPES: Set[str] = {"FF", "FA", "FT", "SI", "FC"}
# Junk / non-pitch rows to drop from velocity summaries.
EXCLUDED_PITCH_TYPES: Set[str] = {"UN", "PO", "XX", "IN"}

# Spray angle beyond which a batted ball counts as pulled (or opposite-field).
ANGLE_PULL_THRESHOLD_DEG = 20.0


@dataclass(frozen=True)
class BarrelDef:
    speed_threshold_mph: float = 98.0
    launch_angle_min_deg: float = 26.0
    launch_angle_max_deg: float = 30.0


DEFAULT_BARREL_DEF = BarrelDef()


def _safe_lower_series(values: pd.Series) -> pd.Series:
    return values.fillna("").astype(str).str.lower()


def compute_in_zone(df: pd.DataFrame) -> pd.Series:
    """
    Compute whether pitch location is "in zone".

    Primary heuristic: Statcast's `zone` grid where `zone` is in 1..9.
    Fallback: compare `plate_z` against `sz_bot`/`sz_top` when `zone` is missing.
    """
    if "zone" not in df.columns:
        # If `zone` isn't available, we can still try geometry-based.
        zone_numeric = pd.Series([np.nan] * len(df), index=df.index)
    else:
        zone_numeric = pd.to_numeric(df["zone"], errors="coerce")

    in_zone_from_zone = zone_numeric.between(1, 9, inclusive="both")

    missing_zone = zone_numeric.isna()
    in_zone_from_geom = pd.Series([False] * len(df), index=df.index)
    if all(c in df.columns for c in ("plate_z", "sz_top", "sz_bot")):
        plate_z = pd.to_numeric(df["plate_z"], errors="coerce")
        sz_top = pd.to_numeric(df["sz_top"], errors="coerce")
        sz_bot = pd.to_numeric(df["sz_bot"], errors="coerce")
        in_zone_from_geom = (plate_z >= sz_bot) & (plate_z <= sz_top)
        in_zone_from_geom = in_zone_from_geom.fillna(False)

    # Prefer `zone` when available; otherwise use geometry fallback.
    in_zone = in_zone_from_zone.fillna(False) | (missing_zone & in_zone_from_geom)
    return in_zone.astype(bool)


def compute_swing_flag(df: pd.DataFrame) -> pd.Series:
    """
    Compute a hitter swing flag from pitch-level `description`.

    Notes:
    - Statcast `description` includes values like:
      `foul`, `hit_into_play`, `called_strike`, `ball`, etc.
    - We can classify "swing" primarily by:
      - description contains `swinging_strike`
      - description contains `foul`
      - description contains `hit_into_play`
    """
    if "description" not in df.columns:
        raise ValueError("Expected column `description` to derive swing flag.")

    desc = _safe_lower_series(df["description"])

    is_swinging_strike = desc.str.contains("swinging_strike", na=False)
    is_foul = desc.str.contains("foul", na=False)
    is_hit_into_play = desc.str.contains("hit_into_play", na=False)

    # Can sometimes also be `missed_bunt` / `bunt_foul` etc.
    is_missed_bunt = desc.str.contains("missed_bunt", na=False)
    is_bunt_foul = desc.str.contains("bunt_foul", na=False)

    swing = (
        is_swinging_strike
        | is_foul
        | is_hit_into_play
        | is_missed_bunt
        | is_bunt_foul
    )
    return swing.fillna(False).astype(bool)


def compute_barrel_flag(
    df: pd.DataFrame,
    *,
    barrel_def: BarrelDef = DEFAULT_BARREL_DEF,
) -> pd.Series:
    """Compute barrel flag from `launch_speed` and `launch_angle`."""
    if "launch_speed" not in df.columns or "launch_angle" not in df.columns:
        raise ValueError("Expected columns `launch_speed` and `launch_angle` to compute barrel.")

    launch_speed = pd.to_numeric(df["launch_speed"], errors="coerce")
    launch_angle = pd.to_numeric(df["launch_angle"], errors="coerce")

    has_required = launch_speed.notna() & launch_angle.notna()
    barrel = (
        has_required
        & (launch_speed >= barrel_def.speed_threshold_mph)
        & (launch_angle >= barrel_def.launch_angle_min_deg)
        & (launch_angle <= barrel_def.launch_angle_max_deg)
    )
    return barrel.fillna(False).astype(bool)


def spray_angle_degrees(hc_x: pd.Series, hc_y: pd.Series) -> pd.Series:
    """Horizontal spray angle in degrees (Statcast hc_x / hc_y convention)."""
    # astype guards against nullable inputs (Float64/Arrow): pd.NA -> np.nan so the
    # numpy ops and comparisons downstream stay well-defined.
    x = pd.to_numeric(hc_x, errors="coerce").astype("float64")
    y = pd.to_numeric(hc_y, errors="coerce").astype("float64")
    rad = np.arctan2(x - _HC_HOME_X, _HC_HOME_Y - y)
    return pd.Series(np.degrees(rad), index=hc_x.index)


def _nan_series(index: pd.Index) -> pd.Series:
    return pd.Series(np.nan, index=index)


def first_pitch_strike_flag(raw: pd.DataFrame, desc: pd.Series) -> pd.Series:
    """
    0/1 on first pitches of a PA (strike or in play), NaN elsewhere.

    ``desc`` is the pre-lowercased ``description`` column, shared across feature helpers.
    """
    if not all(c in raw.columns for c in ("pitch_number", "game_pk", "at_bat_number")):
        return _nan_series(raw.index)
    is_first = pd.to_numeric(raw["pitch_number"], errors="coerce") == 1
    if "type" in raw.columns:
        # B = ball; S = strike; X = in play (counts toward FPS).
        t = raw["type"].fillna("").astype(str).str.strip().str.upper()
        flag = t.isin(["S", "X"])
    else:
        flag = desc.str.contains(
            "called_strike|swinging_strike|foul|hit_into_play|missed_bunt|bunt_foul",
            na=False,
            regex=True,
        )
    return flag.astype(float).where(is_first)


def platoon_xwoba_columns(raw: pd.DataFrame, desc: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Estimated xwOBA on balls in play, split into vs-LHB / vs-RHB columns (NaN elsewhere).

    ``desc`` is the pre-lowercased ``description`` column, shared across feature helpers.
    """
    if "stand" not in raw.columns or "estimated_woba_using_speedangle" not in raw.columns:
        return _nan_series(raw.index), _nan_series(raw.index)
    stand = raw["stand"].fillna("").astype(str).str.strip().str.upper()
    w = pd.to_numeric(raw["estimated_woba_using_speedangle"], errors="coerce")
    bip = desc.str.contains("hit_into_play", na=False)
    if "launch_speed" in raw.columns:
        bip = bip | pd.to_numeric(raw["launch_speed"], errors="coerce").notna()
    return w.where(bip & stand.eq("L")), w.where(bip & stand.eq("R"))


def batted_ball_flag_columns(
    raw: pd.DataFrame,
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    """(ground_ball, line_drive, fly_ball, popup) 0/1 flags among rows with a bb_type."""
    if "bb_type" not in raw.columns:
        nan = _nan_series(raw.index)
        return nan, nan, nan, nan
    bt = raw["bb_type"].fillna("").astype(str).str.lower().str.strip()
    mask = bt.ne("") & raw["bb_type"].notna()

    def flag(name: str) -> pd.Series:
        return bt.eq(name).astype(float).where(mask)

    return flag("ground_ball"), flag("line_drive"), flag("fly_ball"), flag("popup")


def pull_oppo_columns(raw: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """Handedness-aware pull/oppo 0/1 flags among rows with valid stand and hit coordinates."""
    if not all(c in raw.columns for c in ("stand", "hc_x", "hc_y")):
        return _nan_series(raw.index), _nan_series(raw.index)
    stand = raw["stand"].fillna("").astype(str).str.strip().str.upper()
    ang = spray_angle_degrees(raw["hc_x"], raw["hc_y"])
    valid = stand.isin(["R", "L"]) & ang.notna()
    is_r = stand.eq("R")
    # RHB pulls toward 3B (negative spray angle); LHB pulls toward 1B (positive).
    pull = np.where(is_r, ang < -ANGLE_PULL_THRESHOLD_DEG, ang > ANGLE_PULL_THRESHOLD_DEG)
    oppo = np.where(is_r, ang > ANGLE_PULL_THRESHOLD_DEG, ang < -ANGLE_PULL_THRESHOLD_DEG)
    return (
        pd.Series(pull, index=raw.index, dtype=float).where(valid),
        pd.Series(oppo, index=raw.index, dtype=float).where(valid),
    )
