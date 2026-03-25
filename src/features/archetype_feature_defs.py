"""
Shared definitions for pitch-derived archetype-friendly features.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Tuple

import numpy as np
import pandas as pd

@dataclass(frozen=True)
class BarrelDef:
    speed_threshold_mph: float = 98.0
    launch_angle_min_deg: float = 26.0
    launch_angle_max_deg: float = 30.0


DEFAULT_BARREL_DEF = BarrelDef()


def nan_iqr(values: pd.Series) -> float:
    """Interquartile range (75th - 25th) with NA-safe handling."""
    x = pd.to_numeric(values, errors="coerce").dropna().to_numpy()
    if x.size < 2:
        return float("nan")
    q25 = np.nanpercentile(x, 25)
    q75 = np.nanpercentile(x, 75)
    return float(q75 - q25)


def nan_entropy_from_counts(counts: Dict[str, int]) -> float:
    """Shannon entropy computed from discrete counts."""
    total = sum(counts.values())
    if total <= 0:
        return float("nan")
    probs = np.array([c / total for c in counts.values() if c > 0], dtype=float)
    if probs.size <= 1:
        return 0.0
    return float(-(probs * np.log(probs)).sum())


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
    - Statcast `description` in your sample includes values like:
      `foul`, `hit_into_play`, `called_strike`, `ball`, etc.
    - We classify "swing" primarily by:
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

    # Rare cases can appear as `missed_bunt` / `bunt_foul` etc.
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
    """Compute Statcast/MLB barrel flag from `launch_speed` and `launch_angle`."""
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


def iqr_mean_summary(values: pd.Series) -> Tuple[float, float]:
    """Return (mean, iqr) with NA-safe handling."""
    x = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    x = x[~np.isnan(x)]
    if x.size == 0:
        return (float("nan"), float("nan"))
    return (float(np.nanmean(x)), nan_iqr(pd.Series(x)))


def pitch_type_shares_and_entropy(df: pd.DataFrame, *, pitch_type_col: str = "pitch_type") -> Dict[str, float]:
    """Return per-pitch-type shares plus `pitch_type_entropy` for a player-year."""
    if pitch_type_col not in df.columns:
        return {}

    pitch_types = df[pitch_type_col].dropna()
    n = len(pitch_types)
    if n <= 0:
        return {"pitch_type_entropy": float("nan")}

    counts = pitch_types.value_counts()
    shares: Dict[str, float] = {}
    for pt, c in counts.items():
        shares[f"pitch_type_{pt}_share"] = float(c / n)

    entropy = nan_entropy_from_counts(counts.to_dict())
    shares["pitch_type_entropy"] = entropy
    return shares

