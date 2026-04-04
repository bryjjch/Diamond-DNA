"""
Shared definitions for pitch-derived archetype-friendly features.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Set, Tuple

import numpy as np
import pandas as pd

# Statcast field coordinates (feet): back tip of home plate to hit location.
_HC_HOME_X = 125.42
_HC_HOME_Y = 198.27

# Fastball pitch types.
_FASTBALL_PITCH_TYPES: Set[str] = {"FF", "FA", "FT", "SI", "FC"}
# Junk / non-pitch rows to drop from velocity summaries.
_EXCLUDED_PITCH_TYPES_MINIMAL: Set[str] = {"UN", "PO", "XX", "IN"}


def _mean_numeric_to_float(values: pd.Series) -> float:
    """Mean of coerced numeric series as a Python float; handles all-NA and pd.NA from pandas."""
    x = pd.to_numeric(values, errors="coerce")
    m = x.mean(skipna=True)
    if m is None or pd.isna(m):
        return float("nan")
    return float(m)


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


def spray_angle_degrees(hc_x: pd.Series, hc_y: pd.Series) -> pd.Series:
    """Horizontal spray angle in degrees (Statcast hc_x / hc_y convention)."""
    x = pd.to_numeric(hc_x, errors="coerce")
    y = pd.to_numeric(hc_y, errors="coerce")
    rad = np.arctan2(x - _HC_HOME_X, _HC_HOME_Y - y)
    return pd.Series(np.degrees(rad), index=hc_x.index)


def pull_oppo_center_rates(
    df: pd.DataFrame,
    *,
    stand_col: str = "stand",
    hc_x_col: str = "hc_x",
    hc_y_col: str = "hc_y",
    angle_pull_threshold_deg: float = 20.0,
) -> Tuple[float, float, float]:
    """
    Pull / opposite-field / center rates among rows with valid stand, hc_x, hc_y.

    Uses handedness-specific pull sides (Statcast: hc_x increases toward first base).
    """
    if stand_col not in df.columns or hc_x_col not in df.columns or hc_y_col not in df.columns:
        return (float("nan"), float("nan"), float("nan"))

    stand = _safe_lower_series(df[stand_col]).str.strip().str.upper()
    ang = spray_angle_degrees(df[hc_x_col], df[hc_y_col])
    valid = stand.isin(["R", "L"]) & ang.notna()
    if not valid.any():
        return (float("nan"), float("nan"), float("nan"))

    s = stand[valid]
    a = ang[valid]
    # RHB pulls toward 3B (lower hc_x) -> negative spray angle; LHB pulls toward 1B -> positive.
    pull_side = np.where(s == "R", a < -angle_pull_threshold_deg, a > angle_pull_threshold_deg)
    oppo_side = np.where(s == "R", a > angle_pull_threshold_deg, a < -angle_pull_threshold_deg)
    center = ~(pull_side | oppo_side)

    n = int(valid.sum())
    return (float(pull_side.mean()), float(oppo_side.mean()), float(center.mean()))


def batted_ball_type_rates(
    df: pd.DataFrame,
    *,
    bb_type_col: str = "bb_type",
) -> Dict[str, float]:
    """Ground / line drive / fly ball / popup rates among pitches with non-null bb_type."""
    if bb_type_col not in df.columns:
        return {
            "gb_percent": float("nan"),
            "ld_percent": float("nan"),
            "fb_percent": float("nan"),
            "iffb_percent": float("nan"),
        }

    bt = _safe_lower_series(df[bb_type_col]).str.strip()
    mask = bt.ne("") & df[bb_type_col].notna()
    if not mask.any():
        return {
            "gb_percent": float("nan"),
            "ld_percent": float("nan"),
            "fb_percent": float("nan"),
            "iffb_percent": float("nan"),
        }

    bt = bt[mask]
    n = len(bt)
    return {
        "gb_percent": float(bt.eq("ground_ball").mean()),
        "ld_percent": float(bt.eq("line_drive").mean()),
        "fb_percent": float(bt.eq("fly_ball").mean()),
        "iffb_percent": float(bt.eq("popup").mean()),
    }


def sweet_spot_rate(launch_angle: pd.Series) -> float:
    """Share of batted balls with launch angle in [8, 32] degrees (MLB sweet-spot band)."""
    la = pd.to_numeric(launch_angle, errors="coerce")
    mask = la.notna()
    if not mask.any():
        return float("nan")
    la = la[mask]
    return float(la.between(8.0, 32.0, inclusive="both").mean())


def zone_edge_and_meatball_rates(df: pd.DataFrame) -> Tuple[float, float]:
    """
    Among pitches mapped to the 3x3 strike-zone grid (zone 1..9), share on the rim (edge)
    vs center (zone 5).
    """
    if "zone" not in df.columns:
        return (float("nan"), float("nan"))
    z = pd.to_numeric(df["zone"], errors="coerce")
    mask = z.between(1, 9, inclusive="both")
    if not mask.any():
        return (float("nan"), float("nan"))
    z9 = z[mask].astype(int)
    meatball = float((z9 == 5).mean())
    edge = float((z9 != 5).mean())
    return (edge, meatball)


def first_pitch_strike_rate(df: pd.DataFrame) -> float:
    """Share of plate appearances where the first pitch is not a ball (uses `pitch_number` and `type`)."""
    need = ("pitch_number", "game_pk", "at_bat_number")
    if not all(c in df.columns for c in need):
        return float("nan")

    pn = pd.to_numeric(df["pitch_number"], errors="coerce")
    sub = df.loc[pn == 1].copy()
    if sub.empty:
        return float("nan")

    if "type" in sub.columns:
        t = _safe_lower_series(sub["type"]).str.strip().str.upper()
        # B = ball; S = strike; X = in play (counts toward FPS).
        known = t.notna()
        if known.any():
            fps = t.loc[known].isin(["S", "X"])
            return float(fps.mean())

    desc = _safe_lower_series(sub["description"]) if "description" in sub.columns else pd.Series("", index=sub.index)
    strike_like = desc.str.contains(
        "called_strike|swinging_strike|foul|hit_into_play|missed_bunt|bunt_foul",
        na=False,
        regex=True,
    )
    return float(strike_like.mean())


def platoon_estimated_woba_means(
    df: pd.DataFrame,
    *,
    stand_col: str = "stand",
    woba_col: str = "estimated_woba_using_speedangle",
    bip_only: bool = True,
) -> Tuple[float, float, float]:
    """
    Mean estimated xwOBA vs LHB and RHB, and (LHB - RHB) differential.

    Rows with batter stand 'S' are excluded. If `bip_only`, restricts to balls in play.
    """
    if stand_col not in df.columns or woba_col not in df.columns:
        return (float("nan"), float("nan"), float("nan"))

    stand = _safe_lower_series(df[stand_col]).str.strip().str.upper()
    w = pd.to_numeric(df[woba_col], errors="coerce")

    if bip_only:
        if "description" in df.columns:
            desc = _safe_lower_series(df["description"])
            bip = desc.str.contains("hit_into_play", na=False)
        else:
            bip = pd.Series(False, index=df.index)
        if "launch_speed" in df.columns:
            bip = bip | pd.to_numeric(df["launch_speed"], errors="coerce").notna()
        stand = stand.loc[bip]
        w = w.loc[bip]

    out_l = _mean_numeric_to_float(w.loc[stand == "L"])
    out_r = _mean_numeric_to_float(w.loc[stand == "R"])
    diff = out_l - out_r if (not np.isnan(out_l) and not np.isnan(out_r)) else float("nan")
    return (out_l, out_r, diff)


def pitch_type_physical_means(
    df: pd.DataFrame,
    *,
    min_pitches_per_type: int,
    pitch_type_col: str = "pitch_type",
) -> Dict[str, float]:
    """
    Per pitch-type means for release_speed, release_spin_rate, pfx_x (keys like pt_FF_release_speed_mean).
    """
    cols = ("release_speed", "release_spin_rate", "pfx_x")
    if pitch_type_col not in df.columns or not all(c in df.columns for c in cols):
        return {}

    pt = df[pitch_type_col].astype(str).str.strip().str.upper()
    out: Dict[str, float] = {}
    for ptype in pt.dropna().unique():
        if not ptype or ptype in _EXCLUDED_PITCH_TYPES_MINIMAL:
            continue
        m = pt == ptype
        if int(m.sum()) < min_pitches_per_type:
            continue
        sub = df.loc[m]
        out[f"pt_{ptype}_release_speed_mean"] = _mean_numeric_to_float(sub["release_speed"])
        out[f"pt_{ptype}_release_spin_rate_mean"] = _mean_numeric_to_float(sub["release_spin_rate"])
        out[f"pt_{ptype}_pfx_x_mean"] = _mean_numeric_to_float(sub["pfx_x"])
    return out


def fastball_offspeed_velo_means_and_diff(df: pd.DataFrame) -> Tuple[float, float, float]:
    """Mean fastball-group velo, non-fastball velo, and FB minus offspeed (pitch-type level)."""
    if "pitch_type" not in df.columns or "release_speed" not in df.columns:
        return (float("nan"), float("nan"), float("nan"))

    pt = df["pitch_type"].astype(str).str.strip().str.upper()
    spd = pd.to_numeric(df["release_speed"], errors="coerce")
    valid = spd.notna() & pt.notna() & ~pt.isin(_EXCLUDED_PITCH_TYPES_MINIMAL)

    fb_mask = valid & pt.isin(_FASTBALL_PITCH_TYPES)
    off_mask = valid & ~pt.isin(_FASTBALL_PITCH_TYPES)

    if not fb_mask.any() or not off_mask.any():
        return (float("nan"), float("nan"), float("nan"))

    fb_m = _mean_numeric_to_float(spd[fb_mask])
    off_m = _mean_numeric_to_float(spd[off_mask])
    return (fb_m, off_m, float(fb_m - off_m))


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

