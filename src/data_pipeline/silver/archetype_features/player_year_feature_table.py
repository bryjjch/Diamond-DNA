"""
Vectorized bronze→silver player-year feature computation.

``compute_player_year_features`` produces the same rows as the loop-based reference
``player_year_features_from_df`` (see ``silver_features_build``), but as a single
SQL-style pipeline over the whole pitch frame:

1. derive per-pitch flag/value columns once (the SELECT expressions),
2. one ``groupby([player, year]).agg(...)`` per role (the GROUP BY),
3. sample-size filters on the aggregated table (the HAVING clause),
4. pitch-type-level aggregates pivoted wide (GROUP BY + PIVOT).

Conditional rates use the AVG(CASE WHEN ...) pattern: a float column holding the value
where the condition holds and NaN elsewhere, so the group mean has the right denominator.

Parity with the reference implementation is enforced in
``tests/test_vectorized_features_parity.py``.
"""

from __future__ import annotations

import logging
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from .archetype_feature_defs import (
    DEFAULT_BARREL_DEF,
    EXCLUDED_PITCH_TYPES,
    FASTBALL_PITCH_TYPES,
    BarrelDef,
    compute_barrel_flag,
    compute_in_zone,
    compute_swing_flag,
    spray_angle_degrees,
)

logger = logging.getLogger(__name__)

_ANGLE_PULL_THRESHOLD_DEG = 20.0

_REQUIRED_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "pitcher": (
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
    ),
    "batter": (
        "zone",
        "description",
        "launch_speed",
        "launch_angle",
        "iso_value",
        "estimated_slg_using_speedangle",
        "woba_value",
        "estimated_woba_using_speedangle",
    ),
}


def _nan_series(index: pd.Index) -> pd.Series:
    return pd.Series(np.nan, index=index)


def _first_pitch_strike_flag(raw: pd.DataFrame, desc: pd.Series) -> pd.Series:
    """0/1 on first pitches of a PA (strike or in play), NaN elsewhere."""
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


def _platoon_xwoba_columns(raw: pd.DataFrame, desc: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """Estimated xwOBA on balls in play, split into vs-LHB / vs-RHB columns (NaN elsewhere)."""
    if "stand" not in raw.columns or "estimated_woba_using_speedangle" not in raw.columns:
        return _nan_series(raw.index), _nan_series(raw.index)
    stand = raw["stand"].fillna("").astype(str).str.strip().str.upper()
    w = pd.to_numeric(raw["estimated_woba_using_speedangle"], errors="coerce")
    bip = desc.str.contains("hit_into_play", na=False)
    if "launch_speed" in raw.columns:
        bip = bip | pd.to_numeric(raw["launch_speed"], errors="coerce").notna()
    return w.where(bip & stand.eq("L")), w.where(bip & stand.eq("R"))


def _batted_ball_flag_columns(
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


def _pull_oppo_columns(raw: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    """Handedness-aware pull/oppo 0/1 flags among rows with valid stand and hit coordinates."""
    if not all(c in raw.columns for c in ("stand", "hc_x", "hc_y")):
        return _nan_series(raw.index), _nan_series(raw.index)
    stand = raw["stand"].fillna("").astype(str).str.strip().str.upper()
    ang = spray_angle_degrees(raw["hc_x"], raw["hc_y"])
    valid = stand.isin(["R", "L"]) & ang.notna()
    is_r = stand.eq("R")
    # RHB pulls toward 3B (negative spray angle); LHB pulls toward 1B (positive).
    pull = np.where(is_r, ang < -_ANGLE_PULL_THRESHOLD_DEG, ang > _ANGLE_PULL_THRESHOLD_DEG)
    oppo = np.where(is_r, ang > _ANGLE_PULL_THRESHOLD_DEG, ang < -_ANGLE_PULL_THRESHOLD_DEG)
    return (
        pd.Series(pull, index=raw.index, dtype=float).where(valid),
        pd.Series(oppo, index=raw.index, dtype=float).where(valid),
    )


def _group_iqr(
    g: "pd.core.groupby.DataFrameGroupBy",
    col_map: Dict[str, str],
) -> pd.DataFrame:
    """Per-group IQR (75th - 25th percentile) for each column, renamed via ``col_map``."""
    cols = list(col_map)
    q = g[cols].quantile([0.25, 0.75]).unstack(level=-1)
    iqr = q.xs(0.75, axis=1, level=-1) - q.xs(0.25, axis=1, level=-1)
    # nan_iqr returns NaN when a group has fewer than 2 observations.
    iqr = iqr.where(g[cols].count() >= 2)
    return iqr.rename(columns=col_map)


def _pitch_type_physical_means_wide(
    raw: pd.DataFrame,
    work: pd.DataFrame,
    *,
    min_pitches_per_type: int,
    valid_index: pd.MultiIndex,
) -> pd.DataFrame:
    """Per pitch-type means pivoted wide (columns like pt_FF_release_speed_mean)."""
    pt_u = raw["pitch_type"].astype(str).str.strip().str.upper()
    sub = pd.DataFrame(
        {
            "player_id": work["player_id"],
            "year": work["year"],
            "pitch_type": pt_u,
            "release_speed": work["release_speed"],
            "release_spin_rate": work["release_spin_rate"],
            "pfx_x": work["pfx_x"],
        }
    )
    sub = sub[pt_u.notna() & pt_u.ne("") & ~pt_u.isin(EXCLUDED_PITCH_TYPES)]
    # Restrict to player-years that survived the sample-size gates so the output
    # column set matches (columns exist only for pitch types someone qualified on).
    sub = sub[pd.MultiIndex.from_frame(sub[["player_id", "year"]]).isin(valid_index)]
    if sub.empty:
        return pd.DataFrame(index=valid_index[:0])

    pt = sub.groupby(["player_id", "year", "pitch_type"], dropna=True).agg(
        n=("pitch_type", "size"),
        release_speed_mean=("release_speed", "mean"),
        release_spin_rate_mean=("release_spin_rate", "mean"),
        pfx_x_mean=("pfx_x", "mean"),
    )
    pt = pt[pt["n"] >= min_pitches_per_type].drop(columns="n")
    wide = pt.unstack("pitch_type")
    wide.columns = [f"pt_{ptype}_{metric}" for metric, ptype in wide.columns]
    return wide


def _pitch_type_shares_and_entropy_wide(
    raw: pd.DataFrame,
    work: pd.DataFrame,
    *,
    valid_index: pd.MultiIndex,
) -> pd.DataFrame:
    """Per pitch-type usage shares plus Shannon entropy, pivoted wide."""
    keyed = pd.DataFrame(
        {
            "player_id": work["player_id"],
            "year": work["year"],
            "pitch_type": raw["pitch_type"],
        }
    )
    keyed = keyed[pd.MultiIndex.from_frame(keyed[["player_id", "year"]]).isin(valid_index)]
    counts = (
        keyed.groupby(["player_id", "year"], dropna=True)["pitch_type"].value_counts().unstack()
    )
    # NaN (not 0) where a player never threw a type, matching the reference's missing keys.
    shares = counts.div(counts.sum(axis=1), axis=0)
    entropy = -(shares * np.log(shares)).sum(axis=1)  # skipna: only thrown types contribute
    shares.columns = [f"pitch_type_{c}_share" for c in shares.columns]
    shares["pitch_type_entropy"] = entropy
    return shares


def compute_player_year_features(
    raw: pd.DataFrame,
    *,
    role: str,
    min_pitches_pitcher: int,
    min_pitches_batter: int,
    min_batted_ball_batter: int,
    hard_hit_speed_mph: float,
    min_pitches_per_pitch_type: int,
    barrel_def: BarrelDef = DEFAULT_BARREL_DEF,
) -> pd.DataFrame:
    """
    Aggregate pitch-level Statcast rows into one feature row per (player_id, year).

    ``raw`` must carry a ``year`` column and the ``role`` id column ("batter"/"pitcher").
    Returns a DataFrame with columns role, player_id, year, n_pitches_total, and the
    role-specific feature set (identical to ``player_year_features_from_df`` rows).
    """
    if role not in ("pitcher", "batter"):
        raise ValueError(f"Unknown role: {role}")
    for c in (role, "year"):
        if c not in raw.columns:
            raise ValueError(f"Column '{c}' not found in bronze data")
    missing = [c for c in _REQUIRED_COLUMNS[role] if c not in raw.columns]
    if missing:
        raise ValueError(f"{role.capitalize()} parquet missing required columns: {missing}")

    in_zone = compute_in_zone(raw)
    swing = compute_swing_flag(raw)
    desc = raw["description"].fillna("").astype(str).str.lower()

    work = pd.DataFrame(
        {
            "player_id": pd.to_numeric(raw[role], errors="coerce"),
            "year": raw["year"],
            "swing": swing,
            "zone_swing": swing & in_zone,
            "chase": swing & ~in_zone,
            "whiff": swing & desc.str.contains("swinging_strike", na=False),
        }
    )

    # Pitcher swing metrics describe opposing batters, hence the prefixed names.
    swing_prefix = "batter_" if role == "pitcher" else ""
    agg_kwargs: Dict[str, Tuple[str, str]] = {
        "n_pitches_total": ("swing", "size"),
        f"{swing_prefix}swing_rate": ("swing", "mean"),
        f"{swing_prefix}zone_swing_rate": ("zone_swing", "mean"),
        f"{swing_prefix}chase_rate": ("chase", "mean"),
        f"{swing_prefix}whiff_rate": ("whiff", "mean"),
    }

    if role == "pitcher":
        work["in_zone"] = in_zone
        for c in (
            "release_speed",
            "release_spin_rate",
            "release_extension",
            "pfx_x",
            "pfx_z",
            "plate_x",
            "plate_z",
        ):
            work[c] = pd.to_numeric(raw[c], errors="coerce")

        # Zone-grid location quality (edge vs meatball) among pitches on the 3x3 grid.
        z = pd.to_numeric(raw["zone"], errors="coerce")
        on_grid = z.between(1, 9, inclusive="both")
        work["meatball_flag"] = (z == 5).astype(float).where(on_grid)
        work["edge_flag"] = (z != 5).astype(float).where(on_grid)

        work["fps"] = _first_pitch_strike_flag(raw, desc)
        work["xwoba_vs_lhb"], work["xwoba_vs_rhb"] = _platoon_xwoba_columns(raw, desc)
        (
            work["gb_flag"],
            work["ld_flag"],
            work["fb_flag"],
            work["iffb_flag"],
        ) = _batted_ball_flag_columns(raw)

        # Fastball vs offspeed velocity, excluding junk and missing pitch types.
        pt_u = raw["pitch_type"].astype(str).str.strip().str.upper()
        usable = (
            work["release_speed"].notna() & pt_u.notna() & ~pt_u.isin(EXCLUDED_PITCH_TYPES)
        )
        work["fastball_speed"] = work["release_speed"].where(
            usable & pt_u.isin(FASTBALL_PITCH_TYPES)
        )
        work["offspeed_speed"] = work["release_speed"].where(
            usable & ~pt_u.isin(FASTBALL_PITCH_TYPES)
        )

        if "delta_run_exp" in raw.columns:
            work["delta_run_exp"] = pd.to_numeric(raw["delta_run_exp"], errors="coerce")
        else:
            logger.warning("Missing `delta_run_exp`; setting run value stats to NA.")
            work["delta_run_exp"] = np.nan

        agg_kwargs.update(
            in_zone_rate=("in_zone", "mean"),
            release_speed_max=("release_speed", "max"),
            fastball_velo_mean=("fastball_speed", "mean"),
            offspeed_velo_mean=("offspeed_speed", "mean"),
            pfx_x_mean=("pfx_x", "mean"),
            release_extension_mean=("release_extension", "mean"),
            pfx_z_mean=("pfx_z", "mean"),
            plate_x_mean=("plate_x", "mean"),
            plate_z_mean=("plate_z", "mean"),
            edge_percent=("edge_flag", "mean"),
            meatball_percent=("meatball_flag", "mean"),
            first_pitch_strike_rate=("fps", "mean"),
            xwoba_allowed_lhb_mean=("xwoba_vs_lhb", "mean"),
            xwoba_allowed_rhb_mean=("xwoba_vs_rhb", "mean"),
            gb_percent_allowed=("gb_flag", "mean"),
            ld_percent_allowed=("ld_flag", "mean"),
            fb_percent_allowed=("fb_flag", "mean"),
            iffb_percent_allowed=("iffb_flag", "mean"),
            delta_run_exp_mean=("delta_run_exp", "mean"),
        )
        iqr_cols = {
            "release_speed": "release_speed_iqr",
            "release_spin_rate": "release_spin_rate_iqr",
            "pfx_x": "pfx_x_iqr",
            "release_extension": "release_extension_iqr",
            "pfx_z": "pfx_z_iqr",
        }
        sd_cols = {"plate_x": "plate_x_sd", "plate_z": "plate_z_sd"}
        min_pitches = min_pitches_pitcher
    else:
        for c in (
            "launch_speed",
            "launch_angle",
            "iso_value",
            "estimated_slg_using_speedangle",
            "woba_value",
            "estimated_woba_using_speedangle",
        ):
            work[c] = pd.to_numeric(raw[c], errors="coerce")
        work["sprint_speed"] = (
            pd.to_numeric(raw["sprint_speed"], errors="coerce")
            if "sprint_speed" in raw.columns
            else np.nan
        )

        has_launch = work["launch_speed"].notna() & work["launch_angle"].notna()
        work["is_batted_ball"] = has_launch
        barrel = compute_barrel_flag(raw, barrel_def=barrel_def)
        work["barrel_on_bb"] = barrel.astype(float).where(has_launch)
        work["hard_hit_on_bb"] = (
            (work["launch_speed"] >= hard_hit_speed_mph).astype(float).where(has_launch)
        )
        work["sweet_spot_flag"] = (
            work["launch_angle"]
            .between(8.0, 32.0, inclusive="both")
            .astype(float)
            .where(work["launch_angle"].notna())
        )
        work["pull_flag"], work["oppo_flag"] = _pull_oppo_columns(raw)
        (
            work["gb_flag"],
            work["ld_flag"],
            work["fb_flag"],
            work["iffb_flag"],
        ) = _batted_ball_flag_columns(raw)

        # Walk rate (BB%) = walks / plate appearances. `events` is only populated on
        # the final pitch of each PA, so non-null events count PAs.
        if "events" in raw.columns:
            events = raw["events"].astype("object")
            work["is_pa_end"] = events.notna() & events.astype(str).str.strip().ne("")
            work["is_walk"] = events.isin(["walk", "intent_walk"])
        else:
            work["is_pa_end"] = False
            work["is_walk"] = False

        agg_kwargs.update(
            walk_count=("is_walk", "sum"),
            pa_count=("is_pa_end", "sum"),
            n_batted_balls=("is_batted_ball", "sum"),
            barrel_rate=("barrel_on_bb", "mean"),
            hard_hit_rate=("hard_hit_on_bb", "mean"),
            pull_percent=("pull_flag", "mean"),
            opposite_field_percent=("oppo_flag", "mean"),
            gb_percent=("gb_flag", "mean"),
            ld_percent=("ld_flag", "mean"),
            fb_percent=("fb_flag", "mean"),
            iffb_percent=("iffb_flag", "mean"),
            sweet_spot_percent=("sweet_spot_flag", "mean"),
            launch_speed_mean=("launch_speed", "mean"),
            launch_angle_mean=("launch_angle", "mean"),
            iso_value_mean=("iso_value", "mean"),
            estimated_slg_using_speedangle_mean=("estimated_slg_using_speedangle", "mean"),
            woba_value_mean=("woba_value", "mean"),
            estimated_woba_using_speedangle_mean=("estimated_woba_using_speedangle", "mean"),
            sprint_speed_mean=("sprint_speed", "mean"),
        )
        iqr_cols = {"launch_speed": "launch_speed_iqr", "launch_angle": "launch_angle_iqr"}
        sd_cols = {}
        min_pitches = min_pitches_batter

    g = work.groupby(["player_id", "year"], dropna=True)
    feats = g.agg(**agg_kwargs)
    if sd_cols:
        feats = feats.join(g[list(sd_cols)].std(ddof=0).rename(columns=sd_cols))
    feats = feats.join(_group_iqr(g, iqr_cols))

    # HAVING: sample-size gates.
    feats = feats[feats["n_pitches_total"] >= min_pitches]
    if role == "batter":
        feats = feats[feats["n_batted_balls"] >= min_batted_ball_batter]
        feats["walk_rate"] = feats["walk_count"] / feats["pa_count"].where(feats["pa_count"] > 0)
        feats = feats.drop(columns=["walk_count", "pa_count", "n_batted_balls"])
    else:
        feats["velo_differential"] = feats["fastball_velo_mean"] - feats["offspeed_velo_mean"]
        # The reference reports all three as NaN unless both pitch groups are present.
        one_sided = feats["fastball_velo_mean"].isna() | feats["offspeed_velo_mean"].isna()
        feats.loc[
            one_sided, ["fastball_velo_mean", "offspeed_velo_mean", "velo_differential"]
        ] = np.nan
        feats["platoon_xwoba_allowed_diff"] = (
            feats["xwoba_allowed_lhb_mean"] - feats["xwoba_allowed_rhb_mean"]
        )
        feats = feats.join(
            _pitch_type_physical_means_wide(
                raw,
                work,
                min_pitches_per_type=min_pitches_per_pitch_type,
                valid_index=feats.index,
            )
        )
        feats = feats.join(_pitch_type_shares_and_entropy_wide(raw, work, valid_index=feats.index))

    feats = feats.reset_index()
    feats["player_id"] = feats["player_id"].astype("int64")
    feats["year"] = feats["year"].astype("int64")
    feats.insert(0, "role", role)
    return feats
