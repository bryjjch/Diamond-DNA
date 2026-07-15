"""
Load raw defensive Parquet layers from S3 and aggregate to MLBAM player_id -> metrics.

Used to enrich batter player-year feature rows. All sources are Savant
leaderboards keyed by MLBAM player id.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ....common.s3_helpers import read_parquet_from_s3

logger = logging.getLogger(__name__)

_DEFENCE_METRIC_KEYS: Tuple[str, ...] = (
    "def_oaa_total",
    "def_actual_fielding_success_rate_mean",
    "def_adj_estimated_fielding_success_rate_mean",
    "def_outfield_catch_completion_rate",
    "def_arm_strength_max_mph",
    "def_pop_time_2b_sec",
    "def_framing_runs",
    "def_frv_total",
)

# Map Statcast/MLB numeric position codes to short labels. Catcher (2) is sourced from
# the catcher-specific framing/pop-time tables; OAA leaderboards exclude catchers.
POSITION_CODE_TO_LABEL: Dict[int, str] = {
    2: "C",
    3: "1B",
    4: "2B",
    5: "3B",
    6: "SS",
    7: "LF",
    8: "CF",
    9: "RF",
}
UNKNOWN_POSITION = "UNK"


def _empty_metrics() -> Dict[str, float]:
    """Return a dictionary with all defence metrics set to NaN."""
    return {k: float("nan") for k in _DEFENCE_METRIC_KEYS}


def _parse_pct_cell(x: object) -> float:
    """Parse a percentage cell from a string and return a float."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return float("nan")
    s = str(x).strip().replace("%", "")
    try:
        return float(s) / 100.0
    except ValueError:
        return float("nan")


def _col_ci(df: pd.DataFrame, *names: str) -> Optional[str]:
    """Return the column name in lowercase if it exists in the DataFrame."""
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def _weighted_of_catch_completion(df: pd.DataFrame) -> pd.Series:
    """
    Overall outs / opportunities across Statcast stars (outfield catch probability leaderboard).
    """
    star_cols: List[Tuple[str, str]] = [
        ("n_fieldout_5stars", "n_opp_5stars"),
        ("n_fieldout_4stars", "n_opp_4stars"),
        ("n_fieldout_3stars", "n_opp_3stars"),
        ("n_fieldout_2stars", "n_opp_2stars"),
        ("n_fieldout_1stars", "n_opp_1stars"),
    ]
    pid_col = _col_ci(df, "player_id", "id")
    if not pid_col:
        return pd.Series(dtype=float)

    outs = np.zeros(len(df), dtype=float)
    opps = np.zeros(len(df), dtype=float)
    for fo, opp in star_cols:
        if fo in df.columns and opp in df.columns:
            outs += pd.to_numeric(df[fo], errors="coerce").fillna(0).to_numpy()
            opps += pd.to_numeric(df[opp], errors="coerce").fillna(0).to_numpy()
    rate = np.where(opps > 0, outs / opps, np.nan)
    pids = pd.to_numeric(df[pid_col], errors="coerce")
    return pd.Series(rate, index=pids)


def load_defence_metrics_by_player_year(
    bucket: str,
    raw_defence_prefix: str,
    year: int,
) -> Dict[int, Dict[str, float]]:
    """
    Returns mapping MLBAM player_id -> defensive feature columns (nan if unknown).
    """
    out: Dict[int, Dict[str, float]] = {}

    # --- OAA (sum across positions; mean success rates across position rows) ---
    oaa_key = f"{raw_defence_prefix.strip('/')}/year={year}/statcast_oaa.parquet"
    oaa_df = read_parquet_from_s3(bucket, oaa_key, log_read=False, missing_key_log="none")
    if oaa_df is not None and not oaa_df.empty:
        pid_c = _col_ci(oaa_df, "player_id")
        oaa_col = _col_ci(oaa_df, "outs_above_average")
        if pid_c and oaa_col:
            grp = oaa_df.groupby(pid_c, dropna=True)
            # Sum the outs above average for each player.
            oaa_sum = grp[oaa_col].apply(lambda s: pd.to_numeric(s, errors="coerce").sum(min_count=1))
            # Get the column name for the actual success rate.
            act_c = _col_ci(oaa_df, "actual_success_rate_formatted")
            # Get the column name for the adjusted estimated success rate.
            adj_c = _col_ci(oaa_df, "adj_estimated_success_rate_formatted")
            
            # Mean the actual success rate for each player.
            act_mean = (
                grp[act_c].apply(lambda s: pd.to_numeric(s.map(_parse_pct_cell), errors="coerce").mean(skipna=True))
                if act_c
                else None
            )
            # Mean the adjusted estimated success rate for each player.
            adj_mean = (
                grp[adj_c].apply(lambda s: pd.to_numeric(s.map(_parse_pct_cell), errors="coerce").mean(skipna=True))
                if adj_c
                else None
            )
            # Loop through each player and set the defensive metrics.
            for pid in oaa_sum.index:
                try:
                    pid_i = int(float(pid))
                except (TypeError, ValueError):
                    continue
                # Set the defensive metrics for the player.
                row = out.setdefault(pid_i, _empty_metrics())
                # Set the outs above average for the player.
                v = oaa_sum.loc[pid]
                row["def_oaa_total"] = float(v) if pd.notna(v) else float("nan")
                if act_mean is not None and pid in act_mean.index:
                    # Set the actual fielding success rate for the player.
                    row["def_actual_fielding_success_rate_mean"] = float(act_mean.loc[pid])
                if adj_mean is not None and pid in adj_mean.index:
                    # Set the adjusted estimated fielding success rate for the player.
                    row["def_adj_estimated_fielding_success_rate_mean"] = float(adj_mean.loc[pid])

    # --- Outfield catch probability -> completion rate ---
    cp_key = (
        f"{raw_defence_prefix.strip('/')}/year={year}/statcast_outfield_catch_probability.parquet"
    )
    cp_df = read_parquet_from_s3(bucket, cp_key, log_read=False, missing_key_log="none")
    if cp_df is not None and not cp_df.empty:
        # Get the weighted outfield catch completion rate for each player.
        rates = _weighted_of_catch_completion(cp_df)
        # Loop through each player and set the defensive metrics.
        for pid, r in rates.items():
            if pd.isna(pid):
                continue
            try:
                pid_i = int(float(pid))
            except (TypeError, ValueError):
                continue
            row = out.setdefault(pid_i, _empty_metrics())
            # Set the outfield catch completion rate for the player.
            row["def_outfield_catch_completion_rate"] = float(r) if pd.notna(r) else float("nan")

    # --- Arm strength (Savant max arm ~ top-end throws) ---
    arm_key = f"{raw_defence_prefix.strip('/')}/year={year}/statcast_arm_strength.parquet"
    arm_df = read_parquet_from_s3(bucket, arm_key, log_read=False, missing_key_log="none")
    if arm_df is not None and not arm_df.empty:
        pid_c = _col_ci(arm_df, "player_id")
        max_c = _col_ci(arm_df, "max_arm_strength")
        if pid_c and max_c:
            arm_df = arm_df.copy()
            arm_df[pid_c] = pd.to_numeric(arm_df[pid_c], errors="coerce")
            arm_df[max_c] = pd.to_numeric(arm_df[max_c], errors="coerce")
            # Get the maximum arm strength for each player.
            grp_max = arm_df.groupby(pid_c, dropna=True)[max_c].max()
            for pid, val in grp_max.items():
                if pd.isna(pid) or pd.isna(val):
                    continue
                pid_i = int(pid)
                row = out.setdefault(pid_i, _empty_metrics())
                row["def_arm_strength_max_mph"] = float(val)

    # --- Catcher pop time (to 2B) ---
    pop_key = f"{raw_defence_prefix.strip('/')}/year={year}/statcast_catcher_poptime.parquet"
    pop_df = read_parquet_from_s3(bucket, pop_key, log_read=False, missing_key_log="none")
    if pop_df is not None and not pop_df.empty:
        pid_c = _col_ci(pop_df, "entity_id", "player_id")
        pop_c = _col_ci(pop_df, "pop_2b_sba", "pop_2b")
        if pid_c and pop_c:
            for pid, val in zip(pop_df[pid_c], pd.to_numeric(pop_df[pop_c], errors="coerce")):
                if pd.isna(pid) or pd.isna(val):
                    continue
                pid_i = int(pid)
                row = out.setdefault(pid_i, _empty_metrics())
                # Set the pop time to 2B for the player.
                row["def_pop_time_2b_sec"] = float(val)

    # --- Catcher framing (runs) ---
    frm_key = f"{raw_defence_prefix.strip('/')}/year={year}/statcast_catcher_framing.parquet"
    frm_df = read_parquet_from_s3(bucket, frm_key, log_read=False, missing_key_log="none")
    if frm_df is not None and not frm_df.empty:
        pid_c = _col_ci(frm_df, "id", "player_id")
        rv_c = _col_ci(frm_df, "rv_tot", "framing_runs")
        if pid_c and rv_c:
            for pid, val in zip(frm_df[pid_c], pd.to_numeric(frm_df[rv_c], errors="coerce")):
                if pd.isna(pid) or pd.isna(val):
                    continue
                pid_i = int(pid)
                # Set the framing runs for the player.
                row = out.setdefault(pid_i, _empty_metrics())
                row["def_framing_runs"] = float(val)

    # --- Fielding run value (Savant; season total defensive runs, one row per player) ---
    frv_key = f"{raw_defence_prefix.strip('/')}/year={year}/statcast_fielding_run_value.parquet"
    frv_df = read_parquet_from_s3(bucket, frv_key, log_read=False, missing_key_log="none")
    if frv_df is not None and not frv_df.empty:
        pid_c = _col_ci(frv_df, "id", "player_id")
        frv_c = _col_ci(frv_df, "total_runs")
        if pid_c and frv_c:
            for pid, val in zip(
                pd.to_numeric(frv_df[pid_c], errors="coerce"),
                pd.to_numeric(frv_df[frv_c], errors="coerce"),
            ):
                if pd.isna(pid) or pd.isna(val):
                    continue
                row = out.setdefault(int(pid), _empty_metrics())
                # Set the total fielding run value for the player.
                row["def_frv_total"] = float(val)

    return out


def merge_defence_features(
    features: pd.DataFrame,
    defence_by_year: Dict[int, Dict[int, Dict[str, float]]],
) -> pd.DataFrame:
    """
    Left-join defensive metric columns onto ``(player_id, year)`` feature rows (batters).

    Unknown player-years get NaN for every defensive metric.
    """
    rows = [
        {"player_id": pid, "year": y, **metrics}
        for y, by_pid in defence_by_year.items()
        for pid, metrics in by_pid.items()
    ]
    defence_df = pd.DataFrame(rows, columns=["player_id", "year", *_DEFENCE_METRIC_KEYS]).astype(
        {"player_id": "int64", "year": "int64"}
    )
    return features.merge(defence_df, on=["player_id", "year"], how="left")


def load_primary_positions_by_player_year(
    bucket: str,
    raw_defence_prefix: str,
    year: int,
) -> Dict[int, str]:
    """
    Return ``player_id -> primary_position`` for batters in ``year``.

    A player is classified as ``C`` when they appear in either the catcher framing or
    catcher pop-time leaderboard (Savant publishes those only for catchers). Otherwise
    the primary position is the OAA leaderboard position with the most attempts (or, if
    attempts columns are absent, the most leaderboard rows). Players with no defensive
    data fall through to ``UNKNOWN_POSITION`` so downstream code can decide how to handle
    them (typically grouped with non-catcher batters).
    """
    positions: Dict[int, str] = {}

    catcher_ids: set[int] = set()
    for filename in ("statcast_catcher_framing.parquet", "statcast_catcher_poptime.parquet"):
        key = f"{raw_defence_prefix.strip('/')}/year={year}/{filename}"
        df = read_parquet_from_s3(bucket, key, log_read=False, missing_key_log="none")
        if df is None or df.empty:
            continue
        pid_c = _col_ci(df, "player_id", "entity_id", "id")
        if not pid_c:
            continue
        pids = pd.to_numeric(df[pid_c], errors="coerce")
        for pid in pids.dropna().unique():
            try:
                catcher_ids.add(int(pid))
            except (TypeError, ValueError):
                continue
    for pid in catcher_ids:
        positions[pid] = "C"

    oaa_key = f"{raw_defence_prefix.strip('/')}/year={year}/statcast_oaa.parquet"
    oaa_df = read_parquet_from_s3(bucket, oaa_key, log_read=False, missing_key_log="none")
    if oaa_df is not None and not oaa_df.empty:
        pid_c = _col_ci(oaa_df, "player_id")
        pos_c = _col_ci(oaa_df, "oaa_leaderboard_position", "primary_pos", "position")
        if pid_c and pos_c:
            att_c = _col_ci(oaa_df, "attempts", "n_attempts", "n_opp")
            tmp = oaa_df[[pid_c, pos_c]].copy()
            tmp[pid_c] = pd.to_numeric(tmp[pid_c], errors="coerce")
            tmp[pos_c] = pd.to_numeric(tmp[pos_c], errors="coerce")
            tmp = tmp.dropna(subset=[pid_c, pos_c])
            if att_c is not None:
                tmp["_weight"] = pd.to_numeric(oaa_df[att_c], errors="coerce").fillna(0.0)
            else:
                tmp["_weight"] = 1.0
            grouped = (
                tmp.groupby([pid_c, pos_c])["_weight"].sum().reset_index()
            )
            # For each player, pick the position with the largest weight.
            for pid, sub in grouped.groupby(pid_c):
                try:
                    pid_i = int(pid)
                except (TypeError, ValueError):
                    continue
                if pid_i in positions:
                    continue
                best = sub.sort_values("_weight", ascending=False).iloc[0]
                code = int(best[pos_c])
                positions[pid_i] = POSITION_CODE_TO_LABEL.get(code, UNKNOWN_POSITION)

    return positions


def merge_primary_position_features(
    features: pd.DataFrame,
    positions_by_year: Dict[int, Dict[int, str]],
) -> pd.DataFrame:
    """
    Left-join ``primary_position`` onto ``(player_id, year)`` feature rows (batters).

    Player-years with no defensive data get ``UNKNOWN_POSITION``.
    """
    rows = [
        {"player_id": pid, "year": y, "primary_position": pos}
        for y, by_pid in positions_by_year.items()
        for pid, pos in by_pid.items()
    ]
    positions_df = pd.DataFrame(rows, columns=["player_id", "year", "primary_position"]).astype(
        {"player_id": "int64", "year": "int64"}
    )
    out = features.merge(positions_df, on=["player_id", "year"], how="left")
    out["primary_position"] = out["primary_position"].fillna(UNKNOWN_POSITION)
    return out
