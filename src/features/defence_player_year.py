"""
Load raw defensive Parquet layers from S3 and aggregate to MLBAM player_id -> metrics.

Used to enrich batter player-year feature rows. FanGraphs rows are mapped through
Chadwick ``key_fangraphs`` -> ``key_mlbam``.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from pybaseball import chadwick_register
except Exception:  # pragma: no cover
    chadwick_register = None

from ..s3_parquet import read_parquet_from_s3

logger = logging.getLogger(__name__)

_DEFENCE_METRIC_KEYS: Tuple[str, ...] = (
    "def_oaa_total",
    "def_actual_fielding_success_rate_mean",
    "def_adj_estimated_fielding_success_rate_mean",
    "def_outfield_catch_completion_rate",
    "def_arm_strength_max_mph",
    "def_pop_time_2b_sec",
    "def_framing_runs",
    "def_drs_total",
)


def _empty_metrics() -> Dict[str, float]:
    return {k: float("nan") for k in _DEFENCE_METRIC_KEYS}


def _parse_pct_cell(x: object) -> float:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return float("nan")
    s = str(x).strip().replace("%", "")
    try:
        return float(s) / 100.0
    except ValueError:
        return float("nan")


def _col_ci(df: pd.DataFrame, *names: str) -> Optional[str]:
    lower = {c.lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def _weighted_of_catch_completion(df: pd.DataFrame) -> pd.Series:
    """
    Overall outs / opportunities across Statcast star bins (outfield catch prob leaderboard).
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


def fangraphs_to_mlbam_map() -> Dict[int, int]:
    if chadwick_register is None:
        logger.warning("pybaseball.chadwick_register unavailable; FanGraphs DRS merge skipped.")
        return {}
    cw = chadwick_register()
    fg = pd.to_numeric(cw["key_fangraphs"], errors="coerce")
    mlb = pd.to_numeric(cw["key_mlbam"], errors="coerce")
    mask = fg.notna() & mlb.notna()
    # Last write wins on duplicate key_fangraphs (rare).
    return {int(a): int(b) for a, b in zip(fg[mask], mlb[mask])}


def load_defence_metrics_by_player_year(
    bucket: str,
    raw_defence_prefix: str,
    year: int,
    *,
    fg_id_map: Optional[Dict[int, int]] = None,
) -> Dict[int, Dict[str, float]]:
    """
    Returns mapping MLBAM player_id -> defensive feature columns (nan if unknown).
    """
    out: Dict[int, Dict[str, float]] = {}
    base = f"{raw_defence_prefix}/year={year}"

    # --- OAA (sum across positions; mean success rates across position rows) ---
    oaa_key = f"{base}/statcast_oaa.parquet"
    oaa_df = read_parquet_from_s3(bucket, oaa_key, log_read=False, missing_key_log="none")
    if oaa_df is not None and not oaa_df.empty:
        pid_c = _col_ci(oaa_df, "player_id")
        oaa_col = _col_ci(oaa_df, "outs_above_average")
        if pid_c and oaa_col:
            grp = oaa_df.groupby(pid_c, dropna=True)
            oaa_sum = grp[oaa_col].apply(lambda s: pd.to_numeric(s, errors="coerce").sum(min_count=1))
            act_c = _col_ci(oaa_df, "actual_success_rate_formatted")
            adj_c = _col_ci(oaa_df, "adj_estimated_success_rate_formatted")
            act_mean = (
                grp[act_c].apply(lambda s: pd.to_numeric(s.map(_parse_pct_cell), errors="coerce").mean(skipna=True))
                if act_c
                else None
            )
            adj_mean = (
                grp[adj_c].apply(lambda s: pd.to_numeric(s.map(_parse_pct_cell), errors="coerce").mean(skipna=True))
                if adj_c
                else None
            )
            for pid in oaa_sum.index:
                try:
                    pid_i = int(float(pid))
                except (TypeError, ValueError):
                    continue
                row = out.setdefault(pid_i, _empty_metrics())
                v = oaa_sum.loc[pid]
                row["def_oaa_total"] = float(v) if pd.notna(v) else float("nan")
                if act_mean is not None and pid in act_mean.index:
                    row["def_actual_fielding_success_rate_mean"] = float(act_mean.loc[pid])
                if adj_mean is not None and pid in adj_mean.index:
                    row["def_adj_estimated_fielding_success_rate_mean"] = float(adj_mean.loc[pid])

    # --- Outfield catch probability -> completion rate ---
    cp_key = f"{base}/statcast_outfield_catch_probability.parquet"
    cp_df = read_parquet_from_s3(bucket, cp_key, log_read=False, missing_key_log="none")
    if cp_df is not None and not cp_df.empty:
        rates = _weighted_of_catch_completion(cp_df)
        for pid, r in rates.items():
            if pd.isna(pid):
                continue
            try:
                pid_i = int(float(pid))
            except (TypeError, ValueError):
                continue
            row = out.setdefault(pid_i, _empty_metrics())
            row["def_outfield_catch_completion_rate"] = float(r) if pd.notna(r) else float("nan")

    # --- Arm strength (Savant max arm ~ top-end throws) ---
    arm_key = f"{base}/statcast_arm_strength.parquet"
    arm_df = read_parquet_from_s3(bucket, arm_key, log_read=False, missing_key_log="none")
    if arm_df is not None and not arm_df.empty:
        pid_c = _col_ci(arm_df, "player_id")
        max_c = _col_ci(arm_df, "max_arm_strength")
        if pid_c and max_c:
            arm_df = arm_df.copy()
            arm_df[pid_c] = pd.to_numeric(arm_df[pid_c], errors="coerce")
            arm_df[max_c] = pd.to_numeric(arm_df[max_c], errors="coerce")
            grp_max = arm_df.groupby(pid_c, dropna=True)[max_c].max()
            for pid, val in grp_max.items():
                if pd.isna(pid) or pd.isna(val):
                    continue
                pid_i = int(pid)
                row = out.setdefault(pid_i, _empty_metrics())
                row["def_arm_strength_max_mph"] = float(val)

    # --- Catcher pop time (to 2B) ---
    pop_key = f"{base}/statcast_catcher_poptime.parquet"
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
                row["def_pop_time_2b_sec"] = float(val)

    # --- Catcher framing (runs) ---
    frm_key = f"{base}/statcast_catcher_framing.parquet"
    frm_df = read_parquet_from_s3(bucket, frm_key, log_read=False, missing_key_log="none")
    if frm_df is not None and not frm_df.empty:
        pid_c = _col_ci(frm_df, "id", "player_id")
        rv_c = _col_ci(frm_df, "rv_tot", "framing_runs")
        if pid_c and rv_c:
            for pid, val in zip(frm_df[pid_c], pd.to_numeric(frm_df[rv_c], errors="coerce")):
                if pd.isna(pid) or pd.isna(val):
                    continue
                pid_i = int(pid)
                row = out.setdefault(pid_i, _empty_metrics())
                row["def_framing_runs"] = float(val)

    # --- FanGraphs DRS (sum across position lines) ---
    fg_key = f"{base}/fangraphs_fielding.parquet"
    fg_df = read_parquet_from_s3(bucket, fg_key, log_read=False, missing_key_log="none")
    if fg_df is not None and not fg_df.empty:
        idfg_c = _col_ci(fg_df, "IDfg", "idfg")
        drs_c = _col_ci(fg_df, "DRS")
        season_c = _col_ci(fg_df, "Season", "season")
        if idfg_c and drs_c:
            tmp = fg_df.copy()
            if season_c:
                sy = pd.to_numeric(tmp[season_c], errors="coerce")
                tmp = tmp[sy == year]
            id_map = fg_id_map if fg_id_map is not None else fangraphs_to_mlbam_map()
            if id_map:
                tmp[idfg_c] = pd.to_numeric(tmp[idfg_c], errors="coerce")
                drs_vals = pd.to_numeric(tmp[drs_c], errors="coerce")
                grp_sum = tmp.assign(_drs=drs_vals).groupby(idfg_c)["_drs"].sum(min_count=1)
                for idfg, drs in grp_sum.items():
                    if pd.isna(idfg):
                        continue
                    mlb = id_map.get(int(idfg))
                    if mlb is None:
                        continue
                    row = out.setdefault(mlb, _empty_metrics())
                    row["def_drs_total"] = float(drs) if pd.notna(drs) else float("nan")

    return out


def merge_defence_into_row(
    row: Dict[str, object],
    defence: Dict[int, Dict[str, float]],
) -> None:
    """Mutates ``row`` with defensive columns for this player-year (batters)."""
    pid = int(row["player_id"])
    m = defence.get(pid)
    if not m:
        for k in _DEFENCE_METRIC_KEYS:
            row[k] = float("nan")
        return
    for k in _DEFENCE_METRIC_KEYS:
        row[k] = m.get(k, float("nan"))
