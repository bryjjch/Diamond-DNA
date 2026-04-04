"""
Silver-layer helpers for sprint-speed side inputs (S3 parquet reads, lookup builders).
"""

from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

from ..pipeline.s3_interaction import raw_sprint_speed_key, read_parquet_from_s3

logger = logging.getLogger(__name__)


def build_sprint_speed_lookups_by_year(
    bucket: str,
    raw_running_prefix: str,
    start_year: int,
    end_year: int,
    sprint_speed_min_opp: int,
) -> Dict[int, Dict[int, float]]:
    """
    Load bronze sprint-speed leaderboards per season; return ``year -> {mlbam_id: sprint_speed}``.
    """
    sprint_lookup_by_year: Dict[int, Dict[int, float]] = {}
    for y in range(start_year, end_year + 1):
        key = raw_sprint_speed_key(raw_running_prefix, y)
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
            logger.warning(
                "Sprint speed parquet missing expected id/sprint_speed columns: s3://%s/%s",
                bucket,
                key,
            )
            continue

        tmp = running_df.copy()
        tmp[id_col] = pd.to_numeric(tmp[id_col], errors="coerce")
        tmp[ss_col] = pd.to_numeric(tmp[ss_col], errors="coerce")
        tmp = tmp[tmp[id_col].notna() & tmp[ss_col].notna()]
        if opp_col and opp_col in tmp.columns:
            tmp[opp_col] = pd.to_numeric(tmp[opp_col], errors="coerce")
            tmp = tmp[(tmp[opp_col].isna()) | (tmp[opp_col] >= sprint_speed_min_opp)]

        sprint_lookup_by_year[y] = {int(pid): float(ss) for pid, ss in zip(tmp[id_col], tmp[ss_col])}
    return sprint_lookup_by_year
