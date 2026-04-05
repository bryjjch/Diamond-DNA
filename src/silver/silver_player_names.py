"""
Resolve MLBAM player display names for silver player-year rows.

Both batters and pitchers are labeled from the Chadwick register (``name_last``, ``name_first``)
as ``Last, First``.
"""

from __future__ import annotations

import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)


def build_mlbam_statcast_style_name_map(cw: pd.DataFrame) -> Dict[int, str]:
    """Map MLBAM id -> ``Last, First`` string from a Chadwick register dataframe."""
    if cw.empty:
        return {}
    need = ("key_mlbam", "name_last", "name_first")
    if not all(c in cw.columns for c in need):
        logger.warning("Chadwick register missing columns %s; name map empty.", need)
        return {}
    # key_mlbam is the MLBAM id, name_last is the last name, and name_first is the first name.
    mlb = pd.to_numeric(cw["key_mlbam"], errors="coerce")
    last = cw["name_last"].fillna("").astype(str).str.strip()
    first = cw["name_first"].fillna("").astype(str).str.strip()
    out: Dict[int, str] = {}
    for mid, ln, fn in zip(mlb, last, first):
        if pd.isna(mid):
            continue
        key = int(mid)
        if not ln and not fn:
            continue
        out[key] = f"{ln}, {fn}" if ln and fn else (ln or fn)
    return out


def resolve_mlbam_display_name(player_id: int, mlbam_to_name: Dict[int, str]) -> str:
    """Return Chadwick-derived display name for ``player_id``, or empty string if unknown."""
    return mlbam_to_name.get(int(player_id), "")
