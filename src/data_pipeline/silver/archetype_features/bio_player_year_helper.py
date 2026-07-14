"""
Load bronze player-bio Parquet from S3 and map MLBAM player_id -> bio columns.

Used to enrich both batter and pitcher player-year feature rows. Bio columns are
prefixed ``bio_`` so gold preprocessing can exclude them from archetype training
features; they ride through silver for projection models.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Dict, Tuple

import pandas as pd

from ....common.s3_helpers import read_parquet_from_s3

logger = logging.getLogger(__name__)

_BIO_NUMERIC_KEYS: Tuple[str, ...] = ("bio_age", "bio_height_in", "bio_weight_lb")
_BIO_TEXT_KEYS: Tuple[str, ...] = ("bio_birth_date", "bio_birth_place")
BIO_KEYS: Tuple[str, ...] = _BIO_NUMERIC_KEYS + _BIO_TEXT_KEYS


def _empty_bio() -> Dict[str, object]:
    """Creates a new dict with all bio columns set to nan/empty."""
    row: Dict[str, object] = {k: float("nan") for k in _BIO_NUMERIC_KEYS}
    for k in _BIO_TEXT_KEYS:
        row[k] = ""
    return row


def season_age(birth_date_str: object, year: int) -> float:
    """Baseball 'seasonal age': the player's age on June 30 of ``year``."""
    try:
        bd = datetime.strptime(str(birth_date_str).strip(), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return float("nan")
    ref = date(year, 6, 30)
    return float(ref.year - bd.year - ((ref.month, ref.day) < (bd.month, bd.day)))


def _birth_place(row: pd.Series) -> str:
    """Transform birth_city, birth_state_province, birth_country into a single string."""
    parts = []
    for col in ("birth_city", "birth_state_province", "birth_country"):
        v = row.get(col)
        if v is not None and pd.notna(v) and str(v).strip():
            parts.append(str(v).strip())
    return ", ".join(parts)


def load_player_bios_by_year(
    bucket: str,
    raw_bio_prefix: str,
    year: int,
) -> Dict[int, Dict[str, object]]:
    """
    Returns mapping MLBAM player_id -> bio columns (nan/empty if unknown).
    """
    out: Dict[int, Dict[str, object]] = {}

    key = f"{raw_bio_prefix.strip('/')}/year={year}/mlb_player_bios.parquet"
    df = read_parquet_from_s3(bucket, key, log_read=False, missing_key_log="none")
    if df is None or df.empty or "player_id" not in df.columns:
        return out

    pids = pd.to_numeric(df["player_id"], errors="coerce")
    heights = pd.to_numeric(df["height_in"], errors="coerce") if "height_in" in df.columns else None
    weights = pd.to_numeric(df["weight_lb"], errors="coerce") if "weight_lb" in df.columns else None
    for i, pid in enumerate(pids):
        if pd.isna(pid):
            continue
        r = df.iloc[i]
        row = out.setdefault(int(pid), _empty_bio())
        birth_date = r.get("birth_date")
        birth_date = str(birth_date).strip() if pd.notna(birth_date) else ""
        row["bio_age"] = season_age(birth_date, year)
        row["bio_birth_date"] = birth_date
        row["bio_birth_place"] = _birth_place(r)
        if heights is not None and pd.notna(heights.iloc[i]):
            row["bio_height_in"] = float(heights.iloc[i])
        if weights is not None and pd.notna(weights.iloc[i]):
            row["bio_weight_lb"] = float(weights.iloc[i])

    return out


def merge_bio_into_row(
    row: Dict[str, object],
    bios: Dict[int, Dict[str, object]],
) -> None:
    """Mutates ``row`` with bio columns for this player-year (both roles)."""
    pid = int(row["player_id"])
    m = bios.get(pid) or _empty_bio()
    for k in BIO_KEYS:
        row[k] = m.get(k, float("nan") if k in _BIO_NUMERIC_KEYS else "")
