"""Load archetype assignments and KNN neighbor tables from S3 or a local directory."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..ml.archetype_clustering import archetype_cluster_label
from ..pipeline.runtime import current_utc_year
from ..pipeline.s3_interaction import (
    gold_archetype_assignments_key,
    gold_player_similar_neighbors_key,
    read_parquet_from_s3,
)
from ..pipeline.settings import PipelineSettings

_ROLES = ("batter", "pitcher")


@dataclass(frozen=True)
class LakeTables:
    """In-memory archetype + neighbor frames for both roles (one season)."""

    year: int
    archetypes: pd.DataFrame
    neighbors: pd.DataFrame
    source: str
    notes: str


def _load_from_local_dir(data_dir: Path, year: int) -> Tuple[Optional[LakeTables], str]:
    """
    Expect files (produced by copying from the lake):

    - archetypes_batter.parquet / archetypes_pitcher.parquet
    - neighbors_batter.parquet / neighbors_pitcher.parquet
    """
    paths_a = [data_dir / f"archetypes_{r}.parquet" for r in _ROLES]
    paths_n = [data_dir / f"neighbors_{r}.parquet" for r in _ROLES]
    if not all(p.is_file() for p in paths_a + paths_n):
        return None, (
            f"WEBAPP_DATA_DIR={data_dir} must contain archetypes_batter.parquet, "
            "archetypes_pitcher.parquet, neighbors_batter.parquet, neighbors_pitcher.parquet"
        )

    a_frames: List[pd.DataFrame] = []
    n_frames: List[pd.DataFrame] = []
    for role in _ROLES:
        adf = pd.read_parquet(data_dir / f"archetypes_{role}.parquet")
        ndf = pd.read_parquet(data_dir / f"neighbors_{role}.parquet")
        if "role" not in adf.columns:
            adf = adf.copy()
            adf["role"] = role
        if "role" not in ndf.columns:
            ndf = ndf.copy()
            ndf["role"] = role
        a_frames.append(adf)
        n_frames.append(ndf)

    archetypes = pd.concat(a_frames, ignore_index=True)
    neighbors = pd.concat(n_frames, ignore_index=True)
    return (
        LakeTables(
            year=year,
            archetypes=archetypes,
            neighbors=neighbors,
            source=f"local:{data_dir}",
            notes="Loaded from WEBAPP_DATA_DIR parquet bundle.",
        ),
        "",
    )


def _load_from_s3(
    bucket: str,
    gold_prefix: str,
    year: int,
) -> Tuple[Optional[LakeTables], str]:
    a_frames: List[pd.DataFrame] = []
    n_frames: List[pd.DataFrame] = []
    errors: List[str] = []

    for role in _ROLES:
        ak = gold_archetype_assignments_key(gold_prefix, role, year)
        nk = gold_player_similar_neighbors_key(gold_prefix, role, year)
        adf = read_parquet_from_s3(bucket, ak, missing_key_log="none")
        ndf = read_parquet_from_s3(bucket, nk, missing_key_log="none")
        if adf is None or adf.empty:
            errors.append(f"missing or empty archetypes s3://{bucket}/{ak}")
            continue
        if ndf is None or ndf.empty:
            errors.append(f"missing or empty neighbors s3://{bucket}/{nk}")
            continue
        if "role" not in adf.columns:
            adf = adf.copy()
            adf["role"] = role
        if "role" not in ndf.columns:
            ndf = ndf.copy()
            ndf["role"] = role
        a_frames.append(adf)
        n_frames.append(ndf)

    if not a_frames or len(a_frames) != len(n_frames):
        return None, "; ".join(errors) if errors else "no data for this year/roles"

    archetypes = pd.concat(a_frames, ignore_index=True)
    neighbors = pd.concat(n_frames, ignore_index=True)
    return (
        LakeTables(
            year=year,
            archetypes=archetypes,
            neighbors=neighbors,
            source=f"s3://{bucket}/{gold_prefix}/…/year={year}/",
            notes="Loaded from S3 gold partitions (batter + pitcher).",
        ),
        "",
    )


def load_lake_tables(
    *,
    year: Optional[int] = None,
    settings: Optional[PipelineSettings] = None,
) -> Tuple[Optional[LakeTables], str]:
    """
    Prefer ``WEBAPP_DATA_DIR`` when set; otherwise read from S3 using pipeline settings.

    Year defaults to ``WEBAPP_YEAR`` or UTC current year minus one.
    """
    settings = settings or PipelineSettings.from_environ()
    if year is None:
        year = int(os.environ.get("WEBAPP_YEAR", str(current_utc_year() - 1)))

    local = os.environ.get("WEBAPP_DATA_DIR", "").strip()
    if local:
        data_dir = Path(local).expanduser().resolve()
        return _load_from_local_dir(data_dir, year)

    return _load_from_s3(settings.s3_bucket, settings.gold_prefix, year)


def clusters_payload(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Group players by role + cluster_id with display labels."""
    need = {"player_id", "player_name", "cluster_id", "role"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"archetypes frame missing columns: {sorted(missing)}")

    out: List[Dict[str, Any]] = []
    for (role, cid), g in df.sort_values(["role", "cluster_id", "player_name"]).groupby(
        ["role", "cluster_id"], sort=True
    ):
        r = str(role)
        cid_i = int(cid)
        players = [
            {"player_id": int(rw["player_id"]), "player_name": str(rw["player_name"])}
            for _, rw in g.iterrows()
        ]
        out.append(
            {
                "role": r,
                "cluster_id": cid_i,
                "label": archetype_cluster_label(r, cid_i),
                "players": players,
            }
        )
    return out


def search_players(df: pd.DataFrame, q: str, limit: int = 50) -> List[Dict[str, Any]]:
    qn = q.strip().lower()
    if not qn:
        return []
    need = {"player_id", "player_name", "cluster_id", "role"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"archetypes frame missing columns: {sorted(missing)}")

    mask = df["player_name"].str.lower().str.contains(qn, na=False)
    sub = df.loc[mask].sort_values("player_name").head(limit)
    rows: List[Dict[str, Any]] = []
    for _, rw in sub.iterrows():
        role = str(rw["role"])
        cid = int(rw["cluster_id"])
        rows.append(
            {
                "player_id": int(rw["player_id"]),
                "player_name": str(rw["player_name"]),
                "role": role,
                "year": int(rw["year"])
                if "year" in df.columns and pd.notna(rw.get("year"))
                else None,
                "cluster_id": cid,
                "cluster_label": archetype_cluster_label(role, cid),
            }
        )
    return rows


def neighbors_for_player(
    neighbors: pd.DataFrame,
    *,
    player_id: int,
    role: str,
) -> List[Dict[str, Any]]:
    role_l = role.strip().lower()
    sub = neighbors[
        (neighbors["player_id"] == player_id) & (neighbors["role"].str.lower() == role_l)
    ].sort_values("neighbor_rank")
    cols = {"neighbor_rank", "neighbor_player_id", "neighbor_player_name", "distance"}
    if not cols.issubset(sub.columns):
        raise ValueError("neighbors frame missing expected columns")

    out: List[Dict[str, Any]] = []
    for _, rw in sub.iterrows():
        out.append(
            {
                "rank": int(rw["neighbor_rank"]),
                "player_id": int(rw["neighbor_player_id"]),
                "player_name": str(rw["neighbor_player_name"]),
                "distance": float(rw["distance"]),
            }
        )
    return out


def player_leaderboard(archetypes: pd.DataFrame, role: str, limit: int = 500) -> List[Dict[str, Any]]:
    """Distinct players in a role for the loaded season, sorted by name."""
    role_l = role.strip().lower()
    sub = archetypes[archetypes["role"].str.lower() == role_l]
    if sub.empty:
        return []
    use = sub[["player_id", "player_name"]].drop_duplicates(subset=["player_id"])
    use = use.sort_values("player_name").head(limit)
    return [
        {"player_id": int(r["player_id"]), "player_name": str(r["player_name"])}
        for _, r in use.iterrows()
    ]
