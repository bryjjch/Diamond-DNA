"""Load archetype assignments, KNN neighbor tables, and cluster labels from S3 or a local directory."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..pipeline.runtime import current_utc_year
from ..pipeline.s3_interaction import (
    ARCHETYPE_CLUSTER_LABELS_FILENAME,
    get_s3_client,
    gold_archetype_assignments_key,
    gold_archetype_cluster_labels_key,
    gold_player_similar_neighbors_key,
    read_parquet_from_s3,
)
from ..pipeline.settings import PipelineSettings

logger = logging.getLogger(__name__)

_ROLES = ("batter", "pitcher", "catcher")

# Mapping: role -> {cluster_id -> display label}. Empty dict means "no LLM labels
# were generated for this role/year"; the lookup helper falls back to "Cluster N".
ClusterLabelLookup = Dict[str, Dict[int, str]]

_SOFT_REQUIRED = frozenset({"prob_primary", "prob_secondary", "cluster_id_secondary"})


def _has_soft(df: pd.DataFrame) -> bool:
    """True when the frame carries GMM soft-responsibility columns."""
    return _SOFT_REQUIRED.issubset(df.columns)


def _prob_cols(df: pd.DataFrame) -> List[str]:
    """Per-cluster probability columns (``prob_<int>``) sorted by cluster index."""
    cols = [
        c
        for c in df.columns
        if c.startswith("prob_") and c not in {"prob_primary", "prob_secondary"}
    ]
    return sorted(cols, key=lambda c: int(c.split("_", 1)[1]))


@dataclass(frozen=True)
class LakeTables:
    """In-memory archetype + neighbor frames + cluster labels for both roles (one season)."""

    year: int
    archetypes: pd.DataFrame
    neighbors: pd.DataFrame
    source: str
    notes: str
    labels: ClusterLabelLookup = field(default_factory=dict)


def cluster_label(labels: ClusterLabelLookup, role: str, cluster_id: int) -> str:
    """Look up a role/cluster label from the sidecar, falling back to ``Cluster <id>``."""
    r = role.strip().lower()
    table = labels.get(r) or {}
    return table.get(int(cluster_id), f"Cluster {int(cluster_id)}")


def _parse_cluster_labels_json(raw: bytes, role: str) -> Dict[int, str]:
    """Parse a ``cluster_labels.json`` payload into ``{cluster_id: display_label}``."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("Could not parse cluster_labels.json for role=%s: %s", role, exc)
        return {}
    entries = payload.get("labels") if isinstance(payload, dict) else None
    if not isinstance(entries, dict):
        logger.warning("cluster_labels.json for role=%s missing 'labels' object", role)
        return {}
    out: Dict[int, str] = {}
    for k, v in entries.items():
        try:
            cid = int(k)
        except (TypeError, ValueError):
            continue
        if isinstance(v, dict):
            name = str(v.get("name", "")).strip()
            if name:
                out[cid] = name
        elif isinstance(v, str) and v.strip():
            out[cid] = v.strip()
    return out


def _load_labels_from_local_dir(data_dir: Path) -> ClusterLabelLookup:
    """Pick up ``cluster_labels_<role>.json`` files if present. Missing files are silently OK."""
    labels: ClusterLabelLookup = {}
    for role in _ROLES:
        path = data_dir / f"cluster_labels_{role}.json"
        if not path.is_file():
            continue
        try:
            raw = path.read_bytes()
        except OSError as exc:
            logger.warning("Could not read %s: %s", path, exc)
            continue
        parsed = _parse_cluster_labels_json(raw, role)
        if parsed:
            labels[role] = parsed
    return labels


def _load_labels_from_s3(bucket: str, gold_prefix: str, year: int) -> ClusterLabelLookup:
    """Best-effort fetch of ``cluster_labels.json`` per role; missing keys yield empty dicts."""
    client = get_s3_client()
    labels: ClusterLabelLookup = {}
    for role in _ROLES:
        key = gold_archetype_cluster_labels_key(gold_prefix, role, year)
        try:
            obj = client.get_object(Bucket=bucket, Key=key)
            raw = obj["Body"].read()
        except client.exceptions.NoSuchKey:
            logger.info("No %s for role=%s year=%d at s3://%s/%s", ARCHETYPE_CLUSTER_LABELS_FILENAME, role, year, bucket, key)
            continue
        except Exception as exc:
            logger.warning("Failed reading s3://%s/%s: %s", bucket, key, exc)
            continue
        parsed = _parse_cluster_labels_json(raw, role)
        if parsed:
            labels[role] = parsed
    return labels


def _load_from_local_dir(data_dir: Path, year: int) -> Tuple[Optional[LakeTables], str]:
    """
    Expect files (produced by copying from the lake):

    - archetypes_batter.parquet / archetypes_pitcher.parquet / archetypes_catcher.parquet
    - neighbors_batter.parquet / neighbors_pitcher.parquet / neighbors_catcher.parquet
    - cluster_labels_batter.json / cluster_labels_pitcher.json / cluster_labels_catcher.json (optional)

    Catcher files are optional so the loader still works against snapshots produced before
    the catcher cohort split. Batter and pitcher are required.
    """
    required = ("batter", "pitcher")
    missing_required = [
        p.name
        for r in required
        for p in (data_dir / f"archetypes_{r}.parquet", data_dir / f"neighbors_{r}.parquet")
        if not p.is_file()
    ]
    if missing_required:
        return None, (
            f"WEBAPP_DATA_DIR={data_dir} is missing required file(s): "
            + ", ".join(missing_required)
        )

    a_frames: List[pd.DataFrame] = []
    n_frames: List[pd.DataFrame] = []
    for role in _ROLES:
        apath = data_dir / f"archetypes_{role}.parquet"
        npath = data_dir / f"neighbors_{role}.parquet"
        if not (apath.is_file() and npath.is_file()):
            continue
        adf = pd.read_parquet(apath)
        ndf = pd.read_parquet(npath)
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
    labels = _load_labels_from_local_dir(data_dir)
    return (
        LakeTables(
            year=year,
            archetypes=archetypes,
            neighbors=neighbors,
            source=f"local:{data_dir}",
            notes="Loaded from WEBAPP_DATA_DIR parquet bundle.",
            labels=labels,
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
    labels = _load_labels_from_s3(bucket, gold_prefix, year)
    return (
        LakeTables(
            year=year,
            archetypes=archetypes,
            neighbors=neighbors,
            source=f"s3://{bucket}/{gold_prefix}/…/year={year}/",
            notes="Loaded from S3 gold partitions (batter + pitcher).",
            labels=labels,
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


def clusters_payload(df: pd.DataFrame, labels: ClusterLabelLookup) -> List[Dict[str, Any]]:
    """Group players by role + cluster_id with display labels from the sidecar lookup."""
    need = {"player_id", "player_name", "cluster_id", "role"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"archetypes frame missing columns: {sorted(missing)}")

    soft = _has_soft(df)

    out: List[Dict[str, Any]] = []
    for (role, cid), g in df.sort_values(["role", "cluster_id", "player_name"]).groupby(
        ["role", "cluster_id"], sort=True
    ):
        r = str(role)
        cid_i = int(cid)
        players: List[Dict[str, Any]] = []
        for _, rw in g.iterrows():
            entry: Dict[str, Any] = {
                "player_id": int(rw["player_id"]),
                "player_name": str(rw["player_name"]),
            }
            if soft:
                sec_cid = int(rw["cluster_id_secondary"])
                entry["prob_primary"] = float(rw["prob_primary"])
                entry["cluster_id_secondary"] = sec_cid
                entry["prob_secondary"] = float(rw["prob_secondary"])
                entry["secondary_label"] = cluster_label(labels, r, sec_cid)
            players.append(entry)
        out.append(
            {
                "role": r,
                "cluster_id": cid_i,
                "label": cluster_label(labels, r, cid_i),
                "players": players,
            }
        )
    return out


def search_players(
    df: pd.DataFrame,
    q: str,
    labels: ClusterLabelLookup,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    qn = q.strip().lower()
    if not qn:
        return []
    need = {"player_id", "player_name", "cluster_id", "role"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"archetypes frame missing columns: {sorted(missing)}")

    mask = df["player_name"].str.lower().str.contains(qn, na=False)
    sub = df.loc[mask].sort_values("player_name").head(limit)
    soft = _has_soft(df)
    prob_cols = _prob_cols(df) if soft else []
    rows: List[Dict[str, Any]] = []
    for _, rw in sub.iterrows():
        role = str(rw["role"])
        cid = int(rw["cluster_id"])
        row: Dict[str, Any] = {
            "player_id": int(rw["player_id"]),
            "player_name": str(rw["player_name"]),
            "role": role,
            "year": int(rw["year"])
            if "year" in df.columns and pd.notna(rw.get("year"))
            else None,
            "cluster_id": cid,
            "cluster_label": cluster_label(labels, role, cid),
        }
        if soft:
            sec_cid = int(rw["cluster_id_secondary"])
            row["prob_primary"] = float(rw["prob_primary"])
            row["cluster_id_secondary"] = sec_cid
            row["prob_secondary"] = float(rw["prob_secondary"])
            row["secondary_cluster_label"] = cluster_label(labels, role, sec_cid)
            row["probs"] = [float(rw[c]) for c in prob_cols]
        rows.append(row)
    return rows


def player_soft_profile(
    df: pd.DataFrame,
    labels: ClusterLabelLookup,
    *,
    player_id: int,
    role: str,
) -> Optional[Dict[str, Any]]:
    """Return one player's full sorted-by-prob cluster membership profile, or ``None`` if missing.

    Returns ``None`` when the player+role row is absent or when the frame lacks soft columns.
    """
    if not _has_soft(df):
        return None
    role_l = role.strip().lower()
    sub = df.loc[
        (df["player_id"] == player_id) & (df["role"].str.lower() == role_l)
    ]
    if sub.empty:
        return None
    rw = sub.iloc[0]
    prob_cols = _prob_cols(df)
    sorted_probs = sorted(
        (
            {
                "cluster_id": int(c.split("_", 1)[1]),
                "label": cluster_label(labels, role_l, int(c.split("_", 1)[1])),
                "prob": float(rw[c]),
            }
            for c in prob_cols
        ),
        key=lambda d: d["prob"],
        reverse=True,
    )
    cid = int(rw["cluster_id"])
    return {
        "player_id": int(rw["player_id"]),
        "player_name": str(rw["player_name"]),
        "role": role_l,
        "cluster_id": cid,
        "cluster_label": cluster_label(labels, role_l, cid),
        "probs": sorted_probs,
    }


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
