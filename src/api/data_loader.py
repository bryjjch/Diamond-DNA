"""Lazy lake-backed store + payload builders for the xWAR Engine API.

All lake keys come from ``common.lake_keys`` so the API cannot drift from the
producers. Two byte sources sit behind one interface: when ``WEBAPP_DATA_DIR``
is set it must mirror the lake layout (the same relative keys on disk),
populated with e.g.::

    aws s3 sync s3://<bucket>/gold/predictions <dir>/gold/predictions
    aws s3 sync s3://<bucket>/gold/features/performance_prediction <dir>/gold/features/performance_prediction
    aws s3 sync s3://<bucket>/models <dir>/models --exclude "*" --include "*/metrics.json"

Otherwise objects are read from S3 using ``PipelineSettings``. Everything is
lazy and cached per artifact for the lifetime of the Lambda container; misses
are cached as ``None`` (a container started before a year's outputs were
written serves 404 until recycled).
"""

from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ..common.lake_keys import PERFORMANCE_PREDICTION, gold_feature_key, model_key, prediction_key
from ..common.runtime_helpers import current_utc_year
from ..common.s3_helpers import get_s3_client, read_parquet_from_s3
from ..common.settings import PipelineSettings
from ..data_pipeline.gold.gold_performance_predict_preprocessing import (
    ROLE_LAG_STAT_COLUMNS,
    ROLE_SKILL_COLUMNS,
)
from ..ml.baselines.marcel_baseline import (
    ROLE_ALL_TARGETS,
    ROLE_LOWER_IS_BETTER,
    VALID_ROLES,
)
from ..ml.evaluation.compare_projections import build_comparison_rows, summarize_comparison

logger = logging.getLogger(__name__)

ARCHETYPE_ROLES = ("batter", "pitcher", "catcher")
PROJECTION_MODELS = ("xgboost", "marcel")
BASELINE_MODEL = "marcel"
CANDIDATE_MODEL = "xgboost"
N_LAG_SEASONS = 3

# Mapping: role -> {cluster_id -> {"name": str, "description": str}}.
# Empty dict means "no LLM labels were generated for this role/year"; helpers fall back gracefully.
ClusterLabelLookup = Dict[str, Dict[int, Dict[str, str]]]

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


def _clean(v: Any) -> Any:
    """JSON-safe scalar: NaN/NA -> None, numpy scalars -> Python scalars."""
    if v is None:
        return None
    if isinstance(v, np.generic):
        v = v.item()
    if isinstance(v, float) and (v != v):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


# ---------------------------------------------------------------------------
# Cluster label helpers (unchanged contract)
# ---------------------------------------------------------------------------


def cluster_label(labels: ClusterLabelLookup, role: str, cluster_id: int) -> str:
    """Look up a role/cluster label from the sidecar, falling back to ``Cluster <id>``."""
    r = role.strip().lower()
    entry = (labels.get(r) or {}).get(int(cluster_id))
    if isinstance(entry, dict):
        return entry.get("name") or f"Cluster {int(cluster_id)}"
    return f"Cluster {int(cluster_id)}"


def cluster_description(labels: ClusterLabelLookup, role: str, cluster_id: int) -> str:
    """Look up a role/cluster description from the sidecar, returning empty string on miss."""
    r = role.strip().lower()
    entry = (labels.get(r) or {}).get(int(cluster_id))
    if isinstance(entry, dict):
        return entry.get("description") or ""
    return ""


def _parse_cluster_labels_json(raw: bytes, role: str) -> Dict[int, Dict[str, str]]:
    """Parse a ``cluster_labels.json`` payload into ``{cluster_id: {name, description}}``."""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("Could not parse cluster_labels.json for role=%s: %s", role, exc)
        return {}
    entries = payload.get("labels") if isinstance(payload, dict) else None
    if not isinstance(entries, dict):
        logger.warning("cluster_labels.json for role=%s missing 'labels' object", role)
        return {}
    out: Dict[int, Dict[str, str]] = {}
    for k, v in entries.items():
        try:
            cid = int(k)
        except (TypeError, ValueError):
            continue
        if isinstance(v, dict):
            name = str(v.get("name", "")).strip()
            if name:
                out[cid] = {
                    "name": name,
                    "description": str(v.get("description", "")).strip(),
                }
        elif isinstance(v, str) and v.strip():
            out[cid] = {"name": v.strip(), "description": ""}
    return out


# ---------------------------------------------------------------------------
# LakeStore
# ---------------------------------------------------------------------------


def _training_matrix_columns(role: str) -> List[str]:
    """Columns the player endpoint needs from the wide training matrix."""
    cols: List[str] = ["player_id", "year", "role", "player_name", "primary_position", "age"]
    for k in range(1, N_LAG_SEASONS + 1):
        cols.append(f"lag{k}_available")
        cols.extend(f"lag{k}_{stat}" for stat in ROLE_LAG_STAT_COLUMNS[role])
    cols.append("skill_lag1_available")
    cols.extend(f"skill_lag1_{c}" for c in ROLE_SKILL_COLUMNS[role])
    return cols


class LakeStore:
    """Lazily loads and caches lake artifacts from S3 or a local lake mirror."""

    def __init__(
        self,
        settings: Optional[PipelineSettings] = None,
        local_dir: Optional[str] = None,
    ) -> None:
        self.settings = settings or PipelineSettings.from_environ()
        local = (
            local_dir
            if local_dir is not None
            else os.environ.get("WEBAPP_DATA_DIR", "").strip()
        )
        self.local_dir: Optional[Path] = (
            Path(local).expanduser().resolve() if local else None
        )
        self._cache: Dict[Tuple, Any] = {}

    @property
    def source(self) -> str:
        if self.local_dir is not None:
            return f"local:{self.local_dir}"
        return f"s3://{self.settings.s3_bucket}"

    def default_year(self) -> int:
        return int(os.environ.get("WEBAPP_YEAR", str(current_utc_year() - 1)))

    def cached_artifacts(self) -> List[str]:
        return sorted("/".join(str(p) for p in k) for k in self._cache)

    # -- byte fetchers ------------------------------------------------------

    def _fetch_parquet(
        self, rel_key: str, columns: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        if self.local_dir is not None:
            path = self.local_dir / rel_key
            if not path.is_file():
                return None
            df = pd.read_parquet(path)
            if columns is not None:
                df = df[[c for c in columns if c in df.columns]]
            return df
        return read_parquet_from_s3(
            self.settings.s3_bucket, rel_key, columns=columns, missing_key_log="none"
        )

    def _fetch_json(self, rel_key: str) -> Optional[bytes]:
        if self.local_dir is not None:
            path = self.local_dir / rel_key
            if not path.is_file():
                return None
            try:
                return path.read_bytes()
            except OSError as exc:
                logger.warning("Could not read %s: %s", path, exc)
                return None
        client = get_s3_client()
        try:
            obj = client.get_object(Bucket=self.settings.s3_bucket, Key=rel_key)
            return obj["Body"].read()
        except client.exceptions.NoSuchKey:
            return None

    def _cached(self, key: Tuple, loader) -> Any:
        if key in self._cache:
            return self._cache[key]
        value = loader()
        self._cache[key] = value
        return value

    # -- artifacts ----------------------------------------------------------

    def projections(self, model: str, role: str, year: int) -> Optional[pd.DataFrame]:
        rel_key = prediction_key(
            self.settings.predictions_prefix, model, role, year, f"{model}_projections.parquet"
        )
        return self._cached(
            ("projections", model, role, year), lambda: self._fetch_parquet(rel_key)
        )

    def archetypes(self, role: str, year: int) -> Optional[pd.DataFrame]:
        rel_key = prediction_key(
            self.settings.predictions_prefix,
            "archetypes",
            role,
            year,
            "player_year_archetypes.parquet",
        )
        return self._cached(
            ("archetypes", role, year), lambda: self._fetch_parquet(rel_key)
        )

    def neighbors(self, role: str, year: int) -> Optional[pd.DataFrame]:
        rel_key = prediction_key(
            self.settings.predictions_prefix,
            "neighbors",
            role,
            year,
            "player_year_similar_neighbors.parquet",
        )
        return self._cached(
            ("neighbors", role, year), lambda: self._fetch_parquet(rel_key)
        )

    def role_labels(self, role: str, year: int) -> Dict[int, Dict[str, str]]:
        rel_key = prediction_key(
            self.settings.predictions_prefix, "archetypes", role, year, "cluster_labels.json"
        )

        def load() -> Dict[int, Dict[str, str]]:
            raw = self._fetch_json(rel_key)
            return _parse_cluster_labels_json(raw, role) if raw else {}

        return self._cached(("labels", role, year), load)

    def labels(self, year: int) -> ClusterLabelLookup:
        return {role: self.role_labels(role, year) for role in ARCHETYPE_ROLES}

    def training_frame(self, role: str, year: int) -> Optional[pd.DataFrame]:
        rel_key = gold_feature_key(
            self.settings.gold_prefix, PERFORMANCE_PREDICTION, role, year, "training_matrix.parquet"
        )

        def load() -> Optional[pd.DataFrame]:
            df = self._fetch_parquet(rel_key, columns=_training_matrix_columns(role))
            if df is None or df.empty or "player_id" not in df.columns:
                return None
            return df.set_index("player_id", drop=False)

        return self._cached(("training", role, year), load)

    def training_row(self, player_id: int, role: str, year: int) -> Optional[pd.Series]:
        df = self.training_frame(role, year)
        if df is None or player_id not in df.index:
            return None
        sub = df.loc[player_id]
        return sub.iloc[0] if isinstance(sub, pd.DataFrame) else sub

    def metrics(self, model: str, role: str, year: int) -> Optional[Dict[str, Any]]:
        rel_key = model_key(self.settings.models_prefix, model, role, year, "metrics.json")

        def load() -> Optional[Dict[str, Any]]:
            raw = self._fetch_json(rel_key)
            if raw is None:
                return None
            try:
                return json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.warning("Bad metrics.json at %s: %s", rel_key, exc)
                return None

        return self._cached(("metrics", model, role, year), load)

    def name_index(self, year: int) -> pd.DataFrame:
        """Per-year search index: one row per (player_id, role) with cluster info when known."""

        def load() -> pd.DataFrame:
            frames: List[pd.DataFrame] = []
            for role in ARCHETYPE_ROLES:
                adf = self.archetypes(role, year)
                if adf is None or adf.empty:
                    continue
                keep = [
                    c
                    for c in (
                        "player_id",
                        "player_name",
                        "role",
                        "year",
                        "cluster_id",
                        "cluster_id_secondary",
                        "prob_primary",
                        "prob_secondary",
                    )
                    if c in adf.columns
                ]
                sub = adf[keep].copy()
                if "role" not in sub.columns:
                    sub["role"] = role
                frames.append(sub)
            for role in VALID_ROLES:
                pdf = self.projections(CANDIDATE_MODEL, role, year)
                if pdf is None or pdf.empty:
                    continue
                sub = pdf[[c for c in ("player_id", "player_name", "year") if c in pdf.columns]].copy()
                sub["role"] = role
                frames.append(sub)
            if not frames:
                return pd.DataFrame(columns=["player_id", "player_name", "role"])
            index = pd.concat(frames, ignore_index=True)
            return index.drop_duplicates(subset=["player_id", "role"], keep="first")

        return self._cached(("name_index", year), load)


_store: Optional[LakeStore] = None


def get_store() -> LakeStore:
    """Module-level store, built once per container."""
    global _store
    if _store is None:
        _store = LakeStore()
    return _store


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------


def _stat_block(row: Any, stats: Tuple[str, ...], prefix: str) -> Optional[Dict[str, Any]]:
    """``{stat: value}`` from ``{prefix}{stat}`` columns; None when every value is missing."""
    block = {stat: _clean(row.get(f"{prefix}{stat}")) for stat in stats}
    if all(v is None for v in block.values()):
        return None
    return block


def projection_rows(
    df: pd.DataFrame,
    *,
    role: str,
    sort: str,
    order: str,
    limit: int,
) -> List[Dict[str, Any]]:
    """Leaderboard rows from a projections frame, sorted and truncated."""
    stats = ROLE_ALL_TARGETS[role]
    sort_col = f"proj_{sort}" if sort in stats else sort
    if sort_col in df.columns:
        df = df.sort_values(sort_col, ascending=(order == "asc"), na_position="last")
    out: List[Dict[str, Any]] = []
    for _, rw in df.head(limit).iterrows():
        out.append(
            {
                "player_id": int(rw["player_id"]),
                "player_name": _clean(rw.get("player_name")),
                "age": _clean(rw.get("age")),
                "primary_position": _clean(rw.get("primary_position")),
                "projections": _stat_block(rw, stats, "proj_"),
                "actuals": _stat_block(rw, stats, "target_"),
            }
        )
    return out


def default_sort(role: str) -> str:
    return "ops" if role == "batter" else "era"


def default_order(role: str, sort: str) -> str:
    if sort == "player_name" or sort in ROLE_LOWER_IS_BETTER[role]:
        return "asc"
    return "desc"


def player_history_payload(row: pd.Series, role: str, year: int) -> Dict[str, Any]:
    """Last-3-season actuals + season N-1 Statcast skill features from a training-matrix row."""
    seasons: List[Dict[str, Any]] = []
    for k in range(1, N_LAG_SEASONS + 1):
        seasons.append(
            {
                "season": year - k,
                "lag": k,
                "available": bool(_clean(row.get(f"lag{k}_available")) or 0),
                "stats": {
                    stat: _clean(row.get(f"lag{k}_{stat}"))
                    for stat in ROLE_LAG_STAT_COLUMNS[role]
                },
            }
        )
    statcast: Dict[str, Any] = {
        "available": bool(_clean(row.get("skill_lag1_available")) or 0)
    }
    for col in ROLE_SKILL_COLUMNS[role]:
        statcast[col] = _clean(row.get(f"skill_lag1_{col}"))
    return {"seasons": seasons, "statcast_last_season": statcast}


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


def _archetype_block(
    store: LakeStore, *, player_id: int, role: str, year: int
) -> Tuple[Optional[Dict[str, Any]], Optional[List[Dict[str, Any]]]]:
    """Soft archetype profile + neighbors, trying the catcher table for batters.

    Catchers cluster in their own cohort but project as batters, so a batter
    profile falls back to the catcher archetype/neighbor tables and reports
    which one matched via ``archetype_role``.
    """
    arch_roles = ("batter", "catcher") if role == "batter" else (role,)
    for arch_role in arch_roles:
        adf = store.archetypes(arch_role, year)
        ndf = store.neighbors(arch_role, year)
        archetype: Optional[Dict[str, Any]] = None
        neighbor_rows: Optional[List[Dict[str, Any]]] = None
        if adf is not None and not adf.empty:
            lookup = {arch_role: store.role_labels(arch_role, year)}
            profile = player_soft_profile(
                adf, lookup, player_id=player_id, role=arch_role
            )
            if profile is not None:
                cid = profile["cluster_id"]
                archetype = {
                    "archetype_role": arch_role,
                    "cluster_id": cid,
                    "label": profile["cluster_label"],
                    "description": cluster_description(lookup, arch_role, cid),
                    "probs": profile["probs"],
                }
        if ndf is not None and not ndf.empty:
            rows = neighbors_for_player(ndf, player_id=player_id, role=arch_role)
            if rows:
                neighbor_rows = rows
        if archetype is not None or neighbor_rows is not None:
            return archetype, neighbor_rows
    return None, None


def player_detail_payload(
    store: LakeStore,
    *,
    player_id: int,
    year: int,
    model: str,
    role: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Full /api/player payload, one profile block per role found; None when unknown."""
    roles = (role,) if role else VALID_ROLES
    profiles: List[Dict[str, Any]] = []
    player_name: Optional[str] = None

    for r in roles:
        projection: Optional[Dict[str, Any]] = None
        age: Optional[Any] = None
        primary_position: Optional[Any] = None

        pdf = store.projections(model, r, year)
        if pdf is not None and not pdf.empty:
            match = pdf.loc[pdf["player_id"] == player_id]
            if not match.empty:
                rw = match.iloc[0]
                projection = {
                    "stats": _stat_block(rw, ROLE_ALL_TARGETS[r], "proj_"),
                    "actuals": _stat_block(rw, ROLE_ALL_TARGETS[r], "target_"),
                }
                age = _clean(rw.get("age"))
                primary_position = _clean(rw.get("primary_position"))
                player_name = player_name or _clean(rw.get("player_name"))

        history: Optional[Dict[str, Any]] = None
        trow = store.training_row(player_id, r, year)
        if trow is not None:
            history = player_history_payload(trow, r, year)
            if age is None:
                age = _clean(trow.get("age"))
            if primary_position is None:
                primary_position = _clean(trow.get("primary_position"))
            player_name = player_name or _clean(trow.get("player_name"))

        archetype, neighbor_rows = _archetype_block(
            store, player_id=player_id, role=r, year=year
        )
        if archetype is not None and player_name is None:
            adf = store.archetypes(archetype["archetype_role"], year)
            match = adf.loc[adf["player_id"] == player_id]
            if not match.empty:
                player_name = _clean(match.iloc[0].get("player_name"))

        if projection is None and history is None and archetype is None and neighbor_rows is None:
            continue
        profiles.append(
            {
                "role": r,
                "age": age,
                "primary_position": primary_position,
                "projection": projection,
                "history": history,
                "archetype": archetype,
                "neighbors": neighbor_rows,
            }
        )

    if not profiles:
        return None
    return {
        "player_id": player_id,
        "player_name": player_name,
        "year": year,
        "model": model,
        "profiles": profiles,
    }


_NON_WORD = re.compile(r"[^a-z0-9]+")


def name_tokens(text: Any) -> List[str]:
    """Lowercased, accent-folded word tokens: ``Acuña Jr., Ronald`` → ``[acuna, jr, ronald]``."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return []
    folded = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode()
    return [t for t in _NON_WORD.split(folded.lower()) if t]


def search_players(
    index: pd.DataFrame,
    q: str,
    labels: ClusterLabelLookup,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Name matches over the per-year name index, with cluster info when known.

    Matching is token-wise rather than a raw substring scan: every term in the
    query must be a substring of some token of the player's name, in any order.
    Names are stored ``Last, First``, so this is what lets a natural-order query
    ("Ernie Clement") find the same player as "Clement, Ernie" or "clement".
    """
    terms = name_tokens(q)
    if not terms or index.empty:
        return []
    tokens = index["player_name"].map(name_tokens)
    mask = tokens.map(lambda toks: all(any(t in tok for tok in toks) for t in terms))
    sub = index.loc[mask].sort_values("player_name").head(limit)
    rows: List[Dict[str, Any]] = []
    for _, rw in sub.iterrows():
        role = str(rw["role"])
        row: Dict[str, Any] = {
            "player_id": int(rw["player_id"]),
            "player_name": str(rw["player_name"]),
            "role": role,
            "year": int(rw["year"]) if _clean(rw.get("year")) is not None else None,
        }
        cid = _clean(rw.get("cluster_id"))
        if cid is not None:
            cid = int(cid)
            row["cluster_id"] = cid
            row["cluster_label"] = cluster_label(labels, role, cid)
            sec_cid = _clean(rw.get("cluster_id_secondary"))
            if sec_cid is not None:
                row["prob_primary"] = _clean(rw.get("prob_primary"))
                row["prob_secondary"] = _clean(rw.get("prob_secondary"))
                row["secondary_cluster_label"] = cluster_label(labels, role, int(sec_cid))
        rows.append(row)
    return rows


def accuracy_payload(
    store: LakeStore,
    *,
    role_filter: str,
    start_year: int,
    end_year: int,
) -> Optional[Dict[str, Any]]:
    """xgboost-vs-marcel backtest comparison assembled from metrics.json sidecars.

    Role-years missing either model's sidecar are skipped; None when nothing
    was comparable in the window.
    """
    cache_key = ("accuracy", role_filter, start_year, end_year)
    if cache_key in store._cache:
        return store._cache[cache_key]

    roles = VALID_ROLES if role_filter == "all" else (role_filter,)
    rows: List[Dict[str, Any]] = []
    for r in roles:
        for year in range(start_year, end_year + 1):
            base = store.metrics(BASELINE_MODEL, r, year)
            cand = store.metrics(CANDIDATE_MODEL, r, year)
            if base is None or cand is None:
                continue
            rows.extend(
                build_comparison_rows(
                    base.get("metrics", {}),
                    cand.get("metrics", {}),
                    role=r,
                    year=year,
                )
            )

    if not rows:
        store._cache[cache_key] = None
        return None

    comparison = pd.DataFrame(rows)
    detail = [
        {
            "role": rw["role"],
            "year": int(rw["year"]),
            "stat": rw["stat"],
            "n": int(rw["n_candidate"]),
            CANDIDATE_MODEL: {
                "mae": _clean(rw["mae_candidate"]),
                "rmse": _clean(rw["rmse_candidate"]),
                "bias": _clean(rw["bias_candidate"]),
            },
            BASELINE_MODEL: {
                "mae": _clean(rw["mae_baseline"]),
                "rmse": _clean(rw["rmse_baseline"]),
                "bias": _clean(rw["bias_baseline"]),
            },
            "mae_pct_improvement": _clean(rw["mae_pct_improvement"]),
            "rmse_pct_improvement": _clean(rw["rmse_pct_improvement"]),
            "candidate_wins_mae": bool(rw["candidate_wins_mae"]),
        }
        for rw in rows
    ]
    summary = [
        {k: _clean(v) for k, v in rec.items()}
        for rec in summarize_comparison(comparison).to_dict(orient="records")
    ]
    payload = {
        "baseline_model": BASELINE_MODEL,
        "candidate_model": CANDIDATE_MODEL,
        "start_year": start_year,
        "end_year": end_year,
        "years_compared": sorted(comparison["year"].unique().tolist()),
        "detail": detail,
        "summary": summary,
    }
    store._cache[cache_key] = payload
    return payload
