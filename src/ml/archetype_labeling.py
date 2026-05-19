"""LLM-generated archetype names for clustering outputs.

After a (role, year) GMM run, this module summarises each cluster as the top-N
features whose centroid deviates most from the league baseline (z-score units),
asks Gemini for a short archetype name + description per cluster, and writes the
result as a ``cluster_labels.json`` sidecar next to the parquet in S3.

The clustering pipeline calls :func:`build_cluster_labels` after writing the
parquet/joblib/metadata; the API layer reads the sidecar at startup so labels
adapt to any cluster count rather than being tied to hardcoded ids.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence

import boto3
import numpy as np
import pandas as pd

from ..common.s3_interaction import get_s3_client

logger = logging.getLogger(__name__)

DEFAULT_GEMINI_MODEL = "gemini-2.5-flash"
DEFAULT_TOP_N_FEATURES = 12
DEFAULT_SECRET_ENV_VAR = "GEMINI_API_KEY_SECRET_NAME"
DEFAULT_API_KEY_ENV_VAR = "GEMINI_API_KEY"


def compute_cluster_profiles(
    labeled_df: pd.DataFrame,
    feature_cols: Sequence[str],
    *,
    top_n: int = DEFAULT_TOP_N_FEATURES,
) -> Dict[int, List[Dict[str, float]]]:
    """
    Per cluster, return the ``top_n`` features ranked by |z-score of the cluster mean|.

    The z-score is ``(cluster_mean - global_mean) / global_std``, computed against
    the whole role/year frame so the LLM sees how each cluster deviates from league
    baseline. Features with zero variance are retained with a ``z_score`` of ``0.0``
    to avoid divide-by-zero, and may therefore appear in the ``top_n`` results.
    """
    if "cluster_id" not in labeled_df.columns:
        raise ValueError("labeled_df must contain a 'cluster_id' column.")
    if top_n < 1:
        raise ValueError(f"top_n must be >= 1; got {top_n!r}.")

    feats = [c for c in feature_cols if c in labeled_df.columns]
    if not feats:
        raise ValueError("No feature_cols are present in labeled_df.")

    X = labeled_df[feats].to_numpy(dtype=np.float64, copy=False)
    global_mean = X.mean(axis=0)
    global_std = X.std(axis=0, ddof=0)
    # Avoid divide-by-zero for constant features; their z is forced to 0 below.
    safe_std = np.where(global_std > 0.0, global_std, 1.0)
    constant_mask = global_std == 0.0

    profiles: Dict[int, List[Dict[str, float]]] = {}
    for cluster_id, group in labeled_df.groupby("cluster_id", sort=True):
        cluster_mean = group[feats].to_numpy(dtype=np.float64, copy=False).mean(axis=0)
        z = (cluster_mean - global_mean) / safe_std
        z[constant_mask] = 0.0
        # Rank by absolute deviation from league baseline.
        order = np.argsort(-np.abs(z))[:top_n]
        profiles[int(cluster_id)] = [
            {
                "feature": feats[i],
                "z_score": float(z[i]),
                "cluster_mean": float(cluster_mean[i]),
                "global_mean": float(global_mean[i]),
            }
            for i in order
        ]
    return profiles


def _get_gemini_api_key() -> str:
    """
    Resolve the Gemini API key.

    Preferred: AWS Secrets Manager, with the secret name from the
    ``GEMINI_API_KEY_SECRET_NAME`` env var. The secret may be either a raw
    string or a JSON blob ``{"api_key": "..."}`` / ``{"GEMINI_API_KEY": "..."}``.

    Fallback: ``GEMINI_API_KEY`` env var, useful for local notebook runs.
    """
    secret_name = os.environ.get(DEFAULT_SECRET_ENV_VAR, "").strip()
    if secret_name:
        client = boto3.client("secretsmanager")
        resp = client.get_secret_value(SecretId=secret_name)
        raw = resp.get("SecretString") or ""
        if not raw:
            raise RuntimeError(f"Secret {secret_name!r} has no SecretString payload.")
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return raw.strip()
        if isinstance(parsed, str):
            return parsed.strip()
        for k in ("api_key", "GEMINI_API_KEY", "gemini_api_key"):
            v = parsed.get(k) if isinstance(parsed, dict) else None
            if isinstance(v, str) and v.strip():
                return v.strip()
        raise RuntimeError(
            f"Secret {secret_name!r} JSON did not contain an api_key/GEMINI_API_KEY field."
        )
    env_key = os.environ.get(DEFAULT_API_KEY_ENV_VAR, "").strip()
    if env_key:
        return env_key
    raise RuntimeError(
        f"Set {DEFAULT_SECRET_ENV_VAR} (Secrets Manager) or {DEFAULT_API_KEY_ENV_VAR} (env) "
        "before generating cluster labels."
    )


def _build_gemini_prompt(
    profiles: Mapping[int, List[Dict[str, float]]],
    *,
    role: str,
    year: int,
) -> str:
    """Compose the role/year-aware user prompt with the per-cluster z-score profile."""
    lines: List[str] = [
        f"You are labelling Major League Baseball player archetype clusters for role={role!r} in season {year}.",
        "",
        "Each cluster below lists the top features whose cluster centroid deviates most from the league baseline, in z-score units.",
        "Positive z = the cluster is above league average on that feature; negative = below.",
        "",
        "For each cluster, generate:",
        "  - name: a short, evocative archetype name (3-6 words). Prefer a leading 'The '.",
        "  - description: 1-2 sentences explaining the archetype in baseball terms a fan would recognise.",
        "",
        "Return a JSON object mapping each cluster_id (as a string) to {name, description}. Use only the cluster_ids listed below.",
        "",
        "Clusters:",
    ]
    for cluster_id in sorted(profiles):
        lines.append(f"- cluster_id={cluster_id}:")
        for row in profiles[cluster_id]:
            lines.append(
                f"    {row['feature']}: z={row['z_score']:+.2f} "
                f"(cluster_mean={row['cluster_mean']:.3f}, league_mean={row['global_mean']:.3f})"
            )
    return "\n".join(lines)


def _response_schema_for_clusters(cluster_ids: Sequence[int]) -> Dict[str, Any]:
    """Strict JSON schema requiring a {name, description} object for every cluster_id."""
    properties: Dict[str, Any] = {
        str(cid): {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "description": {"type": "string"},
            },
            "required": ["name", "description"],
        }
        for cid in cluster_ids
    }
    return {
        "type": "object",
        "properties": properties,
        "required": [str(cid) for cid in cluster_ids],
    }


def generate_cluster_labels(
    profiles: Mapping[int, List[Dict[str, float]]],
    *,
    role: str,
    year: int,
    model: str = DEFAULT_GEMINI_MODEL,
    api_key: Optional[str] = None,
) -> Dict[str, Dict[str, str]]:
    """
    Ask Gemini for ``{cluster_id: {name, description}}`` using a JSON response schema.

    ``api_key`` is normally resolved from Secrets Manager / env via
    :func:`_get_gemini_api_key`; injecting it directly is for tests.
    """
    if not profiles:
        return {}
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key or _get_gemini_api_key())
    prompt = _build_gemini_prompt(profiles, role=role, year=year)
    schema = _response_schema_for_clusters(sorted(profiles))

    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=schema,
            temperature=0.4,
        ),
    )
    text = (response.text or "").strip()
    if not text:
        raise RuntimeError("Gemini returned an empty response when labelling clusters.")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Gemini response was not valid JSON: {text[:200]!r}") from exc

    out: Dict[str, Dict[str, str]] = {}
    for cluster_id in profiles:
        key = str(cluster_id)
        entry = parsed.get(key)
        if not isinstance(entry, dict):
            raise RuntimeError(f"Gemini response missing entry for cluster {key!r}: {parsed!r}")
        name = str(entry.get("name", "")).strip()
        description = str(entry.get("description", "")).strip()
        if not name or not description:
            raise RuntimeError(
                f"Gemini response for cluster {key!r} is missing name/description: {entry!r}"
            )
        out[key] = {"name": name, "description": description}
    return out


def _write_json_to_s3(bucket: str, key: str, payload: Mapping[str, Any]) -> None:
    client = get_s3_client()
    body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode()
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def build_cluster_labels(
    labeled_df: pd.DataFrame,
    *,
    role: str,
    year: int,
    feature_cols: Sequence[str],
    bucket: str,
    key: str,
    top_n: int = DEFAULT_TOP_N_FEATURES,
    model: str = DEFAULT_GEMINI_MODEL,
) -> Dict[str, Any]:
    """
    Compute per-cluster profiles, ask Gemini for names, and write the sidecar JSON.

    Returns the payload that was uploaded (also useful for tests).
    """
    profiles = compute_cluster_profiles(labeled_df, feature_cols, top_n=top_n)
    labels = generate_cluster_labels(profiles, role=role, year=year, model=model)
    payload: Dict[str, Any] = {
        "role": role,
        "year": int(year),
        "n_clusters": len(profiles),
        "model": model,
        "top_n_features": int(top_n),
        "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "labels": labels,
        "profiles": {str(cid): profiles[cid] for cid in profiles},
    }
    _write_json_to_s3(bucket, key, payload)
    logger.info(
        "Wrote %d cluster labels for role=%s year=%d to s3://%s/%s",
        len(labels),
        role,
        year,
        bucket,
        key,
    )
    return payload
