"""S3 data lake path helpers and Parquet read/write for pipelines."""

from __future__ import annotations

import io
import logging
from datetime import date
from typing import Any, Literal, Optional

import boto3
import pandas as pd

logger = logging.getLogger(__name__)

# --- Lake key layout (pure builders; no I/O) ---

STATCAST_PITCHES_FILENAME = "statcast_pitches.parquet"
STATCAST_SPRINT_SPEED_FILENAME = "statcast_sprint_speed.parquet"
PLAYER_YEAR_FEATURES_FILENAME = "player_year_features.parquet"
GOLD_FEATURES_FILENAME = "player_year_features_preprocessed.parquet"
GOLD_PREPROCESSING_METADATA_FILENAME = "preprocessing_metadata.json"
PLAYER_YEAR_ARCHETYPES_FILENAME = "player_year_archetypes.parquet"
ARCHETYPE_CLUSTERING_MODEL_FILENAME = "archetype_clustering.joblib"
ARCHETYPE_CLUSTERING_METADATA_FILENAME = "archetype_clustering_metadata.json"
PLAYER_YEAR_SIMILAR_NEIGHBORS_FILENAME = "player_year_similar_neighbors.parquet"
PLAYER_SIMILARITY_METADATA_FILENAME = "player_similarity_metadata.json"

# Bronze defence layer filenames (must match uploads in bronze.defence_ingestion).
DEFENCE_OAA_PARQUET = "statcast_oaa.parquet"
DEFENCE_OUTFIELD_CATCH_PARQUET = "statcast_outfield_catch_probability.parquet"
DEFENCE_CATCHER_POPTIME_PARQUET = "statcast_catcher_poptime.parquet"
DEFENCE_CATCHER_FRAMING_PARQUET = "statcast_catcher_framing.parquet"
DEFENCE_ARM_STRENGTH_PARQUET = "statcast_arm_strength.parquet"
DEFENCE_FANGRAPHS_FIELDING_PARQUET = "fangraphs_fielding.parquet"


def raw_statcast_day_key(prefix: str, d: date) -> str:
    """Daily bronze Statcast pitches: {prefix}/year=Y/date=YYYY-MM-DD/statcast_pitches.parquet."""
    p = prefix.strip("/")
    return f"{p}/year={d.year}/date={d.strftime('%Y-%m-%d')}/{STATCAST_PITCHES_FILENAME}"


def feature_player_year_output_key(prefix: str, role: str, year: int) -> str:
    """Player-year engineered feature table (silver): {prefix}/{role}/year=Y/player_year_features.parquet."""
    p = prefix.strip("/")
    return f"{p}/{role}/year={year}/{PLAYER_YEAR_FEATURES_FILENAME}"


def raw_sprint_speed_key(prefix: str, year: int) -> str:
    """Bronze sprint speed leaderboard: {prefix}/year=Y/statcast_sprint_speed.parquet."""
    p = prefix.strip("/")
    return f"{p}/year={year}/{STATCAST_SPRINT_SPEED_FILENAME}"


def raw_defence_dataset_key(prefix: str, year: int, dataset_filename: str) -> str:
    """Single bronze defence dataset object under a year partition."""
    p = prefix.strip("/")
    return f"{p}/year={year}/{dataset_filename}"


def gold_player_year_output_key(prefix: str, role: str, year: int) -> str:
    """Player-year preprocessed feature table (gold): {prefix}/{role}/year=Y/file.parquet."""
    p = prefix.strip("/")
    return f"{p}/{role}/year={year}/{GOLD_FEATURES_FILENAME}"


def gold_preprocessing_metadata_key(prefix: str, role: str, year: int) -> str:
    """Gold preprocessing metadata JSON under each role/year partition."""
    p = prefix.strip("/")
    return f"{p}/{role}/year={year}/{GOLD_PREPROCESSING_METADATA_FILENAME}"


def gold_archetype_assignments_key(prefix: str, role: str, year: int) -> str:
    """Player-year rows with cluster_id under each gold role/year partition."""
    p = prefix.strip("/")
    return f"{p}/{role}/year={year}/{PLAYER_YEAR_ARCHETYPES_FILENAME}"


def gold_archetype_clustering_model_key(prefix: str, role: str, year: int) -> str:
    """Serialized scaler + PCA + GaussianMixture (joblib) under each gold role/year partition."""
    p = prefix.strip("/")
    return f"{p}/{role}/year={year}/{ARCHETYPE_CLUSTERING_MODEL_FILENAME}"


def gold_archetype_clustering_metadata_key(prefix: str, role: str, year: int) -> str:
    """Clustering run metadata JSON (K, metrics curves, seeds) under each gold role/year partition."""
    p = prefix.strip("/")
    return f"{p}/{role}/year={year}/{ARCHETYPE_CLUSTERING_METADATA_FILENAME}"


def gold_player_similar_neighbors_key(prefix: str, role: str, year: int) -> str:
    """Long-form KNN neighbor rows (Parquet) under each gold role/year partition."""
    p = prefix.strip("/")
    return f"{p}/{role}/year={year}/{PLAYER_YEAR_SIMILAR_NEIGHBORS_FILENAME}"


def gold_player_similarity_metadata_key(prefix: str, role: str, year: int) -> str:
    """KNN similarity run metadata JSON under each gold role/year partition."""
    p = prefix.strip("/")
    return f"{p}/{role}/year={year}/{PLAYER_SIMILARITY_METADATA_FILENAME}"


# --- S3 Parquet I/O ---

_s3_client: Optional[Any] = None


def get_s3_client() -> Any:
    """Return a shared Boto3 S3 client (credentials from env / IAM role)."""
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3")
    return _s3_client


def read_parquet_from_s3(
    bucket: str,
    key: str,
    *,
    client=None,
    log_read: bool = True,
    missing_key_log: Literal["none", "info", "warning"] = "info",
) -> Optional[pd.DataFrame]:
    """
    Read a Parquet object from S3 as a DataFrame.

    Returns None if the object does not exist (NoSuchKey). Other errors are logged
    and re-raised.
    """
    if client is None:
        client = get_s3_client()
    try:
        if log_read:
            logger.info("Reading s3://%s/%s", bucket, key)
        obj = client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return pd.read_parquet(io.BytesIO(body))
    except client.exceptions.NoSuchKey:
        if missing_key_log == "info":
            logger.info("No existing object at s3://%s/%s (treat as empty)", bucket, key)
        elif missing_key_log == "warning":
            logger.warning("No such parquet at s3://%s/%s", bucket, key)
        return None
    except Exception as exc:
        logger.error("Error reading s3://%s/%s: %s", bucket, key, exc)
        raise


def write_parquet_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    *,
    client=None,
    log_write: bool = True,
) -> None:
    """Write a DataFrame to S3 as Snappy-compressed Parquet."""
    if client is None:
        client = get_s3_client()
    if log_write:
        logger.info("Writing %d rows to s3://%s/%s", len(df), bucket, key)
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    buf.seek(0)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/x-parquet",
    )
