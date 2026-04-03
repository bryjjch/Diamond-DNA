"""
Pure S3 key builders for the medallion data lake layout (no I/O).

Keep partition paths in one place so bronze/silver/gold jobs stay aligned.
"""

from __future__ import annotations

from datetime import date

STATCAST_PITCHES_FILENAME = "statcast_pitches.parquet"
STATCAST_SPRINT_SPEED_FILENAME = "statcast_sprint_speed.parquet"
PLAYER_YEAR_FEATURES_FILENAME = "player_year_features.parquet"
GOLD_FEATURES_FILENAME = "player_year_features_preprocessed.parquet"
GOLD_PREPROCESSING_METADATA_FILENAME = "preprocessing_metadata.json"
PLAYER_YEAR_ARCHETYPES_FILENAME = "player_year_archetypes.parquet"
ARCHETYPE_CLUSTERING_MODEL_FILENAME = "archetype_clustering.joblib"
ARCHETYPE_CLUSTERING_METADATA_FILENAME = "archetype_clustering_metadata.json"

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
