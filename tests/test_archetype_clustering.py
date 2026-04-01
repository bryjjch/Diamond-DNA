import io
import json

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.datasets import make_blobs

from src.ml.archetype_clustering import (
    ArchetypeClusteringConfig,
    build_gold_archetype_clustering,
    fit_archetype_clustering,
    numeric_feature_columns,
)


def test_numeric_feature_columns_excludes_ids_and_pitch_count():
    df = pd.DataFrame(
        {
            "player_id": [1, 2],
            "year": [2024, 2024],
            "role": ["batter", "batter"],
            "n_pitches_total": [100, 200],
            "swing_rate": [0.4, 0.5],
            "contact_rate": [0.8, 0.75],
        }
    )
    cols = numeric_feature_columns(df)
    assert "player_id" not in cols
    assert "year" not in cols
    assert "n_pitches_total" not in cols
    assert cols == ["contact_rate", "swing_rate"]


def test_fit_archetype_clustering_deterministic_k_on_blobs():
    X, _ = make_blobs(
        n_samples=300,
        centers=4,
        n_features=6,
        random_state=42,
        cluster_std=0.6,
    )
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(6)])
    df.insert(0, "player_id", np.arange(300))
    df.insert(1, "year", 2024)
    df.insert(2, "role", "batter")

    cfg = ArchetypeClusteringConfig(
        pca_variance_threshold=0.95,
        max_pca_components=20,
        k_min=2,
        k_max_cap=8,
        random_state=7,
        n_init=10,
    )
    out, meta, bundle = fit_archetype_clustering(df, role="batter", year=2024, config=cfg)

    assert meta["chosen_k"] == 4
    assert "cluster_id" in out.columns
    assert out["cluster_id"].nunique() == 4
    assert bundle["chosen_k"] == 4
    assert bundle["kmeans"].n_clusters == 4


def test_fit_archetype_clustering_raises_on_too_few_rows():
    df = pd.DataFrame(
        {
            "player_id": [1, 2],
            "year": [2024, 2024],
            "role": ["batter", "batter"],
            "x": [1.0, 2.0],
            "y": [0.0, 1.0],
        }
    )
    with pytest.raises(ValueError, match="at least"):
        fit_archetype_clustering(
            df,
            role="batter",
            year=2024,
            config=ArchetypeClusteringConfig(),
        )


def test_fit_archetype_clustering_raises_on_nan_features():
    df = pd.DataFrame(
        {
            "player_id": [1, 2, 3],
            "year": [2024, 2024, 2024],
            "role": ["batter"] * 3,
            "x": [1.0, np.nan, 3.0],
        }
    )
    with pytest.raises(ValueError, match="NaN"):
        fit_archetype_clustering(df, role="batter", year=2024, config=ArchetypeClusteringConfig())


def test_build_gold_archetype_clustering_writes_artifacts(monkeypatch):
    X, _ = make_blobs(n_samples=120, centers=3, n_features=4, random_state=0)
    gold_df = pd.DataFrame(X, columns=[f"f{i}" for i in range(4)])
    gold_df.insert(0, "player_id", np.arange(120))
    gold_df.insert(1, "year", 2025)
    gold_df.insert(2, "role", "pitcher")

    writes: list[tuple[str, pd.DataFrame]] = []
    joblib_writes: dict[str, bytes] = {}
    json_writes: dict[str, dict] = {}

    def fake_read(bucket, key, **kwargs):
        if "pitcher/year=2025" in key and key.endswith("player_year_features_preprocessed.parquet"):
            return gold_df.copy()
        return None

    def fake_write_parquet(df, bucket, key, **kwargs):
        writes.append((key, df.copy()))

    def fake_write_joblib(bundle: dict, bucket: str, key: str) -> None:
        buf = io.BytesIO()
        joblib.dump(bundle, buf)
        joblib_writes[key] = buf.getvalue()

    def fake_write_json(bucket: str, key: str, payload: dict) -> None:
        json_writes[key] = json.loads(json.dumps(payload, default=str))

    monkeypatch.setattr(
        "src.ml.archetype_clustering.read_parquet_from_s3",
        fake_read,
    )
    monkeypatch.setattr(
        "src.ml.archetype_clustering.write_parquet_to_s3",
        fake_write_parquet,
    )
    monkeypatch.setattr(
        "src.ml.archetype_clustering._write_joblib_to_s3",
        fake_write_joblib,
    )
    monkeypatch.setattr(
        "src.ml.archetype_clustering._write_json_to_s3",
        fake_write_json,
    )

    result = build_gold_archetype_clustering(
        bucket="test-bucket",
        gold_prefix="gold/statcast",
        start_year=2025,
        end_year=2025,
        role_filter="pitcher",
        config=ArchetypeClusteringConfig(k_max_cap=6, random_state=0),
    )

    assert result["status"] == "ok"
    assert result["rows_written"] == 120
    assert len(writes) == 1
    assert "player_year_archetypes.parquet" in writes[0][0]
    assert "cluster_id" in writes[0][1].columns
    assert len(joblib_writes) == 1
    assert any("archetype_clustering.joblib" in k for k in joblib_writes)
    assert len(json_writes) == 1
    meta = next(iter(json_writes.values()))
    assert meta["chosen_k"] >= 2
    assert "k_sweep_metrics" in meta
