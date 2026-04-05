import io
import json

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.datasets import make_blobs

from src.ml.archetype_clustering import ArchetypeClusteringConfig, fit_archetype_clustering
from src.ml.player_similarity import (
    PlayerSimilarityConfig,
    build_gold_player_similarity,
    build_neighbor_long_table,
    features_pca_from_bundle,
)


def _gold_like_frame(n: int, seed: int = 0) -> pd.DataFrame:
    X, _ = make_blobs(n_samples=n, centers=3, n_features=5, random_state=seed)
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(5)])
    df.insert(0, "player_id", np.arange(1000, 1000 + n))
    df.insert(1, "player_name", [f"P, {i}" for i in range(n)])
    df.insert(2, "year", 2025)
    df.insert(3, "role", "pitcher")
    df.insert(4, "n_pitches_total", np.arange(200, 200 + n))
    return df


def test_features_pca_from_bundle_matches_archetype_pipeline():
    df = _gold_like_frame(40, seed=1)
    cfg = ArchetypeClusteringConfig(pca_n_components=3, n_clusters=3, random_state=0)
    _out, _meta, bundle = fit_archetype_clustering(df, role="pitcher", year=2025, config=cfg)
    X_pca = features_pca_from_bundle(df, bundle)
    assert X_pca.shape == (40, 3)
    assert not np.isnan(X_pca).any()


def test_build_neighbor_long_table_excludes_self_and_respects_k():
    df = _gold_like_frame(50, seed=2)
    cfg = ArchetypeClusteringConfig(pca_n_components=4, n_clusters=4, random_state=0)
    _out, _meta, bundle = fit_archetype_clustering(df, role="batter", year=2024, config=cfg)
    X_pca = features_pca_from_bundle(df, bundle)
    k = 7
    neighbors = build_neighbor_long_table(
        df,
        X_pca,
        role="batter",
        year=2024,
        k_neighbors=k,
        metric="minkowski",
        minkowski_p=2,
        algorithm="auto",
    )
    assert len(neighbors) == 50 * k
    assert (neighbors["player_id"] != neighbors["neighbor_player_id"]).all()
    counts = neighbors.groupby("player_id").size()
    assert (counts == k).all()


def test_build_neighbor_long_table_small_cohort_caps_neighbors():
    df = _gold_like_frame(4, seed=3)
    cfg = ArchetypeClusteringConfig(pca_n_components=2, n_clusters=2, random_state=0)
    _out, _meta, bundle = fit_archetype_clustering(df, role="pitcher", year=2023, config=cfg)
    X_pca = features_pca_from_bundle(df, bundle)
    neighbors = build_neighbor_long_table(
        df,
        X_pca,
        role="pitcher",
        year=2023,
        k_neighbors=20,
        metric="euclidean",
        minkowski_p=2,
        algorithm="auto",
    )
    assert len(neighbors) == 4 * 3
    assert (neighbors.groupby("player_id").size() == 3).all()


def test_build_neighbor_long_table_single_row_empty():
    df = pd.DataFrame(
        {
            "player_id": [99],
            "player_name": ["Solo, P"],
            "year": [2022],
            "role": ["pitcher"],
            "n_pitches_total": [100],
            "f0": [0.5],
        }
    )
    X_pca = np.array([[0.0, 1.0]], dtype=np.float64)
    neighbors = build_neighbor_long_table(
        df,
        X_pca,
        role="pitcher",
        year=2022,
        k_neighbors=5,
        metric="minkowski",
        minkowski_p=2,
        algorithm="auto",
    )
    assert neighbors.empty


def test_build_gold_player_similarity_requires_positive_k():
    r = build_gold_player_similarity(
        bucket="b",
        gold_prefix="g",
        start_year=2024,
        end_year=2024,
        role_filter="pitcher",
        config=PlayerSimilarityConfig(k_neighbors=0),
    )
    assert r["status"] == "error"


def test_build_gold_player_similarity_missing_model_records_error(monkeypatch):
    gold_df = _gold_like_frame(30, seed=5)

    def fake_read(bucket, key, **kwargs):
        if key.endswith("player_year_features_preprocessed.parquet"):
            return gold_df.copy()
        return None

    monkeypatch.setattr("src.ml.player_similarity.read_parquet_from_s3", fake_read)
    monkeypatch.setattr(
        "src.ml.player_similarity.load_archetype_clustering_bundle_from_s3",
        lambda bucket, key: None,
    )

    result = build_gold_player_similarity(
        bucket="test-bucket",
        gold_prefix="gold/statcast",
        start_year=2025,
        end_year=2025,
        role_filter="pitcher",
        config=PlayerSimilarityConfig(k_neighbors=5),
    )
    assert result["status"] == "no_data"
    assert result["rows_written"] == 0
    assert result["errors"]
    assert "missing archetype model" in result["errors"][0]


def test_build_gold_player_similarity_writes_artifacts(monkeypatch):
    gold_df = _gold_like_frame(35, seed=6)
    cfg = ArchetypeClusteringConfig(pca_n_components=3, n_clusters=3, random_state=1)
    _out, _meta, bundle = fit_archetype_clustering(
        gold_df, role="pitcher", year=2025, config=cfg
    )

    parquet_writes: list[tuple[str, pd.DataFrame]] = []
    json_writes: dict[str, dict] = {}

    def fake_read_parquet(bucket, key, **kwargs):
        if key.endswith("player_year_features_preprocessed.parquet"):
            return gold_df.copy()
        return None

    def fake_write_parquet(df, bucket, key, **kwargs):
        parquet_writes.append((key, df.copy()))

    def fake_write_json(bucket, key, payload):
        json_writes[key] = json.loads(json.dumps(payload, default=str))

    monkeypatch.setattr("src.ml.player_similarity.read_parquet_from_s3", fake_read_parquet)
    monkeypatch.setattr("src.ml.player_similarity.write_parquet_to_s3", fake_write_parquet)
    monkeypatch.setattr(
        "src.ml.player_similarity.load_archetype_clustering_bundle_from_s3",
        lambda b, k: bundle,
    )
    monkeypatch.setattr("src.ml.player_similarity._write_json_to_s3", fake_write_json)

    result = build_gold_player_similarity(
        bucket="test-bucket",
        gold_prefix="gold/statcast",
        start_year=2025,
        end_year=2025,
        role_filter="pitcher",
        config=PlayerSimilarityConfig(k_neighbors=5, metric="euclidean"),
    )

    assert result["status"] == "ok"
    assert result["rows_written"] == 35 * 5
    assert len(parquet_writes) == 1
    assert "player_year_similar_neighbors.parquet" in parquet_writes[0][0]
    nbr = parquet_writes[0][1]
    assert list(nbr.columns) == [
        "player_id",
        "player_name",
        "year",
        "role",
        "neighbor_rank",
        "neighbor_player_id",
        "neighbor_player_name",
        "distance",
    ]
    assert (nbr["player_id"] != nbr["neighbor_player_id"]).all()
    assert len(json_writes) == 1
    meta = next(iter(json_writes.values()))
    assert meta["similarity_method"] == "knn_pca_space"
    assert meta["k_neighbors_requested"] == 5
    assert meta["metric"] == "euclidean"
    assert "source_archetype_clustering_model_s3_key" in meta


def test_features_pca_from_bundle_raises_on_missing_columns():
    df = pd.DataFrame({"player_id": [1], "f0": [0.0]})
    bundle = {"feature_columns": ["f0", "f1"], "scaler": None, "pca": None}
    with pytest.raises(ValueError, match="missing"):
        features_pca_from_bundle(df, bundle)
