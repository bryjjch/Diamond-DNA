import io
import json

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.datasets import make_blobs

from src.ml.archetype_clustering import (
    ARCHETYPE_CLUSTER_LABELS_BATTER,
    ARCHETYPE_CLUSTER_LABELS_PITCHER,
    ArchetypeClusteringConfig,
    ArchetypeClusteringConfigsByRole,
    archetype_cluster_label,
    build_gold_archetype_clustering,
    fit_archetype_clustering,
    numeric_feature_columns,
    prepare_dataframe_for_archetype_clustering,
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
    df_i = prepare_dataframe_for_archetype_clustering(df)
    cols = numeric_feature_columns(df_i)
    assert "player_id" not in cols
    assert "year" not in cols
    assert "n_pitches_total" not in cols
    assert cols == ["contact_rate", "swing_rate"]


def test_archetype_cluster_label_mappings():
    assert len(ARCHETYPE_CLUSTER_LABELS_BATTER) == 6
    assert len(ARCHETYPE_CLUSTER_LABELS_PITCHER) == 6
    assert ARCHETYPE_CLUSTER_LABELS_BATTER[0] == "The Power Slugger"
    assert ARCHETYPE_CLUSTER_LABELS_PITCHER[5] == "The High-Leverage Power Reliever"
    assert archetype_cluster_label("batter", 3) == "The Contact Hitter"
    assert archetype_cluster_label("pitcher", 4) == "The Groundball Specialist"
    assert archetype_cluster_label("batter", 99) == "Cluster 99"
    assert archetype_cluster_label("unknown_role", 0) == "Cluster 0"


def test_numeric_feature_columns_excludes_imputation_flags_pt_junk_xwoba():
    df = pd.DataFrame(
        {
            "player_id": [1],
            "year": [2024],
            "role": ["pitcher"],
            "n_pitches_total": [500],
            "delta_run_exp_mean": [0.0],
            "pitch_type_UN_share": [0.01],
            "pitch_type_FF_share": [0.5],
            "pitch_type_entropy": [1.2],
            "pt_FF_release_speed_mean": [95.0],
            "xwoba_allowed_lhb_mean": [0.3],
            "xwoba_allowed_rhb_mean": [0.31],
            "platoon_xwoba_allowed_diff": [0.01],
            "foo_was_missing": [0],
        }
    )
    df_i = prepare_dataframe_for_archetype_clustering(df)
    cols = numeric_feature_columns(df_i)
    assert "foo_was_missing" not in cols
    assert "pitch_type_UN_share" not in cols
    assert "pitch_type_FF_share" not in cols
    assert "pitch_type_entropy" in cols
    assert "pt_FF_release_speed_mean" not in cols
    assert "xwoba_allowed_lhb_mean" not in cols
    assert "xwoba_allowed_rhb_mean" not in cols
    assert "platoon_xwoba_allowed_diff" in cols
    assert "delta_run_exp_mean" in cols


def test_fit_archetype_clustering_fixed_pca_and_k():
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
        pca_n_components=4,
        n_clusters=4,
        random_state=7,
        n_init=10,
    )
    out, meta, bundle = fit_archetype_clustering(df, role="batter", year=2024, config=cfg)

    assert meta["n_clusters"] == 4
    assert meta["pca_n_components"] == 4
    assert "cluster_id" in out.columns
    assert out["cluster_id"].nunique() == 4
    assert bundle["n_clusters"] == 4
    assert bundle["gmm"].n_components == 4
    assert meta["clustering_method"] == "gaussian_mixture"
    assert "gmm_bic" in meta
    assert "silhouette_score" in meta


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
            config=ArchetypeClusteringConfig(pca_n_components=2, n_clusters=2),
        )


def test_fit_archetype_clustering_raises_on_bad_covariance_type():
    X, _ = make_blobs(n_samples=50, centers=2, n_features=4, random_state=0)
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(4)])
    df.insert(0, "player_id", np.arange(50))
    df.insert(1, "year", 2024)
    df.insert(2, "role", "batter")
    with pytest.raises(ValueError, match="covariance_type"):
        fit_archetype_clustering(
            df,
            role="batter",
            year=2024,
            config=ArchetypeClusteringConfig(
                pca_n_components=2, n_clusters=2, covariance_type="not_a_type"
            ),
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
        fit_archetype_clustering(
            df,
            role="batter",
            year=2024,
            config=ArchetypeClusteringConfig(pca_n_components=1, n_clusters=2),
        )


def test_build_gold_archetype_clustering_requires_config():
    result = build_gold_archetype_clustering(
        bucket="b",
        gold_prefix="g",
        start_year=2024,
        end_year=2024,
        role_filter="pitcher",
        config=None,
    )
    assert result["status"] == "error"


def test_build_gold_archetype_clustering_rejects_config_and_configs_by_role():
    c = ArchetypeClusteringConfig(pca_n_components=2, n_clusters=2)
    by = ArchetypeClusteringConfigsByRole(pitcher=c, batter=c)
    result = build_gold_archetype_clustering(
        bucket="b",
        gold_prefix="g",
        start_year=2024,
        end_year=2024,
        role_filter="all",
        config=c,
        configs_by_role=by,
    )
    assert result["status"] == "error"
    assert "not both" in result["message"].lower()


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
        config=ArchetypeClusteringConfig(pca_n_components=3, n_clusters=3, random_state=0),
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
    assert meta["n_clusters"] == 3
    assert meta["pca_n_components"] == 3
    assert meta["clustering_method"] == "gaussian_mixture"
    assert "feature_exclusion_rules" in meta
    assert meta["clustering_index_columns"]


def test_build_gold_archetype_clustering_configs_by_role_different_k(monkeypatch):
    """Pitcher and batter partitions get different n_clusters when configs differ."""
    Xp, _ = make_blobs(n_samples=80, centers=2, n_features=4, random_state=1)
    gold_p = pd.DataFrame(Xp, columns=[f"f{i}" for i in range(4)])
    gold_p.insert(0, "player_id", np.arange(80))
    gold_p.insert(1, "year", 2025)
    gold_p.insert(2, "role", "pitcher")

    Xb, _ = make_blobs(n_samples=90, centers=3, n_features=4, random_state=2)
    gold_b = pd.DataFrame(Xb, columns=[f"f{i}" for i in range(4)])
    gold_b.insert(0, "player_id", np.arange(90))
    gold_b.insert(1, "year", 2025)
    gold_b.insert(2, "role", "batter")

    json_writes: dict[str, dict] = {}

    def fake_read(bucket, key, **kwargs):
        if "pitcher/year=2025" in key and key.endswith("player_year_features_preprocessed.parquet"):
            return gold_p.copy()
        if "batter/year=2025" in key and key.endswith("player_year_features_preprocessed.parquet"):
            return gold_b.copy()
        return None

    def fake_write_parquet(df, bucket, key, **kwargs):
        pass

    def fake_write_joblib(bundle: dict, bucket: str, key: str) -> None:
        pass

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
        role_filter="all",
        configs_by_role=ArchetypeClusteringConfigsByRole(
            pitcher=ArchetypeClusteringConfig(
                pca_n_components=3, n_clusters=3, random_state=0
            ),
            batter=ArchetypeClusteringConfig(
                pca_n_components=2, n_clusters=4, random_state=0
            ),
        ),
    )

    assert result["status"] == "ok"
    assert result["rows_written"] == 170
    assert len(json_writes) == 2
    metas = list(json_writes.values())
    n_by_role = {m["role"]: m["n_clusters"] for m in metas}
    assert n_by_role["pitcher"] == 3
    assert n_by_role["batter"] == 4
