import io
import json

import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.datasets import make_blobs

from src.ml.archetypes.archetype_clustering import (
    ArchetypeClusteringConfig,
    ArchetypeClusteringConfigsByRole,
    build_gold_archetype_clustering,
    fit_archetype_clustering,
    numeric_feature_columns,
    prepare_dataframe_for_archetype_clustering,
)


def test_numeric_feature_columns_excludes_ids_and_pitch_count():
    df = pd.DataFrame(
        {
            "player_id": [1, 2],
            "player_name": ["A, Alpha", "B, Beta"],
            "year": [2024, 2024],
            "role": ["batter", "batter"],
            "n_pitches_total": [100, 200],
            "swing_rate": [0.4, 0.5],
            "whiff_rate": [0.2, 0.25],
        }
    )
    df_i = prepare_dataframe_for_archetype_clustering(df)
    cols = numeric_feature_columns(df_i)
    assert "player_id" not in cols
    assert "player_name" not in cols
    assert "year" not in cols
    assert "n_pitches_total" not in cols
    assert cols == ["swing_rate", "whiff_rate"]


def test_archetype_clustering_config_validates_mutex():
    import pytest

    with pytest.raises(ValueError, match="exactly one of pca"):
        ArchetypeClusteringConfig(pca_n_components=3, pca_variance_target=0.9, n_clusters=4)
    with pytest.raises(ValueError, match="exactly one of pca"):
        ArchetypeClusteringConfig(n_clusters=4)
    with pytest.raises(ValueError, match="exactly one of n_clusters"):
        ArchetypeClusteringConfig(pca_n_components=3)
    with pytest.raises(ValueError, match="exactly one of n_clusters"):
        ArchetypeClusteringConfig(pca_n_components=3, n_clusters=4, bic_k_range=(2, 8))


def test_numeric_feature_columns_does_not_filter_gold_dropped_columns():
    """Clustering uses every numeric column except ids / pitch count; gold must drop the rest."""
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
    assert "foo_was_missing" in cols
    assert "pitch_type_UN_share" in cols
    assert "pitch_type_FF_share" in cols
    assert "pitch_type_entropy" in cols
    assert "pt_FF_release_speed_mean" in cols
    assert "xwoba_allowed_lhb_mean" in cols
    assert "xwoba_allowed_rhb_mean" in cols
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
    df.insert(1, "player_name", [f"Player, {i}" for i in range(300)])
    df.insert(2, "year", 2024)
    df.insert(3, "role", "batter")
    df.insert(4, "n_pitches_total", np.arange(500, 500 + 300))

    cfg = ArchetypeClusteringConfig(
        pca_n_components=4,
        n_clusters=4,
        random_state=7,
        n_init=10,
    )
    out, meta, bundle = fit_archetype_clustering(df, role="batter", year=2024, config=cfg)

    assert meta["n_clusters"] == 4
    assert meta["pca_n_components"] == 4
    assert meta["clustering_index_columns"] == [
        "player_id",
        "player_name",
        "year",
        "role",
        "n_pitches_total",
    ]
    assert "cluster_id" in out.columns
    assert "player_name" in out.columns
    assert out["cluster_id"].nunique() == 4
    assert bundle["n_clusters"] == 4
    assert bundle["gmm"].n_components == 4
    assert meta["clustering_method"] == "gaussian_mixture"
    assert meta["scaler"] == "RobustScaler"
    assert meta["pca_mode"] == "fixed"
    assert meta["n_clusters_mode"] == "fixed"
    assert "gmm_bic" in meta
    assert "silhouette_score" in meta


def test_fit_archetype_clustering_variance_target_pca():
    """pca_variance_target trims to the smallest n_components hitting the target."""
    X, _ = make_blobs(
        n_samples=200,
        centers=4,
        n_features=8,
        random_state=11,
        cluster_std=0.7,
    )
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(8)])
    df.insert(0, "player_id", np.arange(200))
    df.insert(1, "player_name", [f"P, {i}" for i in range(200)])
    df.insert(2, "year", 2024)
    df.insert(3, "role", "batter")
    df.insert(4, "n_pitches_total", np.arange(500, 700))

    cfg = ArchetypeClusteringConfig(
        pca_variance_target=0.9,
        n_clusters=4,
        random_state=11,
        n_init=5,
    )
    _out, meta, bundle = fit_archetype_clustering(df, role="batter", year=2024, config=cfg)

    assert meta["pca_mode"] == "variance_target"
    assert meta["pca_total_explained_variance"] >= 0.9
    # Lowering by one component would drop us below the target.
    if meta["pca_n_components"] > 1:
        cum = float(sum(meta["pca_explained_variance_ratio"][:-1]))
        assert cum < 0.9
    assert bundle["pca"].n_components_ == meta["pca_n_components"]


def test_fit_archetype_clustering_bic_selected_k():
    """bic_k_range sweeps the range and picks the k with the lowest BIC."""
    X, _ = make_blobs(
        n_samples=240,
        centers=3,
        n_features=5,
        random_state=23,
        cluster_std=0.5,
    )
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(5)])
    df.insert(0, "player_id", np.arange(240))
    df.insert(1, "player_name", [f"P, {i}" for i in range(240)])
    df.insert(2, "year", 2024)
    df.insert(3, "role", "pitcher")
    df.insert(4, "n_pitches_total", np.arange(1000, 1240))

    cfg = ArchetypeClusteringConfig(
        pca_n_components=4,
        bic_k_range=(2, 6),
        random_state=23,
        n_init=4,
    )
    _out, meta, bundle = fit_archetype_clustering(df, role="pitcher", year=2024, config=cfg)

    assert meta["n_clusters_mode"] == "bic_selected"
    assert 2 <= meta["n_clusters"] <= 6
    sweep = meta["bic_sweep"]
    assert sweep is not None and len(sweep) == 5
    best_bic = min(row["bic"] for row in sweep)
    chosen = next(row for row in sweep if int(row["k"]) == meta["n_clusters"])
    assert chosen["bic"] == best_bic
    assert bundle["gmm"].n_components == meta["n_clusters"]


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
        predictions_prefix="p",
        models_prefix="m",
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
        predictions_prefix="p",
        models_prefix="m",
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
        "src.ml.archetypes.archetype_clustering.read_parquet_from_s3",
        fake_read,
    )
    monkeypatch.setattr(
        "src.ml.archetypes.archetype_clustering.write_parquet_to_s3",
        fake_write_parquet,
    )
    monkeypatch.setattr(
        "src.ml.archetypes.archetype_clustering._write_joblib_to_s3",
        fake_write_joblib,
    )
    monkeypatch.setattr(
        "src.ml.archetypes.archetype_clustering._write_json_to_s3",
        fake_write_json,
    )
    monkeypatch.setattr(
        "src.ml.archetypes.archetype_clustering._build_cluster_labels_for_role_year",
        lambda **kwargs: None,
    )

    result = build_gold_archetype_clustering(
        bucket="test-bucket",
        gold_prefix="gold/statcast",
        predictions_prefix="gold/predictions",
        models_prefix="models",
        start_year=2025,
        end_year=2025,
        role_filter="pitcher",
        config=ArchetypeClusteringConfig(pca_n_components=3, n_clusters=3, random_state=0),
    )

    assert result["status"] == "ok"
    assert result["rows_written"] == 120
    assert len(writes) == 1
    assert (
        writes[0][0]
        == "gold/predictions/archetypes/pitcher/year=2025/player_year_archetypes.parquet"
    )
    written_cols = set(writes[0][1].columns)
    assert "cluster_id" in written_cols
    assert "cluster_id_secondary" in written_cols
    assert "prob_primary" in written_cols
    assert "prob_secondary" in written_cols
    assert {f"prob_{k}" for k in range(3)}.issubset(written_cols)
    assert len(joblib_writes) == 1
    assert "models/archetypes/pitcher/year=2025/model.joblib" in joblib_writes
    assert len(json_writes) == 1
    assert "models/archetypes/pitcher/year=2025/metadata.json" in json_writes
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
        "src.ml.archetypes.archetype_clustering.read_parquet_from_s3",
        fake_read,
    )
    monkeypatch.setattr(
        "src.ml.archetypes.archetype_clustering.write_parquet_to_s3",
        fake_write_parquet,
    )
    monkeypatch.setattr(
        "src.ml.archetypes.archetype_clustering._write_joblib_to_s3",
        fake_write_joblib,
    )
    monkeypatch.setattr(
        "src.ml.archetypes.archetype_clustering._write_json_to_s3",
        fake_write_json,
    )
    monkeypatch.setattr(
        "src.ml.archetypes.archetype_clustering._build_cluster_labels_for_role_year",
        lambda **kwargs: None,
    )

    result = build_gold_archetype_clustering(
        bucket="test-bucket",
        gold_prefix="gold/statcast",
        predictions_prefix="gold/predictions",
        models_prefix="models",
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


def test_fit_archetype_clustering_emits_prob_columns():
    """`fit_archetype_clustering` returns full predict_proba columns plus top-2 summary."""
    X, _ = make_blobs(n_samples=200, centers=4, n_features=6, random_state=42, cluster_std=0.6)
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(6)])
    df.insert(0, "player_id", np.arange(200))
    df.insert(1, "player_name", [f"P, {i}" for i in range(200)])
    df.insert(2, "year", 2024)
    df.insert(3, "role", "batter")
    df.insert(4, "n_pitches_total", np.arange(500, 700))

    cfg = ArchetypeClusteringConfig(
        pca_n_components=4, n_clusters=4, random_state=7, n_init=10
    )
    out, _meta, _bundle = fit_archetype_clustering(df, role="batter", year=2024, config=cfg)

    prob_cols = [f"prob_{k}" for k in range(4)]
    for c in prob_cols + ["prob_primary", "prob_secondary", "cluster_id_secondary"]:
        assert c in out.columns

    probs = out[prob_cols].to_numpy()
    assert np.isfinite(probs).all()
    assert np.allclose(probs.sum(axis=1), 1.0, atol=1e-6)

    # cluster_id is argmax; cluster_id_secondary is second-argmax.
    sorted_idx = np.argsort(-probs, axis=1)
    assert (out["cluster_id"].to_numpy() == sorted_idx[:, 0]).all()
    assert (out["cluster_id_secondary"].to_numpy() == sorted_idx[:, 1]).all()
    assert (out["cluster_id"] != out["cluster_id_secondary"]).all()

    # Primary >= secondary >= 0.
    assert (out["prob_primary"] >= out["prob_secondary"]).all()
    assert (out["prob_secondary"] >= 0).all()
    # Top-2 columns match the chosen indices in the full vector.
    row_idx = np.arange(len(out))
    assert np.allclose(out["prob_primary"].to_numpy(), probs[row_idx, sorted_idx[:, 0]])
    assert np.allclose(out["prob_secondary"].to_numpy(), probs[row_idx, sorted_idx[:, 1]])


def test_fit_archetype_clustering_metadata_includes_soft_schema():
    """Metadata advertises the soft-assignment schema and aggregate confidence stats."""
    X, _ = make_blobs(n_samples=180, centers=3, n_features=5, random_state=11, cluster_std=0.7)
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(5)])
    df.insert(0, "player_id", np.arange(180))
    df.insert(1, "player_name", [f"P, {i}" for i in range(180)])
    df.insert(2, "year", 2024)
    df.insert(3, "role", "pitcher")
    df.insert(4, "n_pitches_total", np.arange(500, 680))

    cfg = ArchetypeClusteringConfig(
        pca_n_components=3, n_clusters=3, random_state=11, n_init=5
    )
    _out, meta, _bundle = fit_archetype_clustering(df, role="pitcher", year=2024, config=cfg)

    assert meta["soft_assignment_schema_version"] == 1
    assert meta["prob_columns"] == ["prob_0", "prob_1", "prob_2"]
    assert 0.0 <= meta["mean_max_prob"] <= 1.0
    assert 0.0 <= meta["frac_confident_p_ge_0_7"] <= 1.0
