import json

import numpy as np
import pandas as pd
import pytest

from src.ml.baselines.marcel_baseline import ROLE_ALL_TARGETS
from src.ml.boosted_trees.xgboost_projection import (
    XGBoostProjectionConfig,
    _postprocess_predictions,
    build_xgboost_projections,
    modeled_target_stats,
    predict_role_year,
    select_feature_columns,
    train_role_year_models,
)

# Small trees and a single thread keep the suite fast; targets in the synthetic
# matrix are strong functions of lag1_* so even tiny models learn signal.
_FAST_CONFIG = XGBoostProjectionConfig(
    n_estimators=15,
    max_depth=2,
    early_stopping_rounds=5,
    min_train_years=2,
    n_jobs=1,
)

_BATTER_TARGETS = ("pa", "avg", "obp", "slg", "ops", "bb_pct", "k_pct", "hr", "sb")


def _synthetic_matrix(year: int, n: int = 40, seed: int = 0) -> pd.DataFrame:
    """A gold-style batter training matrix with learnable targets."""
    rng = np.random.default_rng(seed + year)
    pa = rng.uniform(100, 700, n)
    obp = rng.uniform(0.250, 0.420, n)
    slg = rng.uniform(0.300, 0.600, n)
    df = pd.DataFrame(
        {
            "player_id": np.arange(1, n + 1),
            "player_name": [f"Player {i}" for i in range(1, n + 1)],
            "primary_position": ["1B"] * n,
            "year": year,
            "role": "batter",
            "age": rng.uniform(21, 38, n).round(1),
            "lag1_pa": pa,
            "lag2_pa": pa * rng.uniform(0.8, 1.2, n),
            "lag3_pa": pa * rng.uniform(0.7, 1.3, n),
            "lag1_available": 1,
            "lag2_available": 1,
            "lag3_available": rng.integers(0, 2, n),
            "lag1_hr": (pa * slg * 0.08).round(),
            "lag1_sb": rng.integers(0, 30, n),
            "lag_weighted_avg": obp - 0.060,
            "lag_weighted_obp": obp,
            "lag_weighted_slg": slg,
            "lag_weighted_bb_pct": rng.uniform(0.04, 0.15, n),
            "lag_weighted_k_pct": rng.uniform(0.10, 0.32, n),
            "skill_lag1_barrel_rate": rng.uniform(0.02, 0.18, n),
            "skill_lag1_whiff_rate": rng.uniform(0.15, 0.40, n),
            "skill_lag1_available": 1,
        }
    )
    noise = rng.normal(0, 0.010, n)
    df["target_pa"] = (pa * rng.uniform(0.85, 1.15, n)).round()
    df["target_avg"] = df["lag_weighted_avg"] + noise
    df["target_obp"] = obp + noise
    df["target_slg"] = slg + rng.normal(0, 0.020, n)
    df["target_ops"] = df["target_obp"] + df["target_slg"]
    df["target_bb_pct"] = df["lag_weighted_bb_pct"] + rng.normal(0, 0.005, n)
    df["target_k_pct"] = df["lag_weighted_k_pct"] + rng.normal(0, 0.010, n)
    df["target_hr"] = (df["lag1_hr"] * rng.uniform(0.8, 1.2, n)).round()
    df["target_sb"] = (df["lag1_sb"] * rng.uniform(0.7, 1.3, n)).round()
    return df


def test_select_feature_columns_keeps_features_and_drops_ids_and_targets():
    matrix = _synthetic_matrix(2023, n=5)
    matrix["lag1_team"] = ["NYY"] * 5  # string column matching a feature prefix
    matrix["unrelated"] = 1.0  # numeric but outside the feature families

    cols = select_feature_columns(matrix)

    assert "age" in cols
    assert "lag1_pa" in cols
    assert "lag3_available" in cols
    assert "lag_weighted_obp" in cols
    assert "skill_lag1_barrel_rate" in cols
    assert "skill_lag1_available" in cols
    for excluded in (
        "player_id", "player_name", "primary_position", "year", "role",
        "target_obp", "lag1_team", "unrelated",
    ):
        assert excluded not in cols
    assert cols == sorted(cols)


def test_config_validation_rejects_bad_values():
    with pytest.raises(ValueError):
        XGBoostProjectionConfig(min_train_years=0)
    with pytest.raises(ValueError):
        XGBoostProjectionConfig(learning_rate=0.0)
    with pytest.raises(ValueError):
        XGBoostProjectionConfig(subsample=1.5)
    with pytest.raises(ValueError):
        XGBoostProjectionConfig(early_stopping_rounds=10, validation_years=0)


def test_modeled_target_stats_derives_ops():
    cfg = XGBoostProjectionConfig()
    batter_stats = modeled_target_stats("batter", cfg)
    assert "ops" not in batter_stats
    assert set(batter_stats) == set(ROLE_ALL_TARGETS["batter"]) - {"ops"}
    assert modeled_target_stats("pitcher", cfg) == ROLE_ALL_TARGETS["pitcher"]


def test_train_role_year_models_fits_per_stat_with_early_stopping():
    train = pd.concat(
        [_synthetic_matrix(2021), _synthetic_matrix(2022)], ignore_index=True
    )
    features = select_feature_columns(train)

    models, info = train_role_year_models(
        train, role="batter", feature_columns=features, config=_FAST_CONFIG
    )

    expected = set(modeled_target_stats("batter", _FAST_CONFIG))
    assert set(models) == expected
    assert info["train_years"] == [2021, 2022]
    assert info["eval_years"] == [2022]
    assert info["fit_years"] == [2021]
    for stat in expected:
        assert info["best_iterations"][stat] is not None
        assert info["eval_rmse"][stat] is not None
        assert info["n_rows"][stat] > 0
    assert info["skipped_stats"] == []


def test_train_role_year_models_skips_all_nan_target():
    train = pd.concat(
        [_synthetic_matrix(2021), _synthetic_matrix(2022)], ignore_index=True
    )
    train["target_sb"] = np.nan
    features = select_feature_columns(train)

    models, info = train_role_year_models(
        train, role="batter", feature_columns=features, config=_FAST_CONFIG
    )

    assert "sb" not in models
    assert info["skipped_stats"] == ["sb"]


def test_train_role_year_models_single_year_trains_without_eval_set():
    train = _synthetic_matrix(2021)
    features = select_feature_columns(train)

    models, info = train_role_year_models(
        train, role="batter", feature_columns=features, config=_FAST_CONFIG
    )

    assert set(models) == set(modeled_target_stats("batter", _FAST_CONFIG))
    assert info["eval_years"] == []
    assert all(v is None for v in info["best_iterations"].values())


def test_predict_role_year_schema_ops_and_all_nan_row():
    train = pd.concat(
        [_synthetic_matrix(2021), _synthetic_matrix(2022)], ignore_index=True
    )
    features = select_feature_columns(train)
    models, _ = train_role_year_models(
        train, role="batter", feature_columns=features, config=_FAST_CONFIG
    )

    target = _synthetic_matrix(2023, n=10)
    # One row with every feature missing must still get a finite prediction.
    target.loc[0, features] = np.nan

    proj = predict_role_year(
        target, models, role="batter", year=2023,
        feature_columns=features, config=_FAST_CONFIG,
    )

    assert list(proj["player_id"]) == list(target["player_id"])
    assert set(proj["year"]) == {2023}
    assert set(proj["role"]) == {"batter"}
    assert "player_name" in proj.columns and "primary_position" in proj.columns
    for stat in _BATTER_TARGETS:
        assert f"proj_{stat}" in proj.columns
        assert f"target_{stat}" in proj.columns
    assert proj["proj_ops"].equals(proj["proj_obp"] + proj["proj_slg"])
    assert np.isfinite(proj[[f"proj_{s}" for s in _BATTER_TARGETS]].to_numpy()).all()


def test_predict_role_year_skipped_stat_emits_nan_column():
    train = pd.concat(
        [_synthetic_matrix(2021), _synthetic_matrix(2022)], ignore_index=True
    )
    train["target_sb"] = np.nan
    features = select_feature_columns(train)
    models, _ = train_role_year_models(
        train, role="batter", feature_columns=features, config=_FAST_CONFIG
    )

    proj = predict_role_year(
        _synthetic_matrix(2023, n=5), models, role="batter", year=2023,
        feature_columns=features, config=_FAST_CONFIG,
    )

    assert proj["proj_sb"].isna().all()
    assert proj["proj_obp"].notna().all()


def test_postprocess_predictions_clips_at_zero():
    cfg = XGBoostProjectionConfig(n_jobs=1)
    pred = np.array([-1.5, 0.0, 2.5])
    assert list(_postprocess_predictions(pred, config=cfg)) == [0.0, 0.0, 2.5]

    no_clip = XGBoostProjectionConfig(n_jobs=1, clip_predictions_at_zero=False)
    assert list(_postprocess_predictions(pred, config=no_clip)) == [-1.5, 0.0, 2.5]


def _patch_s3(monkeypatch, matrices_by_key):
    """Route the module's S3 IO through in-memory fakes; returns the capture dicts."""
    parquet_writes: dict = {}
    object_writes: dict = {}

    def fake_read(bucket, key, **kwargs):
        df = matrices_by_key.get(key)
        return df.copy() if df is not None else None

    def fake_write(df, bucket, key, **kwargs):
        parquet_writes[key] = df.copy()

    class _DummyS3:
        def put_object(self, **kwargs):
            object_writes[kwargs["Key"]] = kwargs["Body"]

    module = "src.ml.boosted_trees.xgboost_projection"
    monkeypatch.setattr(f"{module}.read_parquet_from_s3", fake_read)
    monkeypatch.setattr(f"{module}.write_parquet_to_s3", fake_write)
    monkeypatch.setattr(f"{module}.get_s3_client", lambda: _DummyS3())
    return parquet_writes, object_writes


def test_build_xgboost_projections_walk_forward(monkeypatch):
    matrices = {
        f"gold/features/performance_prediction/batter/year={y}/training_matrix.parquet":
            _synthetic_matrix(y)
        for y in (2021, 2022, 2023)
    }
    parquet_writes, object_writes = _patch_s3(monkeypatch, matrices)

    result = build_xgboost_projections(
        bucket="bucket",
        gold_prefix="gold/features",
        predictions_prefix="gold/predictions",
        models_prefix="models",
        start_year=2021,
        end_year=2023,
        history_start_year=2019,
        role_filter="batter",
        config=_FAST_CONFIG,
    )

    # 2021 has no history and 2022 only one prior season; only 2023 is projectable.
    assert result["status"] == "ok"
    assert result["years_written"] == [2023]
    assert "batter/2021" in result["message"] and "batter/2022" in result["message"]

    proj_key = "gold/predictions/xgboost/batter/year=2023/xgboost_projections.parquet"
    assert set(parquet_writes) == {proj_key}
    proj = parquet_writes[proj_key]
    assert "proj_obp" in proj.columns and "target_obp" in proj.columns

    assert "models/xgboost/batter/year=2023/model.joblib" in object_writes
    meta = json.loads(
        object_writes["models/xgboost/batter/year=2023/metrics.json"].decode("utf-8")
    )
    assert meta["method"] == "xgboost_walk_forward"
    assert meta["train_years"] == [2021, 2022]
    assert meta["eval_years"] == [2022]
    assert "obp" in meta["metrics"]
    assert {"n", "mae", "rmse", "bias"} <= set(meta["metrics"]["obp"])
    assert meta["config"]["max_depth"] == 2


def test_build_xgboost_projections_rejects_bad_inputs():
    common = dict(
        bucket="b", gold_prefix="g", predictions_prefix="p", models_prefix="m"
    )
    assert build_xgboost_projections(
        **common, start_year=2025, end_year=2024
    )["status"] == "error"
    assert build_xgboost_projections(
        **common, start_year=2024, end_year=2025, role_filter="catcher"
    )["status"] == "error"
    assert build_xgboost_projections(
        **common, start_year=2024, end_year=2025, history_start_year=2024
    )["status"] == "error"


def test_build_xgboost_projections_no_data(monkeypatch):
    _patch_s3(monkeypatch, {})
    result = build_xgboost_projections(
        bucket="bucket",
        gold_prefix="gold/features",
        predictions_prefix="gold/predictions",
        models_prefix="models",
        start_year=2024,
        end_year=2025,
        role_filter="batter",
        config=_FAST_CONFIG,
    )
    assert result["status"] == "no_data"
    assert result["rows_written"] == 0
