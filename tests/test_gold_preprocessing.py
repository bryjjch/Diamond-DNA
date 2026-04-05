import json

import numpy as np
import pandas as pd

from src.gold.silver_to_gold_preprocessing import (
    GoldPreprocessingConfig,
    build_silver_to_gold_preprocessing,
    is_column_excluded_from_archetype_training_features,
    preprocess_role_year_df,
)


def test_preprocess_role_year_df_imputes_sparse_columns_and_drops_pt():
    # Enough rows/variety that correlation and NZV pruning do not drop catcher columns.
    df = pd.DataFrame(
        {
            "player_id": [1, 2, 3, 4],
            "year": [2024, 2024, 2024, 2024],
            "role": ["batter"] * 4,
            "pt_FF_release_speed_mean": [np.nan, 93.5, 92.0, 94.0],
            "def_pop_time_2b_sec": [np.nan, 1.95, 2.0, 1.9],
            "def_framing_runs": [np.nan, 3.2, 2.0, 4.0],
            "estimated_woba_using_speedangle_mean": [0.31, np.nan, 0.33, 0.29],
            "woba_value_mean": [0.3, 0.33, 0.32, 0.31],
        }
    )

    out, artifacts = preprocess_role_year_df(
        df, role="batter", year=2024, config=GoldPreprocessingConfig()
    )

    assert "woba_value_mean" not in out.columns
    assert "pt_FF_release_speed_mean" not in out.columns
    assert not any(c.endswith("_was_missing") for c in out.columns)
    assert "def_pop_time_2b_sec" in out.columns
    assert "def_framing_runs" in out.columns
    assert float(out["estimated_woba_using_speedangle_mean"].iloc[1]) == 0.0
    assert artifacts.hard_dropped_columns == ["woba_value_mean"]
    assert (
        "pt_FF_release_speed_mean" in artifacts.archetype_training_dropped_columns
        or "pt_FF_release_speed_mean" in artifacts.dropped_columns.get("correlation_drop", [])
    )


def test_preprocess_role_year_df_drops_archetype_non_features():
    df = pd.DataFrame(
        {
            "player_id": [1, 2, 3],
            "year": [2024, 2024, 2024],
            "role": ["pitcher"] * 3,
            "pitch_type_FF_share": [0.5, 0.4, 0.45],
            "pitch_type_other_share": [0.05, 0.06, 0.04],
            "pitch_type_entropy": [1.2, 1.3, 1.25],
            "xwoba_allowed_lhb_mean": [0.3, 0.29, 0.31],
            "xwoba_allowed_rhb_mean": [0.31, 0.30, 0.32],
        }
    )
    out, artifacts = preprocess_role_year_df(
        df,
        role="pitcher",
        year=2024,
        config=GoldPreprocessingConfig(
            correlation_threshold=1.01,
            near_zero_variance_unique_ratio=0.0,
        ),
    )
    assert "pitch_type_FF_share" not in out.columns
    assert "xwoba_allowed_lhb_mean" not in out.columns
    assert "pitch_type_entropy" in out.columns
    assert "pitch_type_other_share" in out.columns
    assert "pitch_type_FF_share" in artifacts.archetype_training_dropped_columns


def test_is_column_excluded_from_archetype_training_features_keeps_ids():
    assert not is_column_excluded_from_archetype_training_features("player_id")
    assert is_column_excluded_from_archetype_training_features("pt_FF_spin_rate_mean")
    assert is_column_excluded_from_archetype_training_features("col_was_missing")


def test_preprocess_role_year_df_prefers_estimated_when_correlated():
    df = pd.DataFrame(
        {
            "player_id": [1, 2, 3, 4],
            "year": [2024, 2024, 2024, 2024],
            "role": ["batter"] * 4,
            "estimated_woba_using_speedangle_mean": [0.3, 0.32, 0.34, 0.36],
            "actual_quality_mean": [0.3, 0.32, 0.34, 0.36],
        }
    )

    out, artifacts = preprocess_role_year_df(
        df,
        role="batter",
        year=2024,
        config=GoldPreprocessingConfig(correlation_threshold=0.9, near_zero_variance_unique_ratio=0.0),
    )

    assert "estimated_woba_using_speedangle_mean" in out.columns
    assert "actual_quality_mean" not in out.columns
    assert artifacts.dropped_columns["correlation_drop"] == ["actual_quality_mean"]


def test_build_silver_to_gold_preprocessing_writes_gold_and_metadata(monkeypatch):
    silver_df = pd.DataFrame(
        {
            "player_id": [99],
            "year": [2025],
            "role": ["pitcher"],
            "feature_a": [1.0],
            "woba_value_mean": [0.4],
        }
    )

    writes: list[tuple[str, pd.DataFrame]] = []
    metadata_writes: dict[str, dict] = {}

    def fake_read(bucket, key, **kwargs):
        if "pitcher/year=2025" in key:
            return silver_df.copy()
        return None

    def fake_write(df, bucket, key, **kwargs):
        writes.append((key, df.copy()))

    class _DummyS3:
        def put_object(self, **kwargs):
            metadata_writes[kwargs["Key"]] = json.loads(kwargs["Body"].decode("utf-8"))

    monkeypatch.setattr("src.gold.silver_to_gold_preprocessing.read_parquet_from_s3", fake_read)
    monkeypatch.setattr("src.gold.silver_to_gold_preprocessing.write_parquet_to_s3", fake_write)
    monkeypatch.setattr("src.gold.silver_to_gold_preprocessing.get_s3_client", lambda: _DummyS3())

    result = build_silver_to_gold_preprocessing(
        bucket="bucket",
        silver_prefix="silver",
        gold_prefix="gold/statcast",
        start_year=2025,
        end_year=2025,
        role_filter="pitcher",
    )

    assert result["status"] == "ok"
    assert len(writes) == 1
    out_key, out_df = writes[0]
    assert "gold/statcast/pitcher/year=2025/player_year_features_preprocessed.parquet" in out_key
    assert "woba_value_mean" not in out_df.columns
    assert any("preprocessing_metadata.json" in k for k in metadata_writes)
