import json

import numpy as np
import pandas as pd

from src.gold.silver_to_gold_preprocessing import (
    GoldPreprocessingConfig,
    build_silver_to_gold_preprocessing,
    preprocess_role_year_df,
)


def test_preprocess_role_year_df_imputes_and_adds_missing_indicators():
    df = pd.DataFrame(
        {
            "player_id": [1, 2],
            "year": [2024, 2024],
            "role": ["batter", "batter"],
            "pt_FF_release_speed_mean": [np.nan, 93.5],
            "def_pop_time_2b_sec": [np.nan, 1.95],
            "def_framing_runs": [np.nan, 3.2],
            "estimated_woba_using_speedangle_mean": [0.31, np.nan],
            "woba_value_mean": [0.3, 0.33],
        }
    )

    out, artifacts = preprocess_role_year_df(
        df, role="batter", year=2024, config=GoldPreprocessingConfig()
    )

    assert "woba_value_mean" not in out.columns
    assert out["pt_FF_release_speed_mean_was_missing"].tolist() == [1, 0]
    assert out["def_pop_time_2b_sec_was_missing"].tolist() == [1, 0]
    assert out["def_framing_runs_was_missing"].tolist() == [1, 0]
    assert float(out["estimated_woba_using_speedangle_mean"].iloc[1]) == 0.0
    assert artifacts.hard_dropped_columns == ["woba_value_mean"]


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
