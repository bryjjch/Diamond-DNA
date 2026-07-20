import json

import numpy as np
import pandas as pd
import pytest

from src.data_pipeline.gold.gold_performance_predict_preprocessing import (
    PerformancePredictConfig,
    build_performance_training_matrices,
    build_role_year_training_matrix,
)


def _batter_stats(player_ids, *, ops, pa, names=None):
    n = len(player_ids)
    return pd.DataFrame(
        {
            "player_id": player_ids,
            "full_name": names or [f"Player {p}" for p in player_ids],
            "g": [100] * n,
            "pa": pa,
            "avg": [0.270] * n,
            "obp": [0.340] * n,
            "slg": [0.450] * n,
            "ops": ops,
            "bb_pct": [0.09] * n,
            "k_pct": [0.22] * n,
            "hr": [20] * n,
            "sb": [5] * n,
        }
    )


def test_build_role_year_training_matrix_batter_lags_weights_age_skills_targets():
    target = _batter_stats([1, 2], ops=[0.850, 0.700], pa=[600, 450])
    lag1 = _batter_stats([1, 2], ops=[0.800, 0.720], pa=[600, 300])
    lag2 = _batter_stats([1], ops=[0.900], pa=[300])
    lag1_features = pd.DataFrame(
        {
            "player_id": [1, 2],
            "bio_age": [27.0, 30.0],
            "primary_position": ["RF", "C"],
            "barrel_rate": [0.11, 0.06],
            "whiff_rate": [0.24, 0.31],
            "launch_speed_mean": [90.5, 87.0],
        }
    )

    out, artifacts = build_role_year_training_matrix(
        target_stats=target,
        lag_stats={1: lag1, 2: lag2, 3: None},
        lag_features={1: lag1_features, 2: None, 3: None},
        role="batter",
        year=2025,
        config=PerformancePredictConfig(),
    )

    assert list(out["player_id"]) == [1, 2]
    assert set(out["year"]) == {2025}
    assert set(out["role"]) == {"batter"}

    p1 = out.loc[out["player_id"] == 1].iloc[0]
    # Lag stat columns come from the right seasons.
    assert p1["lag1_ops"] == pytest.approx(0.800)
    assert p1["lag2_ops"] == pytest.approx(0.900)
    assert np.isnan(p1["lag3_ops"])
    assert p1["lag1_available"] == 1.0
    assert p1["lag2_available"] == 1.0
    assert p1["lag3_available"] == 0.0

    # Playing-time * recency weighted blend: (0.8*5*600 + 0.9*4*300) / (5*600 + 4*300).
    expected = (0.8 * 5 * 600 + 0.9 * 4 * 300) / (5 * 600 + 4 * 300)
    assert p1["lag_weighted_ops"] == pytest.approx(expected)

    # Age at season N = bio_age at N-1 plus one; position rides along from silver.
    assert p1["age"] == pytest.approx(28.0)
    assert p1["primary_position"] == "RF"

    # Season N-1 Statcast skills, prefixed.
    assert p1["skill_lag1_barrel_rate"] == pytest.approx(0.11)
    assert p1["skill_lag1_whiff_rate"] == pytest.approx(0.24)
    assert p1["skill_lag1_available"] == 1.0
    # Skill columns absent from silver stay NaN and are recorded in metadata.
    assert np.isnan(p1["skill_lag1_sprint_speed_mean"])
    assert "sprint_speed_mean" in artifacts.skill_columns_missing_in_silver

    # Targets are the season-N stat line.
    assert p1["target_ops"] == pytest.approx(0.850)
    assert p1["target_pa"] == pytest.approx(600)
    assert artifacts.target_columns[0] == "target_pa"
    assert artifacts.row_count == 2

    # Player 2 only has lag1; the blend equals the lag1 value.
    p2 = out.loc[out["player_id"] == 2].iloc[0]
    assert p2["lag_weighted_ops"] == pytest.approx(0.720)


def test_build_role_year_training_matrix_drops_players_without_lag_history():
    target = _batter_stats([1, 99], ops=[0.850, 0.650], pa=[600, 200])
    lag1 = _batter_stats([1], ops=[0.800], pa=[500])

    out, artifacts = build_role_year_training_matrix(
        target_stats=target,
        lag_stats={1: lag1, 2: None, 3: None},
        lag_features={1: None, 2: None, 3: None},
        role="batter",
        year=2025,
        config=PerformancePredictConfig(),
    )

    # Player 99 (a rookie in season N) has no lag seasons and is dropped.
    assert list(out["player_id"]) == [1]
    assert artifacts.rows_dropped_no_lag_history == 1
    # With no silver features in any lag season, age/skills are NaN but flagged.
    assert np.isnan(out["age"].iloc[0])
    assert out["skill_lag1_available"].iloc[0] == 0.0


def test_build_role_year_training_matrix_pitcher_derives_innings():
    def pitcher_stats(era, ip):
        return pd.DataFrame(
            {
                "player_id": [7],
                "full_name": ["Ace"],
                "g": [30],
                "gs": [30],
                "ip": [ip],
                "era": [era],
                "whip": [1.10],
                "k_per_9": [9.5],
                "bb_per_9": [2.5],
                "hr_per_9": [1.0],
                "sv": [0],
            }
        )

    out, artifacts = build_role_year_training_matrix(
        target_stats=pitcher_stats(3.20, "180.1"),
        lag_stats={1: pitcher_stats(3.50, "150.2"), 2: None, 3: None},
        lag_features={1: None, 2: None, 3: None},
        role="pitcher",
        year=2025,
        config=PerformancePredictConfig(),
    )

    row = out.iloc[0]
    # statsapi .1/.2 thirds notation -> true innings.
    assert row["target_innings"] == pytest.approx(180 + 1 / 3)
    assert row["lag1_innings"] == pytest.approx(150 + 2 / 3)
    assert row["lag1_era"] == pytest.approx(3.50)
    assert row["lag_weighted_era"] == pytest.approx(3.50)
    assert artifacts.playing_time_column == "innings"


def test_build_performance_training_matrices_writes_matrix_and_metadata(monkeypatch):
    target = _batter_stats([1], ops=[0.850], pa=[600])
    lag1 = _batter_stats([1], ops=[0.800], pa=[500])

    def fake_read(bucket, key, **kwargs):
        if "batter/year=2025/standard_stats_player_year.parquet" in key:
            return target.copy()
        if "batter/year=2024/standard_stats_player_year.parquet" in key:
            return lag1.copy()
        return None

    writes: list[tuple[str, pd.DataFrame]] = []
    metadata_writes: dict[str, dict] = {}

    def fake_write(df, bucket, key, **kwargs):
        writes.append((key, df.copy()))

    class _DummyS3:
        def put_object(self, **kwargs):
            metadata_writes[kwargs["Key"]] = json.loads(kwargs["Body"].decode("utf-8"))

    module = "src.data_pipeline.gold.gold_performance_predict_preprocessing"
    monkeypatch.setattr(f"{module}.read_parquet_from_s3", fake_read)
    monkeypatch.setattr(f"{module}.write_parquet_to_s3", fake_write)
    monkeypatch.setattr(f"{module}.get_s3_client", lambda: _DummyS3())

    result = build_performance_training_matrices(
        bucket="bucket",
        silver_prefix="silver",
        gold_prefix="gold/features",
        start_year=2025,
        end_year=2025,
        role_filter="batter",
    )

    assert result["status"] == "ok"
    assert result["rows_written"] == 1
    assert len(writes) == 1
    out_key, out_df = writes[0]
    assert (
        "gold/features/performance_prediction/batter/year=2025/training_matrix.parquet"
        in out_key
    )
    assert "target_ops" in out_df.columns
    assert "lag1_ops" in out_df.columns
    assert any("training_matrix_metadata.json" in k for k in metadata_writes)
    meta = next(iter(metadata_writes.values()))
    assert meta["role"] == "batter"
    assert meta["n_lag_seasons"] == 3


def test_build_performance_training_matrices_rejects_bad_inputs():
    result = build_performance_training_matrices(
        bucket="b",
        silver_prefix="silver",
        gold_prefix="gold",
        start_year=2025,
        end_year=2024,
    )
    assert result["status"] == "error"

    result = build_performance_training_matrices(
        bucket="b",
        silver_prefix="silver",
        gold_prefix="gold",
        start_year=2024,
        end_year=2025,
        role_filter="catcher",
    )
    assert result["status"] == "error"
