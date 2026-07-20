import json

import numpy as np
import pandas as pd
import pytest

from src.ml.baselines.marcel_baseline import (
    MarcelConfig,
    _age_factor,
    _league_rate,
    _regress_to_mean,
    _weighted_count_rate,
    _weighted_playing_time,
    build_marcel_baseline,
    project_role_year,
    score_projections,
)


def _batter_matrix(rows):
    """Build a minimal gold-style training matrix for the columns Marcel reads."""
    return pd.DataFrame(rows)


def test_age_factor_two_slope_and_unknown_age():
    cfg = MarcelConfig()
    age = pd.Series([27.0, 29.0, 31.0, np.nan])
    factor = _age_factor(age, cfg)
    # +0.6%/yr below the peak, -0.3%/yr above it, exactly 1 at the peak.
    assert factor.iloc[0] == pytest.approx(1.0 + 2 * 0.006)
    assert factor.iloc[1] == pytest.approx(1.0)
    assert factor.iloc[2] == pytest.approx(1.0 - 2 * 0.003)
    # Unknown age is neutral, never dropped.
    assert factor.iloc[3] == pytest.approx(1.0)


def test_regress_to_mean_shrinks_low_pt_and_collapses_missing():
    weighted_pt = pd.Series([7200.0, 1200.0, 0.0])
    rate = pd.Series([0.300, 0.200, np.nan])
    league = 0.260
    out = _regress_to_mean(rate, weighted_pt, league, regression_pt=1200.0)
    # Full-timer barely moves; part-timer pulled halfway; missing collapses to league.
    assert out.iloc[0] == pytest.approx((0.300 * 7200 + 0.260 * 1200) / (7200 + 1200))
    assert out.iloc[1] == pytest.approx((0.200 * 1200 + 0.260 * 1200) / (1200 + 1200))
    assert out.iloc[2] == pytest.approx(league)
    # Regression toward the mean: closer to league than the raw rate for the part-timer.
    assert abs(out.iloc[1] - league) < abs(0.200 - league)


def test_league_rate_is_playing_time_weighted():
    rate = pd.Series([0.300, 0.200])
    weighted_pt = pd.Series([7200.0, 1200.0])
    expected = (0.300 * 7200 + 0.200 * 1200) / (7200 + 1200)
    assert _league_rate(rate, weighted_pt) == pytest.approx(expected)


def test_weighted_playing_time_and_count_rate():
    cfg = MarcelConfig()
    df = _batter_matrix(
        [
            {"lag1_pa": 600, "lag2_pa": 600, "lag3_pa": 600, "lag1_hr": 30, "lag2_hr": 24, "lag3_hr": 18},
        ]
    )
    wpt = _weighted_playing_time(df, role="batter", config=cfg)
    assert wpt.iloc[0] == pytest.approx(5 * 600 + 4 * 600 + 3 * 600)
    hr_rate = _weighted_count_rate(df, "hr", role="batter", config=cfg)
    expected = (5 * 30 + 4 * 24 + 3 * 18) / (5 * 600 + 4 * 600 + 3 * 600)
    assert hr_rate.iloc[0] == pytest.approx(expected)


def test_project_role_year_batter_regression_age_counts_and_ops():
    cfg = MarcelConfig()
    # Two players: a full-time high-OBP hitter and a part-time low-OBP hitter.
    df = _batter_matrix(
        [
            {
                "player_id": 1,
                "player_name": "Star",
                "age": 27.0,
                "lag1_pa": 600, "lag2_pa": 600, "lag3_pa": 600,
                "lag1_hr": 30, "lag2_hr": 30, "lag3_hr": 30,
                "lag1_sb": 10, "lag2_sb": 10, "lag3_sb": 10,
                "lag_weighted_avg": 0.300,
                "lag_weighted_obp": 0.400,
                "lag_weighted_slg": 0.550,
                "lag_weighted_bb_pct": 0.12,
                "lag_weighted_k_pct": 0.18,
                "target_pa": 640, "target_obp": 0.395, "target_hr": 32,
            },
            {
                "player_id": 2,
                "player_name": "Scrub",
                "age": 31.0,
                "lag1_pa": 100, "lag2_pa": 100, "lag3_pa": 100,
                "lag1_hr": 2, "lag2_hr": 2, "lag3_hr": 2,
                "lag1_sb": 1, "lag2_sb": 1, "lag3_sb": 1,
                "lag_weighted_avg": 0.220,
                "lag_weighted_obp": 0.280,
                "lag_weighted_slg": 0.330,
                "lag_weighted_bb_pct": 0.06,
                "lag_weighted_k_pct": 0.28,
                "target_pa": 90, "target_obp": 0.290, "target_hr": 1,
            },
        ]
    )

    proj, league = project_role_year(df, role="batter", year=2025, config=cfg)

    assert list(proj["player_id"]) == [1, 2]
    assert set(proj["role"]) == {"batter"}
    assert set(proj["year"]) == {2025}

    # League OBP is playing-time weighted, so the full-timer dominates it.
    wpt1, wpt2 = 12 * 600, 12 * 100
    exp_league_obp = (0.400 * wpt1 + 0.280 * wpt2) / (wpt1 + wpt2)
    assert league["obp"] == pytest.approx(exp_league_obp)

    # Player 1 OBP: light regression (heavy PT) then a young-age boost.
    reg1 = (0.400 * wpt1 + exp_league_obp * 1200) / (wpt1 + 1200)
    assert proj["proj_obp"].iloc[0] == pytest.approx(reg1 * (1.0 + 2 * 0.006))
    # The part-timer is pulled hard toward the league mean.
    reg2 = (0.280 * wpt2 + exp_league_obp * 1200) / (wpt2 + 1200)
    assert proj["proj_obp"].iloc[1] == pytest.approx(reg2 * (1.0 - 2 * 0.003))

    # OPS stays internally consistent as OBP + SLG.
    assert proj["proj_ops"].iloc[0] == pytest.approx(
        proj["proj_obp"].iloc[0] + proj["proj_slg"].iloc[0]
    )

    # k_pct is lower-is-better: the young player's age factor should *reduce* it.
    league_k = league["k_pct"]
    reg_k1 = (0.18 * wpt1 + league_k * 1200) / (wpt1 + 1200)
    assert proj["proj_k_pct"].iloc[0] == pytest.approx(reg_k1 / (1.0 + 2 * 0.006))

    # Playing time: 0.5*lag1 + 0.1*lag2 + 200.
    assert proj["proj_pa"].iloc[0] == pytest.approx(0.5 * 600 + 0.1 * 600 + 200)

    # Counting HR = regressed+aged per-PA rate * projected PA, and rides with actuals.
    hr_rate1 = 30 / 600.0  # equal lag counts/PA -> weighted rate is 30/600
    league_hr = league["hr_per_pa"]
    reg_hr1 = (hr_rate1 * wpt1 + league_hr * 1200) / (wpt1 + 1200)
    exp_hr1 = reg_hr1 * (1.0 + 2 * 0.006) * proj["proj_pa"].iloc[0]
    assert proj["proj_hr"].iloc[0] == pytest.approx(exp_hr1)
    assert proj["target_hr"].iloc[0] == 32


def test_project_role_year_missing_age_and_rate_are_handled():
    cfg = MarcelConfig()
    df = _batter_matrix(
        [
            {
                "player_id": 3,
                "age": np.nan,  # unknown age -> neutral factor
                "lag1_pa": 300, "lag2_pa": np.nan, "lag3_pa": np.nan,
                "lag1_hr": 5, "lag2_hr": np.nan, "lag3_hr": np.nan,
                "lag1_sb": 0, "lag2_sb": np.nan, "lag3_sb": np.nan,
                "lag_weighted_avg": 0.250,
                "lag_weighted_obp": 0.320,
                "lag_weighted_slg": 0.400,
                "lag_weighted_bb_pct": np.nan,  # missing rate -> fully regressed to league
                "lag_weighted_k_pct": 0.25,
            },
            {
                "player_id": 4,
                "age": 28.0,
                "lag1_pa": 500, "lag2_pa": 500, "lag3_pa": 500,
                "lag1_hr": 20, "lag2_hr": 20, "lag3_hr": 20,
                "lag1_sb": 8, "lag2_sb": 8, "lag3_sb": 8,
                "lag_weighted_avg": 0.270,
                "lag_weighted_obp": 0.340,
                "lag_weighted_slg": 0.450,
                "lag_weighted_bb_pct": 0.09,
                "lag_weighted_k_pct": 0.22,
            },
        ]
    )
    proj, league = project_role_year(df, role="batter", year=2025, config=cfg)
    # A missing rate collapses to the league mean (defined by the other player).
    assert proj["proj_bb_pct"].iloc[0] == pytest.approx(league["bb_pct"])
    assert np.isfinite(proj["proj_obp"].iloc[0])
    # Unknown age means no age adjustment was applied: proj_avg is the pure regression.
    wpt = 5 * 300
    exp_avg = (0.250 * wpt + league["avg"] * 1200) / (wpt + 1200)
    assert proj["proj_avg"].iloc[0] == pytest.approx(exp_avg)


def test_project_role_year_pitcher_lower_is_better_and_innings():
    cfg = MarcelConfig()
    df = pd.DataFrame(
        [
            {
                "player_id": 7,
                "age": 25.0,  # young -> should be projected to improve (lower ERA)
                "lag1_innings": 180, "lag2_innings": 180, "lag3_innings": 180,
                "lag_weighted_era": 3.20,
                "lag_weighted_whip": 1.05,
                "lag_weighted_k_per_9": 10.0,
                "lag_weighted_bb_per_9": 2.0,
                "lag_weighted_hr_per_9": 1.0,
                "target_innings": 175, "target_era": 3.10,
            },
            {
                "player_id": 8,
                "age": 34.0,
                "lag1_innings": 60, "lag2_innings": 60, "lag3_innings": 60,
                "lag_weighted_era": 5.00,
                "lag_weighted_whip": 1.45,
                "lag_weighted_k_per_9": 7.0,
                "lag_weighted_bb_per_9": 4.0,
                "lag_weighted_hr_per_9": 1.6,
                "target_innings": 55, "target_era": 5.20,
            },
        ]
    )
    proj, league = project_role_year(df, role="pitcher", year=2025, config=cfg)

    # Innings projection: 0.5*lag1 + 0.1*lag2 + 60.
    assert proj["proj_innings"].iloc[0] == pytest.approx(0.5 * 180 + 0.1 * 180 + 60)

    # ERA is lower-is-better: the young ace's age factor should push ERA *down*
    # relative to his regressed value; the old arm's should push it up.
    wpt1 = 12 * 180
    reg_era1 = (3.20 * wpt1 + league["era"] * 130) / (wpt1 + 130)
    assert proj["proj_era"].iloc[0] == pytest.approx(reg_era1 / (1.0 + 4 * 0.006))
    assert proj["proj_era"].iloc[0] < reg_era1
    # k_per_9 is higher-is-better: young pitcher's factor lifts it.
    assert proj["proj_k_per_9"].iloc[0] > (
        (10.0 * wpt1 + league["k_per_9"] * 130) / (wpt1 + 130)
    )


def test_score_projections_reports_error_metrics():
    proj = pd.DataFrame(
        {
            "proj_obp": [0.350, 0.300],
            "target_obp": [0.360, 0.280],
            "proj_hr": [20.0, np.nan],
            "target_hr": [18, 12],
        }
    )
    metrics = score_projections(proj, role="batter")
    assert metrics["obp"]["n"] == 2
    assert metrics["obp"]["mae"] == pytest.approx((0.010 + 0.020) / 2)
    assert metrics["obp"]["bias"] == pytest.approx(((-0.010) + 0.020) / 2)
    # HR has one missing projection, so only the one complete pair is scored.
    assert metrics["hr"]["n"] == 1


def test_build_marcel_baseline_writes_projections_and_metrics(monkeypatch):
    matrix = pd.DataFrame(
        [
            {
                "player_id": 1,
                "player_name": "Star",
                "age": 27.0,
                "lag1_pa": 600, "lag2_pa": 600, "lag3_pa": 600,
                "lag1_hr": 30, "lag2_hr": 30, "lag3_hr": 30,
                "lag1_sb": 5, "lag2_sb": 5, "lag3_sb": 5,
                "lag_weighted_avg": 0.300,
                "lag_weighted_obp": 0.400,
                "lag_weighted_slg": 0.550,
                "lag_weighted_bb_pct": 0.12,
                "lag_weighted_k_pct": 0.18,
                "target_pa": 640, "target_obp": 0.395, "target_hr": 31,
            }
        ]
    )

    def fake_read(bucket, key, **kwargs):
        if "performance_prediction/batter/year=2025/training_matrix.parquet" in key:
            return matrix.copy()
        return None

    writes: list = []
    metadata_writes: dict = {}

    def fake_write(df, bucket, key, **kwargs):
        writes.append((key, df.copy()))

    class _DummyS3:
        def put_object(self, **kwargs):
            metadata_writes[kwargs["Key"]] = json.loads(kwargs["Body"].decode("utf-8"))

    module = "src.ml.baselines.marcel_baseline"
    monkeypatch.setattr(f"{module}.read_parquet_from_s3", fake_read)
    monkeypatch.setattr(f"{module}.write_parquet_to_s3", fake_write)
    monkeypatch.setattr(f"{module}.get_s3_client", lambda: _DummyS3())

    result = build_marcel_baseline(
        bucket="bucket",
        gold_prefix="gold/features",
        predictions_prefix="gold/predictions",
        models_prefix="models",
        start_year=2025,
        end_year=2025,
        role_filter="batter",
    )

    assert result["status"] == "ok"
    assert result["rows_written"] == 1
    assert len(writes) == 1
    out_key, out_df = writes[0]
    assert out_key == "gold/predictions/marcel/batter/year=2025/marcel_projections.parquet"
    assert "proj_obp" in out_df.columns and "proj_hr" in out_df.columns
    assert "target_obp" in out_df.columns  # actuals carried alongside for scoring

    assert "models/marcel/batter/year=2025/metrics.json" in metadata_writes
    meta = next(iter(metadata_writes.values()))
    assert meta["role"] == "batter"
    assert meta["method"] == "marcel_the_monkey_baseline"
    assert "obp" in meta["metrics"]
    assert meta["config"]["recency_weights"] == [5.0, 4.0, 3.0]


def test_build_marcel_baseline_rejects_bad_inputs():
    assert build_marcel_baseline(
        bucket="b", gold_prefix="g", predictions_prefix="p", models_prefix="m",
        start_year=2025, end_year=2024
    )["status"] == "error"
    assert build_marcel_baseline(
        bucket="b", gold_prefix="g", predictions_prefix="p", models_prefix="m",
        start_year=2024, end_year=2025, role_filter="catcher"
    )["status"] == "error"
