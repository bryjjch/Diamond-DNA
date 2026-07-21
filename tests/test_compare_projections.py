import io
import json

import pytest

from src.ml.evaluation.compare_projections import (
    compare_model_metrics,
    load_model_metrics,
    summarize_comparison,
    write_comparison_reports,
)


def _metrics_payload(role, year, stats):
    return {
        "role": role,
        "year": year,
        "metrics": {
            stat: {
                "n": 100,
                "mae": mae,
                "rmse": rmse,
                "bias": bias,
                "actual_mean": 0.0,
                "proj_mean": 0.0,
            }
            for stat, (mae, rmse, bias) in stats.items()
        },
    }


class _NoSuchKey(Exception):
    pass


class _DummyS3:
    """In-memory get/put stand-in exposing the boto3 NoSuchKey surface."""

    def __init__(self, objects):
        self.objects = objects
        self.exceptions = type("E", (), {"NoSuchKey": _NoSuchKey})

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise _NoSuchKey(Key)
        return {"Body": io.BytesIO(json.dumps(self.objects[Key]).encode("utf-8"))}

    def put_object(self, **kwargs):
        self.objects[kwargs["Key"]] = json.loads(kwargs["Body"].decode("utf-8"))


@pytest.fixture
def s3_objects():
    return {
        "models/marcel/batter/year=2023/metrics.json": _metrics_payload(
            "batter", 2023,
            {"obp": (0.020, 0.030, 0.001), "hr": (5.0, 7.0, -0.5)},
        ),
        "models/xgboost/batter/year=2023/metrics.json": _metrics_payload(
            "batter", 2023,
            {"obp": (0.016, 0.024, 0.002), "hr": (5.5, 7.7, 0.3)},
        ),
        # 2024: only Marcel exists -> excluded from the comparison.
        "models/marcel/batter/year=2024/metrics.json": _metrics_payload(
            "batter", 2024, {"obp": (0.021, 0.031, 0.0)},
        ),
    }


@pytest.fixture
def patched_s3(monkeypatch, s3_objects):
    client = _DummyS3(s3_objects)
    monkeypatch.setattr(
        "src.ml.evaluation.compare_projections.get_s3_client", lambda: client
    )
    return client


def test_load_model_metrics_returns_payload_or_none(patched_s3):
    meta = load_model_metrics(
        bucket="b", models_prefix="models", model="marcel", role="batter", year=2023
    )
    assert meta["metrics"]["obp"]["mae"] == 0.020

    assert load_model_metrics(
        bucket="b", models_prefix="models", model="xgboost", role="batter", year=2019
    ) is None


def test_compare_model_metrics_deltas_and_missing_years(patched_s3):
    df = compare_model_metrics(
        bucket="b",
        models_prefix="models",
        start_year=2023,
        end_year=2024,
        role_filter="batter",
    )

    # 2024 has no xgboost metrics, so only 2023's two stats are compared.
    assert sorted(df["stat"]) == ["hr", "obp"]
    assert set(df["year"]) == {2023}

    obp = df[df["stat"] == "obp"].iloc[0]
    assert obp["mae_delta"] == pytest.approx(0.016 - 0.020)
    assert obp["mae_pct_improvement"] == pytest.approx(20.0)
    assert obp["rmse_pct_improvement"] == pytest.approx(20.0)
    assert bool(obp["candidate_wins_mae"]) is True

    hr = df[df["stat"] == "hr"].iloc[0]
    assert hr["mae_delta"] == pytest.approx(0.5)
    assert hr["mae_pct_improvement"] == pytest.approx(-10.0)
    assert bool(hr["candidate_wins_mae"]) is False


def test_compare_model_metrics_empty_when_nothing_matches(patched_s3):
    df = compare_model_metrics(
        bucket="b",
        models_prefix="models",
        start_year=2019,
        end_year=2020,
    )
    assert df.empty


def test_summarize_comparison_win_rate(patched_s3):
    df = compare_model_metrics(
        bucket="b",
        models_prefix="models",
        start_year=2023,
        end_year=2023,
        role_filter="batter",
    )
    summary = summarize_comparison(df)

    assert set(summary["stat"]) == {"obp", "hr"}
    obp = summary[summary["stat"] == "obp"].iloc[0]
    assert obp["n_years"] == 1
    assert obp["win_rate_mae"] == pytest.approx(1.0)
    hr = summary[summary["stat"] == "hr"].iloc[0]
    assert hr["win_rate_mae"] == pytest.approx(0.0)


def test_summarize_comparison_empty_frame():
    import pandas as pd

    summary = summarize_comparison(pd.DataFrame())
    assert summary.empty
    assert "win_rate_mae" in summary.columns


def test_write_comparison_reports(patched_s3):
    df = compare_model_metrics(
        bucket="b",
        models_prefix="models",
        start_year=2023,
        end_year=2023,
        role_filter="batter",
    )
    written = write_comparison_reports(
        df,
        bucket="b",
        models_prefix="models",
        baseline_model="marcel",
        candidate_model="xgboost",
    )

    assert written == ["models/evaluation/batter/year=2023/xgboost_vs_marcel.json"]
    report = patched_s3.objects[written[0]]
    assert report["baseline_model"] == "marcel"
    assert report["candidate_model"] == "xgboost"
    assert {s["stat"] for s in report["stats"]} == {"obp", "hr"}
