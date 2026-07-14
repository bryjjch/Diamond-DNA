"""Tests for the standard stat-line silver derivations.

The S3 read/write is monkeypatched so no network is touched; we exercise the
innings-pitched conversion and the derived rate-stat math, which are the parts
most prone to subtle bugs.
"""

import numpy as np
import pandas as pd

from src.data_pipeline.silver import standard_stats_player_year as sss


def test_outs_from_innings_pitched_thirds():
    ip = pd.Series([12.0, 12.1, 12.2, 0.1, 0.2, float("nan")])
    outs = sss.outs_from_innings_pitched(ip)
    # 12.1 = 12 innings + 1 out = 37 outs; 12.2 = 38 outs; 0.1 = 1 out; 0.2 = 2 outs.
    assert list(outs[:5]) == [36.0, 37.0, 38.0, 1.0, 2.0]
    assert np.isnan(outs.iloc[5])


def test_derive_pitching_per9_uses_true_innings():
    # 9 full innings, 9 K, 3 BB, 1 HR -> exactly 9.0 / 3.0 / 1.0 per 9.
    df = pd.DataFrame({"ip": [9.0], "so": [9], "bb": [3], "hr": [1]})
    out = sss._derive_pitching(df)
    assert out["k_per_9"].iloc[0] == 9.0
    assert out["bb_per_9"].iloc[0] == 3.0
    assert out["hr_per_9"].iloc[0] == 1.0


def test_derive_pitching_fractional_innings():
    # 12.2 IP = 38 outs = 12.6667 innings; 19 K -> 19 / (38/3) * 9 = 13.5 K/9.
    df = pd.DataFrame({"ip": [12.2], "so": [19], "bb": [0], "hr": [0]})
    out = sss._derive_pitching(df)
    assert out["k_per_9"].iloc[0] == 13.5


def test_derive_batting_rate_stats():
    df = pd.DataFrame({"pa": [100], "bb": [10], "so": [25]})
    out = sss._derive_batting(df)
    assert out["bb_pct"].iloc[0] == 0.10
    assert out["k_pct"].iloc[0] == 0.25


def test_build_standard_stats_for_year_writes_per_role(monkeypatch):
    batting = pd.DataFrame({"player_id": [1, 2], "pa": [100, 200], "bb": [10, 20], "so": [25, 40]})
    pitching = pd.DataFrame({"player_id": [3], "ip": [9.0], "so": [9], "bb": [3], "hr": [1]})

    def fake_read(bucket, key, **kwargs):
        return batting if "batting" in key else pitching

    writes = {}

    def fake_write(df, bucket, key, **kwargs):
        writes[key] = df

    monkeypatch.setattr(sss, "read_parquet_from_s3", fake_read)
    monkeypatch.setattr(sss, "write_parquet_to_s3", fake_write)

    written = sss.build_standard_stats_for_year("b", "bronze/standard_stats", "silver", 2025)

    assert written == {"batter": 2, "pitcher": 1}
    assert any("batter/year=2025" in k for k in writes)
    assert any("pitcher/year=2025" in k for k in writes)
    # year column is stamped and player_id is coerced to int.
    batter_out = next(v for k, v in writes.items() if "batter" in k)
    assert (batter_out["year"] == 2025).all()
    assert batter_out["player_id"].dtype == np.dtype("int64")


def test_build_standard_stats_for_year_skips_missing(monkeypatch):
    monkeypatch.setattr(sss, "read_parquet_from_s3", lambda *a, **k: None)
    monkeypatch.setattr(sss, "write_parquet_to_s3", lambda *a, **k: None)
    written = sss.build_standard_stats_for_year("b", "bronze/standard_stats", "silver", 2025)
    assert written == {}


def test_build_standard_stats_range_no_data(monkeypatch):
    monkeypatch.setattr(sss, "read_parquet_from_s3", lambda *a, **k: None)
    monkeypatch.setattr(sss, "write_parquet_to_s3", lambda *a, **k: None)
    result = sss.build_standard_stats_range("b", "bronze/standard_stats", "silver", 2024, 2025)
    assert result["status"] == "no_data"
    assert result["rows_written"] == 0
