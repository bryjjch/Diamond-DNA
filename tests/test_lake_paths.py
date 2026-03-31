from datetime import date

from src.pipeline.lake_paths import (
    DEFENCE_OAA_PARQUET,
    feature_player_year_output_key,
    processed_statcast_list_prefix,
    processed_statcast_player_year_key,
    raw_defence_dataset_key,
    raw_sprint_speed_key,
    raw_statcast_day_key,
)


def test_raw_statcast_day_key():
    d = date(2024, 7, 4)
    assert raw_statcast_day_key("raw-data/statcast", d) == (
        "raw-data/statcast/year=2024/date=2024-07-04/statcast_pitches.parquet"
    )


def test_raw_statcast_day_key_strips_outer_slashes():
    d = date(2024, 1, 1)
    k = raw_statcast_day_key("/my/prefix/", d)
    assert k == "my/prefix/year=2024/date=2024-01-01/statcast_pitches.parquet"


def test_processed_statcast_player_year_key():
    assert processed_statcast_player_year_key("proc", "batter", 592450, 2023) == (
        "proc/batter/batter_id=592450/year=2023/statcast_pitches.parquet"
    )


def test_processed_statcast_list_prefix():
    assert processed_statcast_list_prefix("proc", "pitcher") == "proc/pitcher/pitcher_id="


def test_feature_player_year_output_key():
    assert feature_player_year_output_key("feat", "batter", 2022) == (
        "feat/batter/year=2022/player_year_features.parquet"
    )


def test_raw_sprint_speed_key():
    assert raw_sprint_speed_key("raw/run", 2025) == "raw/run/year=2025/statcast_sprint_speed.parquet"


def test_raw_defence_dataset_key():
    assert raw_defence_dataset_key("raw/def", 2024, DEFENCE_OAA_PARQUET) == (
        "raw/def/year=2024/statcast_oaa.parquet"
    )
