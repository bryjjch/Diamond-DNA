from datetime import date

from src.pipeline.s3_interaction import (
    DEFENCE_OAA_PARQUET,
    feature_player_year_output_key,
    gold_archetype_assignments_key,
    gold_archetype_clustering_metadata_key,
    gold_archetype_clustering_model_key,
    gold_player_similar_neighbors_key,
    gold_player_similarity_metadata_key,
    gold_player_year_output_key,
    gold_preprocessing_metadata_key,
    raw_defence_dataset_key,
    raw_sprint_speed_key,
    raw_statcast_day_key,
)


def test_raw_statcast_day_key():
    d = date(2024, 7, 4)
    assert raw_statcast_day_key("bronze/statcast", d) == (
        "bronze/statcast/year=2024/date=2024-07-04/statcast_pitches.parquet"
    )


def test_raw_statcast_day_key_strips_outer_slashes():
    d = date(2024, 1, 1)
    k = raw_statcast_day_key("/my/prefix/", d)
    assert k == "my/prefix/year=2024/date=2024-01-01/statcast_pitches.parquet"


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


def test_gold_player_year_output_key():
    assert gold_player_year_output_key("gold/statcast", "pitcher", 2026) == (
        "gold/statcast/pitcher/year=2026/player_year_features_preprocessed.parquet"
    )


def test_gold_preprocessing_metadata_key():
    assert gold_preprocessing_metadata_key("gold/statcast", "batter", 2025) == (
        "gold/statcast/batter/year=2025/preprocessing_metadata.json"
    )


def test_gold_archetype_clustering_keys():
    assert gold_archetype_assignments_key("gold/statcast", "pitcher", 2024) == (
        "gold/statcast/pitcher/year=2024/player_year_archetypes.parquet"
    )
    assert gold_archetype_clustering_model_key("gold/statcast", "batter", 2023) == (
        "gold/statcast/batter/year=2023/archetype_clustering.joblib"
    )
    assert gold_archetype_clustering_metadata_key("gold/statcast", "pitcher", 2022) == (
        "gold/statcast/pitcher/year=2022/archetype_clustering_metadata.json"
    )


def test_gold_player_similarity_keys():
    assert gold_player_similar_neighbors_key("gold/statcast", "batter", 2024) == (
        "gold/statcast/batter/year=2024/player_year_similar_neighbors.parquet"
    )
    assert gold_player_similarity_metadata_key("gold/statcast", "pitcher", 2023) == (
        "gold/statcast/pitcher/year=2023/player_similarity_metadata.json"
    )
