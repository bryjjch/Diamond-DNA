"""Tests for player-bio ingestion (bronze flattening) and silver bio enrichment."""

import math

import pandas as pd

from src.data_pipeline.bronze.bio_ingestion import parse_height_inches, person_to_bio_row
from src.data_pipeline.silver.archetype_features import bio_player_year_helper as bio_player_year


SAMPLE_PERSON = {
    "id": 671096,
    "fullName": "Andrew Abbott",
    "birthDate": "1999-06-01",
    "birthCity": "Lynchburg",
    "birthStateProvince": "VA",
    "birthCountry": "USA",
    "height": "6' 0\"",
    "weight": 192,
    "active": True,
    "currentTeam": {"id": 113, "name": "Cincinnati Reds"},
    "primaryPosition": {"code": "1", "name": "Pitcher", "abbreviation": "P"},
    "mlbDebutDate": "2023-06-05",
    "draftYear": 2021,
    "batSide": {"code": "L", "description": "Left"},
    "pitchHand": {"code": "L", "description": "Left"},
}


def test_parse_height_inches():
    assert parse_height_inches("6' 0\"") == 72.0
    assert parse_height_inches("5' 11\"") == 71.0
    assert math.isnan(parse_height_inches(None))
    assert math.isnan(parse_height_inches("tall"))


def test_person_to_bio_row_flattens_nested_fields():
    row = person_to_bio_row(SAMPLE_PERSON)
    assert row["player_id"] == 671096
    assert row["birth_date"] == "1999-06-01"
    assert row["height_in"] == 72.0
    assert row["weight_lb"] == 192
    assert row["bat_side"] == "L"
    assert row["primary_position"] == "P"
    assert row["current_team"] == "Cincinnati Reds"


def test_person_to_bio_row_handles_missing_fields():
    row = person_to_bio_row({"id": 1})
    assert row["player_id"] == 1
    assert row["bat_side"] is None
    assert row["current_team"] is None
    assert math.isnan(row["height_in"])


def test_season_age_is_age_on_june_30():
    # Born June 1: birthday passed by June 30.
    assert bio_player_year.season_age("1999-06-01", 2024) == 25.0
    # Born July 1: birthday not yet reached on June 30.
    assert bio_player_year.season_age("2000-07-01", 2024) == 23.0
    assert math.isnan(bio_player_year.season_age(None, 2024))
    assert math.isnan(bio_player_year.season_age("not-a-date", 2024))


def test_load_and_merge_bio(monkeypatch):
    bronze_df = pd.DataFrame(
        [person_to_bio_row(SAMPLE_PERSON), person_to_bio_row({"id": 2})]
    )
    monkeypatch.setattr(
        bio_player_year, "read_parquet_from_s3", lambda *a, **k: bronze_df
    )

    bios = bio_player_year.load_player_bios_by_year("b", "bronze/bio", 2024)
    assert bios[671096]["bio_age"] == 25.0
    assert bios[671096]["bio_height_in"] == 72.0
    assert bios[671096]["bio_weight_lb"] == 192.0
    assert bios[671096]["bio_birth_place"] == "Lynchburg, VA, USA"
    assert math.isnan(bios[2]["bio_age"])
    assert bios[2]["bio_birth_place"] == ""

    features = pd.DataFrame({"player_id": [671096, 999], "year": [2024, 2024]})
    merged = bio_player_year.merge_bio_features(features, {2024: bios}).set_index("player_id")
    assert merged.at[671096, "bio_age"] == 25.0
    assert merged.at[671096, "bio_birth_date"] == "1999-06-01"

    # Unknown player still gets the full column set with NaN/empty values.
    assert math.isnan(merged.at[999, "bio_age"])
    assert merged.at[999, "bio_birth_place"] == ""
