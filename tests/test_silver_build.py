"""Tests for the silver build orchestrator (build_all_silver).

The underlying step functions are monkeypatched so no network / S3 is touched;
we only exercise the aggregation, year-defaulting, and step-filtering logic.
"""

import pytest

from src.data_pipeline.silver import silver_build
from src.data_pipeline.silver.archetype_features import silver_features_build
from src.data_pipeline.silver.standard_stats import silver_standard_player_year_stats

ALL = {"features", "standard"}


@pytest.fixture
def patched_steps(monkeypatch):
    """Replace the two build functions with recorders returning configurable status."""
    calls = {}

    def make(name, status="ok"):
        def _fn(*args, **kwargs):
            calls[name] = {"args": args, "kwargs": kwargs}
            return {"status": status, "message": f"{name} {status}", "errors": []}

        return _fn

    state = {"calls": calls, "make": make, "monkeypatch": monkeypatch}

    def set_status(features="ok", standard="ok"):
        monkeypatch.setattr(
            silver_features_build, "build_bronze_to_silver_features", make("features", features)
        )
        monkeypatch.setattr(
            silver_standard_player_year_stats,
            "build_standard_stats_range",
            make("standard", standard),
        )

    state["set_status"] = set_status
    return state


def _build(**overrides):
    kwargs = dict(
        bucket="b",
        bronze_statcast_prefix="bronze/statcast",
        raw_running_prefix="bronze/statcast_running",
        raw_defence_prefix="bronze/defence",
        raw_bio_prefix="bronze/bio",
        raw_standard_stats_prefix="bronze/standard_stats",
        silver_prefix="silver",
        start_date_str="2026-06-19",
        end_date_str="2026-06-19",
    )
    kwargs.update(overrides)
    return silver_build.build_all_silver(**kwargs)


def test_all_ok(patched_steps):
    patched_steps["set_status"]()
    result = _build()
    assert result["status"] == "ok"
    assert set(result["steps"]) == ALL
    assert result["errors"] == []


def test_one_step_error_is_partial(patched_steps):
    patched_steps["set_status"](standard="error")
    result = _build()
    assert result["status"] == "partial"
    # The features step still ran.
    assert "features" in patched_steps["calls"]
    assert any(e.startswith("standard:") for e in result["errors"])


def test_all_error(patched_steps):
    patched_steps["set_status"](features="error", standard="error")
    result = _build()
    assert result["status"] == "error"


def test_no_data_counts_as_ok(patched_steps):
    patched_steps["set_status"](standard="no_data")
    result = _build()
    assert result["status"] == "ok"


def test_one_step_raising_does_not_abort_others(patched_steps):
    mp = patched_steps["monkeypatch"]
    patched_steps["set_status"]()

    def _boom(*args, **kwargs):
        raise RuntimeError("kaboom")

    mp.setattr(silver_features_build, "build_bronze_to_silver_features", _boom)

    result = _build()
    assert result["status"] == "partial"
    assert result["steps"]["features"]["status"] == "error"
    assert "standard" in patched_steps["calls"]
    assert any("kaboom" in e for e in result["errors"])


def test_kwargs_threaded_through(patched_steps):
    patched_steps["set_status"]()
    _build(start_date_str="2026-05-01", end_date_str="2026-05-02", year_to_date=False)
    features_kwargs = patched_steps["calls"]["features"]["kwargs"]
    assert features_kwargs["bucket"] == "b"
    assert features_kwargs["bronze_statcast_prefix"] == "bronze/statcast"
    assert features_kwargs["start_date_str"] == "2026-05-01"
    assert features_kwargs["end_date_str"] == "2026-05-02"
    assert features_kwargs["year_to_date"] is False
    standard_kwargs = patched_steps["calls"]["standard"]["kwargs"]
    assert standard_kwargs["raw_standard_stats_prefix"] == "bronze/standard_stats"
    assert standard_kwargs["silver_prefix"] == "silver"


def test_years_default_to_last_and_current_season(patched_steps):
    from src.common.runtime_helpers import current_utc_year

    patched_steps["set_status"]()
    _build()
    cy = current_utc_year()
    standard_kwargs = patched_steps["calls"]["standard"]["kwargs"]
    assert standard_kwargs["start_year"] == cy - 1
    assert standard_kwargs["end_year"] == cy


def test_explicit_years_override_defaults(patched_steps):
    patched_steps["set_status"]()
    _build(start_year=2020, end_year=2022)
    standard_kwargs = patched_steps["calls"]["standard"]["kwargs"]
    assert standard_kwargs["start_year"] == 2020
    assert standard_kwargs["end_year"] == 2022


def test_steps_filter_skips_unselected(patched_steps):
    patched_steps["set_status"]()
    result = _build(steps=["features"])
    assert set(result["steps"]) == {"features"}
    assert "standard" not in patched_steps["calls"]


def test_unknown_step_is_error(patched_steps):
    patched_steps["set_status"]()
    result = _build(steps=["features", "bogus"])
    assert result["status"] == "error"
    assert result["steps"] == {}


def test_empty_steps_is_error(patched_steps):
    patched_steps["set_status"]()
    result = _build(steps=[])
    assert result["status"] == "error"
