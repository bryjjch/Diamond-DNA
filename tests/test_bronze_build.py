"""Tests for the bronze ingestion orchestrator (ingest_all_bronze).

The underlying source functions are monkeypatched so no network / S3 is touched;
we only exercise the aggregation, year-derivation, and source-filtering logic.
"""

import pytest

from src.data_pipeline.bronze import (
    bio_ingestion,
    bronze_build,
    defence_ingestion,
    standard_stats_ingestion,
    statcast_ingestion,
    statcast_running_ingestion,
)

ALL = {"statcast", "running", "defence", "bio", "standard"}


@pytest.fixture
def patched_sources(monkeypatch):
    """Replace the four ingest functions with recorders returning configurable status."""
    calls = {}

    def make(name, status="ok"):
        def _fn(*args, **kwargs):
            calls[name] = {"args": args, "kwargs": kwargs}
            return {"status": status, "message": f"{name} {status}", "errors": []}

        return _fn

    state = {"calls": calls, "make": make, "monkeypatch": monkeypatch}

    def set_status(statcast="ok", running="ok", defence="ok", bio="ok", standard="ok"):
        monkeypatch.setattr(statcast_ingestion, "ingest_date_range", make("statcast", statcast))
        monkeypatch.setattr(
            statcast_running_ingestion, "ingest_year_range", make("running", running)
        )
        monkeypatch.setattr(defence_ingestion, "ingest_year_range", make("defence", defence))
        monkeypatch.setattr(bio_ingestion, "ingest_year_range", make("bio", bio))
        monkeypatch.setattr(
            standard_stats_ingestion, "ingest_year_range", make("standard", standard)
        )

    state["set_status"] = set_status
    return state


def _ingest(**overrides):
    kwargs = dict(
        s3_bucket="b",
        statcast_prefix="bronze/statcast",
        running_prefix="bronze/statcast_running",
        defence_prefix="bronze/defence",
        bio_prefix="bronze/bio",
        standard_prefix="bronze/standard_stats",
        start_date_str="2026-06-19",
        end_date_str="2026-06-19",
    )
    kwargs.update(overrides)
    return bronze_build.ingest_all_bronze(**kwargs)


def test_all_ok(patched_sources):
    patched_sources["set_status"]()
    result = _ingest()
    assert result["status"] == "ok"
    assert set(result["sources"]) == ALL
    assert result["errors"] == []


def test_one_source_error_is_partial(patched_sources):
    patched_sources["set_status"](defence="error")
    result = _ingest()
    assert result["status"] == "partial"
    # The other two still ran.
    assert "statcast" in patched_sources["calls"]
    assert "running" in patched_sources["calls"]
    assert any(e.startswith("defence:") for e in result["errors"])


def test_all_error(patched_sources):
    patched_sources["set_status"](
        statcast="error", running="error", defence="error", bio="error", standard="error"
    )
    result = _ingest()
    assert result["status"] == "error"


def test_one_source_raising_does_not_abort_others(patched_sources):
    mp = patched_sources["monkeypatch"]
    patched_sources["set_status"]()

    def _boom(*args, **kwargs):
        raise RuntimeError("kaboom")

    mp.setattr(statcast_running_ingestion, "ingest_year_range", _boom)

    result = _ingest()
    assert result["status"] == "partial"
    assert result["sources"]["running"]["status"] == "error"
    assert "statcast" in patched_sources["calls"]
    assert "defence" in patched_sources["calls"]
    assert any("kaboom" in e for e in result["errors"])


def test_years_derived_from_dates(patched_sources):
    patched_sources["set_status"]()
    _ingest(start_date_str="2024-03-01", end_date_str="2025-09-30")
    assert patched_sources["calls"]["running"]["args"][:2] == (2024, 2025)
    assert patched_sources["calls"]["defence"]["args"][:2] == (2024, 2025)
    assert patched_sources["calls"]["bio"]["args"][:2] == (2024, 2025)
    assert patched_sources["calls"]["standard"]["args"][:2] == (2024, 2025)


def test_explicit_years_override_dates(patched_sources):
    patched_sources["set_status"]()
    _ingest(start_year=2020, end_year=2022)
    assert patched_sources["calls"]["running"]["args"][:2] == (2020, 2022)


def test_sources_filter_skips_unselected(patched_sources):
    patched_sources["set_status"]()
    result = _ingest(sources=["statcast"])
    assert set(result["sources"]) == {"statcast"}
    assert "running" not in patched_sources["calls"]
    assert "defence" not in patched_sources["calls"]
    assert "bio" not in patched_sources["calls"]
    assert "standard" not in patched_sources["calls"]


def test_unknown_source_is_error(patched_sources):
    patched_sources["set_status"]()
    result = _ingest(sources=["statcast", "bogus"])
    assert result["status"] == "error"
    assert result["sources"] == {}


def test_empty_sources_is_error(patched_sources):
    patched_sources["set_status"]()
    result = _ingest(sources=[])
    assert result["status"] == "error"
