"""Tests for the gold build orchestrator (build_all_gold).

The underlying step functions are monkeypatched so no network / S3 is touched;
we only exercise the aggregation and step-filtering logic.
"""

import pytest

from src.data_pipeline.gold import (
    gold_archetype_preprocessing,
    gold_build,
    gold_performance_predict_preprocessing,
)

ALL = {"archetypes", "performance"}


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

    def set_status(archetypes="ok", performance="ok"):
        monkeypatch.setattr(
            gold_archetype_preprocessing,
            "build_silver_to_gold_preprocessing",
            make("archetypes", archetypes),
        )
        monkeypatch.setattr(
            gold_performance_predict_preprocessing,
            "build_performance_training_matrices",
            make("performance", performance),
        )

    state["set_status"] = set_status
    return state


def _build(**overrides):
    kwargs = dict(
        bucket="b",
        silver_prefix="silver",
        gold_prefix="gold/features",
        start_year=2025,
        end_year=2026,
    )
    kwargs.update(overrides)
    return gold_build.build_all_gold(**kwargs)


def test_all_ok(patched_steps):
    patched_steps["set_status"]()
    result = _build()
    assert result["status"] == "ok"
    assert set(result["steps"]) == ALL
    assert result["errors"] == []


def test_one_step_error_is_partial(patched_steps):
    patched_steps["set_status"](performance="error")
    result = _build()
    assert result["status"] == "partial"
    # The archetypes step still ran.
    assert "archetypes" in patched_steps["calls"]
    assert any(e.startswith("performance:") for e in result["errors"])


def test_all_error(patched_steps):
    patched_steps["set_status"](archetypes="error", performance="error")
    result = _build()
    assert result["status"] == "error"


def test_no_data_counts_as_ok(patched_steps):
    patched_steps["set_status"](performance="no_data")
    result = _build()
    assert result["status"] == "ok"


def test_one_step_raising_does_not_abort_others(patched_steps):
    mp = patched_steps["monkeypatch"]
    patched_steps["set_status"]()

    def _boom(*args, **kwargs):
        raise RuntimeError("kaboom")

    mp.setattr(gold_archetype_preprocessing, "build_silver_to_gold_preprocessing", _boom)

    result = _build()
    assert result["status"] == "partial"
    assert result["steps"]["archetypes"]["status"] == "error"
    assert "performance" in patched_steps["calls"]
    assert any("kaboom" in e for e in result["errors"])


def test_kwargs_threaded_through(patched_steps):
    patched_steps["set_status"]()
    _build(start_year=2023, end_year=2024, role_filter="pitcher", n_lag_seasons=2)
    arch_kwargs = patched_steps["calls"]["archetypes"]["kwargs"]
    assert arch_kwargs["bucket"] == "b"
    assert arch_kwargs["silver_prefix"] == "silver"
    assert arch_kwargs["gold_prefix"] == "gold/features"
    assert arch_kwargs["start_year"] == 2023
    assert arch_kwargs["end_year"] == 2024
    assert arch_kwargs["role_filter"] == "pitcher"
    perf_kwargs = patched_steps["calls"]["performance"]["kwargs"]
    assert perf_kwargs["role_filter"] == "pitcher"
    assert perf_kwargs["config"].n_lag_seasons == 2


def test_steps_filter_skips_unselected(patched_steps):
    patched_steps["set_status"]()
    result = _build(steps=["archetypes"])
    assert set(result["steps"]) == {"archetypes"}
    assert "performance" not in patched_steps["calls"]


def test_unknown_step_is_error(patched_steps):
    patched_steps["set_status"]()
    result = _build(steps=["archetypes", "bogus"])
    assert result["status"] == "error"
    assert result["steps"] == {}


def test_empty_steps_is_error(patched_steps):
    patched_steps["set_status"]()
    result = _build(steps=[])
    assert result["status"] == "error"
