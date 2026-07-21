#!/usr/bin/env python3
"""
Compare two projection models' per-stat scores, role-year by role-year.

Every projection model writes a ``metrics.json`` sidecar to
``{models_prefix}/{model}/{role}/year=Y/metrics.json`` whose ``metrics`` block
is produced by the Marcel baseline's ``score_projections`` (per-stat n / MAE /
RMSE / bias). This module reads the sidecars for a baseline model (default
``marcel``) and a candidate model (default ``xgboost``) over a year range and
reports, per (role, year, stat), the error deltas and which model won —
plus a per-(role, stat) summary across years.

Read-only by default: results print to stdout. ``--write-report`` additionally
writes one JSON report per role-year to
``{models_prefix}/evaluation/{role}/year=Y/{candidate}_vs_{baseline}.json``.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from ...common.lake_keys import model_key
from ...common.s3_helpers import get_s3_client
from ...common.settings import PipelineSettings
from ..baselines.marcel_baseline import ROLE_ALL_TARGETS, VALID_ROLES

logger = logging.getLogger(__name__)


def load_model_metrics(
    *, bucket: str, models_prefix: str, model: str, role: str, year: int
) -> Optional[Dict[str, Any]]:
    """Read one model's ``metrics.json`` sidecar, or None if it doesn't exist."""
    key = model_key(models_prefix, model, role, year, "metrics.json")
    client = get_s3_client()
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except client.exceptions.NoSuchKey:
        logger.info("No metrics at s3://%s/%s", bucket, key)
        return None
    return json.loads(obj["Body"].read().decode("utf-8"))


def _pct_improvement(baseline: float, candidate: float) -> Optional[float]:
    """Percent error reduction vs the baseline; positive means candidate is better."""
    if baseline == 0:
        return None
    return (baseline - candidate) / baseline * 100.0


def compare_model_metrics(
    *,
    bucket: str,
    models_prefix: str,
    start_year: int,
    end_year: int,
    baseline_model: str = "marcel",
    candidate_model: str = "xgboost",
    role_filter: str = "all",
) -> pd.DataFrame:
    """Long-format comparison: one row per (role, year, stat) scored by both models.

    Role-years where either model's ``metrics.json`` is missing are skipped, as
    are stats absent from either ``metrics`` block, so every row is a true
    head-to-head on the same season's actuals.
    """
    roles = VALID_ROLES if role_filter == "all" else (role_filter,)
    rows: List[Dict[str, Any]] = []

    for role in roles:
        for year in range(start_year, end_year + 1):
            base = load_model_metrics(
                bucket=bucket, models_prefix=models_prefix,
                model=baseline_model, role=role, year=year,
            )
            cand = load_model_metrics(
                bucket=bucket, models_prefix=models_prefix,
                model=candidate_model, role=role, year=year,
            )
            if base is None or cand is None:
                continue
            base_metrics = base.get("metrics", {})
            cand_metrics = cand.get("metrics", {})
            for stat in ROLE_ALL_TARGETS[role]:
                b = base_metrics.get(stat)
                c = cand_metrics.get(stat)
                if not b or not c:
                    continue
                rows.append(
                    {
                        "role": role,
                        "year": year,
                        "stat": stat,
                        "n_baseline": int(b["n"]),
                        "n_candidate": int(c["n"]),
                        "mae_baseline": float(b["mae"]),
                        "mae_candidate": float(c["mae"]),
                        "mae_delta": float(c["mae"]) - float(b["mae"]),
                        "mae_pct_improvement": _pct_improvement(
                            float(b["mae"]), float(c["mae"])
                        ),
                        "rmse_baseline": float(b["rmse"]),
                        "rmse_candidate": float(c["rmse"]),
                        "rmse_delta": float(c["rmse"]) - float(b["rmse"]),
                        "rmse_pct_improvement": _pct_improvement(
                            float(b["rmse"]), float(c["rmse"])
                        ),
                        "bias_baseline": float(b["bias"]),
                        "bias_candidate": float(c["bias"]),
                        "candidate_wins_mae": float(c["mae"]) < float(b["mae"]),
                    }
                )

    return pd.DataFrame(rows)


def summarize_comparison(comparison: pd.DataFrame) -> pd.DataFrame:
    """Per-(role, stat) rollup across years: mean improvements and win rate."""
    if comparison.empty:
        return pd.DataFrame(
            columns=[
                "role", "stat", "n_years", "mean_mae_pct_improvement",
                "mean_rmse_pct_improvement", "win_rate_mae",
            ]
        )
    grouped = comparison.groupby(["role", "stat"], sort=False)
    summary = grouped.agg(
        n_years=("year", "nunique"),
        mean_mae_pct_improvement=("mae_pct_improvement", "mean"),
        mean_rmse_pct_improvement=("rmse_pct_improvement", "mean"),
        win_rate_mae=("candidate_wins_mae", "mean"),
    ).reset_index()
    return summary


def _write_json_to_s3(bucket: str, key: str, payload: Dict[str, Any]) -> None:
    client = get_s3_client()
    body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def write_comparison_reports(
    comparison: pd.DataFrame,
    *,
    bucket: str,
    models_prefix: str,
    baseline_model: str,
    candidate_model: str,
) -> List[str]:
    """Write one JSON report per (role, year) under ``{models_prefix}/evaluation/``."""
    written: List[str] = []
    for (role, year), group in comparison.groupby(["role", "year"], sort=False):
        payload = {
            "role": role,
            "year": int(year),
            "baseline_model": baseline_model,
            "candidate_model": candidate_model,
            "stats": group.drop(columns=["role", "year"]).to_dict(orient="records"),
            "generated_at_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }
        key = model_key(
            models_prefix,
            "evaluation",
            role,
            int(year),
            f"{candidate_model}_vs_{baseline_model}.json",
        )
        _write_json_to_s3(bucket, key, payload)
        written.append(key)
    return written


def main() -> None:
    cfg = PipelineSettings.from_environ()
    parser = argparse.ArgumentParser(
        description=(
            "Compare two projection models' metrics.json sidecars per role-year-stat "
            "(default: xgboost vs the Marcel baseline) and summarize who wins."
        )
    )
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--bucket", type=str, default=cfg.s3_bucket)
    parser.add_argument("--models-prefix", type=str, default=cfg.models_prefix)
    parser.add_argument("--baseline-model", type=str, default="marcel")
    parser.add_argument("--candidate-model", type=str, default="xgboost")
    parser.add_argument(
        "--role",
        choices=("all", *VALID_ROLES),
        default="all",
        help="Compare all roles or one specific role.",
    )
    parser.add_argument(
        "--write-report",
        action="store_true",
        help="Also write one JSON report per role-year under models/evaluation/.",
    )
    args = parser.parse_args()

    comparison = compare_model_metrics(
        bucket=args.bucket,
        models_prefix=args.models_prefix,
        start_year=args.start_year,
        end_year=args.end_year,
        baseline_model=args.baseline_model,
        candidate_model=args.candidate_model,
        role_filter=args.role,
    )
    if comparison.empty:
        logger.error(
            "No role-years where both %s and %s have metrics.json in %s..%s",
            args.baseline_model,
            args.candidate_model,
            args.start_year,
            args.end_year,
        )
        raise SystemExit(1)

    pd.set_option("display.width", 200)
    detail_cols = [
        "role", "year", "stat", "mae_baseline", "mae_candidate",
        "mae_pct_improvement", "rmse_baseline", "rmse_candidate",
        "rmse_pct_improvement", "candidate_wins_mae",
    ]
    print(f"\n=== {args.candidate_model} vs {args.baseline_model}: per role-year-stat ===")
    print(comparison[detail_cols].to_string(index=False, float_format=lambda v: f"{v:.4f}"))

    summary = summarize_comparison(comparison)
    print(f"\n=== {args.candidate_model} vs {args.baseline_model}: per role-stat summary ===")
    print(summary.to_string(index=False, float_format=lambda v: f"{v:.4f}"))

    wins = int(comparison["candidate_wins_mae"].sum())
    print(
        f"\n{args.candidate_model} wins {wins}/{len(comparison)} "
        f"(role, year, stat) comparisons on MAE."
    )

    if args.write_report:
        written = write_comparison_reports(
            comparison,
            bucket=args.bucket,
            models_prefix=args.models_prefix,
            baseline_model=args.baseline_model,
            candidate_model=args.candidate_model,
        )
        for key in written:
            logger.info("Wrote comparison report to s3://%s/%s", args.bucket, key)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
