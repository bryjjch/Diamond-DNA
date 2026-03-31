"""Environment-backed settings for pipeline stages."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping, Optional


def _strip_prefix(p: str) -> str:
    return p.strip().strip("/")


@dataclass(frozen=True)
class PipelineSettings:
    """Shared lake locations from process environment (matches Terraform Lambda env names)."""

    s3_bucket: str
    raw_statcast_prefix: str
    feature_prefix: str
    gold_prefix: str
    raw_running_prefix: str
    raw_defence_prefix: str

    @classmethod
    def from_environ(cls, environ: Optional[Mapping[str, str]] = None) -> PipelineSettings:
        e = dict(environ or os.environ)
        bucket = e.get("S3_BUCKET", "diamond-dna")
        raw = _strip_prefix(e.get("S3_PREFIX") or e.get("RAW_PREFIX") or "bronze/statcast")
        return cls(
            s3_bucket=bucket,
            raw_statcast_prefix=raw,
            feature_prefix=_strip_prefix(e.get("FEATURE_PREFIX", "silver/statcast")),
            gold_prefix=_strip_prefix(e.get("GOLD_PREFIX", "gold/statcast")),
            raw_running_prefix=_strip_prefix(
                e.get("RAW_RUNNING_PREFIX", "bronze/statcast_running")
            ),
            raw_defence_prefix=_strip_prefix(e.get("RAW_DEFENCE_PREFIX", "bronze/defence")),
        )
