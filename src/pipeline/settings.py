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
    processed_prefix: str
    feature_prefix: str
    raw_running_prefix: str
    raw_defence_prefix: str

    @classmethod
    def from_environ(cls, environ: Optional[Mapping[str, str]] = None) -> PipelineSettings:
        e = dict(environ or os.environ)
        bucket = e.get("S3_BUCKET", "diamond-dna")
        raw = _strip_prefix(e.get("S3_PREFIX") or e.get("RAW_PREFIX") or "raw-data/statcast")
        return cls(
            s3_bucket=bucket,
            raw_statcast_prefix=raw,
            processed_prefix=_strip_prefix(e.get("PROCESSED_PREFIX", "processed/statcast")),
            feature_prefix=_strip_prefix(e.get("FEATURE_PREFIX", "features/statcast")),
            raw_running_prefix=_strip_prefix(
                e.get("RAW_RUNNING_PREFIX", "raw-data/statcast_running")
            ),
            raw_defence_prefix=_strip_prefix(e.get("RAW_DEFENCE_PREFIX", "raw-data/defence")),
        )
