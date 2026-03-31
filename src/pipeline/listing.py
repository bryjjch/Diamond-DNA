"""S3 listing helpers keyed to lake path conventions."""

from __future__ import annotations

import re
from typing import List, Tuple

from ..s3_parquet import get_s3_client
from .lake_paths import processed_statcast_list_prefix


def list_processed_statcast_player_year_keys(
    *,
    bucket: str,
    processed_prefix: str,
    role: str,
    start_year: int,
    end_year: int,
) -> List[Tuple[int, int, str]]:
    """
    List statcast player-year parquet objects for a role/year range.

    Returns sorted list of (player_id, year, s3_key).
    """
    list_prefix = processed_statcast_list_prefix(processed_prefix, role)
    out: List[Tuple[int, int, str]] = []
    paginator = get_s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=list_prefix):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key")
            if not key:
                continue
            player_m = re.search(rf"{re.escape(role)}_id=(\d+)", key)
            year_m = re.search(r"year=(\d+)", key)
            if not player_m or not year_m:
                continue
            player_id = int(player_m.group(1))
            year = int(year_m.group(1))
            if year < start_year or year > end_year:
                continue
            out.append((player_id, year, key))
    out.sort(key=lambda x: (x[1], x[0]))
    return out
