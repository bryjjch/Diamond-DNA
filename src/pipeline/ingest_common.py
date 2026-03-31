"""Shared ingestion helpers (retries, etc.)."""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)


def retry_with_backoff(
    label: str,
    fn: Callable[[], T],
    *,
    max_retries: int = 3,
) -> Optional[T]:
    """
    Call ``fn`` up to ``max_retries`` times with quadratic backoff on exceptions.
    Returns None if all attempts fail.
    """
    for attempt in range(max_retries):
        try:
            logger.info("%s (attempt %d)", label, attempt + 1)
            return fn()
        except Exception as exc:
            logger.error("%s failed (attempt %d): %s", label, attempt + 1, exc)
            if attempt < max_retries - 1:
                wait_s = (attempt + 1) ** 2
                logger.info("Retrying in %d seconds...", wait_s)
                time.sleep(wait_s)
            else:
                return None
    return None
