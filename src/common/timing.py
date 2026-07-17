"""Lightweight phase timing for pipeline steps."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def log_phase(logger: logging.Logger, label: str) -> Iterator[None]:
    """Log the wall-clock duration of the wrapped block, e.g. ``[timing] load bronze: 12.34s``."""
    start = time.monotonic()
    try:
        yield
    finally:
        logger.info("[timing] %s: %.2fs", label, time.monotonic() - start)
