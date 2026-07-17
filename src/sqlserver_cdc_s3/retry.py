"""Retry helpers with exponential backoff."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry_with_backoff(
    operation_name: str,
    func: Callable[[], T],
    *,
    logger: logging.Logger,
    attempts: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
    retry_on: tuple[type[BaseException], ...],
) -> T:
    """Run *func* with capped exponential backoff."""
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    attempt = 1
    while True:
        try:
            return func()
        except retry_on as exc:
            if attempt >= attempts:
                raise
            sleep_seconds = min(max_delay_seconds, base_delay_seconds * (2 ** (attempt - 1)))
            sleep_seconds += random.uniform(0, sleep_seconds * 0.1)
            logger.warning(
                "%s failed on attempt %s/%s: %s. Retrying in %.2f seconds.",
                operation_name,
                attempt,
                attempts,
                exc,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
            attempt += 1
