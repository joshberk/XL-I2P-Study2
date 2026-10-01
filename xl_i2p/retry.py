"""Exponential backoff with jitter and retry-exhaustion handling.

Study 1 had no backoff: two sites stuck in CRAWLING accumulated 2,877 failed
attempts because a failure never left the CRAWLING state and never waited.
Every failure now schedules ``next_retry_at`` and, after MAX_RETRIES
consecutive failures, the site goes terminal for the epoch (UNREACHABLE for
verify, ERROR for crawl). Epoch rollover resets ``next_retry_at`` so the
cohort is re-probed each epoch.
"""
from __future__ import annotations

import random
from datetime import timedelta

from .config import settings
from .models import Site, now


def compute_backoff_seconds(
    failure_count: int,
    base_seconds: float | None = None,
    max_seconds: float | None = None,
    jitter_ratio: float = 0.25,
) -> float:
    """Exponential backoff: base * 2**(n-1), jittered by +/- jitter_ratio, capped."""
    base = settings.backoff_base_seconds if base_seconds is None else base_seconds
    cap = settings.backoff_max_seconds if max_seconds is None else max_seconds
    failures = max(1, failure_count)
    delay = base * (2.0 ** (failures - 1))
    jitter = delay * jitter_ratio
    delay = delay + random.uniform(-jitter, jitter)
    return max(0.0, min(cap, delay))


def exhausted(site: Site, max_retries: int | None = None) -> bool:
    limit = settings.max_retries if max_retries is None else max_retries
    return site.failure_count >= limit


def schedule_retry(site: Site, failure_count: int | None = None) -> float:
    """Set next_retry_at from the backoff schedule. Returns the delay used."""
    count = site.failure_count if failure_count is None else failure_count
    delay = compute_backoff_seconds(count)
    site.next_retry_at = now() + timedelta(seconds=delay)
    return delay
