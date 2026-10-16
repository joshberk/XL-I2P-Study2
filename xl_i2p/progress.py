"""Process-wide "last unit of work completed" marker for stall detection.

The watchdog heartbeat proves the event loop is alive; it says nothing
about whether the crawler is getting anywhere. ``seconds_since_last_fetch``
(httpfetch) is a progress signal, but only for the fetch phases: during the
normal inter-cycle sleep, the cross-layer pass (SAM/console I/O, no HTTP
fetch) or the netDb census it keeps growing, so a threshold on it either
false-alarms every cycle or has to be so loose it is useless.

This marker is bumped by every bounded unit of work instead: each completed
fetch, each cycle boundary (start, sleep, proxy-down / no-epoch waits),
each cross-layer lookup, and periodically inside the netDb scans. Because
every one of those units is itself time-bounded (FETCH_TOTAL_TIMEOUT_SECONDS,
SLEEP_SECONDS, socket timeouts), the longest healthy gap between marks is
known, so a stall threshold can be derived instead of guessed — see
``stall_after_seconds``.

Written from the event loop and from worker threads (cross-layer pass,
census). A single float assignment is atomic under the GIL, so no lock.
"""
from __future__ import annotations

import time

from .config import settings

_last_progress_mono: float | None = None


def mark_progress() -> None:
    global _last_progress_mono
    _last_progress_mono = time.monotonic()


def seconds_since_progress() -> float | None:
    if _last_progress_mono is None:
        return None
    return round(time.monotonic() - _last_progress_mono, 1)


def stall_after_seconds(sleep_seconds: float | None = None) -> float:
    """Longest healthy gap between progress marks, plus a safety margin.

    Healthy gaps are bounded by the longest single wait: the inter-cycle
    sleep, or one fetch at its total deadline plus the politeness delay
    (fetches and sleeps never overlap). The margin absorbs one cross-layer
    lookup (SAM + console socket timeouts, ~1 min worst case) and DB latency.
    """
    sleep = settings.sleep_seconds if sleep_seconds is None else sleep_seconds
    longest_wait = max(
        float(sleep),
        settings.fetch_total_timeout_seconds + settings.crawl_delay_seconds,
    )
    return round(longest_wait + settings.stall_margin_seconds, 1)
