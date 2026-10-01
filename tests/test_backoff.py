"""Backoff computation and retry-exhaustion tests."""
from __future__ import annotations

from xl_i2p.models import Site
from xl_i2p.retry import compute_backoff_seconds, exhausted, schedule_retry


def test_backoff_doubles_without_jitter():
    assert compute_backoff_seconds(1, base_seconds=60, max_seconds=3600, jitter_ratio=0) == 60
    assert compute_backoff_seconds(2, base_seconds=60, max_seconds=3600, jitter_ratio=0) == 120
    assert compute_backoff_seconds(3, base_seconds=60, max_seconds=3600, jitter_ratio=0) == 240


def test_backoff_is_capped():
    assert compute_backoff_seconds(20, base_seconds=60, max_seconds=3600, jitter_ratio=0) == 3600


def test_backoff_jitter_stays_within_bounds():
    for _ in range(200):
        delay = compute_backoff_seconds(2, base_seconds=100, max_seconds=100000, jitter_ratio=0.25)
        assert 150 <= delay <= 250


def test_backoff_failure_count_zero_treated_as_one():
    assert compute_backoff_seconds(0, base_seconds=60, max_seconds=3600, jitter_ratio=0) == 60


def test_exhausted():
    site = Site(host="x.i2p", base_url="http://x.i2p/", failure_count=5)
    assert exhausted(site, max_retries=5) is True
    site.failure_count = 4
    assert exhausted(site, max_retries=5) is False


def test_schedule_retry_sets_next_retry_at():
    site = Site(host="x.i2p", base_url="http://x.i2p/", failure_count=2)
    delay = schedule_retry(site)
    assert delay > 0
    assert site.next_retry_at is not None
