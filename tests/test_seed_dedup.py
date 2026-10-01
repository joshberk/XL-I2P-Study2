"""Seed dedup tests: repeats bump count, they don't add rows."""
from __future__ import annotations

from sqlalchemy import func, select

from xl_i2p.models import SeedEvent
from xl_i2p.seeds import record_seed_event


def _count(session):
    return session.scalar(select(func.count()).select_from(SeedEvent))


def test_duplicate_seed_events_deduped(db_session):
    s = db_session
    for _ in range(3):
        record_seed_event(s, "notbob.i2p", "crawl_discovery", "http://x.i2p/")
    s.commit()
    assert _count(s) == 1
    event = s.scalar(select(SeedEvent))
    assert event.count == 3


def test_different_detail_or_source_creates_new_row(db_session):
    s = db_session
    record_seed_event(s, "notbob.i2p", "crawl_discovery", "http://a.i2p/")
    record_seed_event(s, "notbob.i2p", "crawl_discovery", "http://b.i2p/")
    record_seed_event(s, "notbob.i2p", "seed_file", "http://a.i2p/")
    s.commit()
    assert _count(s) == 3


def test_last_seen_bumps_on_repeat(db_session):
    s = db_session
    first = record_seed_event(s, "dup.i2p", "crawl_discovery", "http://a.i2p/")
    s.commit()
    first_seen = first.first_seen_at
    record_seed_event(s, "dup.i2p", "crawl_discovery", "http://a.i2p/")
    s.commit()
    assert first.first_seen_at == first_seen
    assert first.last_seen_at >= first_seen
    assert first.count == 2
