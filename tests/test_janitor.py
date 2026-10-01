"""Janitor recovery tests: stuck VERIFYING/CRAWLING rows and orphaned STARTED attempts."""
from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select, update

from xl_i2p.janitor import run_janitor
from xl_i2p.models import CrawlAttempt, Site, now
from xl_i2p.states import AttemptStatus, AttemptType, SiteState


def _make_site(session, host, state, updated_ago_minutes, last_checked_ago=None, last_crawled_ago=None):
    site = Site(host=host, base_url=f"http://{host}/", state=state)
    session.add(site)
    session.commit()
    values = {"updated_at": now() - timedelta(minutes=updated_ago_minutes)}
    if last_checked_ago is not None:
        values["last_checked_at"] = now() - timedelta(minutes=last_checked_ago)
    if last_crawled_ago is not None:
        values["last_crawled_at"] = now() - timedelta(minutes=last_crawled_ago)
    session.execute(update(Site).where(Site.id == site.id).values(**values))
    session.commit()
    return site.id


def _make_attempt(session, site_id, attempt_type, started_ago_minutes):
    attempt = CrawlAttempt(site_id=site_id, attempt_type=attempt_type,
                           status=AttemptStatus.STARTED.value)
    session.add(attempt)
    session.commit()
    session.execute(
        update(CrawlAttempt).where(CrawlAttempt.id == attempt.id)
        .values(started_at=now() - timedelta(minutes=started_ago_minutes))
    )
    session.commit()
    return attempt.id


def test_janitor_recovers_stuck_sites_and_orphaned_attempts(db_session):
    s = db_session
    stuck_verifying = _make_site(s, "stuckv.i2p", SiteState.VERIFYING.value, 120, last_checked_ago=120)
    stuck_crawling = _make_site(s, "stuckc.i2p", SiteState.CRAWLING.value, 120, last_crawled_ago=120)
    orphaned = _make_attempt(s, stuck_crawling, AttemptType.CRAWL.value, 120)

    counts = run_janitor(s, stale_minutes=30)

    assert counts == {"sites_recovered": 2, "attempts_interrupted": 1}
    assert s.get(Site, stuck_verifying).state == SiteState.RETRY_READY.value
    assert s.get(Site, stuck_crawling).state == SiteState.RETRY_READY.value
    # Crash recovery must not burn the retry budget.
    assert s.get(Site, stuck_verifying).failure_count == 0
    attempt = s.get(CrawlAttempt, orphaned)
    assert attempt.status == AttemptStatus.INTERRUPTED.value
    assert attempt.finished_at is not None


def test_janitor_leaves_fresh_rows_alone(db_session):
    s = db_session
    fresh_verifying = _make_site(s, "freshv.i2p", SiteState.VERIFYING.value, 1)
    fresh_crawling = _make_site(s, "freshc.i2p", SiteState.CRAWLING.value, 1)
    fresh_attempt = _make_attempt(s, fresh_crawling, AttemptType.CRAWL.value, 1)
    healthy = _make_site(s, "healthy.i2p", SiteState.CRAWLED.value, 120, last_crawled_ago=120)

    counts = run_janitor(s, stale_minutes=30)

    assert counts == {"sites_recovered": 0, "attempts_interrupted": 0}
    assert s.get(Site, fresh_verifying).state == SiteState.VERIFYING.value
    assert s.get(Site, fresh_crawling).state == SiteState.CRAWLING.value
    assert s.get(Site, healthy).state == SiteState.CRAWLED.value
    assert s.get(CrawlAttempt, fresh_attempt).status == AttemptStatus.STARTED.value


def test_janitor_recovered_site_is_immediately_eligible(db_session):
    s = db_session
    site_id = _make_site(s, "elig.i2p", SiteState.CRAWLING.value, 120, last_crawled_ago=120)
    run_janitor(s, stale_minutes=30)
    site = s.get(Site, site_id)
    assert site.next_retry_at is not None
    assert site.next_retry_at <= now()
