"""Epoch tagging + longitudinal re-probe tests (mocked HTTP, no real network)."""
from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import func, select

from xl_i2p.epochs import close_epoch, open_epoch
from xl_i2p.crawler import crawl_batch
from xl_i2p.models import CrawlAttempt, Link, Page, Site
from xl_i2p.seeds import record_seed_event, upsert_site
from xl_i2p.states import SiteState
from xl_i2p.verifier import verify_batch


def _seed(session, host="epochtest.i2p"):
    upsert_site(session, host, source="test", state_if_new=SiteState.NEW.value)
    session.commit()


def test_verify_and_crawl_tag_everything_with_epoch(db_session, fake_i2p):
    s = db_session
    _seed(s)
    epoch = open_epoch(s, "e1")

    verify_result = asyncio.run(verify_batch(s, 10, epoch))
    s.expire_all()
    assert verify_result["reachable"] == 1
    site = s.scalar(select(Site).where(Site.host == "epochtest.i2p"))
    assert site.state == SiteState.REACHABLE.value

    crawl_result = asyncio.run(crawl_batch(s, 10, epoch))
    s.expire_all()
    assert crawl_result["sites"] == 1
    assert s.scalar(select(Site).where(Site.host == "epochtest.i2p")).state == SiteState.CRAWLED.value

    # Every observation row carries the epoch.
    assert s.scalar(select(func.count()).select_from(CrawlAttempt).where(CrawlAttempt.epoch_id != epoch.id)) in (None, 0)
    assert s.scalar(select(func.count()).select_from(CrawlAttempt)) >= 2
    for attempt in s.scalars(select(CrawlAttempt)):
        assert attempt.epoch_id == epoch.id
    for page in s.scalars(select(Page)):
        assert page.epoch_id == epoch.id
    for link in s.scalars(select(Link)):
        assert link.epoch_id == epoch.id


def test_second_epoch_reprobes_and_recrawls_cohort(db_session, fake_i2p):
    s = db_session
    _seed(s)
    e1 = open_epoch(s, "e1")
    asyncio.run(verify_batch(s, 10, e1))
    asyncio.run(crawl_batch(s, 10, e1))
    pages_e1 = s.scalar(select(func.count()).select_from(Page).where(Page.epoch_id == e1.id))
    assert pages_e1 > 0
    close_epoch(s, "e1")

    # New epoch: cohort retry schedule resets, the CRAWLED site is re-probed.
    # (e1's crawl also discovered otherexample.i2p, so >= 1 is correct.)
    e2 = open_epoch(s, "e2")
    verify_result = asyncio.run(verify_batch(s, 10, e2))
    s.expire_all()
    assert verify_result["reachable"] >= 1
    cohort_attempts_e2 = s.scalar(
        select(func.count()).select_from(CrawlAttempt)
        .where(CrawlAttempt.epoch_id == e2.id,
               CrawlAttempt.attempt_type == "VERIFY",
               CrawlAttempt.site_id == s.scalar(select(Site.id).where(Site.host == "epochtest.i2p")))
    )
    assert cohort_attempts_e2 == 1, "cohort site must be re-verified in the new epoch"
    crawl_result = asyncio.run(crawl_batch(s, 10, e2))
    s.expire_all()
    assert crawl_result["sites"] >= 1

    # New epoch => new page rows (churn-measurable), old rows untouched.
    # (e2 crawls both the cohort site and the site discovered in e1.)
    pages_e1_after = s.scalar(select(func.count()).select_from(Page).where(Page.epoch_id == e1.id))
    pages_e2 = s.scalar(select(func.count()).select_from(Page).where(Page.epoch_id == e2.id))
    cohort_id = s.scalar(select(Site.id).where(Site.host == "epochtest.i2p"))
    cohort_pages_e1 = s.scalar(select(func.count()).select_from(Page).where(
        Page.epoch_id == e1.id, Page.site_id == cohort_id))
    cohort_pages_e2 = s.scalar(select(func.count()).select_from(Page).where(
        Page.epoch_id == e2.id, Page.site_id == cohort_id))
    assert pages_e1_after == pages_e1
    assert cohort_pages_e2 == cohort_pages_e1 > 0


def test_page_cap_is_strict(db_session, fake_i2p_many_links):
    from xl_i2p.config import settings

    s = db_session
    _seed(s, "manylinks.i2p")
    epoch = open_epoch(s, "e1")
    asyncio.run(verify_batch(s, 10, epoch))
    s.expire_all()
    result = asyncio.run(crawl_batch(s, 10, epoch))
    s.expire_all()
    assert result["pages_fetched"] <= settings.max_pages_per_site
    assert result["pages_fetched"] == settings.max_pages_per_site


def test_seed_events_carry_epoch(db_session, fake_i2p):
    from xl_i2p.models import SeedEvent

    s = db_session
    _seed(s)
    epoch = open_epoch(s, "e1")
    asyncio.run(verify_batch(s, 10, epoch))
    asyncio.run(crawl_batch(s, 10, epoch))
    s.expire_all()
    events = list(s.scalars(select(SeedEvent)))
    assert events, "expected crawl_discovery seed events"
    for event in events:
        assert event.epoch_id == epoch.id
