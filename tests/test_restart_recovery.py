"""Restart-recovery simulation: kill mid-cycle -> restart -> janitor -> clean state."""
from __future__ import annotations

import asyncio
from datetime import timedelta

from sqlalchemy import func, select, update

from xl_i2p.crawler import crawl_batch
from xl_i2p.epochs import open_epoch
from xl_i2p.janitor import run_janitor
from xl_i2p.models import CrawlAttempt, Site, now
from xl_i2p.seeds import upsert_site
from xl_i2p.states import AttemptStatus, AttemptType, SiteState
from xl_i2p.verifier import verify_batch


def _simulate_killed_run(session):
    """A run that died mid-crawl: site stuck in CRAWLING, attempt STARTED."""
    site = upsert_site(session, "killed.i2p", source="test", state_if_new=SiteState.NEW.value)
    site.state = SiteState.CRAWLING.value
    attempt = CrawlAttempt(
        site_id=site.id,
        attempt_type=AttemptType.CRAWL.value,
        status=AttemptStatus.STARTED.value,
    )
    session.add(attempt)
    session.commit()
    old = now() - timedelta(hours=2)
    session.execute(update(Site).where(Site.id == site.id).values(updated_at=old))
    session.execute(update(CrawlAttempt).where(CrawlAttempt.id == attempt.id).values(started_at=old))
    session.commit()


def test_restart_recovery_leaves_no_stuck_rows(db_session, fake_i2p):
    s = db_session
    _simulate_killed_run(s)
    epoch = open_epoch(s, "e1")

    # Restart: janitor runs first.
    janitor_counts = run_janitor(s, stale_minutes=30)
    assert janitor_counts["sites_recovered"] == 1
    assert janitor_counts["attempts_interrupted"] == 1

    # Then the normal loop resumes on the recovered site.
    asyncio.run(verify_batch(s, 10, epoch))
    asyncio.run(crawl_batch(s, 10, epoch))
    s.expire_all()

    site = s.scalar(select(Site).where(Site.host == "killed.i2p"))
    assert site.state in (SiteState.CRAWLED.value, SiteState.RETRY_READY.value,
                          SiteState.UNREACHABLE.value, SiteState.ERROR.value)
    assert site.state not in (SiteState.CRAWLING.value, SiteState.VERIFYING.value)

    stuck_attempts = s.scalar(
        select(func.count()).select_from(CrawlAttempt)
        .where(CrawlAttempt.status == AttemptStatus.STARTED.value)
    )
    assert stuck_attempts == 0


def test_config_defaults_match_study1_validation():
    from xl_i2p.config import settings

    assert settings.verify_limit == 100
    assert settings.crawl_limit == 20
    assert settings.sleep_seconds == 300
    assert settings.max_concurrent_requests == 5
    assert settings.max_concurrent_sites == 2
    assert settings.request_timeout_seconds == 60
    assert settings.max_pages_per_site == 25
    assert settings.max_depth_per_site == 2
    assert settings.crawl_delay_seconds == 2
    assert settings.max_content_bytes == 2_000_000
    assert settings.floodfill_mode is False
    assert "127.0.0.1:3306" in settings.database_url
