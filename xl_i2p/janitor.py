"""Startup janitor: recover rows left in transient states by a killed run.

Study 1 evidence: 2 sites stuck in CRAWLING (2,877 failed attempts, failure
never left CRAWLING) and 2 orphaned STARTED attempts from the final kill.
The janitor runs on every startup before the scheduler:
  * VERIFYING / CRAWLING rows older than STALE_MINUTES -> RETRY_READY
    (eligible immediately; NOT counted as a failure — the crash was not the
    site's fault)
  * STARTED attempts older than STALE_MINUTES -> INTERRUPTED
"""
from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from .config import settings
from .models import CrawlAttempt, Site, now
from .states import AttemptStatus, SiteState

logger = logging.getLogger(__name__)

TRANSIENT_STATES = (SiteState.VERIFYING.value, SiteState.CRAWLING.value)


def run_janitor(session: Session, stale_minutes: int | None = None) -> dict[str, int]:
    stale_after = settings.stale_minutes if stale_minutes is None else stale_minutes
    cutoff = now() - timedelta(minutes=stale_after)
    counts = {"sites_recovered": 0, "attempts_interrupted": 0}

    # Sites stuck in a transient state: treat as crash recovery, not failure.
    stuck = list(
        session.scalars(
            select(Site).where(
                Site.state.in_(TRANSIENT_STATES),
                or_(
                    Site.updated_at < cutoff,
                    # updated_at may equal the mark time; fall back to the
                    # last completed check/crawl timestamps.
                    Site.last_checked_at < cutoff,
                    Site.last_crawled_at < cutoff,
                ),
            )
        )
    )
    for site in stuck:
        logger.warning(
            "janitor: recovering site %s from stuck state %s", site.host, site.state
        )
        site.state = SiteState.RETRY_READY.value
        site.next_retry_at = now()  # eligible immediately
        counts["sites_recovered"] += 1

    # Orphaned attempts: STARTED but the process died before finishing.
    orphaned = list(
        session.scalars(
            select(CrawlAttempt).where(
                CrawlAttempt.status == AttemptStatus.STARTED.value,
                CrawlAttempt.started_at < cutoff,
                CrawlAttempt.finished_at.is_(None),
            )
        )
    )
    for attempt in orphaned:
        logger.warning(
            "janitor: interrupting orphaned %s attempt %d for site %d",
            attempt.attempt_type,
            attempt.id,
            attempt.site_id,
        )
        attempt.status = AttemptStatus.INTERRUPTED.value
        attempt.finished_at = now()
        attempt.error_type = AttemptStatus.INTERRUPTED.value
        attempt.error_message = "orphaned by killed run; recovered by janitor"
        counts["attempts_interrupted"] += 1

    session.commit()
    if counts["sites_recovered"] or counts["attempts_interrupted"]:
        logger.info("janitor recovered: %s", counts)
    return counts
