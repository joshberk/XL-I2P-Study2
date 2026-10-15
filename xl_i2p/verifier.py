"""Reachability verification with epoch tagging, backoff, and containment.

Changes vs Study 1:
  * every attempt is tagged with the active epoch
  * each site is verified in its own short-lived session (no shared session
    across asyncio tasks)
  * failures are classified with the real taxonomy (no more blanket
    UNKNOWN_ERROR) and scheduled with exponential backoff
  * per-site try/except containment: one site's crash cannot kill the batch
  * longitudinal re-probe: CRAWLED/UNREACHABLE/ERROR sites are re-verified
    each epoch (terminal-for-epoch sites already checked this epoch excluded)
"""
from __future__ import annotations

import asyncio
import logging

import httpx
from sqlalchemy import and_, not_, or_, select
from sqlalchemy.orm import Session

from .config import settings
from .httpfetch import fetch_bounded
from .models import CrawlAttempt, Epoch, Site, now
from .proxy import tcp_proxy_available
from .retry import exhausted, schedule_retry
from .states import AttemptStatus, AttemptType, ErrorType, SiteState
from .taxonomy import classify_exception

logger = logging.getLogger(__name__)

VERIFY_STATES = [
    SiteState.NEW.value,
    SiteState.DISCOVERED.value,
    SiteState.RETRY_READY.value,
    SiteState.UNREACHABLE.value,
    SiteState.CRAWLED.value,
    SiteState.ERROR.value,
]
# Longitudinal cohort: known sites whose survival the study must track.
# These get a reserved share of every verify batch (oldest-checked first) so
# re-verification is never starved by the never-checked discovery backlog.
# Cohort membership is states AND success_count > 0: a RETRY_READY row for a
# host that never answered once is discovery backlog, not a known site, and
# must not consume cohort slots.
COHORT_STATES = [
    SiteState.REACHABLE.value,
    SiteState.CRAWLED.value,
    SiteState.RETRY_READY.value,
]

TERMINAL_STATES = (SiteState.UNREACHABLE.value, SiteState.ERROR.value)
# Proxy failures are environmental: retry soon without burning the retry budget.
PROXY_ERRORS = {
    ErrorType.PROXY_UNAVAILABLE.value,
    ErrorType.PROXY_TIMEOUT.value,
    ErrorType.PROXY_ERROR.value,
}


def _claim_verify_jobs(session: Session, stmt, epoch: Epoch) -> list[tuple[int, str, int]]:
    """Claim the sites selected by ``stmt`` for verification.

    Marks each VERIFYING and records a VERIFY attempt; returns
    (site_id, base_url, attempt_id) tuples.
    """
    jobs: list[tuple[int, str, int]] = []
    for site in session.scalars(stmt):
        site.state = SiteState.VERIFYING.value
        attempt = CrawlAttempt(
            site_id=site.id,
            epoch_id=epoch.id,
            attempt_type=AttemptType.VERIFY.value,
            status=AttemptStatus.STARTED.value,
            discovery_source=site.source,
            is_cross_layer=bool(site.is_cross_layer),
        )
        session.add(attempt)
        session.flush()
        jobs.append((site.id, site.base_url, attempt.id))
    session.commit()
    return jobs


def select_sites_for_verification(
    session: Session, limit: int, epoch: Epoch
) -> list[tuple[int, str, int]]:
    """Claim up to ``limit`` sites for verification; returns (site_id, base_url, attempt_id).

    Split-budget selection: ``settings.verify_cohort_share`` of the batch is
    reserved for re-verifying the longitudinal cohort (REACHABLE / CRAWLED /
    RETRY_READY with success_count > 0, oldest-checked first); the remainder
    sweeps the discovery backlog (never-checked first, then oldest). Without
    the reservation, the never-checked backlog (tens of thousands of seeds
    plus the daily lease-set feed) starves cohort re-verification, and the
    churn/survival signal on known sites goes quiet. The success_count gate
    keeps never-reached RETRY_READY rows (failed once, never answered) in
    the discovery pool instead of consuming cohort slots.

    Unused budget in either pool flows to the other, so the full ``limit`` is
    used whenever eligible sites exist. Backoff (``next_retry_at``) and the
    terminal-for-epoch exclusion are honored in both pools.
    """
    share = min(1.0, max(0.0, settings.verify_cohort_share))
    cohort_limit = int(round(limit * share))

    terminal_checked_this_epoch = and_(
        Site.state.in_(TERMINAL_STATES),
        Site.last_checked_at.is_not(None),
        Site.last_checked_at >= epoch.started_at,
    )
    retry_due = or_(Site.next_retry_at.is_(None), Site.next_retry_at <= now())
    eligible = not_(terminal_checked_this_epoch)
    # Never-checked first, then oldest-checked; shared by both pools.
    freshness_order = (
        Site.last_checked_at.is_not(None),
        Site.last_checked_at.asc(),
        Site.id.asc(),
    )

    # Cohort = known sites only: a RETRY_READY row for a host that never
    # answered once is discovery backlog, not cohort.
    is_cohort = and_(Site.state.in_(COHORT_STATES), Site.success_count > 0)

    cohort_stmt = (
        select(Site)
        .where(
            is_cohort,
            retry_due,
            eligible,
        )
        .order_by(*freshness_order)
        .limit(cohort_limit)
    )
    jobs = _claim_verify_jobs(session, cohort_stmt, epoch)

    discovery_stmt = (
        select(Site)
        .where(
            Site.state.in_(VERIFY_STATES),
            not_(is_cohort),
            retry_due,
            eligible,
        )
        .order_by(*freshness_order)
        .limit(max(0, limit - len(jobs)))
    )
    jobs += _claim_verify_jobs(session, discovery_stmt, epoch)
    return jobs


async def verify_url(
    client: httpx.AsyncClient, base_url: str
) -> tuple[bool, int | None, str | None, str | None, int | None]:
    # Bounded fetch: a "cheap" liveness probe must not buffer an enormous
    # landing page either (same wedge as the crawl phase, smaller scale).
    result = await fetch_bounded(client, base_url)
    if result.error_type is None:
        return True, result.status_code, None, None, result.elapsed_ms
    return False, result.status_code, result.error_type, result.error_message, result.elapsed_ms


def _apply_verify_failure(session: Session, site: Site, attempt: CrawlAttempt,
                          error_type: str, error_message: str | None) -> None:
    site.last_checked_at = now()
    site.last_error_type = error_type
    site.last_error_message = (error_message or "")[:2000]
    attempt.finished_at = now()
    attempt.status = AttemptStatus.FAILED.value
    attempt.error_type = error_type
    attempt.error_message = attempt.error_message or site.last_error_message

    if error_type in PROXY_ERRORS:
        # Environmental: retry soon, don't burn the retry budget.
        site.state = SiteState.RETRY_READY.value
        schedule_retry(site, failure_count=1)
        return

    site.failure_count += 1
    if exhausted(site):
        site.state = SiteState.UNREACHABLE.value
        site.next_retry_at = None  # terminal for this epoch; epoch filter excludes it
        logger.info("verify: %s terminal UNREACHABLE after %d failures (%s)",
                    site.host, site.failure_count, error_type)
    else:
        site.state = SiteState.RETRY_READY.value
        delay = schedule_retry(site)
        logger.debug("verify: %s failed (%s), retry in %.0fs", site.host, error_type, delay)


async def _verify_one_site_inner(
    session: Session, site_id: int, attempt_id: int
) -> dict[str, int]:
    site = session.get(Site, site_id)
    attempt = session.get(CrawlAttempt, attempt_id)
    if site is None or attempt is None:
        return {"verified": 0, "reachable": 0, "failed": 0, "skipped": 1}

    async with httpx.AsyncClient(
        proxy=settings.i2p_http_proxy,
        timeout=settings.request_timeout_seconds,
        follow_redirects=True,
    ) as client:
        ok, _status, error_type, error_message, _elapsed = await verify_url(client, site.base_url)

    if ok:
        site.state = SiteState.REACHABLE.value
        site.success_count += 1
        # failure_count is consecutive: a success clears the streak, so the
        # retry-exhaustion limit (retry.py) measures consecutive failures.
        site.failure_count = 0
        site.last_seen_at = now()
        site.last_checked_at = now()
        site.last_error_type = None
        site.last_error_message = None
        site.next_retry_at = None
        attempt.status = AttemptStatus.SUCCESS.value
        attempt.finished_at = now()
        session.commit()
        return {"verified": 1, "reachable": 1, "failed": 0, "skipped": 0}

    _apply_verify_failure(session, site, attempt, error_type or ErrorType.UNKNOWN_ERROR.value, error_message)
    session.commit()
    return {"verified": 1, "reachable": 0, "failed": 1, "skipped": 0}


async def verify_one_site(site_id: int, attempt_id: int) -> dict[str, int]:
    """Verify one site in its own session with full exception containment."""
    from .db import SessionLocal

    with SessionLocal() as session:
        try:
            return await _verify_one_site_inner(session, site_id, attempt_id)
        except Exception as exc:  # containment: never kill the batch
            logger.exception("contained error verifying site %d", site_id)
            error_type, error_message = classify_exception(exc)
            try:
                site = session.get(Site, site_id)
                attempt = session.get(CrawlAttempt, attempt_id)
                if site is not None and attempt is not None:
                    _apply_verify_failure(session, site, attempt, error_type, error_message)
                    session.commit()
                else:
                    session.rollback()
            except Exception:
                session.rollback()
                logger.exception("failed to record contained verify error for site %d", site_id)
            return {"verified": 1, "reachable": 0, "failed": 1, "skipped": 0, "contained": 1}


async def verify_batch(session: Session, limit: int, epoch: Epoch) -> dict[str, int]:
    if not tcp_proxy_available():
        logger.warning("verify batch skipped: I2P proxy unavailable")
        return {"verified": 0, "reachable": 0, "failed": 0, "skipped": 0, "proxy_unavailable": 1}

    jobs = select_sites_for_verification(session, limit, epoch)
    counts = {"verified": 0, "reachable": 0, "failed": 0, "skipped": 0, "proxy_unavailable": 0}
    if not jobs:
        return counts

    semaphore = asyncio.Semaphore(max(1, settings.max_concurrent_requests))

    async def run_one(site_id: int, base_url: str, attempt_id: int):
        async with semaphore:
            return await verify_one_site(site_id, attempt_id)

    results = await asyncio.gather(
        *(run_one(site_id, base_url, attempt_id) for site_id, base_url, attempt_id in jobs),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            logger.exception("verify task raised outside containment: %r", result)
            counts["failed"] += 1
            continue
        for key in ("verified", "reachable", "failed", "skipped"):
            counts[key] += result.get(key, 0)
    return counts
