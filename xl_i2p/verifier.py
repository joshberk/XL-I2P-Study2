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
import time

import httpx
from sqlalchemy import and_, not_, or_, select
from sqlalchemy.orm import Session

from .config import settings
from .models import CrawlAttempt, Epoch, Site, now
from .proxy import tcp_proxy_available
from .retry import exhausted, schedule_retry
from .states import AttemptStatus, AttemptType, ErrorType, SiteState
from .taxonomy import classify_exception, classify_http_status

logger = logging.getLogger(__name__)

VERIFY_STATES = [
    SiteState.NEW.value,
    SiteState.DISCOVERED.value,
    SiteState.RETRY_READY.value,
    SiteState.UNREACHABLE.value,
    SiteState.CRAWLED.value,
    SiteState.ERROR.value,
]

TERMINAL_STATES = (SiteState.UNREACHABLE.value, SiteState.ERROR.value)
# Proxy failures are environmental: retry soon without burning the retry budget.
PROXY_ERRORS = {
    ErrorType.PROXY_UNAVAILABLE.value,
    ErrorType.PROXY_TIMEOUT.value,
    ErrorType.PROXY_ERROR.value,
}


def select_sites_for_verification(
    session: Session, limit: int, epoch: Epoch
) -> list[tuple[int, str, int]]:
    """Claim up to ``limit`` sites for verification; returns (site_id, base_url, attempt_id)."""
    terminal_checked_this_epoch = and_(
        Site.state.in_(TERMINAL_STATES),
        Site.last_checked_at.is_not(None),
        Site.last_checked_at >= epoch.started_at,
    )
    stmt = (
        select(Site)
        .where(
            Site.state.in_(VERIFY_STATES),
            or_(Site.next_retry_at.is_(None), Site.next_retry_at <= now()),
            not_(terminal_checked_this_epoch),
        )
        .order_by(Site.last_checked_at.is_not(None), Site.last_checked_at.asc(), Site.id.asc())
        .limit(limit)
    )
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


async def verify_url(
    client: httpx.AsyncClient, base_url: str
) -> tuple[bool, int | None, str | None, str | None, int | None]:
    started = time.perf_counter()
    try:
        response = await client.get(base_url, headers={"User-Agent": settings.user_agent})
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        error_type, error_message = classify_http_status(response.status_code)
        if error_type is None:
            return True, response.status_code, None, None, elapsed_ms
        return False, response.status_code, error_type, error_message, elapsed_ms
    except Exception as exc:
        error_type, error_message = classify_exception(exc)
        return False, None, error_type, error_message, None


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
