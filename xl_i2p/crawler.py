"""Site crawler with epoch tagging, backoff, and per-site containment.

Ported from the Study 1 pipeline (BFS, 25-page cap, depth limit, politeness
delay, per-site sessions). Hardened for Study 2:
  * pages/links/attempts/seed events tagged with the active epoch; pages are
    keyed per (normalized_url, epoch_id) so re-crawls are new observations
  * every failure classified with the real taxonomy (Study 1 logged every
    crawl failure as UNKNOWN_ERROR)
  * HTTP 4xx/5xx page responses count as page errors (not usable content)
  * exponential backoff with jitter; MAX_RETRIES then terminal ERROR state
    for the epoch (the janitor + epoch rollover re-probe later)
  * per-site try/except containment + asyncio.gather(return_exceptions=True)
  * page cap enforced strictly
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, deque
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import settings
from .models import CrawlAttempt, Epoch, Link, Page, Site, now
from .parser import parse_html
from .proxy import tcp_proxy_available
from .retry import exhausted, schedule_retry
from .seeds import record_seed_event, upsert_site
from .states import AttemptStatus, AttemptType, ErrorType, SiteState
from .taxonomy import classify_exception, classify_http_status
from .utils import host_from_url, normalize_url, site_type_for_host

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchResult:
    url: str
    status_code: int | None
    content_type: str | None
    content: bytes | None
    elapsed_ms: int | None
    error_type: str | None
    error_message: str | None


def select_sites_for_crawling(
    session: Session, limit: int, epoch: Epoch
) -> list[tuple[int, int]]:
    """Claim up to ``limit`` sites for crawling; returns (site_id, attempt_id)."""
    from sqlalchemy import or_

    stmt = (
        select(Site)
        .where(
            Site.state.in_([SiteState.REACHABLE.value, SiteState.RETRY_READY.value]),
            or_(Site.next_retry_at.is_(None), Site.next_retry_at <= now()),
            # A REACHABLE site already crawled this epoch is done for the epoch.
            or_(
                Site.state != SiteState.REACHABLE.value,
                Site.last_crawled_at.is_(None),
                Site.last_crawled_at < epoch.started_at,
            ),
        )
        .order_by(Site.last_crawled_at.is_not(None), Site.last_crawled_at.asc(), Site.id.asc())
        .limit(limit)
    )
    jobs: list[tuple[int, int]] = []
    for site in session.scalars(stmt):
        site.state = SiteState.CRAWLING.value
        attempt = CrawlAttempt(
            site_id=site.id,
            epoch_id=epoch.id,
            attempt_type=AttemptType.CRAWL.value,
            status=AttemptStatus.STARTED.value,
            discovery_source=site.source,
            is_cross_layer=bool(site.is_cross_layer),
        )
        session.add(attempt)
        session.flush()
        jobs.append((site.id, attempt.id))
    session.commit()
    return jobs


async def fetch(client: httpx.AsyncClient, url: str) -> FetchResult:
    started = time.perf_counter()
    try:
        response = await client.get(url, headers={"User-Agent": settings.user_agent})
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        content_type = response.headers.get("content-type")
        content = response.content
        if len(content) > settings.max_content_bytes:
            return FetchResult(url, response.status_code, content_type, None, elapsed_ms,
                               ErrorType.CONTENT_TOO_LARGE.value, "content exceeded limit")
        error_type, error_message = classify_http_status(response.status_code)
        return FetchResult(url, response.status_code, content_type, content, elapsed_ms,
                           error_type, error_message)
    except Exception as exc:
        error_type, error_message = classify_exception(exc)
        return FetchResult(url, None, None, None, None, error_type, error_message)


def page_is_html(result: FetchResult) -> bool:
    if result.content is None:
        return False
    if result.content_type is None:
        return True
    return "html" in result.content_type.lower() or "text/plain" in result.content_type.lower()


def save_page_result(
    session: Session, site: Site, result: FetchResult, depth: int, epoch_id: int | None
) -> Page:
    normalized = normalize_url(result.url)
    page = session.scalar(
        select(Page).where(Page.normalized_url == normalized, Page.epoch_id == epoch_id)
    )
    if not page:
        page = Page(site_id=site.id, epoch_id=epoch_id, url=result.url,
                    normalized_url=normalized, depth=depth)
        session.add(page)
        session.flush()
    page.http_status = result.status_code
    page.content_type = result.content_type
    page.content_length = len(result.content) if result.content is not None else None
    page.response_time_ms = result.elapsed_ms
    page.error_type = result.error_type
    page.error_message = result.error_message
    page.fetched_at = now()
    return page


def save_link(
    session: Session,
    source_site: Site,
    source_page: Page,
    target_url: str,
    target_host: str,
    anchor_text: str | None,
    epoch_id: int | None,
) -> None:
    target = upsert_site(session, target_host, source="crawl_discovery",
                         state_if_new=SiteState.DISCOVERED.value)
    record_seed_event(
        session,
        target_host,
        "crawl_discovery",
        target_url,
        discovered_from_site_id=source_site.id,
        discovered_from_page_id=source_page.id,
        epoch_id=epoch_id,
    )
    existing = session.scalar(
        select(Link).where(Link.source_page_id == source_page.id, Link.target_url == target_url)
    )
    if existing:
        existing.last_seen_at = now()
        return
    session.add(
        Link(
            source_site_id=source_site.id,
            source_page_id=source_page.id,
            epoch_id=epoch_id,
            target_host=target_host,
            target_url=target_url,
            target_site_id=target.id,
            anchor_text=anchor_text,
            link_type=site_type_for_host(target_host),
        )
    )


def _apply_crawl_failure(
    session: Session, site: Site, attempt: CrawlAttempt,
    counts: dict[str, int], page_errors: Counter,
) -> None:
    dominant = page_errors.most_common(1)
    error_type = dominant[0][0] if dominant else ErrorType.UNKNOWN_ERROR.value
    site.last_crawled_at = now()
    site.failure_count += 1
    site.last_error_type = error_type
    site.last_error_message = f"crawl fetched no usable pages ({dict(page_errors)})"
    attempt.finished_at = now()
    attempt.pages_fetched = counts["pages_fetched"]
    attempt.links_found = counts["links_found"]
    attempt.status = AttemptStatus.FAILED.value
    attempt.error_type = error_type
    attempt.error_message = site.last_error_message

    if exhausted(site):
        site.state = SiteState.ERROR.value
        site.next_retry_at = None  # terminal for this epoch
        logger.info("crawl: %s terminal ERROR after %d failures (%s)",
                    site.host, site.failure_count, error_type)
    else:
        site.state = SiteState.RETRY_READY.value
        delay = schedule_retry(site)
        logger.debug("crawl: %s failed (%s), retry in %.0fs", site.host, error_type, delay)


async def _crawl_one_site_inner(
    session: Session, site_id: int, attempt_id: int, epoch_id: int | None
) -> dict[str, int]:
    site = session.get(Site, site_id)
    attempt = session.get(CrawlAttempt, attempt_id)
    if site is None or attempt is None:
        return {"pages_fetched": 0, "links_found": 0, "errors": 1, "skipped": 1}

    counts = {"pages_fetched": 0, "links_found": 0, "errors": 0, "skipped": 0}
    page_errors: Counter = Counter()
    queue: deque[tuple[str, int]] = deque([(site.base_url, 0)])
    seen_urls: set[str] = set()

    try:
        async with httpx.AsyncClient(
            proxy=settings.i2p_http_proxy,
            timeout=settings.request_timeout_seconds,
            follow_redirects=True,
        ) as client:
            while queue and counts["pages_fetched"] < settings.max_pages_per_site:
                url, depth = queue.popleft()
                normalized = normalize_url(url)
                if normalized in seen_urls or depth > settings.max_depth_per_site:
                    continue
                seen_urls.add(normalized)

                result = await fetch(client, normalized)
                page = save_page_result(session, site, result, depth, epoch_id)
                counts["pages_fetched"] += 1

                if result.error_type:
                    counts["errors"] += 1
                    page_errors[result.error_type] += 1
                    session.commit()
                    continue

                if not page_is_html(result):
                    page.error_type = ErrorType.NON_HTML_CONTENT.value
                    page_errors[ErrorType.NON_HTML_CONTENT.value] += 1
                    counts["errors"] += 1
                    session.commit()
                    continue

                try:
                    parsed = parse_html(result.content or b"", normalized)
                    page.title = parsed.title
                    page.text_length = parsed.text_length
                    page.word_count = parsed.word_count
                    page.content_hash = parsed.content_hash
                    for link in parsed.links:
                        save_link(session, site, page, link.url, link.host, link.anchor_text, epoch_id)
                        counts["links_found"] += 1
                        if link.host == site.host and depth + 1 <= settings.max_depth_per_site:
                            queue.append((link.url, depth + 1))
                    session.commit()
                except Exception as exc:
                    error_type, error_message = classify_exception(exc)
                    page.error_type = ErrorType.PARSER_ERROR.value
                    page.error_message = f"{error_type}: {error_message}"
                    page_errors[ErrorType.PARSER_ERROR.value] += 1
                    counts["errors"] += 1
                    session.commit()

                if settings.crawl_delay_seconds > 0:
                    await asyncio.sleep(settings.crawl_delay_seconds)
    except Exception as exc:
        # Transport-level blowup mid-crawl: classify and treat as site failure.
        error_type, error_message = classify_exception(exc)
        logger.warning("crawl of %s aborted: %s %s", site.host, error_type, error_message)
        page_errors[error_type] += 1
        counts["errors"] += 1

    site.last_crawled_at = now()
    attempt.finished_at = now()
    attempt.pages_fetched = counts["pages_fetched"]
    attempt.links_found = counts["links_found"]

    if counts["pages_fetched"] > 0 and counts["errors"] < counts["pages_fetched"]:
        site.state = SiteState.CRAWLED.value
        site.success_count += 1
        site.last_seen_at = now()
        site.last_error_type = None
        site.last_error_message = None
        site.next_retry_at = None
        attempt.status = AttemptStatus.SUCCESS.value
        logger.debug("crawled %s: %d pages, %d links", site.host,
                     counts["pages_fetched"], counts["links_found"])
    else:
        _apply_crawl_failure(session, site, attempt, counts, page_errors)

    session.commit()
    return counts


async def crawl_one_site(site_id: int, attempt_id: int, epoch_id: int | None) -> dict[str, int]:
    """Crawl one site in its own session with full exception containment."""
    from .db import SessionLocal

    with SessionLocal() as session:
        try:
            return await _crawl_one_site_inner(session, site_id, attempt_id, epoch_id)
        except Exception as exc:  # containment: never kill the batch
            logger.exception("contained error crawling site %d", site_id)
            error_type, error_message = classify_exception(exc)
            try:
                site = session.get(Site, site_id)
                attempt = session.get(CrawlAttempt, attempt_id)
                if site is not None and attempt is not None:
                    _apply_crawl_failure(
                        session, site, attempt,
                        {"pages_fetched": 0, "links_found": 0, "errors": 1, "skipped": 0},
                        Counter({error_type: 1}),
                    )
                    session.commit()
                else:
                    session.rollback()
            except Exception:
                session.rollback()
                logger.exception("failed to record contained crawl error for site %d", site_id)
            return {"pages_fetched": 0, "links_found": 0, "errors": 1, "skipped": 0, "contained": 1}


async def crawl_batch(session: Session, limit: int, epoch: Epoch) -> dict[str, int]:
    if not tcp_proxy_available():
        logger.warning("crawl batch skipped: I2P proxy unavailable")
        return {"sites": 0, "pages_fetched": 0, "links_found": 0, "errors": 0,
                "skipped": 0, "proxy_unavailable": 1}

    jobs = select_sites_for_crawling(session, limit, epoch)
    totals = {"sites": 0, "pages_fetched": 0, "links_found": 0, "errors": 0,
              "skipped": 0, "proxy_unavailable": 0}
    if not jobs:
        return totals

    semaphore = asyncio.Semaphore(max(1, settings.max_concurrent_sites))

    async def run_one(site_id: int, attempt_id: int):
        async with semaphore:
            return await crawl_one_site(site_id, attempt_id, epoch.id)

    results = await asyncio.gather(
        *(run_one(site_id, attempt_id) for site_id, attempt_id in jobs),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            logger.exception("crawl task raised outside containment: %r", result)
            totals["errors"] += 1
            continue
        totals["sites"] += 1
        for key in ("pages_fetched", "links_found", "errors", "skipped"):
            totals[key] += result.get(key, 0)
    return totals
