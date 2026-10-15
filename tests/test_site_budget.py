"""Per-site crawl budget: a slow site is truncated, not wedged.

A site whose pages each take ~0.4s would previously hold a worker for
25 x 0.4s (plus politeness delays) with no bound; with SITE_BUDGET_SECONDS
the crawl stops at the budget, keeps the partial pages, and records
SITE_BUDGET_EXHAUSTED (truncated, not unreachable).
"""
from __future__ import annotations

import asyncio
import dataclasses

import httpx
from sqlalchemy import select

import xl_i2p.crawler as crawler_mod
from xl_i2p.config import Settings
from xl_i2p.crawler import crawl_batch
from xl_i2p.epochs import open_epoch
from xl_i2p.models import CrawlAttempt
from xl_i2p.seeds import upsert_site
from xl_i2p.states import AttemptType, ErrorType, SiteState
from xl_i2p.verifier import verify_batch


def _many_links_html(n: int, host: str = "manylinks.i2p") -> bytes:
    links = "".join(f'<a href="http://{host}/p{i}">p{i}</a>' for i in range(n))
    return (
        f"<html><head><title>Many</title></head><body><p>x</p>{links}</body></html>"
    ).encode()


def _slow_client(delay_s: float, html: bytes):
    class SlowStreamResponse:
        def __init__(self, url):
            self.url = url
            self.status_code = 200
            self.headers = {"content-type": "text/html"}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def aiter_bytes(self, chunk_size=None):
            await asyncio.sleep(delay_s)
            yield html

    class SlowClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def stream(self, method, url, headers=None):
            return SlowStreamResponse(url)

    return SlowClient


def test_site_budget_truncates_slow_site(db_session, monkeypatch):
    s = db_session
    # Links must stay on the crawled host or the same-host BFS filter drops them.
    monkeypatch.setattr(
        httpx, "AsyncClient", _slow_client(0.4, _many_links_html(40, "slowsite.i2p")))
    monkeypatch.setattr("xl_i2p.verifier.tcp_proxy_available", lambda: True)
    monkeypatch.setattr("xl_i2p.crawler.tcp_proxy_available", lambda: True)
    # Budget (0.6s) fires long before the 25-page cap; the fetch deadline
    # (30s) never fires first, so the budget is the binding constraint.
    # Settings is frozen: replace the module's settings object.
    fast = dataclasses.replace(
        Settings(),
        site_budget_seconds=0.6,
        fetch_total_timeout_seconds=30.0,
        crawl_delay_seconds=0,
        max_pages_per_site=25,
    )
    monkeypatch.setattr(crawler_mod, "settings", fast)

    upsert_site(s, "slowsite.i2p", source="test", state_if_new=SiteState.NEW.value)
    s.commit()
    epoch = open_epoch(s, "budget")
    asyncio.run(verify_batch(s, 10, epoch))
    s.expire_all()

    result = asyncio.run(crawl_batch(s, 10, epoch))
    s.expire_all()

    assert result["sites"] == 1
    attempt = s.scalar(
        select(CrawlAttempt).where(
            CrawlAttempt.attempt_type == AttemptType.CRAWL.value)
    )
    assert attempt is not None
    assert attempt.error_type == ErrorType.SITE_BUDGET_EXHAUSTED.value
    assert "truncated" in (attempt.error_message or "").lower()
    # Partial crawl kept: some pages persisted, far fewer than the cap.
    assert 0 < attempt.pages_fetched < 25
    # Usable pages were retrieved, so this is truncation, not failure.
    assert attempt.status == "SUCCESS"
