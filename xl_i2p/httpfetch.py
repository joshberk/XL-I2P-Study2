"""Bounded HTTP fetching over the I2P proxy.

Streams response bodies and abandons the download as soon as it exceeds
the byte cap, instead of buffering the entire body first. Previously
``await client.get(url)`` downloaded everything and only then checked
``max_content_bytes``: a multi-hundred-MB binary served over a slow I2P
tunnel (e.g. ISO images on a mirror eepsite) wedged the scheduler cycle
for hours, starving the heartbeat and flipping the dashboard to DEAD.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

import httpx

from .config import settings
from .states import ErrorType
from .taxonomy import classify_exception, classify_http_status

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


# Monotonic timestamp of the last completed fetch (success or failure).
# The scheduler's watchdog heartbeat reads this to report progress age: a
# fresh heartbeat with a stale completion timestamp means "alive but stuck".
# Updated by fetch_bounded, which both the crawler and verifier use, so all
# fetch completions are covered. Single event loop: no lock needed.
last_fetch_completed_at: float | None = None


def _mark_fetch_completed() -> None:
    global last_fetch_completed_at
    last_fetch_completed_at = time.monotonic()


def _elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def is_parseable_content_type(content_type: str | None) -> bool:
    """Whether a response body is worth downloading for link extraction.

    Missing/empty content-type is treated as parseable (some eepsites omit
    it); only an explicit non-HTML type aborts the download early. Mirrors
    the crawler's post-download ``page_is_html`` check so the pre-download
    fast-fail never disagrees with it.
    """
    if not content_type:
        return True
    ct = content_type.lower()
    return "html" in ct or "text/plain" in ct


async def fetch_bounded(
    client: httpx.AsyncClient, url: str, cap_bytes: int | None = None,
    timeout_seconds: float | None = None,
) -> FetchResult:
    """GET ``url`` without ever buffering more than ``cap_bytes``.

    Three early exits before/during the download:
      1. the ``content-length`` header already exceeds the cap -> abort
         before reading any body bytes (CONTENT_TOO_LARGE);
      2. the ``content-type`` header declares a non-parseable type (e.g.
         an ISO image) -> abort before reading any body bytes
         (NON_HTML_CONTENT);
      3. the streamed body crosses the cap mid-download -> abort and
         discard the partial buffer (CONTENT_TOO_LARGE).

    A total wall-clock deadline (``timeout_seconds``, default
    ``FETCH_TOTAL_TIMEOUT_SECONDS``) bounds the whole fetch. httpx's
    per-operation timeout cannot do this: a server that keeps dribbling
    bytes defeats per-read timeouts. Exceeding the deadline is reported
    as FETCH_DEADLINE (queryable separately from READ_TIMEOUT), never raised.

    ``elapsed_ms`` measures the full transfer (headers + body until the
    return point), not time-to-headers.

    The timeout catch is version-robust: on Python 3.10
    ``asyncio.TimeoutError`` is a distinct class from the builtin
    ``TimeoutError`` (unified in 3.11), and ``asyncio.wait_for`` raises
    the asyncio one.
    """
    cap = settings.max_content_bytes if cap_bytes is None else cap_bytes
    total_timeout = (
        settings.fetch_total_timeout_seconds if timeout_seconds is None
        else timeout_seconds
    )
    started = time.perf_counter()
    try:
        if total_timeout is not None:
            result = await asyncio.wait_for(
                _fetch_bounded_inner(client, url, cap, started), total_timeout
            )
        else:
            result = await _fetch_bounded_inner(client, url, cap, started)
    except (asyncio.TimeoutError, TimeoutError):
        result = FetchResult(
            url, None, None, None, _elapsed_ms(started),
            ErrorType.FETCH_DEADLINE.value,
            f"total fetch deadline exceeded ({total_timeout}s)",
        )
    _mark_fetch_completed()
    return result


async def _fetch_bounded_inner(
    client: httpx.AsyncClient, url: str, cap: int, started: float
) -> FetchResult:
    try:
        async with client.stream(
            "GET", url, headers={"User-Agent": settings.user_agent}
        ) as response:
            content_type = response.headers.get("content-type")
            if not is_parseable_content_type(content_type):
                return FetchResult(
                    url, response.status_code, content_type, None,
                    _elapsed_ms(started), ErrorType.NON_HTML_CONTENT.value,
                    f"content-type {content_type!r} is not parseable; "
                    "download skipped",
                )
            declared = response.headers.get("content-length")
            if declared is not None:
                try:
                    if int(declared) > cap:
                        return FetchResult(
                            url, response.status_code, content_type, None,
                            _elapsed_ms(started),
                            ErrorType.CONTENT_TOO_LARGE.value,
                            "content-length exceeded limit",
                        )
                except (TypeError, ValueError):
                    pass
            chunks: list[bytes] = []
            total = 0
            async for chunk in response.aiter_bytes(chunk_size=65_536):
                chunks.append(chunk)
                total += len(chunk)
                if total > cap:
                    return FetchResult(
                        url, response.status_code, content_type, None,
                        _elapsed_ms(started),
                        ErrorType.CONTENT_TOO_LARGE.value,
                        "content exceeded limit",
                    )
            content = b"".join(chunks)
        error_type, error_message = classify_http_status(response.status_code, content)
        return FetchResult(
            url, response.status_code, content_type, content,
            _elapsed_ms(started), error_type, error_message,
        )
    except Exception as exc:
        error_type, error_message = classify_exception(exc)
        return FetchResult(url, None, None, None, None, error_type, error_message)
