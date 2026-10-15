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


async def fetch_bounded(
    client: httpx.AsyncClient, url: str, cap_bytes: int | None = None,
    timeout_seconds: float | None = None,
) -> FetchResult:
    """GET ``url`` without ever buffering more than ``cap_bytes``.

    Two early exits, both reported as CONTENT_TOO_LARGE (the same outcome
    the old code produced, minus the multi-hour download):
      1. the ``content-length`` header already exceeds the cap -> abort
         before reading any body bytes;
      2. the streamed body crosses the cap mid-download -> abort and
         discard the partial buffer.

    A total wall-clock deadline (``timeout_seconds``, default
    ``FETCH_TOTAL_TIMEOUT_SECONDS``) bounds the whole fetch. httpx's
    per-operation timeout cannot do this: a server that keeps dribbling
    bytes defeats per-read timeouts. Exceeding the deadline is reported
    as READ_TIMEOUT, never raised.
    """
    cap = settings.max_content_bytes if cap_bytes is None else cap_bytes
    total_timeout = (
        settings.fetch_total_timeout_seconds if timeout_seconds is None
        else timeout_seconds
    )
    started = time.perf_counter()
    try:
        if total_timeout is not None:
            return await asyncio.wait_for(
                _fetch_bounded_inner(client, url, cap, started), total_timeout
            )
        return await _fetch_bounded_inner(client, url, cap, started)
    except TimeoutError:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return FetchResult(
            url, None, None, None, elapsed_ms,
            ErrorType.READ_TIMEOUT.value,
            f"total fetch deadline exceeded ({total_timeout}s)",
        )


async def _fetch_bounded_inner(
    client: httpx.AsyncClient, url: str, cap: int, started: float
) -> FetchResult:
    try:
        async with client.stream(
            "GET", url, headers={"User-Agent": settings.user_agent}
        ) as response:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            content_type = response.headers.get("content-type")
            declared = response.headers.get("content-length")
            if declared is not None:
                try:
                    if int(declared) > cap:
                        return FetchResult(
                            url, response.status_code, content_type, None,
                            elapsed_ms, ErrorType.CONTENT_TOO_LARGE.value,
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
                        elapsed_ms, ErrorType.CONTENT_TOO_LARGE.value,
                        "content exceeded limit",
                    )
            content = b"".join(chunks)
        error_type, error_message = classify_http_status(response.status_code, content)
        return FetchResult(
            url, response.status_code, content_type, content, elapsed_ms,
            error_type, error_message,
        )
    except Exception as exc:
        error_type, error_message = classify_exception(exc)
        return FetchResult(url, None, None, None, None, error_type, error_message)
