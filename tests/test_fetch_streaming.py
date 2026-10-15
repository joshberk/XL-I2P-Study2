"""Streaming fetch: never buffer more than the byte cap.

Regression tests for the wedge where ``await client.get(url)`` buffered a
multi-hundred-MB binary (e.g. ISO images on a mirror eepsite) before the
``max_content_bytes`` check ran, stalling a scheduler cycle for hours and
starving the heartbeat (dashboard STALE -> DEAD).
"""
from __future__ import annotations

import asyncio

import httpx

from xl_i2p.httpfetch import fetch_bounded
from xl_i2p.states import ErrorType
from xl_i2p.verifier import verify_url

PROXY_503_BODY = (
    b"<html><body><H1>I2P ERROR: DESTINATION NOT FOUND</H1>"
    b"That I2P Destination was not found.</body></html>"
)


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def test_small_body_returns_content():
    async def handler(request):
        return httpx.Response(
            200, headers={"content-type": "text/html"}, content=b"<html>hi</html>"
        )

    async def go():
        async with _client(handler) as client:
            return await fetch_bounded(client, "http://x.i2p/")

    r = asyncio.run(go())
    assert r.status_code == 200
    assert r.content == b"<html>hi</html>"
    assert r.content_type == "text/html"
    assert r.error_type is None


def test_content_length_precheck_aborts_before_reading_body():
    chunks_read = 0

    async def body():
        nonlocal chunks_read
        while True:
            chunks_read += 1
            yield b"x" * 65_536

    async def handler(request):
        return httpx.Response(
            200,
            headers={
                "content-type": "application/octet-stream",
                "content-length": "500000000",
            },
            content=body(),
        )

    async def go():
        async with _client(handler) as client:
            return await fetch_bounded(client, "http://x.i2p/big.iso", cap_bytes=100_000)

    r = asyncio.run(go())
    assert r.error_type == ErrorType.CONTENT_TOO_LARGE.value
    assert r.content is None
    assert r.status_code == 200
    assert chunks_read == 0, "body must not be read when content-length already exceeds the cap"


def test_midstream_abort_bounds_bytes_read():
    chunks_read = 0

    async def body():
        nonlocal chunks_read
        for _ in range(1000):
            chunks_read += 1
            yield b"y" * 65_536

    async def handler(request):
        # No content-length: chunked-style endless body.
        return httpx.Response(
            200, headers={"content-type": "application/octet-stream"}, content=body()
        )

    async def go():
        async with _client(handler) as client:
            return await fetch_bounded(client, "http://x.i2p/endless", cap_bytes=100_000)

    r = asyncio.run(go())
    assert r.error_type == ErrorType.CONTENT_TOO_LARGE.value
    assert r.content is None
    # 100_000 / 65_536 -> abort after the 2nd chunk; must not drain all 1000.
    assert chunks_read == 2, f"expected early abort, read {chunks_read} chunks"


def test_body_at_exactly_cap_is_kept():
    async def handler(request):
        return httpx.Response(200, content=b"z" * 100_000)

    async def go():
        async with _client(handler) as client:
            return await fetch_bounded(client, "http://x.i2p/exact", cap_bytes=100_000)

    r = asyncio.run(go())
    assert r.error_type is None
    assert r.content == b"z" * 100_000


def test_proxy_error_marker_still_detected_through_streaming():
    async def handler(request):
        return httpx.Response(503, content=PROXY_503_BODY)

    async def go():
        async with _client(handler) as client:
            return await fetch_bounded(client, "http://x.i2p/")

    r = asyncio.run(go())
    assert r.error_type == ErrorType.I2P_DEST_NOT_FOUND.value
    assert r.content == PROXY_503_BODY


def test_plain_5xx_stays_http_5xx():
    async def handler(request):
        return httpx.Response(503, content=b"<html>Backend exploded</html>")

    async def go():
        async with _client(handler) as client:
            return await fetch_bounded(client, "http://x.i2p/")

    r = asyncio.run(go())
    assert r.error_type == ErrorType.HTTP_5XX.value


def test_verify_url_ok_and_error_contracts():
    async def ok_handler(request):
        return httpx.Response(200, content=b"<html>ok</html>")

    async def err_handler(request):
        return httpx.Response(500, content=b"boom")

    async def go():
        async with _client(ok_handler) as client:
            ok = await verify_url(client, "http://x.i2p/")
        async with _client(err_handler) as client:
            err = await verify_url(client, "http://x.i2p/")
        return ok, err

    ok, err = asyncio.run(go())
    assert ok[0] is True and ok[1] == 200 and ok[2] is None
    assert err[0] is False and err[1] == 500
    assert err[2] == ErrorType.HTTP_5XX.value
