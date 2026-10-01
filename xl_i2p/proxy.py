from __future__ import annotations

import logging
import socket
import time

import httpx

from .config import settings

logger = logging.getLogger(__name__)


def tcp_proxy_available() -> bool:
    # Supports the normal http://127.0.0.1:4444 form.
    from urllib.parse import urlparse

    parsed = urlparse(settings.i2p_http_proxy)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 4444
    try:
        with socket.create_connection((host, port), timeout=5):
            return True
    except OSError:
        return False


def wait_for_proxy() -> None:
    """Block until the I2P HTTP proxy accepts TCP connections.

    Used at startup instead of exiting when the router is not up yet (e.g.
    still integrating tunnels after a reboot). Logs once per retry interval;
    KeyboardInterrupt / CancelledError propagate so the wait stays killable.
    """
    if tcp_proxy_available():
        return
    logger.warning(
        "I2P HTTP proxy %s unavailable; waiting %ds between retries",
        settings.i2p_http_proxy,
        settings.proxy_retry_seconds,
    )
    while not tcp_proxy_available():
        logger.info(
            "I2P HTTP proxy still unavailable; retrying in %ds",
            settings.proxy_retry_seconds,
        )
        time.sleep(settings.proxy_retry_seconds)
    logger.info("I2P HTTP proxy is now available")


async def fetch_test_eepsite() -> tuple[bool, str]:
    if not tcp_proxy_available():
        return False, "TCP connection to I2P HTTP proxy failed"
    try:
        async with httpx.AsyncClient(proxy=settings.i2p_http_proxy, timeout=20, follow_redirects=True) as client:
            response = await client.get(settings.known_test_eepsite, headers={"User-Agent": settings.user_agent})
            return response.status_code < 500, f"HTTP {response.status_code} from {settings.known_test_eepsite}"
    except Exception as exc:
        return False, f"Proxy reachable, but test eepsite failed: {type(exc).__name__}: {exc}"
