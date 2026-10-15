"""Failure taxonomy.

Study 1 logged 3,463/3,463 crawl failures as UNKNOWN_ERROR, which made the
failure data useless. Every exception and HTTP status is now mapped to a
specific, queryable error type; UNKNOWN_ERROR is the last resort.
"""
from __future__ import annotations

import ssl

import httpx

from .states import ErrorType


# Markers emitted by the I2P HTTP proxy (i2ptunnel) on its generated error
# pages, which report an unreachable destination rather than an origin
# server failure.
#
# Two signals are checked because the proxy's error pages changed across
# router versions:
# - b"i2p error:": the hard-coded ERR_DESTINATION_UNKNOWN fallback page in
#   I2PTunnelHTTPClientBase (e.g. "<H1>I2P ERROR: DESTINATION NOT
#   FOUND</H1>").
# - b"proxy.i2p/": every template-based error page since I2P 0.9.49 links
#   its stylesheet/scripts as http://proxy.i2p/themes/console/... This
#   marker is language-independent (the page titles like "Website
#   Unreachable" / "Domain Not Found" / "Destination Not Found" are
#   localized) and an origin eepsite has no reason to reference the
#   router's internal proxy.i2p address, so a 4xx/5xx carrying it is
#   the proxy talking, not the site.
_I2P_PROXY_ERROR_MARKERS = (b"i2p error:", b"proxy.i2p/")
_PROXY_BODY_SCAN_LIMIT = 8192


def _looks_like_i2p_proxy_error(body: bytes | None) -> bool:
    if not body:
        return False
    lowered = bytes(body[:_PROXY_BODY_SCAN_LIMIT]).lower()
    return any(marker in lowered for marker in _I2P_PROXY_ERROR_MARKERS)


def classify_http_status(
    status_code: int | None, body: bytes | None = None
) -> tuple[str | None, str | None]:
    """Return (error_type, message) for an HTTP response; (None, None) if OK-ish.

    ``body`` is the (bounded) response body; when a 4xx/5xx carries one of
    the I2P proxy's error markers it is classified as I2P_DEST_NOT_FOUND
    (the proxy reporting an unreachable destination) rather than HTTP_4XX /
    HTTP_5XX (the origin server itself failing).
    """
    if status_code is None:
        return ErrorType.UNKNOWN_ERROR.value, "no HTTP status received"
    if status_code < 400:
        return None, None
    if _looks_like_i2p_proxy_error(body):
        return (
            ErrorType.I2P_DEST_NOT_FOUND.value,
            f"HTTP {status_code} (I2P proxy: destination not found)",
        )
    if 400 <= status_code < 500:
        return ErrorType.HTTP_4XX.value, f"HTTP {status_code}"
    return ErrorType.HTTP_5XX.value, f"HTTP {status_code}"


def _message(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def classify_exception(exc: BaseException) -> tuple[str, str]:
    """Map an exception to (error_type, message). Never raises."""
    try:
        if isinstance(exc, httpx.ProxyError):
            return ErrorType.PROXY_ERROR.value, _message(exc)
        if isinstance(exc, httpx.ConnectTimeout):
            return ErrorType.CONNECT_TIMEOUT.value, _message(exc)
        if isinstance(exc, httpx.ReadTimeout):
            return ErrorType.READ_TIMEOUT.value, _message(exc)
        if isinstance(exc, httpx.WriteTimeout):
            return ErrorType.WRITE_TIMEOUT.value, _message(exc)
        if isinstance(exc, httpx.PoolTimeout):
            return ErrorType.PROXY_TIMEOUT.value, _message(exc)
        if isinstance(exc, httpx.ConnectError):
            text = str(exc).lower()
            if "connection refused" in text:
                return ErrorType.CONNECTION_REFUSED.value, _message(exc)
            if "name resolution" in text or "nodename nor servname" in text or "dns" in text:
                return ErrorType.DNS_ERROR.value, _message(exc)
            # Through the I2P HTTP proxy a bare ConnectError almost always
            # means the proxy TCP connection itself failed.
            return ErrorType.PROXY_ERROR.value, _message(exc)
        if isinstance(exc, httpx.TimeoutException):
            return ErrorType.PROXY_TIMEOUT.value, _message(exc)
        if isinstance(exc, (ssl.SSLError, ssl.CertificateError)):
            return ErrorType.TLS_ERROR.value, _message(exc)
        if isinstance(exc, (httpx.RemoteProtocolError, httpx.DecodingError, httpx.LocalProtocolError)):
            return ErrorType.PROXY_ERROR.value, _message(exc)
        if isinstance(exc, (ValueError, UnicodeError)) and "pars" in type(exc).__name__.lower():
            return ErrorType.PARSE_ERROR.value, _message(exc)
        if isinstance(exc, TimeoutError):
            return ErrorType.CONNECT_TIMEOUT.value, _message(exc)
        if isinstance(exc, ConnectionRefusedError):
            return ErrorType.CONNECTION_REFUSED.value, _message(exc)
    except Exception:
        pass
    return ErrorType.UNKNOWN_ERROR.value, _message(exc)


def classify_fetch_error(
    error: BaseException | None,
    status_code: int | None,
    body: bytes | None = None,
) -> tuple[str | None, str | None]:
    """Combined classifier for a fetch outcome: exception wins, else HTTP status."""
    if error is not None:
        return classify_exception(error)
    return classify_http_status(status_code, body)
