"""Failure taxonomy.

Study 1 logged 3,463/3,463 crawl failures as UNKNOWN_ERROR, which made the
failure data useless. Every exception and HTTP status is now mapped to a
specific, queryable error type; UNKNOWN_ERROR is the last resort.
"""
from __future__ import annotations

import ssl

import httpx

from .states import ErrorType


def classify_http_status(status_code: int | None) -> tuple[str | None, str | None]:
    """Return (error_type, message) for an HTTP response; (None, None) if OK-ish."""
    if status_code is None:
        return ErrorType.UNKNOWN_ERROR.value, "no HTTP status received"
    if status_code < 400:
        return None, None
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


def classify_fetch_error(error: BaseException | None, status_code: int | None) -> tuple[str | None, str | None]:
    """Combined classifier for a fetch outcome: exception wins, else HTTP status."""
    if error is not None:
        return classify_exception(error)
    return classify_http_status(status_code)
