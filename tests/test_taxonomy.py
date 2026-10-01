"""Taxonomy classifier tests: specific labels, UNKNOWN_ERROR last resort."""
from __future__ import annotations

import ssl

import httpx

from xl_i2p.states import ErrorType
from xl_i2p.taxonomy import classify_exception, classify_http_status


def test_http_status_classification():
    assert classify_http_status(200) == (None, None)
    assert classify_http_status(301) == (None, None)
    etype, _ = classify_http_status(404)
    assert etype == ErrorType.HTTP_4XX.value
    etype, _ = classify_http_status(503)
    assert etype == ErrorType.HTTP_5XX.value
    etype, _ = classify_http_status(None)
    assert etype == ErrorType.UNKNOWN_ERROR.value


def test_httpx_timeout_classification():
    assert classify_exception(httpx.ConnectTimeout("boom"))[0] == ErrorType.CONNECT_TIMEOUT.value
    assert classify_exception(httpx.ReadTimeout("boom"))[0] == ErrorType.READ_TIMEOUT.value
    assert classify_exception(httpx.WriteTimeout("boom"))[0] == ErrorType.WRITE_TIMEOUT.value
    assert classify_exception(httpx.PoolTimeout("boom"))[0] == ErrorType.PROXY_TIMEOUT.value


def test_proxy_and_connect_errors():
    assert classify_exception(httpx.ProxyError("boom"))[0] == ErrorType.PROXY_ERROR.value
    assert classify_exception(httpx.ConnectError("Connection refused"))[0] == ErrorType.CONNECTION_REFUSED.value
    # Through the I2P proxy a bare ConnectError means the proxy TCP leg failed.
    assert classify_exception(httpx.ConnectError("boom"))[0] == ErrorType.PROXY_ERROR.value


def test_tls_and_unknown():
    assert classify_exception(ssl.SSLError("boom"))[0] == ErrorType.TLS_ERROR.value
    etype, msg = classify_exception(RuntimeError("weird"))
    assert etype == ErrorType.UNKNOWN_ERROR.value
    assert "RuntimeError" in msg
