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


I2P_PROXY_503_BODY = (
    b"<html><body><H1>I2P ERROR: DESTINATION NOT FOUND</H1>"
    b"That I2P Destination was not found. Perhaps you pasted in the "
    b"wrong BASE64 I2P Destination.</body></html>"
)


def test_i2p_proxy_destination_not_found_split():
    # 503 carrying the I2P proxy's error marker -> proxy-reported
    # destination failure, not an origin server 5xx.
    etype, msg = classify_http_status(503, I2P_PROXY_503_BODY)
    assert etype == ErrorType.I2P_DEST_NOT_FOUND.value
    assert "503" in msg
    # Marker match is case-insensitive.
    etype, _ = classify_http_status(500, b"<h1>i2p error: timeout</h1>")
    assert etype == ErrorType.I2P_DEST_NOT_FOUND.value
    # A plain 5xx with no proxy marker stays HTTP_5XX (origin server failed).
    etype, _ = classify_http_status(503, b"<html><body>Backend exploded</body></html>")
    assert etype == ErrorType.HTTP_5XX.value
    etype, _ = classify_http_status(500, None)
    assert etype == ErrorType.HTTP_5XX.value
    etype, _ = classify_http_status(500, b"")
    assert etype == ErrorType.HTTP_5XX.value
    # A 4xx carrying the proxy marker is the proxy reporting an unreachable
    # destination (e.g. the template-based "400 Destination Not Found"),
    # not an origin 4xx.
    etype, _ = classify_http_status(404, I2P_PROXY_503_BODY)
    assert etype == ErrorType.I2P_DEST_NOT_FOUND.value
    etype, _ = classify_http_status(400, I2P_PROXY_TEMPLATE_BODY)
    assert etype == ErrorType.I2P_DEST_NOT_FOUND.value
    # A plain 4xx with no proxy marker stays HTTP_4XX.
    etype, _ = classify_http_status(404, b"<html><body>not here</body></html>")
    assert etype == ErrorType.HTTP_4XX.value
    # 2xx never classifies, regardless of body.
    assert classify_http_status(200, I2P_PROXY_503_BODY) == (None, None)


I2P_PROXY_TEMPLATE_BODY = (
    b"<html><head><title>502 Domain Not Found</title>"
    b'<link rel="stylesheet" href="http://proxy.i2p/themes/console/classic/console.css">'
    b"</head><body><h1>Domain Not Found</h1></body></html>"
)


def test_i2p_proxy_template_marker():
    # Since I2P 0.9.49 the proxy serves error pages from templates that do
    # not contain "I2P ERROR"; they all link proxy.i2p/themes/console/...,
    # which is language-independent.
    etype, msg = classify_http_status(502, I2P_PROXY_TEMPLATE_BODY)
    assert etype == ErrorType.I2P_DEST_NOT_FOUND.value
    assert "502" in msg
    etype, _ = classify_http_status(504, I2P_PROXY_TEMPLATE_BODY)
    assert etype == ErrorType.I2P_DEST_NOT_FOUND.value
    # A 502 page with neither marker is an origin server failure.
    etype, _ = classify_http_status(502, b"<html><body>Bad gateway</body></html>")
    assert etype == ErrorType.HTTP_5XX.value
