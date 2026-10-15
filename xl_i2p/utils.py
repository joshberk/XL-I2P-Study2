from __future__ import annotations

import re
from urllib.parse import urlparse, urlunparse

I2P_HOST_RE = re.compile(r"(?P<host>[a-zA-Z0-9._-]+\.(?:b32\.)?i2p)", re.IGNORECASE)


def extract_i2p_host(value: str) -> str | None:
    value = value.strip()
    if not value or value.startswith("#"):
        return None
    match = I2P_HOST_RE.search(value)
    if not match:
        return None
    return normalize_host(match.group("host"))


def normalize_host(host: str) -> str:
    host = host.strip().lower().rstrip(".")
    if host.startswith("http://") or host.startswith("https://"):
        host = urlparse(host).hostname or host
    return host


def site_type_for_host(host: str) -> str:
    return "B32_I2P" if host.endswith(".b32.i2p") else "I2P"


def base_url_for_host(host: str) -> str:
    return f"http://{normalize_host(host)}/"


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    scheme = parsed.scheme or "http"
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    # Avoid fragments; keep query because pages may differ.
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def host_from_url(url: str) -> str | None:
    """Host for a crawled link: only the URL's own hostname, and only if
    it is an .i2p name.

    The old fallback regex-searched the whole URL for anything looking
    like an .i2p host, which invented I2P edges: ``github.com/i2p/i2p.i2p``
    became an edge to ``i2p.i2p``, and router-console "add to addressbook"
    links (``127.0.0.1:7657/...?hostname=foo.i2p``) became edges to
    ``foo.i2p``. A non-.i2p URL is simply not an I2P edge.
    """
    parsed = urlparse(url)
    if parsed.hostname and parsed.hostname.lower().endswith(".i2p"):
        return normalize_host(parsed.hostname)
    return None
