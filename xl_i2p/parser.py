from __future__ import annotations

import hashlib
from dataclasses import dataclass
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .utils import host_from_url, normalize_url


@dataclass(frozen=True)
class ParsedLink:
    url: str
    host: str
    anchor_text: str | None


@dataclass(frozen=True)
class ParsedPage:
    title: str | None
    text_length: int
    word_count: int
    content_hash: str
    links: list[ParsedLink]


def parse_html(html: bytes | str, base_url: str) -> ParsedPage:
    if isinstance(html, bytes):
        html_text = html.decode("utf-8", errors="replace")
    else:
        html_text = html

    soup = BeautifulSoup(html_text, "lxml")
    title_tag = soup.find("title")
    title = title_tag.get_text(" ", strip=True)[:512] if title_tag else None

    text = soup.get_text(" ", strip=True)
    content_hash = hashlib.sha256(html_text.encode("utf-8", errors="ignore")).hexdigest()

    links: list[ParsedLink] = []
    for a in soup.find_all("a", href=True):
        href = str(a.get("href", "")).strip()
        if not href or href.startswith(("mailto:", "javascript:", "data:")):
            continue
        absolute = normalize_url(urljoin(base_url, href))
        host = host_from_url(absolute)
        if not host:
            continue
        anchor_text = a.get_text(" ", strip=True)[:512] or None
        links.append(ParsedLink(url=absolute, host=host, anchor_text=anchor_text))

    # Deduplicate by URL while preserving order.
    deduped: list[ParsedLink] = []
    seen = set()
    for link in links:
        if link.url not in seen:
            seen.add(link.url)
            deduped.append(link)

    return ParsedPage(
        title=title,
        text_length=len(text),
        word_count=len(text.split()) if text else 0,
        content_hash=content_hash,
        links=deduped,
    )
