"""Test fixtures: in-memory SQLite + fake I2P HTTP proxy.

The per-site workers import ``SessionLocal`` lazily from ``xl_i2p.db``,
so monkeypatching ``xl_i2p.db.SessionLocal`` redirects them to SQLite.
No real network is used anywhere in the suite.
"""
from __future__ import annotations

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import xl_i2p.db
from xl_i2p.models import Base


@pytest.fixture()
def db_session(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    TestSession = sessionmaker(bind=engine, expire_on_commit=False)
    monkeypatch.setattr(xl_i2p.db, "SessionLocal", TestSession)
    session = TestSession()
    yield session
    session.close()


class FakeResponse:
    def __init__(self, url: str, html: bytes, status_code: int = 200):
        self.url = url
        self.status_code = status_code
        self.headers = {"content-type": "text/html"}
        self.content = html


def make_fake_client(html: bytes, status_code: int = 200):
    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def get(self, url, headers=None):
            return FakeResponse(url, html, status_code)

    return FakeClient


SIMPLE_HTML = (
    b"<html><head><title>Test Eepsite</title></head><body>"
    b"<p>hello i2p world</p>"
    b'<a href="/about">about</a>'
    b'<a href="http://otherexample.i2p/">external</a>'
    b"</body></html>"
)


def many_links_html(n: int, host: str = "manylinks.i2p") -> bytes:
    links = "".join(f'<a href="http://{host}/p{i}">p{i}</a>' for i in range(n))
    return (
        f"<html><head><title>Many</title></head><body><p>x</p>{links}</body></html>"
    ).encode()


@pytest.fixture()
def fake_i2p(monkeypatch):
    """Pretend the I2P proxy is up and every fetch returns SIMPLE_HTML."""
    monkeypatch.setattr("xl_i2p.verifier.tcp_proxy_available", lambda: True)
    monkeypatch.setattr("xl_i2p.crawler.tcp_proxy_available", lambda: True)
    monkeypatch.setattr(httpx, "AsyncClient", make_fake_client(SIMPLE_HTML))
    return SIMPLE_HTML


@pytest.fixture()
def fake_i2p_many_links(monkeypatch):
    html = many_links_html(40)
    monkeypatch.setattr("xl_i2p.verifier.tcp_proxy_available", lambda: True)
    monkeypatch.setattr("xl_i2p.crawler.tcp_proxy_available", lambda: True)
    monkeypatch.setattr(httpx, "AsyncClient", make_fake_client(html))
    return html
