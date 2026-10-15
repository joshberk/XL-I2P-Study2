"""host_from_url must only return the URL's own .i2p hostname.

Regression: the old whole-URL regex fallback invented I2P edges, e.g.
github.com/i2p/i2p.i2p became an edge to i2p.i2p, and router-console
"add to addressbook" links (127.0.0.1:7657/...?hostname=foo.i2p) became
edges to foo.i2p.
"""
from __future__ import annotations

from xl_i2p.utils import extract_i2p_host, host_from_url


def test_host_from_url_i2p_hostname():
    assert host_from_url("http://stats.i2p/page") == "stats.i2p"
    assert host_from_url("http://a.b32.i2p/") == "a.b32.i2p"
    assert host_from_url("http://Example.I2P/x") == "example.i2p"


def test_host_from_url_never_invents_edges():
    # .i2p-looking path on a clearnet host: not an I2P edge.
    assert host_from_url("https://github.com/i2p/i2p.i2p") is None
    # Router-console addressbook link: not an edge to foo.i2p.
    assert host_from_url(
        "http://127.0.0.1:7657/add?hostname=foo.i2p") is None
    # Plain clearnet / localhost URLs.
    assert host_from_url("https://example.com/") is None
    assert host_from_url("http://127.0.0.1:7657/") is None


def test_extract_i2p_host_unchanged_for_seed_parsing():
    # extract_i2p_host keeps its substring behavior for seed files and
    # console output, where the input is not a URL with a hostname.
    assert extract_i2p_host("stats.i2p") == "stats.i2p"
    assert extract_i2p_host("visit foo.i2p today") == "foo.i2p"
