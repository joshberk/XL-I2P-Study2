"""Tier 1 cross-layer loop tests (mocked SAM/lookups, fake netDb dir).

No real network: SAM and LeaseSet lookups are stubbed, the netDb census
runs against synthetic routerInfo files built to the documented byte layout.
"""
from __future__ import annotations

import base64
import os
import struct
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select

import xl_i2p.cross_layer as xl_mod
from xl_i2p.cross_layer import (
    CrossLayerResult,
    census_local_netdb,
    parse_routerinfo_bytes,
)
from xl_i2p.epochs import open_epoch
from xl_i2p.models import CrossLayerObservation, NetworkObservation, Site
from xl_i2p.seeds import upsert_site
from xl_i2p.states import SiteState
from xl_i2p.xlayer_pass import run_xlayer_pass, sites_needing_association


def _seed(session, host, state=SiteState.REACHABLE.value):
    upsert_site(session, host, source="test", state_if_new=state)
    session.commit()
    return session.scalar(select(Site).where(Site.host == host))


def _fake_result(host: str) -> CrossLayerResult:
    return CrossLayerResult(
        input_value=host, host=host, canonical_b32="a" * 52 + ".b32.i2p",
        lookup_method="console_cache", leaseset_found=True,
        leaseset_hash="deadbeef", confidence="validated_by_leaseset",
    )


@pytest.fixture()
def stubbed_xlayer(monkeypatch):
    """SAM up, lookups stubbed to validate, floodfill census stubbed."""
    calls = {"lookups": 0}
    monkeypatch.setattr(xl_mod, "sam_alive", lambda: True)
    monkeypatch.setattr(xl_mod, "load_floodfill_hashes", lambda limit=None: ["ff1", "ff2"])

    def fake_lookup(host, floodfill_hashes=None):
        calls["lookups"] += 1
        assert floodfill_hashes == ["ff1", "ff2"], "pass must precompute floodfill hashes once"
        if "bad" in host:
            raise RuntimeError("boom")
        return _fake_result(host)

    monkeypatch.setattr(xl_mod, "lookup_candidate", fake_lookup)
    return calls


def test_pass_only_touches_sites_lacking_epoch_observations(db_session, stubbed_xlayer):
    s = db_session
    epoch = open_epoch(s, "e1")
    s1 = _seed(s, "one.i2p")
    s2 = _seed(s, "two.i2p")
    _seed(s, "three.i2p", state=SiteState.NEW.value)  # not eligible
    # s1 already has an observation this epoch -> must be skipped.
    s.add(CrossLayerObservation(site_id=s1.id, epoch_id=epoch.id, host="one.i2p",
                               lookup_method="console_cache", leaseset_found=True))
    s.commit()

    assert [x.host for x in sites_needing_association(s, epoch.id, 10)] == ["two.i2p"]
    counts = run_xlayer_pass(s, epoch, limit=10)
    assert counts == {"considered": 1, "lookups": 1, "validated": 1,
                      "errors": 0, "sam_down": False}

    rows = list(s.scalars(select(CrossLayerObservation).where(
        CrossLayerObservation.epoch_id == epoch.id)))
    by_host = {r.host: r for r in rows}
    assert set(by_host) == {"one.i2p", "two.i2p"}
    assert by_host["two.i2p"].site_id == s2.id
    assert stubbed_xlayer["lookups"] == 1


def test_pass_sam_down_returns_zeros(db_session, monkeypatch):
    s = db_session
    epoch = open_epoch(s, "e1")
    _seed(s, "one.i2p")
    monkeypatch.setattr(xl_mod, "sam_alive", lambda: False)
    called = []
    monkeypatch.setattr(xl_mod, "lookup_candidate",
                        lambda host, floodfill_hashes=None: called.append(host))
    counts = run_xlayer_pass(s, epoch, limit=10)
    assert counts["lookups"] == 0 and counts["validated"] == 0
    assert counts["sam_down"] is True
    assert called == []
    assert s.scalar(select(func.count()).select_from(CrossLayerObservation)) == 0


def test_pass_survives_per_site_lookup_error(db_session, stubbed_xlayer):
    s = db_session
    epoch = open_epoch(s, "e1")
    _seed(s, "good.i2p")
    _seed(s, "bad.i2p")
    counts = run_xlayer_pass(s, epoch, limit=10)
    assert counts["considered"] == 2
    assert counts["errors"] == 1
    assert counts["validated"] == 1
    assert s.scalar(select(func.count()).select_from(CrossLayerObservation)
                   .where(CrossLayerObservation.host == "good.i2p")) == 1


# ---------------------------------------------------------------------------
# Local netDb census
# ---------------------------------------------------------------------------

def _routerinfo_bytes(version="2.13.0", caps="LRF", published_ms=None):
    blob = b"\x01" * 256 + b"\x02" * 128 + b"\x00" + struct.pack(">H", 0)
    assert len(blob) == 387
    ts = (published_ms if published_ms is not None
          else int(datetime.now(timezone.utc).timestamp() * 1000))
    blob += struct.pack(">Q", ts)
    blob += f"junkrouter.version={version} padding caps={caps} tail".encode()
    return blob


def _b64hash(seed: bytes) -> str:
    return base64.b64encode(seed).decode().replace("+", "-").replace("/", "~")


@pytest.fixture()
def fake_netdb(tmp_path, monkeypatch):
    netdb = tmp_path / "netDb"
    netdb.mkdir()
    h1 = _b64hash(b"r1" * 16)
    h2 = _b64hash(b"r2" * 16)
    (netdb / f"routerInfo-{h1}.dat").write_bytes(_routerinfo_bytes())
    (netdb / f"routerInfo-{h2}.dat").write_bytes(_routerinfo_bytes(version="2.12.0", caps="LR"))
    (netdb / "routerInfo-bad.dat").write_bytes(b"\x00\x01\x02garbage")
    (netdb / "routerInfo-empty.dat").write_bytes(b"")
    monkeypatch.setattr(xl_mod, "local_netdb_path", lambda: netdb)
    return netdb, (h1, h2)


def test_parse_routerinfo_bytes_valid():
    info = parse_routerinfo_bytes(_routerinfo_bytes())
    assert info["version"] == "2.13.0"
    assert info["caps"] == "LRF"
    assert info["floodfill"] is True
    assert info["published_at"] is not None


def test_parse_routerinfo_bytes_malformed_never_raises():
    for blob in (b"", b"\x00" * 10, os.urandom(2000), b"routerInfo-not-a-real-file"):
        info = parse_routerinfo_bytes(blob)
        assert set(info) == {"version", "caps", "floodfill", "published_at"}


def test_census_records_and_dedupes(db_session, fake_netdb):
    s = db_session
    epoch = open_epoch(s, "e1")
    netdb, (h1, h2) = fake_netdb
    first = census_local_netdb(s, epoch.id)
    assert first["files"] == 4
    assert first["recorded"] == 4  # malformed files still get a row; hash is ground truth
    second = census_local_netdb(s, epoch.id)
    assert second["recorded"] == 0
    assert second["skipped_dup"] == 4

    rows = list(s.scalars(select(NetworkObservation).where(
        NetworkObservation.epoch_id == epoch.id)))
    assert len(rows) == 4
    for row in rows:
        assert row.source_type == "local_netdb"
        assert "client-sampled" in row.source_detail
    good = next(r for r in rows if r.router_hash == h1)
    assert '"floodfill": true' in (good.raw_value or "")
    assert '"version": "2.13.0"' in (good.raw_value or "")


def test_census_no_netdb_dir(db_session, monkeypatch):
    monkeypatch.setattr(xl_mod, "local_netdb_path", lambda: None)
    counts = census_local_netdb(db_session, None)
    assert counts == {"files": 0, "recorded": 0, "skipped_dup": 0, "errors": 0}
