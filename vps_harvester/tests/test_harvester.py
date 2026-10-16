"""VPS floodfill harvester tests (stdlib only; no network, no DB driver).

Byte-layout assumptions under test (see netdb_harvester/parse.py):
- routerInfo: 256-byte pubkey + 128-byte signkey + 1-byte cert type +
  2-byte big-endian cert length (=387+cert_len identity), then an 8-byte
  big-endian millisecond published timestamp, then ASCII k=v options.
Fixtures below are constructed to those assumptions.
"""
from __future__ import annotations

import base64
import json
import os
import struct
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from netdb_harvester import harvest as harvest_mod
from netdb_harvester import ship as ship_mod
from netdb_harvester.harvest import harvest_once
from netdb_harvester.parse import (
    i2p_b64_to_b32_host,
    parse_routerinfo,
)
from netdb_harvester.ship import ship_once


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _b64(seed: bytes) -> str:
    return base64.b64encode(seed).decode().replace("+", "-").replace("/", "~")


def make_routerinfo(version="2.13.0", caps="LRF", published_ms=None) -> bytes:
    blob = b"\x01" * 256 + b"\x02" * 128 + b"\x00" + struct.pack(">H", 0)
    assert len(blob) == 387
    ts = published_ms if published_ms is not None else _now_ms() - 3600_000
    blob += struct.pack(">Q", ts)
    blob += f"junkrouter.version={version} padding caps={caps} tail".encode()
    return blob


def test_parse_routerinfo_extracts_metadata():
    info = parse_routerinfo(make_routerinfo())
    assert info["version"] == "2.13.0"
    assert info["caps"] == "LRF"
    assert info["floodfill"] is True
    assert info["published_at"] is not None


def test_parse_routerinfo_length_prefixed_options():
    # Real I2P option encoding: length-prefixed key/value pairs, e.g.
    # b"\x04caps=\x03XfR;". The old parser returned None for all of these.
    blob = b"\x01" * 256 + b"\x02" * 128 + b"\x00" + struct.pack(">H", 0)
    assert len(blob) == 387
    blob += struct.pack(">Q", _now_ms() - 3600_000)
    blob += b"junk"
    blob += bytes([len("router.version")]) + b"router.version="
    blob += bytes([len("2.13.0")]) + b"2.13.0"
    blob += b";padding"
    blob += bytes([len("caps")]) + b"caps="
    blob += bytes([len("XfR")]) + b"XfR"
    blob += b";tail"
    info = parse_routerinfo(blob)
    assert info["version"] == "2.13.0"
    assert info["caps"] == "XfR"
    assert info["floodfill"] is True
    assert info["published_at"] is not None


def _i2p_string(value: str) -> bytes:
    raw = value.encode()
    return bytes([len(raw)]) + raw  # DataHelper.writeString


def _i2p_mapping(options: dict) -> bytes:
    body = b"".join(
        _i2p_string(k) + b"=" + _i2p_string(v) + b";" for k, v in sorted(options.items())
    )
    return struct.pack(">H", len(body)) + body  # DataHelper.writeProperties


def _router_address(style: str, options: dict) -> bytes:
    # RouterAddress: cost(1) + expiration(8) + transport style + options.
    return b"\x0a" + b"\x00" * 8 + _i2p_string(style) + _i2p_mapping(options)


def make_realistic_routerinfo(router_caps: str, ssu2_caps: str) -> bytes:
    """RouterInfo in the real field order: identity, published, addresses
    (each with its own options), peer size, router options, signature."""
    identity = (b"\x01" * 256 + b"\x02" * 128 + b"\x05"
                + struct.pack(">H", 4) + b"\x00\x07\x00\x04")
    published = struct.pack(">Q", _now_ms() - 3600_000)
    addresses = (
        _router_address("NTCP2", {"host": "198.51.100.7", "port": "18443", "v": "2"})
        + _router_address("SSU2", {"caps": ssu2_caps, "host": "198.51.100.7",
                                   "port": "18443", "v": "2"})
    )
    options = _i2p_mapping({"caps": router_caps, "netId": "2",
                            "router.version": "0.9.67"})
    return identity + published + b"\x02" + addresses + b"\x00" + options + b"\x00" * 64


@pytest.mark.parametrize("ssu2_caps", ["BC", "B4", "B", "4", "46"])
def test_parse_routerinfo_uses_router_caps_not_ssu2_address_caps(ssu2_caps):
    """Regression: the first caps= in a RouterInfo is usually the SSU2
    address's ("BC", "B4"...), which reported every floodfill as False."""
    info = parse_routerinfo(make_realistic_routerinfo("XfR", ssu2_caps))
    assert info["caps"] == "XfR"
    assert info["floodfill"] is True
    assert info["version"] == "0.9.67"

    info = parse_routerinfo(make_realistic_routerinfo("LR", ssu2_caps))
    assert info["caps"] == "LR"
    assert info["floodfill"] is False


def test_parse_routerinfo_truncated_never_raises():
    for blob in (b"", b"\x00" * 10, os.urandom(2000)):
        info = parse_routerinfo(blob)
        assert set(info) == {"version", "caps", "floodfill", "published_at"}


def test_b64_to_b32_host_roundtrip():
    raw = os.urandom(32)
    host = i2p_b64_to_b32_host(_b64(raw))
    assert host is not None and host.endswith(".b32.i2p") and len(host) == 60
    assert i2p_b64_to_b32_host("!!!not-base64!!!") is None


@pytest.fixture()
def netdb_dir(tmp_path):
    d = tmp_path / "netDb"
    (d / "r1").mkdir(parents=True)
    (d / "r1" / f"routerInfo-{_b64(b'a' * 32)}.dat").write_bytes(make_routerinfo())
    (d / "r1" / f"routerInfo-{_b64(b'b' * 32)}.dat").write_bytes(
        make_routerinfo(version="2.12.0", caps="LR"))
    return d


def test_harvest_incremental_cursor(tmp_path, netdb_dir):
    spool = tmp_path / "spool"
    first = harvest_once(netdb_dir, spool, "sensor-1")
    assert first == {"scanned": 2, "new": 2, "batches": 1, "errors": 0}
    batches = list(spool.glob("batch-*.jsonl"))
    assert len(batches) == 1
    records = [json.loads(line) for line in batches[0].read_text().splitlines()]
    assert len(records) == 2
    kinds = sorted(r["kind"] for r in records)
    assert kinds == ["routerinfo", "routerinfo"]
    assert all(r["sensor_id"] == "sensor-1" for r in records)
    ri = next(r for r in records if r["kind"] == "routerinfo")
    assert ri["version"] in {"2.13.0", "2.12.0"}

    # Second run: nothing new.
    second = harvest_once(netdb_dir, spool, "sensor-1")
    assert second["new"] == 0 and second["batches"] == 0
    assert len(list(spool.glob("batch-*.jsonl"))) == 1

    # Touch one file with a new mtime -> picked up again.
    target = netdb_dir / "r1" / f"routerInfo-{_b64(b'a' * 32)}.dat"
    target.write_bytes(make_routerinfo(version="2.14.0"))
    os.utime(target, ns=(10**18, 10**18 + 5))
    third = harvest_once(netdb_dir, spool, "sensor-1")
    assert third["new"] == 1 and third["batches"] == 1


def test_harvest_missing_netdb_dir(tmp_path):
    counts = harvest_once(tmp_path / "nope", tmp_path / "spool", "s")
    assert counts["new"] == 0 and counts["batches"] == 0


def test_ship_only_new_batches_with_fake_rsync(tmp_path, monkeypatch):
    spool = tmp_path / "spool"
    spool.mkdir()
    (spool / "batch-a.jsonl").write_text("{}\n")
    (spool / "batch-b.jsonl").write_text("{}\n")
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(ship_mod.subprocess, "run", fake_run)
    first = ship_once(spool, "vm2", "user", "/remote/spool")
    assert first == {"batches": 2, "shipped": 2, "failed": 0}
    assert len(calls) == 2
    assert all(c[0] == "rsync" for c in calls)

    second = ship_once(spool, "vm2", "user", "/remote/spool")
    assert second["batches"] == 0 and len(calls) == 2


def test_ship_failure_not_marked_shipped(tmp_path, monkeypatch):
    spool = tmp_path / "spool"
    spool.mkdir()
    (spool / "batch-a.jsonl").write_text("{}\n")

    def fake_run(cmd, **kwargs):
        return subprocess.CompletedProcess(cmd, 1, "", "boom")

    monkeypatch.setattr(ship_mod.subprocess, "run", fake_run)
    result = ship_once(spool, "vm2", "user", "/remote/spool")
    assert result == {"batches": 1, "shipped": 0, "failed": 1}
    # retry is attempted again on the next run
    result2 = ship_once(spool, "vm2", "user", "/remote/spool")
    assert result2["batches"] == 1


# ---------------------------------------------------------------------------
# ingest_netdb (fake DB: no pymysql needed)
# ---------------------------------------------------------------------------

from ingest.ingest_netdb import ingest_once  # noqa: E402


class FakeCursor:
    def __init__(self, epoch_id):
        self._epoch_id = epoch_id
        self.seen: set = set()
        self.rows: list = []
        self._fetch = None

    def execute(self, sql, params=None):
        if sql.startswith("SELECT id FROM epochs"):
            self._fetch = (self._epoch_id,) if self._epoch_id else None
        elif sql.startswith("SELECT 1 FROM"):
            self._fetch = (1,) if params[0] in self.seen else None
        elif sql.startswith("INSERT INTO"):
            self.rows.append(params)
            self.seen.add(params[1] or params[0])
            self._fetch = None

    def fetchone(self):
        return self._fetch

    def close(self):
        pass


class FakeDB:
    def __init__(self, epoch_id=7):
        self.cur = FakeCursor(epoch_id)
        self.commits = 0

    def cursor(self):
        return self.cur

    def commit(self):
        self.commits += 1


def _write_batch(spool: Path, name: str, records: list[dict]) -> Path:
    fp = spool / name
    fp.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    return fp


def _record(kind="routerinfo", sensor_id="vps-floodfill-01", **kw):
    base = {"kind": kind, "sensor_id": sensor_id,
            "observed_at": "2026-10-03T00:00:00Z", "file": "r1/x.dat",
            "file_mtime": 1.0, "size_bytes": 10}
    if kind == "routerinfo":
        base["router_hash"] = kw.pop("router_hash", _b64(b"r" * 32))
    base.update(kw)
    return base


def test_ingest_inserts_and_dedupes(tmp_path):
    spool = tmp_path / "ispool"
    spool.mkdir()
    _write_batch(spool, "batch-1.jsonl",
                 [_record("routerinfo"),
                  _record("routerinfo", router_hash=_b64(b"s" * 32)),
                  {"kind": "bogus"}])
    db = FakeDB(epoch_id=7)
    first = ingest_once(spool, db)
    assert first["inserted"] == 2 and first["invalid"] == 1
    assert db.commits == 1
    # epoch tagged, source_type correct, sensor in source_detail
    for row in db.cur.rows:
        host, router_hash, epoch_id, source_type, source_detail = row[:5]
        assert epoch_id == 7
        assert source_type == "vps_floodfill_netdb"
        assert source_detail.startswith("sensor=vps-floodfill-01 batch=batch-1.jsonl")

    # rerun after wiping the ingested cursor -> record-level dedup kicks in
    (spool / "ingested.json").unlink()
    second = ingest_once(spool, db)
    assert second["inserted"] == 0 and second["skipped_dup"] == 2


def test_ingest_no_open_epoch_uses_null(tmp_path):
    spool = tmp_path / "ispool"
    spool.mkdir()
    _write_batch(spool, "batch-1.jsonl", [_record("routerinfo")])
    db = FakeDB(epoch_id=None)
    result = ingest_once(spool, db)
    assert result["inserted"] == 1
    assert db.cur.rows[0][2] is None


def test_ingest_skips_bad_lines(tmp_path):
    spool = tmp_path / "ispool"
    spool.mkdir()
    (spool / "batch-1.jsonl").write_text("not json\n" + json.dumps(_record()) + "\n")
    db = FakeDB()
    result = ingest_once(spool, db)
    assert result["inserted"] == 1 and result["invalid"] == 1
