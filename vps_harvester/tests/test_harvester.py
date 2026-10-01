"""VPS floodfill harvester tests (stdlib only; no network, no DB driver).

Byte-layout assumptions under test (see netdb_harvester/parse.py):
- routerInfo: 256-byte pubkey + 128-byte signkey + 1-byte cert type +
  2-byte big-endian cert length (=387+cert_len identity), then an 8-byte
  big-endian millisecond published timestamp, then ASCII k=v options.
- leaseSet: destination hash is ground truth from the filename; expiry is
  the max plausible 8-byte ms timestamp in the trailing 1 KiB.
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
    parse_leaseset,
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


def make_leaseset(expiry_ms=None) -> bytes:
    data = bytearray(b"\x00" * 1500)
    ts = expiry_ms if expiry_ms is not None else _now_ms() + 5 * 60_1000
    struct.pack_into(">Q", data, len(data) - 100, ts)
    return bytes(data)


def test_parse_routerinfo_extracts_metadata():
    info = parse_routerinfo(make_routerinfo())
    assert info["version"] == "2.13.0"
    assert info["caps"] == "LRF"
    assert info["floodfill"] is True
    assert info["published_at"] is not None


def test_parse_routerinfo_truncated_never_raises():
    for blob in (b"", b"\x00" * 10, os.urandom(2000)):
        info = parse_routerinfo(blob)
        assert set(info) == {"version", "caps", "floodfill", "published_at"}


def test_parse_leaseset_expiry_heuristic():
    ts = _now_ms() + 5 * 60_1000
    info = parse_leaseset(make_leaseset(expiry_ms=ts))
    assert info["lease_expiry"] is not None
    # garbage never crashes, and implausible timestamps are rejected
    assert parse_leaseset(b"\x00" * 1500)["lease_expiry"] is None
    assert parse_leaseset(b"")["lease_expiry"] is None


def test_b64_to_b32_host_roundtrip():
    raw = os.urandom(32)
    host = i2p_b64_to_b32_host(_b64(raw))
    assert host is not None and host.endswith(".b32.i2p") and len(host) == 60
    assert i2p_b64_to_b32_host("!!!not-base64!!!") is None


@pytest.fixture()
def netdb_dir(tmp_path):
    d = tmp_path / "netDb"
    (d / "r1").mkdir(parents=True)
    (d / "l2").mkdir(parents=True)
    (d / "r1" / f"routerInfo-{_b64(b'a' * 32)}.dat").write_bytes(make_routerinfo())
    (d / "r1" / f"routerInfo-{_b64(b'b' * 32)}.dat").write_bytes(
        make_routerinfo(version="2.12.0", caps="LR"))
    (d / "l2" / f"leaseSet-{_b64(b'c' * 32)}.dat").write_bytes(make_leaseset())
    return d


def test_harvest_incremental_cursor(tmp_path, netdb_dir):
    spool = tmp_path / "spool"
    first = harvest_once(netdb_dir, spool, "sensor-1")
    assert first == {"scanned": 3, "new": 3, "batches": 1, "errors": 0}
    batches = list(spool.glob("batch-*.jsonl"))
    assert len(batches) == 1
    records = [json.loads(line) for line in batches[0].read_text().splitlines()]
    assert len(records) == 3
    kinds = sorted(r["kind"] for r in records)
    assert kinds == ["leaseset", "routerinfo", "routerinfo"]
    assert all(r["sensor_id"] == "sensor-1" for r in records)
    ri = next(r for r in records if r["kind"] == "routerinfo")
    assert ri["version"] in {"2.13.0", "2.12.0"}
    ls = next(r for r in records if r["kind"] == "leaseset")
    assert ls["dest_b32"].endswith(".b32.i2p")

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
    else:
        base["dest_hash"] = kw.pop("dest_hash", _b64(b"d" * 32))
        base["dest_b32"] = kw.pop("dest_b32", "a" * 52 + ".b32.i2p")
    base.update(kw)
    return base


def test_ingest_inserts_and_dedupes(tmp_path):
    spool = tmp_path / "ispool"
    spool.mkdir()
    _write_batch(spool, "batch-1.jsonl",
                 [_record("routerinfo"), _record("leaseset"), {"kind": "bogus"}])
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
