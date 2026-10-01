"""VPS floodfill sensor: scan the local I2P netDb dir -> JSONL batches.

Incremental: a cursor file (harvest_cursor.json) records {relative_path: mtime_ns}
for every file already processed. Reruns only pick up new or changed files, so
the harvester is safe to run on a timer. Each batch is one JSONL file in
SPOOL_DIR; ship.py moves batches to VM2.

Record schema (one JSON object per line):
  kind: "routerinfo" | "leaseset"
  sensor_id, observed_at (UTC ISO), file (relative path), file_mtime,
  router_hash | dest_hash (+ dest_b32 for leaseSets),
  version / caps / floodfill / published_at (routerinfo, best-effort),
  lease_expiry (leaseset, heuristic),
  size_bytes

Env: NETDB_DIR (default ~/.i2p/netDb), SPOOL_DIR (default ./spool),
SENSOR_ID (default hostname).
"""
from __future__ import annotations

import json
import logging
import os
import socket
from datetime import datetime, timezone
from pathlib import Path

from .parse import (
    LEASESET_RE,
    ROUTERINFO_RE,
    i2p_b64_to_b32_host,
    parse_leaseset,
    parse_routerinfo,
)

logger = logging.getLogger(__name__)

CURSOR_NAME = "harvest_cursor.json"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_cursor(spool_dir: Path) -> dict:
    fp = spool_dir / CURSOR_NAME
    if not fp.exists():
        return {}
    try:
        return json.loads(fp.read_text())
    except Exception:
        logger.warning("cursor unreadable; starting fresh")
        return {}


def save_cursor(spool_dir: Path, cursor: dict) -> None:
    (spool_dir / CURSOR_NAME).write_text(json.dumps(cursor, indent=1))


def iter_changed_files(netdb_dir: Path, cursor: dict):
    """Yield (kind, path, rel, mtime_ns) for new/changed routerInfo/leaseSet files."""
    for fp in sorted(netdb_dir.rglob("routerInfo-*.dat")):
        match = ROUTERINFO_RE.match(fp.name)
        if not match:
            continue
        yield from _check(fp, netdb_dir, cursor, "routerinfo", match.group(1))
    for fp in sorted(netdb_dir.rglob("leaseSet-*.dat")):
        match = LEASESET_RE.match(fp.name)
        if not match:
            continue
        yield from _check(fp, netdb_dir, cursor, "leaseset", match.group(1))


def _check(fp: Path, netdb_dir: Path, cursor: dict, kind: str, file_hash: str):
    try:
        stat = fp.stat()
    except OSError:
        return
    rel = str(fp.relative_to(netdb_dir))
    if cursor.get(rel) == stat.st_mtime_ns:
        return
    yield kind, fp, rel, stat.st_mtime_ns, file_hash


def build_record(kind: str, fp: Path, rel: str, mtime_ns: int, file_hash: str,
                 sensor_id: str, observed_at: str) -> dict | None:
    try:
        data = fp.read_bytes()
    except OSError:
        return None
    record: dict = {
        "kind": kind,
        "sensor_id": sensor_id,
        "observed_at": observed_at,
        "file": rel,
        "file_mtime": mtime_ns / 1e9,
        "size_bytes": len(data),
    }
    if kind == "routerinfo":
        record["router_hash"] = file_hash
        record.update(parse_routerinfo(data))
    else:
        record["dest_hash"] = file_hash
        record["dest_b32"] = i2p_b64_to_b32_host(file_hash)
        record.update(parse_leaseset(data))
    return record


def harvest_once(netdb_dir: Path, spool_dir: Path, sensor_id: str) -> dict:
    """Run one incremental harvest. Returns counts."""
    counts = {"scanned": 0, "new": 0, "batches": 0, "errors": 0}
    if not netdb_dir.exists():
        logger.warning("netDb dir %s does not exist; nothing harvested", netdb_dir)
        return counts
    spool_dir.mkdir(parents=True, exist_ok=True)
    cursor = load_cursor(spool_dir)
    observed_at = utcnow_iso()
    records: list[dict] = []
    for kind, fp, rel, mtime_ns, file_hash in iter_changed_files(netdb_dir, cursor):
        counts["scanned"] += 1
        record = build_record(kind, fp, rel, mtime_ns, file_hash, sensor_id, observed_at)
        if record is None:
            counts["errors"] += 1
            continue
        records.append(record)
        cursor[rel] = mtime_ns
        counts["new"] += 1
    if records:
        batch_name = f"batch-{observed_at.replace(':', '').replace('-', '')}-{len(records)}.jsonl"
        batch_fp = spool_dir / batch_name
        with batch_fp.open("w") as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")
        counts["batches"] = 1
        logger.info("wrote %d records to %s", len(records), batch_fp)
    save_cursor(spool_dir, cursor)
    return counts


def _load_dotenv() -> None:
    """Load KEY=VALUE lines from .env next to the package root (stdlib only).

    Lets manual runs pick up the same config the systemd unit gets via
    EnvironmentFile=. Already-set environment variables always win.
    """
    env_fp = Path(__file__).resolve().parent.parent / ".env"
    if not env_fp.is_file():
        return
    for line in env_fp.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    _load_dotenv()
    netdb_dir = Path(os.getenv("NETDB_DIR", str(Path.home() / ".i2p" / "netDb")))
    spool_dir = Path(os.getenv("SPOOL_DIR", "spool"))
    sensor_id = os.getenv("SENSOR_ID", socket.gethostname())
    counts = harvest_once(netdb_dir, spool_dir, sensor_id)
    print(json.dumps(counts))


if __name__ == "__main__":
    main()
