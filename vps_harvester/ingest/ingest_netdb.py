"""VM2 side: ingest VPS floodfill harvester JSONL batches into network_observations.

Runs on VM2 (cron or systemd timer). Standalone: raw SQL via PyMySQL, no
xl_i2p import. Idempotent: a batch is ingested at most once (ingested.json
cursor), and individual records are skipped when an identical observation
already exists.

Source-type discipline (load-bearing for the dissertation's disclosure):
- 'vps_floodfill_netdb' — full-DHT view from the VPS floodfill sensor (this script).
- 'local_netdb'         — client-sampled view from the VM1 vantage router
                          (recorded by the crawler itself, Tier 1).
Never mix them: analysis must filter on source_type.

Row mapping:
- routerinfo records -> router_hash set, host NULL.
- leaseset records   -> host = dest_b32 (falls back to raw dest_hash),
                        router_hash NULL.
- epoch_id = current OPEN epoch, or NULL if none is open (never fails).
- source_detail = "sensor=<sensor_id> batch=<batch> kind=<kind>" — the
  sensor prefix doubles as the idempotency scope.
- Idempotency key per record: (identity, source_type, observed_at,
  sensor_id), where identity = router_hash or host.

Env: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, INGEST_SPOOL_DIR.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

SOURCE_TYPE = "vps_floodfill_netdb"
INGESTED_NAME = "ingested.json"


def _load_dotenv() -> None:
    """Load KEY=VALUE lines from .env next to this script (stdlib only).

    Lets manual and cron runs pick up DB credentials without wrapper
    boilerplate. Already-set environment variables always win.
    """
    env_fp = Path(__file__).resolve().parent / ".env"
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


def utcnow() -> datetime:
    from datetime import timezone
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_observed_at(value: object) -> datetime:
    if isinstance(value, str):
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    return utcnow()


def like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def load_ingested(spool_dir: Path) -> dict:
    fp = spool_dir / INGESTED_NAME
    if not fp.exists():
        return {}
    try:
        return json.loads(fp.read_text())
    except Exception:
        return {}


def save_ingested(spool_dir: Path, ingested: dict) -> None:
    (spool_dir / INGESTED_NAME).write_text(json.dumps(ingested, indent=1))


def get_open_epoch_id(cur) -> int | None:
    cur.execute(
        "SELECT id FROM epochs WHERE status='OPEN' ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    return int(row[0]) if row else None


def record_identity(record: dict) -> tuple[str | None, str | None]:
    """Return (router_hash, host) for a validated record."""
    if record.get("kind") == "routerinfo":
        return record.get("router_hash"), None
    dest_b32 = record.get("dest_b32")
    return None, dest_b32 or record.get("dest_hash")


def validate_record(record: dict) -> str | None:
    """Return an error string, or None if the record is usable."""
    if not isinstance(record, dict):
        return "not a JSON object"
    if record.get("kind") not in {"routerinfo", "leaseset"}:
        return "bad kind"
    if not record.get("sensor_id"):
        return "missing sensor_id"
    if record.get("kind") == "routerinfo" and not record.get("router_hash"):
        return "routerinfo missing router_hash"
    if record.get("kind") == "leaseset" and not (
            record.get("dest_hash") or record.get("dest_b32")):
        return "leaseset missing dest_hash"
    return None


def ingest_batch(cur, batch_name: str, lines: list[str], epoch_id: int | None) -> dict:
    counts = {"lines": 0, "inserted": 0, "skipped_dup": 0, "invalid": 0}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        counts["lines"] += 1
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            counts["invalid"] += 1
            continue
        err = validate_record(record)
        if err:
            counts["invalid"] += 1
            continue
        sensor_id = str(record["sensor_id"])
        router_hash, host = record_identity(record)
        observed_at = parse_observed_at(record.get("observed_at"))
        source_detail = (f"sensor={sensor_id} batch={batch_name} "
                         f"kind={record['kind']}")
        identity_col = "router_hash" if router_hash else "host"
        identity_val = router_hash or host
        # Idempotency: same identity + source + timestamp + sensor.
        cur.execute(
            f"SELECT 1 FROM network_observations WHERE {identity_col}=%s "
            "AND source_type=%s AND observed_at=%s "
            "AND source_detail LIKE %s ESCAPE '\\\\' LIMIT 1",
            (identity_val, SOURCE_TYPE, observed_at,
             f"sensor={like_escape(sensor_id)} %"),
        )
        if cur.fetchone():
            counts["skipped_dup"] += 1
            continue
        raw = {k: v for k, v in record.items()
               if k not in {"kind", "sensor_id", "observed_at", "router_hash",
                            "dest_hash", "dest_b32"}}
        cur.execute(
            "INSERT INTO network_observations "
            "(host, router_hash, epoch_id, source_type, source_detail, "
            " raw_value, observed_at, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (host, router_hash, epoch_id, SOURCE_TYPE, source_detail,
             json.dumps(raw, default=str)[:4000], observed_at, utcnow()),
        )
        counts["inserted"] += 1
    return counts


def ingest_once(spool_dir: Path, db) -> dict:
    totals = {"batches": 0, "inserted": 0, "skipped_dup": 0, "invalid": 0}
    ingested = load_ingested(spool_dir)
    batches = sorted(p for p in spool_dir.glob("batch-*.jsonl")
                     if p.name not in ingested)
    if not batches:
        return totals
    cur = db.cursor()
    try:
        epoch_id = get_open_epoch_id(cur)
        if epoch_id is None:
            logger.warning("no open epoch; ingesting with epoch_id=NULL")
        for batch in batches:
            totals["batches"] += 1
            try:
                lines = batch.read_text().splitlines()
            except OSError:
                logger.error("cannot read batch %s", batch.name)
                continue
            counts = ingest_batch(cur, batch.name, lines, epoch_id)
            db.commit()
            for key in ("inserted", "skipped_dup", "invalid"):
                totals[key] += counts[key]
            ingested[batch.name] = True
            save_ingested(spool_dir, ingested)
            logger.info("ingested %s: %s", batch.name, counts)
    finally:
        cur.close()
    return totals


def connect_db():
    import pymysql

    return pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "xl_i2p_study2"),
        charset="utf8mb4",
        autocommit=False,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    _load_dotenv()
    spool_dir = Path(os.getenv("INGEST_SPOOL_DIR", "ingest_spool"))
    spool_dir.mkdir(parents=True, exist_ok=True)
    db = connect_db()
    try:
        totals = ingest_once(spool_dir, db)
    finally:
        db.close()
    print(json.dumps(totals))


if __name__ == "__main__":
    main()
