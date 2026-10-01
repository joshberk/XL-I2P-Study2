"""Ship harvested JSONL batches to the VM2 ingest spool dir via rsync/SSH.

Key-based SSH only. A shipped-cursor (shipped.json) records batch filenames
already transferred, so reruns only send new batches. Safe to run hourly.

Env: SPOOL_DIR (default ./spool), VM2_HOST, VM2_USER, VM2_SPOOL_DIR,
SSH_KEY (optional, path to private key), DRY_RUN=1 (log only).
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

SHIPPED_NAME = "shipped.json"


def load_shipped(spool_dir: Path) -> dict:
    fp = spool_dir / SHIPPED_NAME
    if not fp.exists():
        return {}
    try:
        return json.loads(fp.read_text())
    except Exception:
        return {}


def save_shipped(spool_dir: Path, shipped: dict) -> None:
    (spool_dir / SHIPPED_NAME).write_text(json.dumps(shipped, indent=1))


def rsync_file(local: Path, host: str, user: str, remote_dir: str,
               ssh_key: str | None, dry_run: bool) -> bool:
    dest = f"{user}@{host}:{remote_dir}/"
    cmd = ["rsync", "-az"]
    if ssh_key:
        cmd += ["-e", f"ssh -i {ssh_key} -o BatchMode=yes -o StrictHostKeyChecking=accept-new"]
    else:
        cmd += ["-e", "ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new"]
    cmd += [str(local), dest]
    if dry_run:
        logger.info("DRY RUN: %s", " ".join(cmd))
        return True
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except Exception:
        logger.exception("rsync failed for %s", local.name)
        return False
    if proc.returncode != 0:
        logger.error("rsync %s failed rc=%d: %s", local.name, proc.returncode,
                     proc.stderr.strip()[:500])
        return False
    return True


def ship_once(spool_dir: Path, host: str, user: str, remote_dir: str,
             ssh_key: str | None = None, dry_run: bool = False) -> dict:
    counts = {"batches": 0, "shipped": 0, "failed": 0}
    shipped = load_shipped(spool_dir)
    batches = sorted(p for p in spool_dir.glob("batch-*.jsonl") if p.name not in shipped)
    counts["batches"] = len(batches)
    for batch in batches:
        if rsync_file(batch, host, user, remote_dir, ssh_key, dry_run):
            shipped[batch.name] = True
            counts["shipped"] += 1
        else:
            counts["failed"] += 1
    save_shipped(spool_dir, shipped)
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spool_dir = Path(os.getenv("SPOOL_DIR", "spool"))
    host = os.getenv("VM2_HOST", "")
    user = os.getenv("VM2_USER", "")
    remote_dir = os.getenv("VM2_SPOOL_DIR", "")
    ssh_key = os.getenv("SSH_KEY") or None
    dry_run = os.getenv("DRY_RUN", "") in {"1", "true", "yes"}
    if not (host and user and remote_dir):
        raise SystemExit("VM2_HOST, VM2_USER and VM2_SPOOL_DIR must all be set")
    counts = ship_once(spool_dir, host, user, remote_dir, ssh_key, dry_run)
    print(json.dumps(counts))


if __name__ == "__main__":
    main()
