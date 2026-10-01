"""JSON-lines file logging plus a machine-readable heartbeat.

Logs go to LOG_DIR/xl-i2p-YYYYMMDD.log (daily rotation, ~4 months retained).
The heartbeat file (heartbeat.json) is rewritten every HEARTBEAT_SECONDS with
timestamp / pid / epoch / phase / counters so VM2-side monitoring — or a human
with `tail` — can see the crawler is alive without touching the database.
A heartbeat row is also upserted to the DB (best effort) for SQL monitoring.
"""
from __future__ import annotations

import json
import logging
import logging.handlers
import os
import socket
from datetime import datetime, timezone
from pathlib import Path

from .config import settings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0] is not None:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def setup_logging(log_dir: str | None = None) -> Path:
    directory = Path(log_dir or settings.log_dir)
    directory.mkdir(parents=True, exist_ok=True)
    log_path = directory / "xl-i2p.log"

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Avoid duplicate handlers when setup_logging is called twice (e.g. tests).
    if any(isinstance(h, logging.handlers.TimedRotatingFileHandler) for h in root.handlers):
        return directory

    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_path, when="midnight", backupCount=130, encoding="utf-8"
    )
    file_handler.setFormatter(JsonFormatter())

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )

    root.addHandler(file_handler)
    root.addHandler(console_handler)
    return directory


def heartbeat_path(log_dir: str | None = None) -> Path:
    return Path(log_dir or settings.log_dir) / "heartbeat.json"


def write_heartbeat(
    epoch_label: str | None,
    phase: str,
    counters: dict | None = None,
    log_dir: str | None = None,
) -> dict:
    """Write heartbeat.json and upsert the DB heartbeat row (best effort)."""
    beat = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "epoch": epoch_label,
        "phase": phase,
        "counters": counters or {},
    }
    path = heartbeat_path(log_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(beat, indent=2), encoding="utf-8")
    tmp.replace(path)

    try:
        from .db import SessionLocal
        from .models import Heartbeat, now

        with SessionLocal() as session:
            row = session.get(Heartbeat, 1)
            if row is None:
                row = Heartbeat(id=1)
                session.add(row)
            row.updated_at = now()
            row.pid = beat["pid"]
            row.hostname = beat["hostname"]
            row.epoch_label = epoch_label
            row.phase = phase
            row.counters_json = json.dumps(counters or {})
            session.commit()
    except Exception:
        logging.getLogger(__name__).debug("heartbeat DB write failed", exc_info=True)
    return beat
