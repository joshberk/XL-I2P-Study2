"""Per-epoch export: CSV per table + optional SQL dump + SHA-256 manifest.

Every export is immutable and self-describing: manifest.json records the
epoch label, export timestamp, row counts, and the SHA-256 of every file.
Analysis on VM2 should run against these frozen exports, never the live DB.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import settings
from .models import (
    Base,
    CrawlAttempt,
    CrossLayerObservation,
    Epoch,
    Link,
    NetworkObservation,
    Page,
    SeedEvent,
    Site,
)
from .states import AttemptStatus, AttemptType, EpochStatus

logger = logging.getLogger(__name__)

# Tables exported whole vs filtered to the epoch.
EPOCH_TABLES = {
    "crawl_attempts": CrawlAttempt,
    "pages": Page,
    "links": Link,
    "seed_events": SeedEvent,
    "network_observations": NetworkObservation,
    "cross_layer_observations": CrossLayerObservation,
}
GLOBAL_TABLES = {"sites": Site, "epochs": Epoch}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _export_table_csv(session: Session, model: type[Base], path: Path,
                      epoch_id: int | None = None) -> int:
    columns = [c.name for c in model.__table__.columns]
    stmt = select(model).order_by(model.__table__.columns["id"])
    if epoch_id is not None and "epoch_id" in columns:
        stmt = stmt.where(model.__table__.columns["epoch_id"] == epoch_id)
    rows = session.execute(stmt).scalars()
    count = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                col: ("" if (v := getattr(row, col)) is None else v)
                for col in columns
            })
            count += 1
    return count


def _export_sql_dump(out_path: Path) -> bool:
    """Full logical dump via mysqldump; skipped gracefully if unavailable."""
    if shutil.which("mysqldump") is None:
        logger.warning("mysqldump not found; skipping SQL dump")
        return False
    parsed = urlparse(settings.database_url)
    if not parsed.scheme.startswith("mysql"):
        logger.warning("not a MySQL URL; skipping SQL dump")
        return False
    cmd = [
        "mysqldump",
        # NOTE: no --routines/--events: they need privileges the crawler DB
        # user does not have, and a single flag error fails the whole dump.
        # The schema has no routines/events; tables are what we preserve.
        "--single-transaction", "--skip-lock-tables",
        f"--host={parsed.hostname}", f"--port={parsed.port or 3306}",
        f"--user={parsed.username}",
        parsed.path.lstrip("/"),
    ]
    # Password via MYSQL_PWD, never --password= on the command line (which
    # is visible to other users via ps).
    env = dict(os.environ, MYSQL_PWD=parsed.password or "")
    try:
        with out_path.open("wb") as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE,
                           timeout=6 * 3600, check=True, env=env)
        logger.info("wrote SQL dump to %s", out_path)
        return True
    except Exception as exc:
        logger.warning("mysqldump failed (%s); continuing without SQL dump", exc)
        if out_path.exists():
            out_path.unlink()
        return False


def export_epoch(
    session: Session, epoch_label: str, export_dir: str | None = None
) -> Path:
    """Export one epoch; returns the manifest path."""
    epoch = session.scalar(select(Epoch).where(Epoch.label == epoch_label))
    if epoch is None:
        raise ValueError(f"no such epoch '{epoch_label}'")
    if epoch.status == "OPEN":
        logger.warning("exporting epoch '%s' while it is still OPEN", epoch_label)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = Path(export_dir or settings.export_dir) / f"epoch_{epoch_label}_{stamp}"
    out_dir.mkdir(parents=True, exist_ok=False)

    manifest: dict = {
        "epoch_label": epoch_label,
        "epoch_id": epoch.id,
        "epoch_status": epoch.status,
        "epoch_started_at": str(epoch.started_at),
        "epoch_ended_at": str(epoch.ended_at),
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "exporter": "xl-i2p-study2",
        "files": {},
        "row_counts": {},
    }

    for name, model in EPOCH_TABLES.items():
        path = out_dir / f"{name}.csv"
        count = _export_table_csv(session, model, path, epoch_id=epoch.id)
        manifest["row_counts"][name] = count
        manifest["files"][path.name] = _sha256(path)

    for name, model in GLOBAL_TABLES.items():
        path = out_dir / f"{name}.csv"
        count = _export_table_csv(session, model, path)
        manifest["row_counts"][name] = count
        manifest["files"][path.name] = _sha256(path)

    # Graph views (parity with Study 1's v_induced_nodes / v_induced_edges).
    nodes_path = out_dir / "induced_nodes.csv"
    edges_path = out_dir / "induced_edges.csv"
    node_count, edge_count = export_graph_csv(session, nodes_path, edges_path, epoch_id=epoch.id)
    manifest["row_counts"]["induced_nodes"] = node_count
    manifest["row_counts"]["induced_edges"] = edge_count
    manifest["files"][nodes_path.name] = _sha256(nodes_path)
    manifest["files"][edges_path.name] = _sha256(edges_path)

    dump_path = out_dir / "full.sql"
    if _export_sql_dump(dump_path):
        manifest["files"][dump_path.name] = _sha256(dump_path)
    else:
        manifest["files"][dump_path.name] = None

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    manifest["files"]["manifest.json"] = _sha256(manifest_path)

    logger.info("epoch '%s' exported to %s (%s)",
                epoch_label, out_dir, json.dumps(manifest["row_counts"], sort_keys=True))
    return manifest_path


def exported_epoch_labels(export_dir: str | Path | None = None) -> set[str]:
    """Labels of epochs that already have a complete export under ``export_dir``.

    An export is complete once its ``manifest.json`` exists: export_epoch
    writes the manifest last, so a crashed export (directory without a
    manifest) is not counted and gets retried. Labels are read from the
    manifest itself, never parsed from directory names (a label such as
    ``a`` would otherwise match ``epoch_a_b_...``).
    """
    root = Path(export_dir or settings.export_dir)
    labels: set[str] = set()
    if not root.is_dir():
        return labels
    for manifest in root.glob("epoch_*/manifest.json"):
        try:
            data = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            logger.warning("unreadable export manifest %s; ignoring", manifest)
            continue
        label = data.get("epoch_label") if isinstance(data, dict) else None
        if isinstance(label, str) and label:
            labels.add(label)
    return labels


def export_pending_epochs(
    session: Session, export_dir: str | Path | None = None
) -> tuple[list[Path], list[str]]:
    """Export every CLOSED epoch that has no complete export yet.

    Why this exists: two actors can roll an epoch — the VM2 rollover timer
    (daily) and the crawler's in-loop backstop on VM1 (every cycle). The
    backstop almost always wins, minutes after the epoch turns due, so a
    timer that only exported "the epoch I just closed myself" would never
    export anything. Exporting by *state* (closed and not yet exported)
    makes the result independent of who rolled, and backfills
    automatically after downtime. Idempotent: re-running exports nothing.

    Returns (manifest paths written, labels that failed). One failing
    export does not stop the others.
    """
    root = Path(export_dir or settings.export_dir)
    done = exported_epoch_labels(root)
    closed = list(
        session.scalars(
            select(Epoch)
            .where(Epoch.status == EpochStatus.CLOSED.value)
            .order_by(Epoch.id)
        )
    )
    written: list[Path] = []
    failed: list[str] = []
    for epoch in closed:
        if epoch.label in done:
            continue
        try:
            written.append(export_epoch(session, epoch.label, str(root)))
        except Exception:
            logger.exception("export of closed epoch '%s' failed", epoch.label)
            failed.append(epoch.label)
    return written, failed


def export_graph_csv(
    session: Session,
    nodes_path: str | Path,
    edges_path: str | Path,
    epoch_id: int | None = None,
) -> tuple[int, int]:
    """Induced graph: crawled sites and inter-site edges, optionally per epoch.

    Nodes are derived from successful CRAWL attempts, not from the mutable
    ``Site.state`` column: since the cohort-reservation change, CRAWLED
    sites are continuously re-verified and flip back to REACHABLE, so a
    state-based node set shrinks as the epoch goes on and depends on when
    the export runs. Attempts are immutable, so the graph is reproducible.
    """
    nodes_path = Path(nodes_path)
    edges_path = Path(edges_path)

    sites = list(session.scalars(select(Site).order_by(Site.id)))
    attempt_stmt = select(CrawlAttempt.site_id).where(
        CrawlAttempt.attempt_type == AttemptType.CRAWL.value,
        CrawlAttempt.status == AttemptStatus.SUCCESS.value,
    )
    if epoch_id is not None:
        attempt_stmt = attempt_stmt.where(CrawlAttempt.epoch_id == epoch_id)
    crawled_ids = set(session.scalars(attempt_stmt))

    link_stmt = select(Link).order_by(Link.id)
    if epoch_id is not None:
        link_stmt = link_stmt.where(Link.epoch_id == epoch_id)
    links = list(session.scalars(link_stmt))

    with nodes_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id", "host", "base_url", "site_type", "state",
                        "first_seen_at", "last_seen_at", "last_crawled_at"],
        )
        writer.writeheader()
        for site in sites:
            if site.id not in crawled_ids:
                continue
            writer.writerow({
                "id": site.id, "host": site.host, "base_url": site.base_url,
                "site_type": site.site_type, "state": site.state,
                "first_seen_at": site.first_seen_at, "last_seen_at": site.last_seen_at,
                "last_crawled_at": site.last_crawled_at,
            })

    seen_edges: set[tuple[int, int]] = set()
    edge_rows = 0
    with edges_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["source_site_id", "target_site_id", "source_host",
                        "target_host", "target_url", "link_type",
                        "first_seen_at", "last_seen_at"],
        )
        writer.writeheader()
        site_by_id = {s.id: s.host for s in sites}
        for link in links:
            if (link.target_site_id is None or link.source_site_id == link.target_site_id
                    or link.source_site_id not in crawled_ids
                    or link.target_site_id not in crawled_ids):
                continue
            key = (link.source_site_id, link.target_site_id)
            if key in seen_edges:
                continue
            seen_edges.add(key)
            writer.writerow({
                "source_site_id": link.source_site_id,
                "target_site_id": link.target_site_id,
                "source_host": site_by_id.get(link.source_site_id),
                "target_host": link.target_host,
                "target_url": link.target_url,
                "link_type": link.link_type,
                "first_seen_at": link.first_seen_at,
                "last_seen_at": link.last_seen_at,
            })
            edge_rows += 1

    return len(crawled_ids), edge_rows
