"""Seed handling with deduplicated discovery events.

Study 1 wrote 400,720 seed_events rows (one host had 17,993 duplicates).
``record_seed_event`` now upserts on (host, source_type, source_key): repeat
observations bump ``count``/``last_seen_at`` instead of inserting rows.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable

from sqlalchemy import exists, func, select
from sqlalchemy.orm import Session

from .models import NetworkObservation, SeedEvent, Site, now
from .states import SiteState
from .utils import base_url_for_host, extract_i2p_host, site_type_for_host


# Network-observation source carrying the VPS floodfill sensor's lease-set
# harvest (written by vps_harvester/ingest). Lease-set records carry
# host=<dest>.b32.i2p; routerinfo records have host NULL and are ignored.
FLOODFILL_SOURCE_TYPE = "vps_floodfill_netdb"
LEASESET_DISCOVERY_SOURCE = "floodfill_leaseset"


def upsert_site(
    session: Session,
    host: str,
    source: str = "manual",
    state_if_new: str = SiteState.NEW.value,
    network_source: str | None = None,
    discovery_method: str | None = None,
    is_cross_layer: bool = False,
) -> Site:
    existing = session.scalar(select(Site).where(Site.host == host))
    if existing:
        if network_source:
            existing.network_source = network_source
            existing.last_network_observed_at = now()
        if discovery_method:
            existing.discovery_method = discovery_method
        if is_cross_layer:
            existing.is_cross_layer = True
            existing.cross_layer_validated_at = now()
        return existing
    site = Site(
        host=host,
        base_url=base_url_for_host(host),
        site_type=site_type_for_host(host),
        state=state_if_new,
        source=source,
        network_source=network_source,
        discovery_method=discovery_method,
        is_cross_layer=is_cross_layer,
        cross_layer_validated_at=now() if is_cross_layer else None,
        last_network_observed_at=now() if network_source else None,
    )
    session.add(site)
    session.flush()
    return site


def record_seed_event(
    session: Session,
    host: str,
    source_type: str,
    source_detail: str | None = None,
    discovered_from_site_id: int | None = None,
    discovered_from_page_id: int | None = None,
    epoch_id: int | None = None,
) -> SeedEvent:
    """Upsert a discovery event: duplicates increment count instead of rows."""
    key = SeedEvent.make_key(source_detail)
    existing = session.scalar(
        select(SeedEvent).where(
            SeedEvent.host == host,
            SeedEvent.source_type == source_type,
            SeedEvent.source_key == key,
        )
    )
    if existing:
        existing.count += 1
        existing.last_seen_at = now()
        if epoch_id is not None:
            existing.epoch_id = epoch_id
        return existing
    event = SeedEvent(
        host=host,
        source_type=source_type,
        source_detail=(source_detail or "")[:2048] or None,
        source_key=key,
        discovered_from_site_id=discovered_from_site_id,
        discovered_from_page_id=discovered_from_page_id,
        epoch_id=epoch_id,
        count=1,
    )
    session.add(event)
    return event


def import_seed_file(
    session: Session,
    path: str | Path,
    source_type: str = "seed_file",
    epoch_id: int | None = None,
) -> tuple[int, int]:
    path = Path(path)
    inserted = 0
    seen = 0
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        host = extract_i2p_host(line)
        if not host:
            continue
        seen += 1
        before = session.scalar(select(Site.id).where(Site.host == host))
        upsert_site(session, host, source=source_type, state_if_new=SiteState.NEW.value)
        record_seed_event(session, host, source_type, str(path), epoch_id=epoch_id)
        if before is None:
            inserted += 1
    return inserted, seen


def _candidate_values_from_json(obj: object) -> Iterable[str]:
    if isinstance(obj, dict):
        for value in obj.values():
            yield from _candidate_values_from_json(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _candidate_values_from_json(value)
    elif isinstance(obj, str):
        yield obj


def import_cross_layer_file(
    session: Session,
    path: str | Path,
    source_type: str = "floodfill_netdb",
    epoch_id: int | None = None,
) -> tuple[int, int, int]:
    """Import network-layer/floodfill observations and any embedded eepsite hosts.

    Returns: (new_sites, valid_hosts_seen, observations_written).
    """
    path = Path(path)
    text = path.read_text(encoding="utf-8", errors="ignore")
    candidates: list[str] = []

    if path.suffix.lower() in {".json", ".jsonl"}:
        for line in text.splitlines() or [text]:
            line = line.strip()
            if not line:
                continue
            try:
                candidates.extend(_candidate_values_from_json(json.loads(line)))
            except json.JSONDecodeError:
                candidates.append(line)
    elif path.suffix.lower() in {".csv", ".tsv"}:
        dialect = "excel-tab" if path.suffix.lower() == ".tsv" else "excel"
        for row in csv.reader(text.splitlines(), dialect=dialect):
            candidates.extend(row)
    else:
        candidates.extend(text.splitlines())

    inserted = 0
    seen = 0
    observations = 0
    for raw in candidates:
        host = extract_i2p_host(str(raw))
        if not host:
            continue
        seen += 1
        before = session.scalar(select(Site.id).where(Site.host == host))
        upsert_site(
            session,
            host,
            source=source_type,
            state_if_new=SiteState.DISCOVERED.value,
            network_source=source_type,
            discovery_method=source_type,
            is_cross_layer=True,
        )
        record_seed_event(session, host, source_type, str(path), epoch_id=epoch_id)
        session.add(
            NetworkObservation(
                host=host,
                epoch_id=epoch_id,
                source_type=source_type,
                source_detail=str(path),
                raw_value=str(raw)[:4000],
            )
        )
        observations += 1
        if before is None:
            inserted += 1
    return inserted, seen, observations


def admit_leaseset_discoveries(
    session: Session, epoch_id: int | None, limit: int = 500
) -> dict[str, int]:
    """Admit previously unseen .b32.i2p destinations from the VPS floodfill
    lease-set harvest as new DISCOVERED sites.

    Why this exists: outlink crawling can only discover eepsites that are
    linked to, but prior I2P measurements find most eepsites are isolated
    (no incoming/outgoing links). The floodfill sensor's lease-set harvest
    sees published destinations regardless of linkage, so mining it closes
    the link-only discovery blind spot. Admitted hosts still go through the
    normal verify pass, which determines which are actually web services.

    Bounded by ``limit`` (most-recently-observed first) and idempotent:
    re-running admits nothing new.
    """
    site_exists = exists(select(Site.id).where(Site.host == NetworkObservation.host))
    last_seen = func.max(NetworkObservation.observed_at)
    stmt = (
        select(NetworkObservation.host, last_seen.label("last_seen"))
        .where(
            NetworkObservation.source_type == FLOODFILL_SOURCE_TYPE,
            NetworkObservation.host.is_not(None),
            NetworkObservation.host != "",
            ~site_exists,
        )
        .group_by(NetworkObservation.host)
        .order_by(last_seen.desc())
        .limit(limit)
    )
    admitted = 0
    for raw_host, _last_seen in session.execute(stmt):
        host = extract_i2p_host(raw_host or "")
        if not host:
            continue
        if session.scalar(select(Site.id).where(Site.host == host)) is not None:
            continue  # normalized duplicate of an existing site
        upsert_site(
            session,
            host,
            source=LEASESET_DISCOVERY_SOURCE,
            state_if_new=SiteState.DISCOVERED.value,
            network_source=FLOODFILL_SOURCE_TYPE,
            discovery_method=LEASESET_DISCOVERY_SOURCE,
        )
        record_seed_event(
            session,
            host,
            LEASESET_DISCOVERY_SOURCE,
            f"{FLOODFILL_SOURCE_TYPE} leaseset harvest",
            epoch_id=epoch_id,
        )
        admitted += 1
    session.commit()
    return {"admitted": admitted, "limit": limit}
