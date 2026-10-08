"""Lease-set discovery feed: floodfill lease-set hosts become DISCOVERED sites."""
from __future__ import annotations

from sqlalchemy import func, select

from xl_i2p.models import NetworkObservation, SeedEvent, Site
from xl_i2p.seeds import (
    FLOODFILL_SOURCE_TYPE,
    LEASESET_DISCOVERY_SOURCE,
    admit_leaseset_discoveries,
    upsert_site,
)
from xl_i2p.states import SiteState


def _leaseset_obs(host, sensor="vps-floodfill-01"):
    return NetworkObservation(
        host=host,
        epoch_id=None,
        source_type=FLOODFILL_SOURCE_TYPE,
        source_detail=sensor,
        raw_value='{"kind": "leaseset"}',
    )


def _site_count(session):
    return session.scalar(select(func.count()).select_from(Site))


def test_admits_new_leaseset_hosts(db_session):
    s = db_session
    s.add(_leaseset_obs("aaaabbbbccccdddd.b32.i2p"))
    s.add(_leaseset_obs("eeeeffffgggghhhh.b32.i2p"))
    # routerinfo records carry host NULL and must be ignored
    s.add(NetworkObservation(host=None, epoch_id=None,
                             source_type=FLOODFILL_SOURCE_TYPE,
                             source_detail="vps-floodfill-01",
                             raw_value='{"kind": "routerinfo"}'))
    s.commit()

    result = admit_leaseset_discoveries(s, epoch_id=None, limit=500)
    assert result["admitted"] == 2
    assert _site_count(s) == 2
    for site in s.scalars(select(Site)):
        assert site.state == SiteState.DISCOVERED.value
        assert site.source == LEASESET_DISCOVERY_SOURCE
        assert site.discovery_method == LEASESET_DISCOVERY_SOURCE
        assert site.network_source == FLOODFILL_SOURCE_TYPE
    events = s.scalars(select(SeedEvent)).all()
    assert len(events) == 2
    assert all(e.source_type == LEASESET_DISCOVERY_SOURCE for e in events)


def test_skips_existing_sites_and_is_idempotent(db_session):
    s = db_session
    upsert_site(s, "aaaabbbbccccdddd.b32.i2p", source="seed_file")
    s.add(_leaseset_obs("aaaabbbbccccdddd.b32.i2p"))
    s.add(_leaseset_obs("eeeeffffgggghhhh.b32.i2p"))
    s.commit()

    result = admit_leaseset_discoveries(s, epoch_id=None, limit=500)
    assert result["admitted"] == 1  # only the genuinely new host
    assert _site_count(s) == 2

    # Second run admits nothing new.
    result = admit_leaseset_discoveries(s, epoch_id=None, limit=500)
    assert result["admitted"] == 0
    assert _site_count(s) == 2


def test_respects_limit(db_session):
    s = db_session
    for i in range(5):
        s.add(_leaseset_obs(f"host{i:04d}xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.b32.i2p"))
    s.commit()
    result = admit_leaseset_discoveries(s, epoch_id=None, limit=2)
    assert result["admitted"] == 2
    assert _site_count(s) == 2
    result = admit_leaseset_discoveries(s, epoch_id=None, limit=10)
    assert result["admitted"] == 3
    assert _site_count(s) == 5
