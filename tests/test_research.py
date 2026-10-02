"""Tests for the research archive (/research, /api/research, /api/research/site)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from xl_i2p import dashboard as dash_mod
from xl_i2p.models import (
    CrawlAttempt,
    CrossLayerObservation,
    Epoch,
    Page,
    Site,
)
from xl_i2p.states import AttemptStatus, AttemptType, EpochStatus, SiteState


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture()
def rseeded(db_session):
    e1 = Epoch(label="2026-Q4", status=EpochStatus.OPEN.value,
               started_at=_now() - timedelta(days=10))
    db_session.add(e1)
    db_session.flush()

    sites = {
        "flap": Site(host="flap.i2p", base_url="http://flap.i2p/",
                     state=SiteState.RETRY_READY.value,
                     first_seen_at=_now() - timedelta(days=10),
                     success_count=2, failure_count=1),
        "steady": Site(host="steady.i2p", base_url="http://steady.i2p/",
                       state=SiteState.RETRY_READY.value,
                       first_seen_at=_now() - timedelta(days=10)),
        "crawled": Site(host="crawled.i2p", base_url="http://crawled.i2p/",
                        state=SiteState.CRAWLED.value,
                        first_seen_at=_now() - timedelta(days=10)),
        "dead": Site(host="dead.i2p", base_url="http://dead.i2p/",
                     state=SiteState.NEW.value,
                     first_seen_at=_now() - timedelta(days=1)),
        "link": Site(host="link.i2p", base_url="http://link.i2p/",
                     state=SiteState.REACHABLE.value, source="crawl_discovery",
                     discovery_method="crawl_discovery",
                     first_seen_at=_now() - timedelta(days=3)),
    }
    db_session.add_all(sites.values())
    db_session.flush()

    def attempt(site, atype, status, days_ago, err=None):
        db_session.add(CrawlAttempt(
            site_id=site.id, epoch_id=e1.id, attempt_type=atype, status=status,
            started_at=_now() - timedelta(days=days_ago),
            finished_at=_now() - timedelta(days=days_ago) + timedelta(minutes=1),
            error_type=err))

    # flap.i2p: SUCCESS -> FAILED -> SUCCESS = 2 transitions, alive now.
    attempt(sites["flap"], AttemptType.VERIFY.value, AttemptStatus.SUCCESS.value, 9)
    attempt(sites["flap"], AttemptType.VERIFY.value, AttemptStatus.FAILED.value, 8,
            err="READ_TIMEOUT")
    attempt(sites["flap"], AttemptType.VERIFY.value, AttemptStatus.SUCCESS.value, 7)
    # steady.i2p: FAILED -> FAILED = 0 transitions.
    attempt(sites["steady"], AttemptType.VERIFY.value, AttemptStatus.FAILED.value, 9,
            err="DNS_ERROR")
    attempt(sites["steady"], AttemptType.VERIFY.value, AttemptStatus.FAILED.value, 8,
            err="DNS_ERROR")
    # crawled.i2p: one verify + one crawl, both SUCCESS = 0 transitions.
    attempt(sites["crawled"], AttemptType.VERIFY.value, AttemptStatus.SUCCESS.value, 6)
    attempt(sites["crawled"], AttemptType.CRAWL.value, AttemptStatus.SUCCESS.value, 5)
    # link.i2p: single successful verify.
    attempt(sites["link"], AttemptType.VERIFY.value, AttemptStatus.SUCCESS.value, 2)

    db_session.add(Page(site_id=sites["crawled"].id, epoch_id=e1.id,
                        url="http://crawled.i2p/", normalized_url="http://crawled.i2p/"))
    db_session.add(CrossLayerObservation(site_id=sites["flap"].id, host="flap.i2p",
                                         epoch_id=e1.id, lookup_method="epoch-loop:sam",
                                         leaseset_found=True,
                                         canonical_b32="abc123.b32.i2p"))
    db_session.commit()
    return {"epoch": e1, "sites": sites}


@pytest.fixture()
def rapp(rseeded):
    application = dash_mod.create_app(token="secret")
    application.config["TESTING"] = True
    return application


def _get(rapp, path):
    return rapp.test_client().get(path + ("&" if "?" in path else "?") + "token=secret")


def test_research_page_renders(rapp):
    r = _get(rapp, "/research")
    assert r.status_code == 200
    assert b"Research Archive" in r.data
    assert b"Mission Control" in r.data


def test_research_requires_auth(rapp):
    assert rapp.test_client().get("/research").status_code == 401
    assert rapp.test_client().get("/api/research").status_code == 401


def test_ops_page_links_to_research(rapp):
    r = _get(rapp, "/")
    assert r.status_code == 200
    assert b"Research archive" in r.data
    assert b"/research?token=" in r.data


def test_api_research_kpis(rapp):
    data = _get(rapp, "/api/research").get_json()
    k = data["kpis"]
    assert k["ever_reachable"] == 3          # flap, crawled, link
    assert k["ever_crawled"] == 1            # crawled
    assert k["ever_reachable_this_epoch"] == 3
    assert k["cohort_total"] == 5
    assert k["cohort_lifetime_pct"] == 60.0
    assert k["xlayer_validated"] == 1
    assert k["discovered_via_links"] == 1


def test_api_research_survival(rapp):
    data = _get(rapp, "/api/research").get_json()
    sv = data["survival"]
    assert sv["final_reachable"] == 3
    assert sv["final_crawled"] == 1
    assert "<svg" in sv["svg"]


def test_api_research_ledger_single_epoch(rapp):
    data = _get(rapp, "/api/research").get_json()
    assert len(data["ledger"]) == 1
    row = data["ledger"][0]
    assert row["label"] == "2026-Q4"
    assert row["ever_reachable"] == 3
    assert row["ever_crawled"] == 1
    assert row["lost"] is None and row["newly_reachable"] is None


def test_api_research_flap_leaders(rapp):
    data = _get(rapp, "/api/research").get_json()
    flaps = data["flap_leaders"]
    assert flaps, "expected at least one flapping site"
    assert flaps[0]["host"] == "flap.i2p"
    assert flaps[0]["transitions"] == 2
    assert flaps[0]["now"] == "alive"
    assert all(f["host"] != "steady.i2p" for f in flaps)


def test_site_lookup(rapp):
    data = _get(rapp, "/api/research/site?host=flap.i2p").get_json()
    assert data["host"] == "flap.i2p"
    assert data["state"] == SiteState.RETRY_READY.value
    assert len(data["attempts"]) == 3
    # newest first
    assert data["attempts"][0]["status"] == AttemptStatus.SUCCESS.value
    assert data["attempts"][1]["error_type"] == "READ_TIMEOUT"
    assert data["cross_layer"][0]["leaseset_found"] is True
    assert data["cross_layer"][0]["canonical_b32"] == "abc123.b32.i2p"


def test_site_lookup_unknown_and_missing(rapp):
    assert _get(rapp, "/api/research/site?host=nope.i2p").status_code == 404
    assert _get(rapp, "/api/research/site").status_code == 400


def test_site_lookup_case_insensitive(rapp):
    data = _get(rapp, "/api/research/site?host=FLAP.I2P").get_json()
    assert data["host"] == "flap.i2p"
