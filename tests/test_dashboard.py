"""Tests for the read-only monitoring dashboard (Flask test client, SQLite)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from xl_i2p import dashboard as dash_mod
from xl_i2p.models import (
    CrawlAttempt,
    CrossLayerObservation,
    Epoch,
    Heartbeat,
    Link,
    NetworkObservation,
    Page,
    Site,
)
from xl_i2p.states import AttemptStatus, AttemptType, EpochStatus, SiteState


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture()
def seeded(db_session):
    e1 = Epoch(label="2026-Q3", status=EpochStatus.CLOSED.value,
               started_at=_now() - timedelta(days=60), ended_at=_now() - timedelta(days=30))
    e2 = Epoch(label="2026-Q4", status=EpochStatus.OPEN.value,
               started_at=_now() - timedelta(days=10))
    db_session.add_all([e1, e2])
    db_session.flush()

    sites = {
        "s1": Site(host="a.i2p", base_url="http://a.i2p/", state=SiteState.CRAWLED.value,
                   first_seen_at=_now() - timedelta(days=60)),
        "s2": Site(host="b.i2p", base_url="http://b.i2p/", state=SiteState.REACHABLE.value,
                   first_seen_at=_now() - timedelta(days=60)),
        "s3": Site(host="c.i2p", base_url="http://c.i2p/", state=SiteState.UNREACHABLE.value,
                   first_seen_at=_now() - timedelta(days=60)),
        "s4": Site(host="d.i2p", base_url="http://d.i2p/", state=SiteState.VERIFYING.value,
                   first_seen_at=_now() - timedelta(days=5),
                   last_checked_at=_now() - timedelta(hours=2)),  # stuck
        "s5": Site(host="e.i2p", base_url="http://e.i2p/", state=SiteState.NEW.value,
                   first_seen_at=_now() - timedelta(days=1)),
    }
    db_session.add_all(sites.values())
    db_session.flush()

    def attempt(site, epoch, atype, status, err=None, pages=0, links=0):
        db_session.add(CrawlAttempt(
            site_id=site.id, epoch_id=epoch.id, attempt_type=atype, status=status,
            started_at=_now() - timedelta(hours=1),
            finished_at=_now() - timedelta(minutes=50),
            error_type=err, pages_fetched=pages, links_found=links))

    # Epoch 1: s2 and s3 both verified OK.
    attempt(sites["s2"], e1, AttemptType.VERIFY.value, AttemptStatus.SUCCESS.value)
    attempt(sites["s3"], e1, AttemptType.VERIFY.value, AttemptStatus.SUCCESS.value)
    # Epoch 2: s1 crawled OK, s2 verify OK, s3 verify failed, s4 crawl failed.
    attempt(sites["s1"], e2, AttemptType.CRAWL.value, AttemptStatus.SUCCESS.value,
            pages=3, links=7)
    attempt(sites["s2"], e2, AttemptType.VERIFY.value, AttemptStatus.SUCCESS.value)
    attempt(sites["s3"], e2, AttemptType.VERIFY.value, AttemptStatus.FAILED.value, err="DNS_ERROR")
    attempt(sites["s4"], e2, AttemptType.CRAWL.value, AttemptStatus.FAILED.value,
            err="CONNECT_TIMEOUT")

    for i in range(3):
        db_session.add(Page(site_id=sites["s1"].id, epoch_id=e2.id,
                            url=f"http://a.i2p/p{i}", normalized_url=f"http://a.i2p/p{i}"))
    for i in range(7):
        db_session.add(Link(source_site_id=sites["s1"].id, epoch_id=e2.id,
                            target_host="x.i2p", target_url=f"http://x.i2p/{i}"))

    db_session.add(Heartbeat(id=1, updated_at=_now() - timedelta(seconds=30),
                             phase="cycle", epoch_label="2026-Q4",
                             counters_json=json.dumps({"cycles": 42})))
    db_session.add(Heartbeat(id=2, updated_at=_now() - timedelta(hours=5),
                             phase="cycle", epoch_label="2026-Q4",
                             counters_json=json.dumps({"cycles": 10})))

    db_session.add(CrossLayerObservation(site_id=sites["s1"].id, host="a.i2p",
                                         epoch_id=e2.id, lookup_method="epoch-loop:sam",
                                         leaseset_found=True))
    db_session.add(CrossLayerObservation(site_id=sites["s2"].id, host="b.i2p",
                                         epoch_id=e2.id, lookup_method="epoch-loop:sam",
                                         leaseset_found=False))
    db_session.add(CrossLayerObservation(site_id=sites["s2"].id, host="b.i2p",
                                         epoch_id=e1.id, lookup_method="epoch-loop:sam",
                                         leaseset_found=False))

    for i in range(3):
        db_session.add(NetworkObservation(router_hash=f"rh{i}", epoch_id=e2.id,
                                          source_type="local_netdb"))
    for i in range(2):
        db_session.add(NetworkObservation(host=f"h{i}.b32.i2p", epoch_id=e2.id,
                                          source_type="vps_floodfill_netdb",
                                          source_detail="kind=leaseset"))
    db_session.add(NetworkObservation(router_hash="old", epoch_id=e1.id,
                                      source_type="local_netdb"))
    db_session.commit()
    return {"e1": e1, "e2": e2, "sites": sites}


@pytest.fixture()
def app(seeded):
    application = dash_mod.create_app(token="secret")
    application.config["TESTING"] = True
    return application


def test_healthz_no_auth(app):
    client = app.test_client()
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.get_json()["db"] == "ok"


def test_auth_rejects_without_token(app):
    client = app.test_client()
    assert client.get("/").status_code == 401
    assert client.get("/api/stats").status_code == 401


def test_auth_accepts_query_token(app):
    client = app.test_client()
    assert client.get("/?token=secret").status_code == 200
    assert client.get("/api/stats?token=secret").status_code == 200


def test_auth_accepts_bearer(app):
    client = app.test_client()
    r = client.get("/api/stats", headers={"Authorization": "Bearer secret"})
    assert r.status_code == 200


def test_auth_rejects_wrong_token(app):
    client = app.test_client()
    assert client.get("/api/stats?token=nope").status_code == 401


def test_no_token_config_allows_access(seeded):
    application = dash_mod.create_app(token=None)
    application.config["TESTING"] = True
    r = application.test_client().get("/api/stats")
    # settings.dashboard_token is empty in the test env -> open access
    assert r.status_code == 200


def test_api_stats_counts(app):
    data = app.test_client().get("/api/stats?token=secret").get_json()

    assert data["epoch"]["label"] == "2026-Q4"
    assert data["epoch"]["age_days"] == 10
    assert data["epoch"]["days_until_rollover"] == 30 - 10

    assert data["liveness"]["status"] == "ALIVE"
    assert data["liveness"]["seconds_ago"] < 120

    assert data["cohort"]["total"] == 5
    assert data["cohort"]["by_state"]["CRAWLED"] == 1
    assert data["cohort"]["by_state"]["NEW"] == 1

    ep = data["this_epoch"]
    assert ep["verify_attempts"] == 2  # s2 ok, s3 failed
    assert ep["verify_success_rate"] == 0.5
    assert ep["crawl_attempts"] == 2  # s1 ok, s4 failed
    assert ep["crawl_success_rate"] == 0.5
    assert ep["pages_fetched"] == 3
    assert ep["links_found"] == 7
    assert ep["new_sites"] == 2  # s4, s5 within epoch window
    errs = dict(ep["error_top5"])
    assert errs == {"DNS_ERROR": 1, "CONNECT_TIMEOUT": 1}

    xl = data["cross_layer"]["this_epoch"]
    assert xl["cross_layer_observations"] == {"epoch-loop:sam": 2}
    assert xl["network_observations"] == {"local_netdb": 3, "vps_floodfill_netdb": 2}
    assert data["cross_layer"]["cumulative"]["cross_layer_observations"] == 3
    assert data["cross_layer"]["cumulative"]["network_observations"] == 6

    churn = data["churn"]
    assert churn["available"] is True
    assert churn["prev_epoch_label"] == "2026-Q3"
    assert churn["newly_reachable"] == 1  # s1 only in epoch 2
    assert churn["lost"] == 1  # s3: ok in epoch 1, failed in epoch 2

    assert data["health"]["stuck_sites"]["count"] == 1
    assert "d.i2p" in data["health"]["stuck_sites"]["hosts"]
    assert len(data["health"]["heartbeats"]) == 2
    assert data["health"]["heartbeats"][0]["cycles"] == 42


def test_html_contains_key_numbers(app):
    html = app.test_client().get("/?token=secret").get_data(as_text=True)
    assert "2026-Q4" in html
    assert "ALIVE" in html
    assert "d.i2p" in html  # stuck host
    # churn row renders only with two epochs
    assert "2026-Q3" in html


def test_churn_dash_with_single_epoch(db_session):
    e = Epoch(label="only", status=EpochStatus.OPEN.value,
              started_at=_now() - timedelta(days=3))
    db_session.add(e)
    db_session.commit()
    application = dash_mod.create_app(token="t")
    data = application.test_client().get("/api/stats?token=t").get_json()
    assert data["churn"]["available"] is False
    html = application.test_client().get("/?token=t").get_data(as_text=True)
    assert "needs a second epoch" in html


def test_no_open_epoch(db_session):
    application = dash_mod.create_app(token="t")
    data = application.test_client().get("/api/stats?token=t").get_json()
    assert data["epoch"]["label"] is None
    html = application.test_client().get("/?token=t").get_data(as_text=True)
    assert "none" in html


def test_daily_churn_day_over_day(db_session):
    """Day-granular churn: newly reachable / lost with the attempted-day guard."""
    s = db_session
    epoch = Epoch(label="2026-Q4", status=EpochStatus.OPEN.value,
                  started_at=_now() - timedelta(days=10))
    s.add(epoch)
    s.flush()
    sites = {}
    for name in ("a", "b", "c"):
        site = Site(host=f"{name}.i2p", base_url=f"http://{name}.i2p/",
                    state=SiteState.REACHABLE.value,
                    first_seen_at=_now() - timedelta(days=10))
        s.add(site)
        sites[name] = site
    s.flush()

    def attempt(name, days_ago, ok):
        site = sites[name]
        started = _now() - timedelta(days=days_ago, hours=1)
        s.add(CrawlAttempt(
            site_id=site.id, epoch_id=epoch.id,
            attempt_type=AttemptType.VERIFY.value,
            status=AttemptStatus.SUCCESS.value if ok else AttemptStatus.FAILED.value,
            started_at=started, finished_at=started + timedelta(minutes=1),
            error_type=None if ok else "I2P_DEST_NOT_FOUND"))

    # Day -3: a ok, b failed. Day -2: a ok, b ok, c failed.
    # Day -1: a failed, b ok (c not attempted -> never "lost").
    # Day 0: a failed, b failed.
    attempt("a", 3, True);  attempt("b", 3, False)
    attempt("a", 2, True);  attempt("b", 2, True); attempt("c", 2, False)
    attempt("a", 1, False); attempt("b", 1, True)
    attempt("a", 0, False); attempt("b", 0, False)
    s.commit()

    application = dash_mod.create_app(token="secret")
    application.config["TESTING"] = True
    data = application.test_client().get("/api/stats?token=secret").get_json()
    dc = {row["day"]: row for row in data["daily_churn"]}

    def day(days_ago):
        return (_now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")

    d3, d2, d1, d0 = dc[day(3)], dc[day(2)], dc[day(1)], dc[day(0)]
    assert (d3["attempted"], d3["reachable"]) == (2, 1)
    assert d3["newly_reachable"] == 1 and d3["lost"] == 0
    assert (d2["attempted"], d2["reachable"]) == (3, 2)
    assert d2["newly_reachable"] == 1 and d2["lost"] == 0  # b newly reachable
    assert (d1["attempted"], d1["reachable"]) == (2, 1)
    assert d1["newly_reachable"] == 0 and d1["lost"] == 1  # a lost (attempted)
    # c was ok nowhere; never attempted after day -2 -> never counted lost
    assert (d0["attempted"], d0["reachable"]) == (2, 0)
    assert d0["newly_reachable"] == 0 and d0["lost"] == 1  # b lost (attempted)


def test_daily_churn_needs_two_days(db_session):
    s = db_session
    epoch = Epoch(label="2026-Q4", status=EpochStatus.OPEN.value,
                  started_at=_now() - timedelta(days=10))
    s.add(epoch)
    s.flush()
    site = Site(host="a.i2p", base_url="http://a.i2p/",
                state=SiteState.REACHABLE.value,
                first_seen_at=_now() - timedelta(days=10))
    s.add(site)
    s.flush()
    started = _now() - timedelta(hours=1)
    s.add(CrawlAttempt(site_id=site.id, epoch_id=epoch.id,
                       attempt_type=AttemptType.VERIFY.value,
                       status=AttemptStatus.SUCCESS.value,
                       started_at=started, finished_at=started))
    s.commit()

    application = dash_mod.create_app(token="secret")
    application.config["TESTING"] = True
    html = application.test_client().get("/?token=secret").data.decode()
    assert "daily churn needs two days of attempts" in html


# ---------------------------------------------------------------------------
# CSV dataset downloads (/api/download/<name>)
# ---------------------------------------------------------------------------

def _dl_csv(app, name, token="secret"):
    return app.test_client().get(f"/api/download/{name}?token={token}")


def test_download_unknown_dataset_404(app):
    r = _dl_csv(app, "nope")
    assert r.status_code == 404


def test_download_requires_auth(app):
    r = app.test_client().get("/api/download/sites")
    assert r.status_code == 401
    r = app.test_client().get("/api/download/sites?token=wrong")
    assert r.status_code == 401


def test_download_sites_csv(app):
    r = _dl_csv(app, "sites")
    assert r.status_code == 200
    assert r.headers["Content-Type"].startswith("text/csv")
    assert "attachment" in r.headers["Content-Disposition"]
    assert "xl-i2p-study2-sites-" in r.headers["Content-Disposition"]
    lines = r.data.decode().strip().splitlines()
    assert lines[0].startswith("id,host,base_url,")
    assert len(lines) == 6  # header + 5 seeded sites
    assert "a.i2p" in r.data.decode()


def test_download_crawl_attempts_csv(app):
    r = _dl_csv(app, "crawl_attempts")
    assert r.status_code == 200
    lines = r.data.decode().strip().splitlines()
    assert lines[0].startswith("id,site_id,epoch_id,attempt_type,")
    assert len(lines) == 7  # header + 6 seeded attempts


def test_download_links_csv(app):
    r = _dl_csv(app, "links")
    assert r.status_code == 200
    lines = r.data.decode().strip().splitlines()
    assert lines[0].startswith("id,source_site_id,")
    assert len(lines) == 8  # header + 7 seeded links


def test_download_pages_csv(app):
    r = _dl_csv(app, "pages")
    assert r.status_code == 200
    lines = r.data.decode().strip().splitlines()
    assert lines[0].startswith("id,site_id,epoch_id,url,")
    assert len(lines) == 4  # header + 3 seeded pages


def test_download_daily_churn_csv(app):
    r = _dl_csv(app, "daily_churn")
    assert r.status_code == 200
    lines = r.data.decode().strip().splitlines()
    assert lines[0] == "day,attempted,reachable,newly_reachable,lost"
    # seeded attempts are all within the last hour: one day row, 4 sites
    # attempted (s1..s4). s3 counts as reachable for the day: its epoch-1
    # verify succeeded the same day (day-granular ok = max over attempts).
    assert len(lines) == 2
    day, attempted, reachable, newly_reachable, lost = lines[1].split(",")
    assert (attempted, reachable, newly_reachable, lost) == ("4", "3", "3", "0")


def test_download_epochs_and_network_obs_csv(app):
    r = _dl_csv(app, "epochs")
    assert r.status_code == 200
    assert len(r.data.decode().strip().splitlines()) == 3  # header + 2 epochs
    r = _dl_csv(app, "network_observations")
    assert r.status_code == 200
    assert len(r.data.decode().strip().splitlines()) == 7  # header + 6 obs


def test_dashboard_html_has_all_dataset_buttons(app):
    html = app.test_client().get("/?token=secret").data.decode()
    for name in dash_mod.DATASETS:
        assert f"/api/download/{name}?token=secret" in html, name
    # the button styles must live in the served page's own <style> block
    assert ".dl-btn{" in html and ".dl-grid{" in html


def test_download_registry_covers_all_tables(app):
    # every table that matters to Study 2 analysis has a button
    assert set(dash_mod.DATASETS) == {
        "sites", "crawl_attempts", "pages", "links",
        "network_observations", "seed_events", "epochs",
        "cross_layer_observations", "daily_churn",
    }


def test_token_param_xss_neutralized():
    """CWE-79: a hostile ?token= must not break out of the JS string literal.

    The old hand-rolled backslash/quote escaping did not stop a
    </script> breakout (HTML is parsed before JS). The token is now
    embedded via json.dumps with <>& neutralized as \\uXXXX escapes.
    """
    import re

    evil = '</script><script>alert(document.domain)</script>"\\\n\'&<b>'
    for render in (dash_mod._render, dash_mod._render_research):
        html = render(None, evil)
        # The attacker's literal </script> must not survive into the page.
        assert "</script><script>" not in html
        # The embedded literal must still round-trip to the original token
        # (so legitimate tokens keep working).
        m = re.search(r"const TOKEN_QS = (.*?);", html)
        assert m, "TOKEN_QS literal missing from rendered page"
        assert json.loads(m.group(1)) == evil
