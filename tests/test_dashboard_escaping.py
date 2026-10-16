"""Stored-XSS regression: crawled hosts must be escaped in server-rendered HTML.

Site hosts come from links on crawled eepsites, and urlparse accepts markup
in the hostname: ``http://<img src=x onerror=alert(1)>.i2p/`` yields the
host ``<img src=x onerror=alert(1)>.i2p``. Those hosts reach the flap-leader
table (research page) and the stuck-site list (main page).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from xl_i2p import dashboard as dash_mod
from xl_i2p.models import CrawlAttempt, Epoch, Site
from xl_i2p.states import AttemptStatus, AttemptType, EpochStatus, SiteState
from xl_i2p.utils import host_from_url

EVIL_URL = "http://<img src=x onerror=alert(1)>.i2p/"
EVIL_HOST = host_from_url(EVIL_URL)


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _seed(db_session):
    assert EVIL_HOST == "<img src=x onerror=alert(1)>.i2p"  # premise of the test
    epoch = Epoch(label="e1", status=EpochStatus.OPEN.value,
                  started_at=_now() - timedelta(days=3))
    flapper = Site(host=EVIL_HOST, base_url=EVIL_URL, state=SiteState.REACHABLE.value)
    stuck = Site(host="<svg onload=alert(2)>.i2p", base_url="http://x.i2p/",
                 state=SiteState.VERIFYING.value,
                 last_checked_at=_now() - timedelta(hours=3))
    db_session.add_all([epoch, flapper, stuck])
    db_session.flush()
    for i, status in enumerate((AttemptStatus.SUCCESS, AttemptStatus.FAILED,
                                AttemptStatus.SUCCESS)):
        db_session.add(CrawlAttempt(
            site_id=flapper.id, epoch_id=epoch.id,
            attempt_type=AttemptType.VERIFY.value, status=status.value,
            started_at=_now() - timedelta(hours=10 - i)))
    db_session.commit()


def _client():
    app = dash_mod.create_app(token="t")
    app.config["TESTING"] = True
    return app.test_client()


def test_flap_leader_host_is_escaped(db_session):
    _seed(db_session)
    html = _client().get("/research?token=t").get_data(as_text=True)
    assert "&lt;img src=x onerror=alert(1)&gt;.i2p" in html
    assert "<img src=x onerror=alert(1)>" not in html


def test_stuck_host_is_escaped(db_session):
    _seed(db_session)
    html = _client().get("/?token=t").get_data(as_text=True)
    assert "&lt;svg onload=alert(2)&gt;.i2p" in html
    assert "<svg onload=alert(2)>" not in html
