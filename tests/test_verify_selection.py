"""Split-budget verify selection: cohort reservation vs discovery sweep.

The longitudinal cohort (REACHABLE/CRAWLED/RETRY_READY) must be re-verified
regularly even while the never-checked discovery backlog is large; otherwise
the churn/survival signal on known sites goes quiet (the 791-frozen-CRAWLED
incident of 2026-10-14).
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

import xl_i2p.verifier as verifier_mod
from xl_i2p.config import settings
from xl_i2p.models import CrawlAttempt, Epoch, Site, now
from xl_i2p.states import AttemptType, EpochStatus, SiteState
from xl_i2p.verifier import COHORT_STATES, select_sites_for_verification


def _share(monkeypatch, value: float):
    """Point the verifier at a settings copy with the given cohort share.

    Settings is a frozen dataclass, so rebind the module reference instead
    of mutating the instance.
    """
    monkeypatch.setattr(
        verifier_mod, "settings", replace(settings, verify_cohort_share=value)
    )


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


@pytest.fixture()
def epoch(db_session):
    e = Epoch(
        label="2026-Q4",
        status=EpochStatus.OPEN.value,
        started_at=_now() - timedelta(days=10),
    )
    db_session.add(e)
    db_session.commit()
    return e


def _site(db_session, host, state, checked_days_ago=None, next_retry_at=None,
         successes=0):
    s = Site(
        host=host,
        base_url=f"http://{host}/",
        state=state,
        last_checked_at=(
            _now() - timedelta(days=checked_days_ago)
            if checked_days_ago is not None
            else None
        ),
        next_retry_at=next_retry_at,
        success_count=successes,
    )
    db_session.add(s)
    db_session.flush()
    return s


def _claimed(site):
    return site.state == SiteState.VERIFYING.value


def test_cohort_gets_reserved_share_despite_backlog(db_session, epoch, monkeypatch):
    """200 never-checked NEW sites must not starve the 20 known cohort sites."""
    _share(monkeypatch, 0.5)
    cohort = [
        _site(db_session, f"crawled-{i}.i2p", SiteState.CRAWLED.value, checked_days_ago=30, successes=1)
        for i in range(10)
    ] + [
        _site(db_session, f"reach-{i}.i2p", SiteState.REACHABLE.value, checked_days_ago=20, successes=1)
        for i in range(10)
    ]
    backlog = [
        _site(db_session, f"new-{i}.i2p", SiteState.NEW.value)
        for i in range(200)
    ]

    jobs = select_sites_for_verification(db_session, 20, epoch)

    assert len(jobs) == 20
    # Cohort pool: 10 slots, oldest first -> all 10 CRAWLED (30d) win over REACHABLE (20d).
    assert sum(_claimed(s) for s in cohort[:10]) == 10
    assert sum(_claimed(s) for s in cohort[10:]) == 0
    assert sum(_claimed(s) for s in backlog) == 10
    # Every claim records a VERIFY attempt.
    attempts = db_session.query(CrawlAttempt).all()
    assert len(attempts) == 20
    assert {a.attempt_type for a in attempts} == {AttemptType.VERIFY.value}
    assert all(a.epoch_id == epoch.id for a in attempts)


def test_unused_cohort_budget_flows_to_discovery(db_session, epoch, monkeypatch):
    _share(monkeypatch, 0.5)
    cohort = [
        _site(db_session, f"crawled-{i}.i2p", SiteState.CRAWLED.value, checked_days_ago=30, successes=1)
        for i in range(2)
    ]
    backlog = [_site(db_session, f"new-{i}.i2p", SiteState.NEW.value) for i in range(100)]

    jobs = select_sites_for_verification(db_session, 20, epoch)

    assert len(jobs) == 20
    assert sum(_claimed(s) for s in cohort) == 2
    assert sum(_claimed(s) for s in backlog) == 18


def test_cohort_capped_at_share_when_discovery_empty(db_session, epoch, monkeypatch):
    """Cohort never takes more than its share in one batch, even unopposed."""
    _share(monkeypatch, 0.5)
    cohort = [
        _site(db_session, f"crawled-{i}.i2p", SiteState.CRAWLED.value, checked_days_ago=30, successes=1)
        for i in range(100)
    ]

    jobs = select_sites_for_verification(db_session, 20, epoch)

    assert len(jobs) == 10
    assert sum(_claimed(s) for s in cohort) == 10


def test_share_zero_is_discovery_only(db_session, epoch, monkeypatch):
    _share(monkeypatch, 0.0)
    cohort = [
        _site(db_session, f"crawled-{i}.i2p", SiteState.CRAWLED.value, checked_days_ago=30, successes=1)
        for i in range(10)
    ]
    backlog = [_site(db_session, f"new-{i}.i2p", SiteState.NEW.value) for i in range(20)]

    jobs = select_sites_for_verification(db_session, 20, epoch)

    assert len(jobs) == 20
    assert sum(_claimed(s) for s in cohort) == 0
    assert sum(_claimed(s) for s in backlog) == 20


def test_share_one_prefers_cohort_with_slack_to_discovery(
    db_session, epoch, monkeypatch
):
    _share(monkeypatch, 1.0)
    cohort = [
        _site(db_session, f"crawled-{i}.i2p", SiteState.CRAWLED.value, checked_days_ago=30, successes=1)
        for i in range(10)
    ]
    backlog = [_site(db_session, f"new-{i}.i2p", SiteState.NEW.value) for i in range(20)]

    jobs = select_sites_for_verification(db_session, 20, epoch)

    assert len(jobs) == 20
    assert sum(_claimed(s) for s in cohort) == 10
    assert sum(_claimed(s) for s in backlog) == 10


def test_cohort_order_oldest_first(db_session, epoch, monkeypatch):
    _share(monkeypatch, 1.0)
    oldest = _site(db_session, "old.i2p", SiteState.CRAWLED.value, checked_days_ago=30, successes=1)
    mid = _site(db_session, "mid.i2p", SiteState.RETRY_READY.value, checked_days_ago=20, successes=1)
    newest = _site(db_session, "new.i2p", SiteState.REACHABLE.value, checked_days_ago=10, successes=1)

    select_sites_for_verification(db_session, 2, epoch)

    assert _claimed(oldest) and _claimed(mid) and not _claimed(newest)


def test_backoff_still_respected_in_cohort(db_session, epoch, monkeypatch):
    _share(monkeypatch, 1.0)
    backed_off = _site(
        db_session,
        "backoff.i2p",
        SiteState.CRAWLED.value,
        checked_days_ago=30,
        next_retry_at=_now() + timedelta(hours=1),
        successes=1,
    )
    due = _site(db_session, "due.i2p", SiteState.CRAWLED.value, checked_days_ago=30, successes=1)

    select_sites_for_verification(db_session, 10, epoch)

    assert not _claimed(backed_off)
    assert _claimed(due)


def test_terminal_checked_this_epoch_still_excluded(db_session, epoch, monkeypatch):
    """UNREACHABLE sites already checked this epoch are skipped (discovery pool)."""
    _share(monkeypatch, 0.0)
    dead_this_epoch = _site(
        db_session, "dead-new.i2p", SiteState.UNREACHABLE.value, checked_days_ago=1
    )
    dead_last_epoch = _site(
        db_session, "dead-old.i2p", SiteState.UNREACHABLE.value, checked_days_ago=20
    )

    select_sites_for_verification(db_session, 10, epoch)

    assert not _claimed(dead_this_epoch)
    assert _claimed(dead_last_epoch)


def test_cohort_states_constant_matches_intent():
    assert set(COHORT_STATES) == {
        SiteState.REACHABLE.value,
        SiteState.CRAWLED.value,
        SiteState.RETRY_READY.value,
    }


def test_retry_ready_never_reached_is_discovery_not_cohort(
    db_session, epoch, monkeypatch
):
    """A RETRY_READY row for a host that never answered once (success_count=0)
    is discovery backlog and must not consume a cohort slot."""
    _share(monkeypatch, 0.5)
    known = _site(db_session, "known.i2p", SiteState.CRAWLED.value,
                  checked_days_ago=20, successes=1)
    never_reached = _site(db_session, "flaky.i2p", SiteState.RETRY_READY.value,
                          checked_days_ago=30)  # successes=0
    fresh = _site(db_session, "fresh.i2p", SiteState.NEW.value)

    jobs = select_sites_for_verification(db_session, 2, epoch)

    # Cohort's single slot goes to the known site; the never-reached row
    # competes in the discovery pool (never-checked first -> fresh.i2p).
    assert len(jobs) == 2
    assert _claimed(known)
    assert _claimed(fresh)
    assert not _claimed(never_reached)
