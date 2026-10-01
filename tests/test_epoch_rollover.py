"""Automatic epoch rollover, running-epoch resolution, proxy wait, systemd units.

Covers the three pre-deployment review findings: (1) epoch rollover must be
automatic — a four-month run cannot depend on someone remembering to close
and open epochs; (2) the systemd unit must not reference VM2's mariadb.service
and must keep StartLimit* in [Unit]; (3) the crawler must wait for the I2P
proxy instead of exiting when it is down at startup.
"""
from __future__ import annotations

import dataclasses
from datetime import timedelta
from pathlib import Path

import pytest

import xl_i2p.epochs as epochs_mod
from xl_i2p.config import Settings
from xl_i2p.epochs import (
    _next_label,
    close_epoch,
    get_open_epoch,
    open_epoch,
    rollover_if_due,
)
from xl_i2p.models import Epoch, now
from xl_i2p.scheduler import resolve_running_epoch
from xl_i2p.states import EpochStatus


@pytest.fixture()
def auto_settings(monkeypatch):
    """Settings with a 30-day epoch and auto-rollover enabled."""
    s = dataclasses.replace(
        Settings(), epoch_duration_days=30, epoch_auto_rollover=True
    )
    monkeypatch.setattr(epochs_mod, "settings", s)
    return s


def _backdate(session, epoch, days: int):
    epoch.started_at = now() - timedelta(days=days)
    session.commit()
    session.expire_all()


def test_rollover_not_due_is_noop(db_session, auto_settings):
    s = db_session
    epoch = open_epoch(s, "e1")
    _backdate(s, epoch, 5)
    result = rollover_if_due(s)
    s.expire_all()
    assert result.id == epoch.id
    assert result.status == EpochStatus.OPEN.value
    assert get_open_epoch(s).label == "e1"


def test_rollover_due_closes_and_opens(db_session, auto_settings):
    s = db_session
    old = open_epoch(s, "e1")
    _backdate(s, old, 40)
    result = rollover_if_due(s)
    s.expire_all()
    assert result.label == "e1-02"
    assert result.status == EpochStatus.OPEN.value
    assert result.id != old.id
    closed = s.get(Epoch, old.id)
    assert closed.status == EpochStatus.CLOSED.value
    assert closed.ended_at is not None
    assert get_open_epoch(s).id == result.id


def test_rollover_disabled_is_noop(db_session, monkeypatch):
    s = db_session
    off = dataclasses.replace(
        Settings(), epoch_duration_days=30, epoch_auto_rollover=False
    )
    monkeypatch.setattr(epochs_mod, "settings", off)
    epoch = open_epoch(s, "e1")
    _backdate(s, epoch, 90)
    result = rollover_if_due(s)
    s.expire_all()
    assert result.id == epoch.id
    assert result.status == EpochStatus.OPEN.value


def test_next_label_increments():
    assert _next_label("e1") == "e1-02"
    assert _next_label("2026-Q4") == "2026-Q4-02"
    assert _next_label("study2-03") == "study2-04"
    assert _next_label("study2-09") == "study2-10"


def test_resolve_running_epoch_switches_after_rollover(db_session):
    s = db_session
    e1 = open_epoch(s, "e1")
    close_epoch(s, "e1")
    e2 = open_epoch(s, "e1-02")
    resolved = resolve_running_epoch(s, e1.id)
    assert resolved is not None
    assert resolved.id == e2.id


def test_resolve_running_epoch_none_when_no_open_epoch(db_session):
    s = db_session
    e1 = open_epoch(s, "e1")
    close_epoch(s, "e1")
    assert resolve_running_epoch(s, e1.id) is None


def test_resolve_running_epoch_none_when_vanished(db_session):
    assert resolve_running_epoch(db_session, 999999) is None


def test_wait_for_proxy_retries_then_succeeds(monkeypatch):
    import time as time_mod

    import xl_i2p.proxy as proxy_mod

    calls = {"n": 0}

    def fake_available():
        calls["n"] += 1
        return calls["n"] >= 3

    monkeypatch.setattr(proxy_mod, "tcp_proxy_available", fake_available)
    monkeypatch.setattr(time_mod, "sleep", lambda seconds: None)
    proxy_mod.wait_for_proxy()
    assert calls["n"] == 3


def _systemd(name: str) -> str:
    root = Path(__file__).resolve().parent.parent / "systemd" / name
    return root.read_text()


def test_crawler_unit_has_no_mariadb_dependency():
    unit = _systemd("xl-i2p-crawler.service")
    directives = [
        line.split("=", 1)[1]
        for line in unit.splitlines()
        if line.strip().startswith(("After=", "Wants=", "Requires="))
    ]
    assert not any("mariadb" in d for d in directives), directives
    assert "network-online.target" in unit
    # StartLimit* must live in [Unit]; systemd ignores them in [Service].
    # (Split on the newline-delimited header: a comment in the file mentions
    # the literal string "[Service]".)
    unit_section = unit.split("\n[Service]\n")[0]
    assert "StartLimitIntervalSec" in unit_section
    assert "Restart=always" in unit


def test_rollover_timer_units_exist_and_are_sane():
    service = _systemd("xl-i2p-epoch-rollover.service")
    assert "epoch rollover --export-closed" in service
    assert "Type=oneshot" in service
    timer = _systemd("xl-i2p-epoch-rollover.timer")
    assert "OnCalendar=daily" in timer
    assert "Persistent=true" in timer
    assert "xl-i2p-epoch-rollover.service" in timer
