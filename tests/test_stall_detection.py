"""Progress-aware liveness: ALIVE vs STALLED vs STALE/DEAD.

Regressions covered:
- The watchdog overwrote ``phase`` with "watchdog", hiding where a wedged
  cycle was stuck, and a fresh watchdog beat read ALIVE through an entire
  wedge (no stall state).
- ``seconds_since_last_fetch`` alone cannot drive a stall threshold: it
  grows during every normal sleep and the cross-layer pass.
- Two threads writing heartbeat.json through one fixed temp path raced
  (FileNotFoundError on replace) from a call site outside any try.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

import xl_i2p.progress as progress_mod
import xl_i2p.scheduler as sched_mod
from xl_i2p import dashboard as dash_mod
from xl_i2p.config import Settings
from xl_i2p.epochs import open_epoch
from xl_i2p.logging_setup import write_heartbeat
from xl_i2p.models import Heartbeat


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Threshold derivation
# ---------------------------------------------------------------------------

def test_stall_threshold_is_longest_wait_plus_margin(monkeypatch):
    s = dataclasses.replace(Settings(), sleep_seconds=300,
                            fetch_total_timeout_seconds=180,
                            crawl_delay_seconds=2, stall_margin_seconds=300)
    monkeypatch.setattr(progress_mod, "settings", s)
    assert progress_mod.stall_after_seconds() == 600.0          # sleep dominates
    assert progress_mod.stall_after_seconds(sleep_seconds=60) == 482.0  # fetch dominates


def test_fetch_completion_marks_progress(monkeypatch):
    import xl_i2p.httpfetch as httpfetch_mod

    monkeypatch.setattr(progress_mod, "_last_progress_mono", time.monotonic() - 1000)
    httpfetch_mod._mark_fetch_completed()
    assert progress_mod.seconds_since_progress() < 5


def test_xlayer_pass_marks_progress_per_lookup(db_session, monkeypatch):
    import xl_i2p.cross_layer as xl_mod
    from xl_i2p.cross_layer import CrossLayerResult
    from xl_i2p.seeds import upsert_site
    from xl_i2p.states import SiteState
    from xl_i2p.xlayer_pass import run_xlayer_pass

    s = db_session
    epoch = open_epoch(s, "e1")
    upsert_site(s, "one.i2p", source="t", state_if_new=SiteState.REACHABLE.value)
    s.commit()
    monkeypatch.setattr(xl_mod, "sam_alive", lambda: True)
    monkeypatch.setattr(xl_mod, "load_floodfill_hashes", lambda limit=None: [])
    monkeypatch.setattr(xl_mod, "lookup_candidate", lambda host, floodfill_hashes=None:
                        CrossLayerResult(host, host, None, "console_cache", False))
    monkeypatch.setattr(progress_mod, "_last_progress_mono", time.monotonic() - 1000)
    run_xlayer_pass(s, epoch, limit=5)
    assert progress_mod.seconds_since_progress() < 5


# ---------------------------------------------------------------------------
# Dashboard status
# ---------------------------------------------------------------------------

def _beat(db_session, seconds_ago, counters, phase="crawl"):
    db_session.add(Heartbeat(id=1, updated_at=_now() - timedelta(seconds=seconds_ago),
                             phase=phase, epoch_label="e1",
                             counters_json=json.dumps(counters)))
    db_session.commit()
    return dash_mod._collect(db_session)["liveness"]


def test_fresh_beat_with_recent_progress_is_alive(db_session):
    live = _beat(db_session, 10, {"seconds_since_progress": 40.0,
                                  "stall_after_seconds": 600.0})
    assert live["status"] == "ALIVE"
    assert live["phase"] == "crawl"
    assert 49 <= live["seconds_since_progress"] <= 60  # beat age is added


def test_fresh_beat_without_progress_is_stalled(db_session):
    live = _beat(db_session, 10, {"seconds_since_progress": 900.0,
                                  "stall_after_seconds": 600.0})
    assert live["status"] == "STALLED"
    assert live["stall_after_seconds"] == 600.0


def test_beat_age_counts_toward_stall(db_session):
    # 590s at beat time + 30s since the beat > 600s threshold.
    live = _beat(db_session, 30, {"seconds_since_progress": 590.0,
                                  "stall_after_seconds": 600.0})
    assert live["status"] == "STALLED"


def test_stale_and_dead_take_precedence_over_stalled(db_session):
    live = _beat(db_session, 600, {"seconds_since_progress": 5000.0,
                                   "stall_after_seconds": 600.0})
    assert live["status"] == "STALE"


def test_legacy_beat_without_progress_fields_keeps_old_behavior(db_session):
    live = _beat(db_session, 10, {"cycles": 3})
    assert live["status"] == "ALIVE"
    assert live["seconds_since_progress"] is None


def test_stalled_pill_and_phase_render(db_session):
    _beat(db_session, 10, {"seconds_since_progress": 900.0,
                           "stall_after_seconds": 600.0}, phase="<b>crawl</b>")
    app = dash_mod.create_app(token="t")
    app.config["TESTING"] = True
    html = app.test_client().get("/?token=t").get_data(as_text=True)
    assert "STALLED" in html
    assert "&lt;b&gt;crawl&lt;/b&gt;" in html  # phase is escaped
    assert "<b>crawl</b>" not in html


# ---------------------------------------------------------------------------
# Scheduler: wedged cycle keeps reporting its real phase + growing stall age
# ---------------------------------------------------------------------------

def test_wedged_cycle_beats_report_real_phase_and_progress_age(db_session, monkeypatch):
    open_epoch(db_session, "e1")
    beats = []
    monkeypatch.setattr(
        sched_mod, "write_heartbeat",
        lambda label, phase, counters, log_dir=None: beats.append(
            (time.monotonic(), phase, dict(counters))))
    monkeypatch.setattr(sched_mod, "tcp_proxy_available", lambda: True)
    monkeypatch.setattr(sched_mod, "settings", dataclasses.replace(
        Settings(), heartbeat_seconds=0.05, xlink_enabled=False,
        netdb_census_enabled=False))

    async def wedged_run_once(epoch, verify_limit=None, crawl_limit=None, on_phase=None):
        await on_phase("verify")
        await on_phase("crawl")
        await asyncio.sleep(3600)  # one site never finishes

    monkeypatch.setattr(sched_mod, "run_once", wedged_run_once)

    async def go():
        task = asyncio.create_task(sched_mod.run_loop("e1", 1, sleep_seconds=1))
        await asyncio.sleep(0.6)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(go())

    phases = [p for _, p, _ in beats]
    assert "watchdog" not in phases, "watchdog must report the loop's phase"
    late = [c for _, p, c in beats[-3:]]
    assert all(p == "crawl" for _, p, _ in beats[-3:]), phases
    ages = [c["seconds_since_progress"] for c in late]
    assert ages == sorted(ages) and ages[-1] >= 0.3, ages
    assert all(isinstance(c["stall_after_seconds"], float) for c in late)


def test_heartbeat_write_failure_does_not_kill_loop(db_session, monkeypatch):
    """A raising heartbeat used to propagate from outside the cycle's try."""
    open_epoch(db_session, "e1")
    calls = {"n": 0, "cycles_seen": set()}

    def boom(label, phase, counters, log_dir=None):
        calls["n"] += 1
        calls["cycles_seen"].add(counters.get("cycles"))
        raise FileNotFoundError("heartbeat.json.tmp")

    monkeypatch.setattr(sched_mod, "write_heartbeat", boom)
    monkeypatch.setattr(sched_mod, "tcp_proxy_available", lambda: True)
    monkeypatch.setattr(sched_mod, "settings", dataclasses.replace(
        Settings(), heartbeat_seconds=0.05, xlink_enabled=False,
        netdb_census_enabled=False))

    async def quick_run_once(epoch, verify_limit=None, crawl_limit=None, on_phase=None):
        return {"verify": {}, "crawl": {}}

    monkeypatch.setattr(sched_mod, "run_once", quick_run_once)

    async def go():
        task = asyncio.create_task(sched_mod.run_loop("e1", 1, sleep_seconds=0.05))
        await asyncio.sleep(0.5)
        assert not task.done(), "loop died on a heartbeat write error"
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(go())
    assert len(calls["cycles_seen"]) >= 2  # kept cycling


# ---------------------------------------------------------------------------
# Heartbeat file writer
# ---------------------------------------------------------------------------

def test_concurrent_heartbeat_writes_never_raise(db_session, tmp_path):
    errors = []

    def writer(phase):
        for i in range(150):
            try:
                write_heartbeat("e1", phase, {"i": i}, log_dir=str(tmp_path))
            except Exception as exc:  # pragma: no cover - the regression
                errors.append(exc)

    threads = [threading.Thread(target=writer, args=(p,)) for p in ("crawl", "watchdog")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    data = json.loads((tmp_path / "heartbeat.json").read_text())
    assert data["phase"] in {"crawl", "watchdog"}
    assert list(tmp_path.glob("*.tmp")) == [], "temp files leaked"


def test_heartbeat_unwritable_dir_never_raises(db_session, tmp_path):
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("x")
    beat = write_heartbeat("e1", "cycle", {}, log_dir=str(blocker / "logs"))
    assert beat["phase"] == "cycle"
