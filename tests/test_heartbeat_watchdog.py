"""Heartbeat watchdog: liveness must not depend on cycle completion.

Regression: heartbeats were written only at cycle boundaries, so a cycle
wedged mid-fetch (e.g. a multi-hundred-MB download) starved the dashboard
and read as DEAD while the process was alive.
"""
from __future__ import annotations

import asyncio
import dataclasses

import xl_i2p.scheduler as sched_mod
from xl_i2p.config import Settings
from xl_i2p.scheduler import _heartbeat_watchdog


def test_watchdog_fires_on_its_own_timer(monkeypatch):
    calls = []
    monkeypatch.setattr(
        sched_mod, "write_heartbeat",
        lambda label, phase, counters, log_dir=None: calls.append((label, phase)),
    )
    fast = dataclasses.replace(Settings(), heartbeat_seconds=0.05)
    monkeypatch.setattr(sched_mod, "settings", fast)

    async def go():
        stop = asyncio.Event()
        task = asyncio.create_task(
            _heartbeat_watchdog({"epoch_label": "e1"}, {"cycles": 3}, stop)
        )
        await asyncio.sleep(0.22)
        stop.set()
        await task
        return calls

    calls = asyncio.run(go())
    assert len(calls) >= 2, f"watchdog did not keep firing: {calls}"
    assert all(phase == "watchdog" for _, phase in calls)
    assert all(label == "e1" for label, _ in calls)


def test_watchdog_survives_write_failures(monkeypatch):
    monkeypatch.setattr(
        sched_mod, "write_heartbeat",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("disk full")),
    )
    fast = dataclasses.replace(Settings(), heartbeat_seconds=0.05)
    monkeypatch.setattr(sched_mod, "settings", fast)

    async def go():
        stop = asyncio.Event()
        task = asyncio.create_task(_heartbeat_watchdog({}, {}, stop))
        await asyncio.sleep(0.15)
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    asyncio.run(go())  # must not raise
