"""Scheduler: one verify+crawl cycle at a time, fresh session per cycle.

Study 1 held a single long-lived session across the whole run. Study 2
creates a short-lived session per cycle (and per site inside the batches),
so a stale/failed connection can never poison the rest of the campaign.
The scheduler loop itself is wrapped so an unexpected cycle exception is
logged and the loop continues; systemd Restart=always covers process death.
"""
from __future__ import annotations

import asyncio
import logging
import time

from .config import settings
from .crawler import crawl_batch
from .epochs import get_open_epoch, rollover_if_due
from .logging_setup import write_heartbeat
from .models import Epoch
from .proxy import tcp_proxy_available
from .states import EpochStatus
from .verifier import verify_batch

logger = logging.getLogger(__name__)


def resolve_running_epoch(session, epoch_id: int) -> Epoch | None:
    """Return the epoch the loop should currently write to.

    The rollover timer may close the loop's epoch and open a new one while
    the loop is sleeping. In that case the loop switches to the new open
    epoch instead of writing to the closed one forever. Returns None when
    no epoch is open (the loop waits rather than dying).
    """
    epoch = session.get(Epoch, epoch_id)
    if epoch is None:
        return None
    if epoch.status == EpochStatus.OPEN.value:
        return epoch
    logger.info(
        "epoch '%s' (id=%d) was closed by rollover; switching to the new open epoch",
        epoch.label,
        epoch.id,
    )
    return get_open_epoch(session)


async def run_once(
    epoch: Epoch,
    verify_limit: int | None = None,
    crawl_limit: int | None = None,
) -> dict[str, dict[str, int]]:
    from .db import SessionLocal

    verify_limit = settings.verify_limit if verify_limit is None else verify_limit
    crawl_limit = settings.crawl_limit if crawl_limit is None else crawl_limit

    with SessionLocal() as session:
        verify_result = await verify_batch(session, verify_limit, epoch)
    with SessionLocal() as session:
        crawl_result = await crawl_batch(session, crawl_limit, epoch)
    return {"verify": verify_result, "crawl": crawl_result}


def _run_xlayer_pass_sync(epoch_id: int, limit: int) -> dict:
    """Thread entry point: fresh session inside the worker thread.

    Sessions are not thread-safe, so the pass must create its own session
    rather than reuse the loop's.
    """
    from .db import SessionLocal
    from .models import Epoch
    from .xlayer_pass import run_xlayer_pass

    with SessionLocal() as session:
        epoch = session.get(Epoch, epoch_id)
        if epoch is None:
            return {"considered": 0, "lookups": 0, "validated": 0, "errors": 0}
        return run_xlayer_pass(session, epoch, limit)


def _run_census_sync(epoch_id: int) -> dict:
    from .db import SessionLocal
    from .cross_layer import census_local_netdb

    with SessionLocal() as session:
        return census_local_netdb(session, epoch_id)


async def run_loop(
    epoch_label: str,
    epoch_id: int,
    verify_limit: int | None = None,
    crawl_limit: int | None = None,
    sleep_seconds: int | None = None,
) -> None:
    from .db import SessionLocal
    from .models import Epoch

    sleep_seconds = settings.sleep_seconds if sleep_seconds is None else sleep_seconds
    counters: dict[str, int] = {
        "cycles": 0, "verified": 0, "reachable": 0,
        "sites_crawled": 0, "pages_fetched": 0, "links_found": 0, "errors": 0,
        "xlayer_lookups": 0, "xlayer_validated": 0, "netdb_census_recorded": 0,
    }
    last_census_mono: float | None = None
    logger.info("scheduler loop started for epoch '%s'", epoch_label)

    while True:
        counters["cycles"] += 1
        write_heartbeat(epoch_label, "cycle", counters)
        try:
            with SessionLocal() as session:
                # Backstop: roll the epoch here too, in case the systemd
                # rollover timer is down. Idempotent; the timer normally wins.
                rollover_if_due(session)
                epoch = resolve_running_epoch(session, epoch_id)
                if epoch is not None:
                    epoch_label, epoch_id = epoch.label, epoch.id
            if epoch is None:
                logger.warning("no open epoch; waiting for one to be opened")
                write_heartbeat(epoch_label, "no-epoch", counters)
                await asyncio.sleep(sleep_seconds)
                continue
            if not tcp_proxy_available():
                # I2P router restarting: skip the cycle instead of burning
                # verify/crawl attempts against a dead proxy.
                logger.warning("I2P proxy down; skipping cycle %d", counters["cycles"])
                write_heartbeat(epoch_label, "proxy-down", counters)
                await asyncio.sleep(sleep_seconds)
                continue
            result = await run_once(epoch, verify_limit=verify_limit, crawl_limit=crawl_limit)
            verify_result, crawl_result = result["verify"], result["crawl"]
            counters["verified"] += verify_result.get("verified", 0)
            counters["reachable"] += verify_result.get("reachable", 0)
            counters["sites_crawled"] += crawl_result.get("sites", 0)
            counters["pages_fetched"] += crawl_result.get("pages_fetched", 0)
            counters["links_found"] += crawl_result.get("links_found", 0)
            counters["errors"] += verify_result.get("failed", 0) + crawl_result.get("errors", 0)
            logger.info("cycle %d done: verify=%s crawl=%s",
                        counters["cycles"], verify_result, crawl_result)
            # Tier 1 cross-layer: bounded per-epoch association pass. Runs in
            # a worker thread (blocking SAM/console I/O) with its own session.
            # SAM-down degrades to zeros inside the pass; anything else is
            # caught here so the loop always continues.
            try:
                if settings.xlink_enabled:
                    xlayer_result = await asyncio.to_thread(
                        _run_xlayer_pass_sync, epoch_id,
                        settings.xlink_per_cycle_limit)
                    counters["xlayer_lookups"] += xlayer_result.get("lookups", 0)
                    counters["xlayer_validated"] += xlayer_result.get("validated", 0)
                    logger.info("cycle %d xlayer: %s", counters["cycles"], xlayer_result)
            except Exception:
                logger.exception("cross-layer pass failed; continuing")
                counters["errors"] += 1
            # Tier 1 local netDb census, at most once per interval.
            try:
                if settings.netdb_census_enabled:
                    now_mono = time.monotonic()
                    if (last_census_mono is None
                            or now_mono - last_census_mono >= settings.netdb_census_interval_seconds):
                        census_result = await asyncio.to_thread(_run_census_sync, epoch_id)
                        last_census_mono = time.monotonic()
                        counters["netdb_census_recorded"] += census_result.get("recorded", 0)
                        logger.info("cycle %d netdb census: %s", counters["cycles"], census_result)
            except Exception:
                logger.exception("local netDb census failed; continuing")
                counters["errors"] += 1
        except asyncio.CancelledError:
            logger.info("scheduler loop cancelled")
            raise
        except Exception:
            # The loop must survive anything a cycle throws.
            logger.exception("cycle %d failed; continuing", counters["cycles"])
            counters["errors"] += 1

        write_heartbeat(epoch_label, "sleep", counters)
        await asyncio.sleep(sleep_seconds)
