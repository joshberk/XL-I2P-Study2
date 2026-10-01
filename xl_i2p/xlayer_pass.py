"""Per-epoch cross-layer association pass — Tier 1 (client-mode).

Study 1 ran cross-layer lookups by hand (2 observations, both manual). Study 2
runs a bounded association pass every scheduler cycle: for REACHABLE/CRAWLED
sites that have no cross_layer_observations row for the current epoch, run the
client-mode association (SAM naming + LeaseSet lookup via the local router)
and persist the result tagged with the epoch.

This is the eepsite -> router-identity association the dissertation needs,
and it never required floodfill: a client router can query the netDB for any
destination's LeaseSet. Degrades gracefully: if the SAM bridge is down the
pass returns zeros instead of raising; one bad lookup never kills the pass.
"""
from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from . import cross_layer as xl
from .config import settings
from .models import CrossLayerObservation, Epoch, Site
from .states import SiteState

logger = logging.getLogger(__name__)

# Only sites we have actually reached get the association treatment; NEW /
# DISCOVERED / UNREACHABLE rows are handled by the verify/crawl pipeline and
# the one-shot `cross-layer lookup` CLI.
ASSOCIATION_STATES = (SiteState.REACHABLE.value, SiteState.CRAWLED.value)


def sites_needing_association(session: Session, epoch_id: int, limit: int) -> list[Site]:
    already = (
        select(CrossLayerObservation.id)
        .where(
            CrossLayerObservation.site_id == Site.id,
            CrossLayerObservation.epoch_id == epoch_id,
        )
        .exists()
    )
    return list(
        session.scalars(
            select(Site)
            .where(Site.state.in_(ASSOCIATION_STATES), ~already)
            .order_by(Site.id)
            .limit(limit)
        )
    )


def run_xlayer_pass(session: Session, epoch: Epoch, limit: int | None = None) -> dict[str, int]:
    """Run one bounded association pass for the epoch. Never raises for SAM issues."""
    limit = settings.xlink_per_cycle_limit if limit is None else limit
    counts = {"considered": 0, "lookups": 0, "validated": 0, "errors": 0, "sam_down": False}
    if limit <= 0:
        return counts
    if not xl.sam_alive():
        logger.warning("cross-layer pass skipped: SAM bridge is down")
        counts["sam_down"] = True
        return counts
    # One floodfill census for the whole pass: lookup_candidate shells out to
    # `strings` per routerInfo file, which would be ~500 subprocess spawns per
    # site if recomputed inside every lookup.
    floodfill_hashes = xl.load_floodfill_hashes(limit=500)
    sites = sites_needing_association(session, epoch.id, limit)
    for site in sites:
        counts["considered"] += 1
        try:
            result = xl.lookup_candidate(site.host, floodfill_hashes=floodfill_hashes)
            counts["lookups"] += 1
            validated = xl.persist_cross_layer_result(
                session,
                result,
                source_detail=f"epoch-loop:{result.lookup_method}",
                epoch_id=epoch.id,
            )
            if validated:
                counts["validated"] += 1
        except Exception:
            logger.exception("cross-layer lookup failed for %s", site.host)
            counts["errors"] += 1
    session.commit()
    logger.info("cross-layer pass for epoch '%s': %s", epoch.label, counts)
    return counts
