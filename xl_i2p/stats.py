from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .models import (
    CrawlAttempt,
    CrossLayerObservation,
    Link,
    NetworkObservation,
    Page,
    Site,
)


def collect_stats(session: Session, epoch_id: int | None = None) -> dict:
    def _count(model, *filters):
        stmt = select(func.count()).select_from(model)
        for f in filters:
            stmt = stmt.where(f)
        return session.scalar(stmt) or 0

    state_counts = dict(
        session.execute(
            select(Site.state, func.count()).group_by(Site.state)
        ).all()
    )
    site_errors = dict(
        session.execute(
            select(Site.last_error_type, func.count())
            .where(Site.last_error_type.is_not(None))
            .group_by(Site.last_error_type)
        ).all()
    )
    source_counts = dict(
        session.execute(
            select(Site.source, func.count()).group_by(Site.source)
        ).all()
    )
    data = {
        "total_pages": _count(Page, *( [Page.epoch_id == epoch_id] if epoch_id else [])),
        "total_links": _count(Link, *( [Link.epoch_id == epoch_id] if epoch_id else [])),
        "total_attempts": _count(CrawlAttempt, *( [CrawlAttempt.epoch_id == epoch_id] if epoch_id else [])),
        "network_observations": _count(
            NetworkObservation, *( [NetworkObservation.epoch_id == epoch_id] if epoch_id else [])),
        "cross_layer_observations": _count(
            CrossLayerObservation, *( [CrossLayerObservation.epoch_id == epoch_id] if epoch_id else [])),
        "cross_layer_sites": _count(Site, Site.is_cross_layer.is_(True)),
        "cross_layer_crawled": _count(
            Site, Site.is_cross_layer.is_(True), Site.state == "CRAWLED"),
        "state_counts": state_counts,
        "site_errors": site_errors,
        "source_counts": source_counts,
    }
    return data
