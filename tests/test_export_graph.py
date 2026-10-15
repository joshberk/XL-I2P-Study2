"""Induced-graph export must be reproducible: nodes come from successful
CRAWL attempts, not the mutable Site.state column (re-verification flips
CRAWLED sites back to REACHABLE mid-epoch)."""
from __future__ import annotations

import csv

from sqlalchemy import select

from xl_i2p.epochs import open_epoch
from xl_i2p.export import export_graph_csv
from xl_i2p.models import CrawlAttempt, Site
from xl_i2p.seeds import upsert_site
from xl_i2p.states import AttemptStatus, AttemptType, SiteState


def _attempt(s, site, epoch, attempt_type, status):
    s.add(CrawlAttempt(
        site_id=site.id, epoch_id=epoch.id,
        attempt_type=attempt_type, status=status,
    ))
    s.commit()


def test_export_graph_nodes_come_from_attempts_not_state(db_session, tmp_path):
    s = db_session
    epoch = open_epoch(s, "e1")
    crawled = upsert_site(s, "crawled.i2p", source="test")
    failed = upsert_site(s, "failed.i2p", source="test")
    s.commit()
    _attempt(s, crawled, epoch, AttemptType.CRAWL.value, AttemptStatus.SUCCESS.value)
    _attempt(s, failed, epoch, AttemptType.CRAWL.value, AttemptStatus.FAILED.value)
    # Simulate mid-epoch re-verification flipping the crawled site back.
    crawled.state = SiteState.REACHABLE.value
    s.commit()

    nodes_p = tmp_path / "nodes.csv"
    edges_p = tmp_path / "edges.csv"
    node_count, _ = export_graph_csv(s, nodes_p, edges_p, epoch_id=epoch.id)

    assert node_count == 1
    rows = list(csv.DictReader(nodes_p.open()))
    assert [r["host"] for r in rows] == ["crawled.i2p"]
    # ...and a second export at a later state gives the same node set.
    crawled.state = SiteState.CRAWLED.value
    s.commit()
    node_count2, _ = export_graph_csv(s, tmp_path / "n2.csv", tmp_path / "e2.csv",
                                     epoch_id=epoch.id)
    assert node_count2 == node_count == 1


def test_export_graph_epoch_filter(db_session, tmp_path):
    s = db_session
    e1 = open_epoch(s, "e1")
    site = upsert_site(s, "s.i2p", source="test")
    s.commit()
    _attempt(s, site, e1, AttemptType.CRAWL.value, AttemptStatus.SUCCESS.value)
    from xl_i2p.epochs import close_epoch
    close_epoch(s, "e1")
    e2 = open_epoch(s, "e2")

    n1, _ = export_graph_csv(s, tmp_path / "n1.csv", tmp_path / "e1.csv", epoch_id=e1.id)
    n2, _ = export_graph_csv(s, tmp_path / "n2.csv", tmp_path / "e2.csv", epoch_id=e2.id)
    assert n1 == 1
    assert n2 == 0
