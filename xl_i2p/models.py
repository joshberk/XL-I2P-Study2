"""SQLAlchemy 2.x models for XL-I2P Study 2.

Longitudinal design: every observation table carries ``epoch_id`` so each
epoch re-crawls the same reachable cohort and churn (content/link/availability)
is measurable per epoch. Pages are keyed per (normalized_url, epoch_id) so a
re-crawl in a new epoch creates a new page row instead of colliding with the
old one.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def now() -> datetime:
    """Return a UTC timestamp stored as naive UTC for MariaDB compatibility."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Epoch(Base):
    __tablename__ = "epochs"
    __table_args__ = (UniqueConstraint("label", name="uq_epochs_label"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    label: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), default="OPEN", index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    config_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class Site(Base):
    __tablename__ = "sites"
    __table_args__ = (
        UniqueConstraint("host", name="uq_sites_host"),
        Index("ix_sites_state_last_checked", "state", "last_checked_at"),
        Index("ix_sites_retry_due", "state", "next_retry_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    host: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    base_url: Mapped[str] = mapped_column(String(512), nullable=False)
    site_type: Mapped[str] = mapped_column(String(32), default="I2P")
    state: Mapped[str] = mapped_column(String(32), default="NEW", index=True)
    source: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    discovery_method: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    is_cross_layer: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    cross_layer_validated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    network_source: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_network_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_crawled_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    failure_count: Mapped[int] = mapped_column(Integer, default=0)
    success_count: Mapped[int] = mapped_column(Integer, default=0)
    # Retry scheduling: site is eligible for (re)selection when this is NULL or past.
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    last_error_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    last_error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=now, onupdate=now)

    pages: Mapped[list["Page"]] = relationship(back_populates="site")


class Page(Base):
    __tablename__ = "pages"
    __table_args__ = (
        # Per-epoch uniqueness: re-crawling the same URL in a new epoch is a
        # new observation row, which is what makes churn measurable.
        UniqueConstraint("normalized_url", "epoch_id", name="uq_pages_url_epoch"),
        Index("ix_pages_site_depth", "site_id", "depth"),
        Index("ix_pages_epoch", "epoch_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id"), nullable=False, index=True)
    epoch_id: Mapped[Optional[int]] = mapped_column(ForeignKey("epochs.id"), nullable=True, index=True)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    normalized_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    depth: Mapped[int] = mapped_column(Integer, default=0)
    http_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    content_type: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    content_length: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    title: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    text_length: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    word_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    content_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    response_time_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    site: Mapped[Site] = relationship(back_populates="pages")


class Link(Base):
    __tablename__ = "links"
    __table_args__ = (
        UniqueConstraint("source_page_id", "target_url", name="uq_links_page_target"),
        Index("ix_links_source_target", "source_site_id", "target_host"),
        Index("ix_links_epoch", "epoch_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_site_id: Mapped[int] = mapped_column(ForeignKey("sites.id"), nullable=False, index=True)
    source_page_id: Mapped[Optional[int]] = mapped_column(ForeignKey("pages.id"), nullable=True, index=True)
    epoch_id: Mapped[Optional[int]] = mapped_column(ForeignKey("epochs.id"), nullable=True, index=True)
    target_host: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    target_url: Mapped[str] = mapped_column(String(2048), nullable=False)
    target_site_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sites.id"), nullable=True, index=True)
    anchor_text: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    link_type: Mapped[str] = mapped_column(String(32), default="I2P")
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=now, onupdate=now)


class CrawlAttempt(Base):
    __tablename__ = "crawl_attempts"
    __table_args__ = (
        Index("ix_attempts_site_type_started", "site_id", "attempt_type", "started_at"),
        Index("ix_attempts_epoch", "epoch_id"),
        Index("ix_attempts_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[int] = mapped_column(ForeignKey("sites.id"), nullable=False, index=True)
    epoch_id: Mapped[Optional[int]] = mapped_column(ForeignKey("epochs.id"), nullable=True, index=True)
    attempt_type: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="STARTED")
    discovery_source: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    is_cross_layer: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    pages_fetched: Mapped[int] = mapped_column(Integer, default=0)
    links_found: Mapped[int] = mapped_column(Integer, default=0)
    error_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


def _seed_source_key(source_detail: str | None) -> str:
    return hashlib.sha256((source_detail or "").encode("utf-8", errors="ignore")).hexdigest()


class SeedEvent(Base):
    """Deduplicated discovery events.

    Uniqueness is on (host, source_type, source_key) where source_key is the
    SHA-256 of source_detail (too long for a MySQL unique index directly).
    Re-observing the same discovery bumps ``count``/``last_seen_at`` instead
    of inserting a duplicate row — Study 1 produced 400,720 rows this way.
    """

    __tablename__ = "seed_events"
    __table_args__ = (
        UniqueConstraint("host", "source_type", "source_key", name="uq_seed_events_dedup"),
        Index("ix_seed_events_host_source", "host", "source_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    host: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_detail: Mapped[Optional[str]] = mapped_column(String(2048), nullable=True)
    source_key: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    discovered_from_site_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sites.id"), nullable=True)
    discovered_from_page_id: Mapped[Optional[int]] = mapped_column(ForeignKey("pages.id"), nullable=True)
    epoch_id: Mapped[Optional[int]] = mapped_column(ForeignKey("epochs.id"), nullable=True, index=True)
    count: Mapped[int] = mapped_column(Integer, default=1)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime, default=now)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime, default=now)

    @staticmethod
    def make_key(source_detail: str | None) -> str:
        return _seed_source_key(source_detail)


class NetworkObservation(Base):
    """Cross-layer observation imported from network-layer tooling such as floodfill/netDB polling."""

    __tablename__ = "network_observations"
    __table_args__ = (
        Index("ix_network_obs_host_source_seen", "host", "source_type", "observed_at"),
        Index("ix_network_obs_epoch", "epoch_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    host: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    router_hash: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    epoch_id: Mapped[Optional[int]] = mapped_column(ForeignKey("epochs.id"), nullable=True, index=True)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_detail: Mapped[Optional[str]] = mapped_column(String(2048), nullable=True)
    raw_value: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime, default=now, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now)


class CrossLayerObservation(Base):
    """Evidence table for eepsites discovered or validated using network-layer artifacts."""

    __tablename__ = "cross_layer_observations"
    __table_args__ = (
        Index("ix_cross_layer_site_method_seen", "site_id", "lookup_method", "observed_at"),
        Index("ix_cross_layer_host_seen", "host", "observed_at"),
        Index("ix_cross_layer_epoch", "epoch_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    site_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sites.id"), nullable=True, index=True)
    host: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    epoch_id: Mapped[Optional[int]] = mapped_column(ForeignKey("epochs.id"), nullable=True, index=True)
    input_value: Mapped[Optional[str]] = mapped_column(String(2048), nullable=True)
    canonical_b32: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    lookup_method: Mapped[str] = mapped_column(String(128), nullable=False)
    leaseset_found: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    leaseset_hash: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    leaseset_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    routing_key: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    published: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    expires: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    gateway_count: Mapped[int] = mapped_column(Integer, default=0)
    floodfill_count: Mapped[int] = mapped_column(Integer, default=0)
    confidence: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    raw_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    console_template_version: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    lease_parser_status: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime, default=now, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now)


class Heartbeat(Base):
    """Liveness row updated by the scheduler; lets VM2-side monitoring see the crawler is alive."""

    __tablename__ = "heartbeats"
    __table_args__ = (Index("ix_heartbeats_updated", "updated_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=now, onupdate=now)
    pid: Mapped[int] = mapped_column(Integer, default=0)
    hostname: Mapped[str] = mapped_column(String(255), default="")
    epoch_label: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    phase: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    counters_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
