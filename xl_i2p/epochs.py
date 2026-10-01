"""Epoch lifecycle.

Study 1 was additive, not longitudinal: epochs were never modeled. Study 2
opens one epoch at a time; every observation row is tagged with the epoch;
opening a new epoch resets ``next_retry_at`` so the whole cohort is
re-probed (availability churn) and re-crawled (content/link churn).
"""
from __future__ import annotations

import logging

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from .config import settings
from .models import Epoch, Site, now
from .states import EpochStatus

logger = logging.getLogger(__name__)


class EpochError(RuntimeError):
    pass


def get_open_epoch(session: Session) -> Epoch | None:
    return session.scalar(
        select(Epoch).where(Epoch.status == EpochStatus.OPEN.value).order_by(Epoch.id.desc())
    )


def get_epoch(session: Session, label: str) -> Epoch | None:
    return session.scalar(select(Epoch).where(Epoch.label == label))


def open_epoch(session: Session, label: str, note: str | None = None) -> Epoch:
    """Open a new epoch and reset the cohort's retry schedule for re-probing."""
    label = label.strip()
    if not label:
        raise EpochError("epoch label must not be empty")
    already_open = get_open_epoch(session)
    if already_open is not None:
        raise EpochError(
            f"epoch '{already_open.label}' is already OPEN; close it or --resume it first"
        )
    if get_epoch(session, label) is not None:
        raise EpochError(f"epoch '{label}' already exists")
    epoch = Epoch(
        label=label,
        status=EpochStatus.OPEN.value,
        config_json=settings.snapshot_json(),
        note=note,
    )
    session.add(epoch)
    session.flush()
    # Cohort reset: every site becomes eligible for re-verification this epoch.
    # Terminal-for-epoch states (UNREACHABLE/ERROR) are re-probed because their
    # last_checked_at predates this epoch's start.
    # NOTE: updated_at is pinned explicitly — session.execute(update(Site))
    # is ORM-enabled and would otherwise fire onupdate=now on every row,
    # which would mask stuck VERIFYING/CRAWLING rows from the janitor.
    result = session.execute(
        update(Site).values(next_retry_at=None, updated_at=Site.updated_at)
    )
    session.commit()
    logger.info(
        "opened epoch '%s' (id=%d); reset next_retry_at for %d sites",
        label,
        epoch.id,
        result.rowcount,
    )
    return epoch


def close_epoch(session: Session, label: str) -> Epoch:
    epoch = get_epoch(session, label)
    if epoch is None:
        raise EpochError(f"no such epoch '{label}'")
    if epoch.status != EpochStatus.OPEN.value:
        raise EpochError(f"epoch '{label}' is not OPEN")
    epoch.status = EpochStatus.CLOSED.value
    epoch.ended_at = now()
    session.commit()
    logger.info("closed epoch '%s'", label)
    return epoch


def list_epochs(session: Session) -> list[Epoch]:
    return list(session.scalars(select(Epoch).order_by(Epoch.id)))


def _next_label(label: str) -> str:
    """Derive the next epoch label by incrementing a trailing -NN suffix."""
    import re

    match = re.search(r"-(\d+)$", label)
    if match:
        stem, num = label[: match.start()], int(match.group(1))
        return f"{stem}-{num + 1:02d}"
    return f"{label}-02"


def rollover_if_due(session: Session) -> Epoch | None:
    """Close the open epoch and open the next one if it reached its max age.

    Returns the newly opened epoch, the still-current open epoch if no
    rollover was due, or None if no epoch is open. Idempotent and safe to
    call from both the systemd rollover timer and the crawler loop: if two
    actors race, the loser sees the freshly opened epoch and returns it.
    """
    if not settings.epoch_auto_rollover:
        return get_open_epoch(session)
    current = get_open_epoch(session)
    if current is None:
        return None
    age_days = (now() - current.started_at).total_seconds() / 86400
    if age_days < settings.epoch_duration_days:
        return current
    logger.info(
        "epoch '%s' reached %.1f days (limit %d); rolling over",
        current.label,
        age_days,
        settings.epoch_duration_days,
    )
    close_epoch(session, current.label)
    try:
        new_epoch = open_epoch(session, _next_label(current.label),
                               note=f"auto-rollover from '{current.label}'")
    except EpochError:
        # Lost a race with another rollover actor; return whatever is open.
        logger.info("rollover race detected; using the epoch opened by the winner")
        new_epoch = get_open_epoch(session)
        if new_epoch is None:
            raise EpochError("rollover race left no open epoch")
    return new_epoch
