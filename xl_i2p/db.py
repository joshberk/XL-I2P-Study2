from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import settings
from .models import Base

def _engine_kwargs() -> dict:
    """connect_timeout is a pymysql/MySQL-only option; other backends (e.g.
    SQLite via DATABASE_URL override) reject it as an invalid argument."""
    kwargs: dict = {"pool_pre_ping": True, "pool_recycle": 3600, "future": True}
    if settings.database_url.startswith("mysql"):
        kwargs["connect_args"] = {"connect_timeout": 10}
    return kwargs


engine = create_engine(settings.database_url, **_engine_kwargs())
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=Session, future=True)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


@contextmanager
def session_scope() -> Iterator[Session]:
    """Short-lived session for one unit of work. Never held across cycles."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
