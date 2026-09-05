from functools import lru_cache

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from app.config import settings


@lru_cache(maxsize=4)
def _engine_for_url(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True)


def get_engine(database_url: str | None = None) -> Engine:
    """Create the engine only when the database is actually needed."""

    return _engine_for_url(database_url or settings.database_url)


@lru_cache(maxsize=4)
def get_session_factory(database_url: str | None = None) -> sessionmaker:
    return sessionmaker(autocommit=False, autoflush=False, bind=get_engine(database_url))


def check_database(database_url: str | None = None) -> bool:
    """Return readiness without leaking connection details to the caller."""

    with get_engine(database_url).connect() as connection:
        connection.execute(text("SELECT 1"))
    return True
