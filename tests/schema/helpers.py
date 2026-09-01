"""Shared helpers for the schema tests."""

from pathlib import Path

from sqlalchemy import Engine, create_engine
from sqlalchemy.pool import NullPool


def sqlite_engine(database_path: str | Path) -> Engine:
    """Create an engine for a SQLite file that holds no pooled connection.

    The default pool keeps one connection open after a statement completes.
    Windows refuses to delete a file while any handle to it is open, so a
    pooled connection makes the temp-file fixtures in this package fail at
    teardown with ``PermissionError``. ``NullPool`` closes the connection as
    soon as it is released, which keeps the file deletable.

    Disposing the engine would release the handle too, but only on the paths
    that reach the dispose call: a failing assertion would leave the file
    locked and bury the real failure under a teardown error.
    """
    return create_engine(f"sqlite:///{database_path}", poolclass=NullPool)
