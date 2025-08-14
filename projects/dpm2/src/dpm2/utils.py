"""db utilities for using the dpm db."""

from importlib.resources import as_file, files
from importlib.resources.abc import Traversable
from pathlib import Path
from sqlite3 import connect

from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.event import listen
from sqlalchemy.pool import ConnectionPoolEntry


def get_source_db_resource() -> Traversable:
    """Get the path to the bundled SQLite database."""
    package_files = files("dpm2")
    return package_files / "dpm.sqlite"


def set_readonly(connection: Connection, _record: ConnectionPoolEntry) -> None:
    """Set the connection to readonly."""
    connection.execute(text("PRAGMA readonly = true"))


def disk_engine(db_path: Path) -> Engine:
    """Get an engine to the dpm db."""
    engine = create_engine(f"sqlite:///{db_path}?mode=ro", connect_args={"uri": True})
    listen(engine, "connect", set_readonly)
    return engine


def in_memory_engine(db_path: Path) -> Engine:
    """Get an engine to the dpm db."""
    memory_db = connect(":memory:")
    with connect(db_path) as source_db:
        source_db.backup(memory_db)
    return create_engine("sqlite://", creator=lambda: memory_db)


def get_db(*, in_memory: bool = True) -> Engine:
    """Get an engine to the dpm db."""
    db_resource = get_source_db_resource()
    with as_file(db_resource) as db_path:
        if not db_path.exists():
            msg = f"Database file not found: {db_path}"
            raise FileNotFoundError(msg)

        return in_memory_engine(db_path) if in_memory else disk_engine(db_path)
