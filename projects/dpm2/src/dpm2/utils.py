"""db utilities for using the dpm db."""

from pathlib import Path
from sqlite3 import connect

from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.event import listen
from sqlalchemy.pool import ConnectionPoolEntry

db_path = Path(__file__).parent / "dpm.sqlite"


def set_readonly(connection: Connection, _record: ConnectionPoolEntry) -> None:
    """Set the connection to readonly."""
    connection.execute(text("PRAGMA readonly = true"))


def disk_engine() -> Engine:
    """Get an engine to the dpm db."""
    return create_engine(f"sqlite:///{db_path}?mode=ro", connect_args={"uri": True})


def in_memory_engine() -> Engine:
    """Get an engine to the dpm db."""
    memory_db = connect(":memory:")
    with connect(db_path) as source_db:
        source_db.backup(memory_db)
    return create_engine("sqlite://", creator=lambda: memory_db)


def get_db(*, in_memory: bool = False) -> Engine:
    """Get an engine to the dpm db."""
    engine = in_memory_engine() if in_memory else disk_engine()
    listen(engine, "connect", set_readonly)
    return engine
