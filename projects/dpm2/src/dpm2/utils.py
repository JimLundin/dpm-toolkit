"""db utilities for using the dpm db."""

from pathlib import Path
from sqlite3 import Connection, connect
from typing import cast

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session

db_path = Path(__file__).parent / "dpm.sqlite"


def get_engine() -> Engine:
    """Get an engine to the dpm db.

    This function copies the packaged database into memory to prevent
    modifications to the original database file and allow users to
    make arbitrary changes safely.

    Returns:
        Engine: SQLAlchemy engine connected to the in-memory copy of the database

    """
    engine = create_engine("sqlite://")

    with connect(db_path) as source_conn, engine.begin() as target_conn:
        dbapi_conn = cast("Connection | None", target_conn.connection.dbapi_connection)
        if dbapi_conn is None:
            msg = "Failed to get connection to DPM database"
            raise RuntimeError(msg)

        source_conn.backup(dbapi_conn)

    return engine


# TODO(Jim): move this to a context manager
class DB:
    """Represent an instance of the DPM database."""

    def __init__(self) -> None:
        """Initialize the DB instance."""
        self.engine = get_engine()
        self.session = Session(self.engine)
