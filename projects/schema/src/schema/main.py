"""Main module for schema generation."""

from pathlib import Path

from sqlalchemy import Engine, MetaData, create_engine

from schema.generation import Model


def sqlite_read_only(sqlite_location: Path) -> Engine:
    """Create a read-only SQLAlchemy engine for SQLite database.

    Args:
        sqlite_location: Path to SQLite database file

    """
    connection_string = f"sqlite:///file:{sqlite_location}?mode=ro"
    return create_engine(connection_string, connect_args={"uri": True})


def sqlite_to_sqlalchemy_schema(sqlite_database: Engine) -> str:
    """Generate SQLAlchemy schema from migrated SQLite database.

    Args:
        sqlite_database: Engine of the SQLite database

    """
    schema = MetaData()
    schema.reflect(bind=sqlite_database)

    return Model(schema).render()
