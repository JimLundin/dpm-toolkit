"""Main module for schema generation."""

from pathlib import Path

from sqlalchemy import Engine, MetaData, create_engine

from schema.generation import Model


def sqlite_read_only_engine(database_location: Path) -> Engine:
    """Create a read-only SQLAlchemy engine for SQLite database.

    Args:
        database_path: Path to SQLite database file

    """
    connection_string = f"sqlite:///file:{database_location}?mode=ro"
    return create_engine(connection_string, connect_args={"uri": True})


def sqlite_to_sqlalchemy_schema(source_database: Engine) -> str:
    """Generate SQLAlchemy schema from migrated SQLite database.

    Args:
        source_database: Connection to SQLite database

    """
    schema = MetaData()
    schema.reflect(bind=source_database)

    return Model(schema).render()
