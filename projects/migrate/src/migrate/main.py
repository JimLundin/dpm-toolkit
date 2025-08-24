"""Main module for database conversion."""

from sqlite3 import Connection

from sqlalchemy import Engine, create_engine

from migrate.processing import schema_and_data, load_data


def migrate_to_sqlite(source: Engine) -> Connection:
    """Migrate Access database to SQLite.

    Args:
        source: Directory or path to Access database

    Returns:
        Connection to the new SQLite database

    """
    metadata, tables = schema_and_data(source)

    sqlite = create_engine("sqlite://")
    metadata.create_all(sqlite)
    load_data(sqlite, tables)

    return sqlite.raw_connection().connection
