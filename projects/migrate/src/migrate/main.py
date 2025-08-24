"""Main module for database conversion."""

from sqlite3 import Connection

from sqlalchemy import Engine, create_engine

from migrate.processing import load_data, schema_and_data


def access_to_sqlite(source_database: Engine) -> Connection:
    """Migrate Access database to SQLite.

    Args:
        source_database: Directory or path to Access database

    Returns:
        Connection to the new SQLite database

    """
    schema, tables_and_rows = schema_and_data(source_database)

    sqlite_database = create_engine("sqlite://")
    schema.create_all(sqlite_database)
    load_data(sqlite_database, tables_and_rows)

    return sqlite_database.raw_connection().connection
