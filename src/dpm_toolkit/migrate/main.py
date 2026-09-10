"""Main module for database conversion."""

from pathlib import Path
from sqlite3 import Connection
from typing import cast

from sqlalchemy import Engine, MetaData, create_engine

from .processing import (
    TablesWithRows,
    load_data_to_database,
    schema_and_data,
    schema_and_data_from_file,
)


def _to_sqlite(schema: MetaData, tables_with_rows: TablesWithRows) -> Connection:
    """Build an in-memory SQLite database from a schema and its rows."""
    sqlite_database = create_engine("sqlite://")

    schema.create_all(sqlite_database)

    load_data_to_database(sqlite_database, tables_with_rows)

    return cast("Connection", sqlite_database.raw_connection().dbapi_connection)


def access_to_sqlite(source_database: Engine) -> Connection:
    """Migrate Access database to SQLite through the Access ODBC driver."""
    schema, tables_with_rows = schema_and_data(source_database)
    return _to_sqlite(schema, tables_with_rows)


def access_file_to_sqlite(access_location: Path) -> Connection:
    """Migrate Access database to SQLite through mdbtools."""
    schema, tables_with_rows = schema_and_data_from_file(access_location)
    return _to_sqlite(schema, tables_with_rows)
