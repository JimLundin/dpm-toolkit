"""Database processing utilities for handling multiple Access databases."""

from pathlib import Path

from sqlalchemy import Engine, MetaData, Table, create_engine, event, insert, select

from migrate.transformations import (
    TableData,
    add_foreign_keys,
    apply_enums,
    genericize,
    mark_non_nullable,
    parse,
)

type TableDataMap = list[tuple[Table, TableData]]


def create_access_engine(db: Path) -> Engine:
    """Get an engine to an Access database."""
    driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
    conn_str = f"DRIVER={driver};DBQ={db}"
    return create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")


def reflect_schema(source: Engine) -> MetaData:
    """Reflect a database schema."""
    metadata = MetaData()
    event.listen(metadata, "column_reflect", genericize)
    metadata.reflect(bind=source)
    return metadata


def schema_and_data(source: Engine) -> tuple[MetaData, TableDataMap]:
    """Extract data and schema from a single Access database.

    Args:
        source: Engine to the source Access database

    Returns:
        MetaData: Database metadata
        TableData: Table rows

    """
    metadata = reflect_schema(source)

    tables: TableDataMap = []
    with source.begin() as connection:
        for table in metadata.tables.values():
            data = connection.execute(select(table)).all()
            rows, enums, nullables = parse(data)

            # Clear indexes to avoid name collisions and save space
            table.indexes.clear()
            # We are using non-integer primary keys, we disable rowid to save space
            if table.primary_key:
                table.kwargs["sqlite_with_rowid"] = False

            apply_enums(table, enums)
            mark_non_nullable(table, nullables)
            add_foreign_keys(table)

            if rows:
                tables.append((table, rows))

    return metadata, tables


def load_data(target: Engine, tables: TableDataMap) -> None:
    """Populate the target database with data from the source database."""
    with target.begin() as connection:
        for table, data in tables:
            connection.execute(insert(table), data)
