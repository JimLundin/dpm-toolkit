"""Database processing utilities for handling multiple Access databases."""

from logging import getLogger
from pathlib import Path

from sqlalchemy import Engine, MetaData, Table, create_engine, event, insert, select
from sqlalchemy.orm import Session

from convert.transformations import (
    TableData,
    add_foreign_keys,
    apply_enums,
    genericize,
    mark_non_nullable,
    parse,
)

logger = getLogger(__name__)

type TableDataMap = list[tuple[Table, TableData]]


def get_database(source: Path) -> Path | None:
    """Get an Access database. Prioritize DPM databases."""
    databases = list(source.glob("**/*.accdb"))
    dpms = [database for database in databases if "dpm" in database.stem.lower()]
    if dpms:
        return dpms[0]

    logger.warning("No DPM Access database found in %s, using first match", source)
    if databases:
        return databases[0]

    logger.warning("No Access database found in %s", source)
    return None


def create_access_engine(db: str | Path) -> Engine:
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


def extract_schema_and_data(source: Engine) -> tuple[MetaData, TableDataMap]:
    """Extract data and schema from a single Access database.

    Args:
        source: Engine to the source Access database

    Returns:
        MetaData: Database metadata
        TableData: Table rows

    """
    metadata = reflect_schema(source)

    tables: TableDataMap = []
    with Session(source) as session:
        for table in metadata.tables.values():
            data = session.execute(select(table)).all()
            rows, enums, nullables = parse(data)

            # Clear indexes to avoid name collisions and save space
            table.indexes.clear()
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
    with Session(target) as session:
        for table, data in tables:
            session.execute(insert(table), data)

        session.commit()
