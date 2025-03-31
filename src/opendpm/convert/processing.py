"""Database processing utilities for handling multiple Access databases."""

import logging
from pathlib import Path

from sqlalchemy import Engine, MetaData, Table, create_engine, event, insert, select
from sqlalchemy.orm import Session

from opendpm.convert.transformations import (
    Rows,
    genericize,
    parse_data,
    remove_pk_index,
    set_enum_columns,
    set_guid_fk,
    set_null_columns,
)

logger = logging.getLogger(__name__)

type TableRows = list[tuple[Table, Rows]]


def create_access_engine(db_path: str | Path) -> Engine:
    """Get an engine to an Access database."""
    driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
    conn_str = f"DRIVER={driver};DBQ={db_path}"
    return create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")


def reflect_database(source_engine: Engine) -> MetaData:
    """Reflect a database schema."""
    metadata = MetaData()
    event.listen(metadata, "column_reflect", genericize)
    metadata.reflect(bind=source_engine)
    return metadata


def fetch_database(source_engine: Engine) -> tuple[MetaData, TableRows]:
    """Fetch data from a single Access database.

    Args:
        source_engine: Engine to the source Access database

    Returns:
        MetaData: Database metadata
        TableRows: Table rows

    """
    metadata = reflect_database(source_engine)

    table_rows: list[tuple[Table, Rows]] = []
    with Session(source_engine) as source:
        for table in metadata.tables.values():
            data = source.execute(select(table)).all()
            rows, enum_columns, nullable_columns = parse_data(data)

            set_enum_columns(table, enum_columns)
            set_null_columns(table, nullable_columns)
            remove_pk_index(table)
            set_guid_fk(table)

            if rows:
                table_rows.append((table, rows))

    return metadata, table_rows


def populate_database(target_engine: Engine, table_rows: TableRows) -> None:
    """Populate the target database with data from the source database."""
    with Session(target_engine) as target, target.begin():
        for table, rows in table_rows:
            target.execute(insert(table), rows)
