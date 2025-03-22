"""Database processing utilities for handling multiple Access databases."""

import logging
from pathlib import Path

from sqlalchemy import (
    Connection,
    Engine,
    MetaData,
    create_engine,
    event,
    insert,
    select,
    text,
)
from sqlalchemy.orm import Session

from opendpm.convert.generation import generate_models
from opendpm.convert.transformations import (
    Rows,
    genericize_datatypes,
    parse_rows,
    remove_pk_index,
    set_enum_columns,
    set_nullable_columns,
)

logger = logging.getLogger(__name__)


def create_access_engine(db_path: str | Path) -> Engine:
    """Get an engine to an Access database."""
    driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
    conn_str = f"DRIVER={driver};DBQ={db_path}"
    return create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")


def execute_queries(connection: Connection, queries: list[str]) -> None:
    """Execute a list of SQL queries."""
    for query in queries:
        connection.execute(text(query))
    connection.commit()


def process_database(source_engine: Engine, target_engine: Engine) -> str:
    """Process a single Access database.

    Args:
        source_engine: Engine to the source Access database
        target_engine: Engine to the target SQLite database

    Returns:
        str: Generated SQLAlchemy model code

    """
    metadata = MetaData()
    event.listen(metadata, "column_reflect", genericize_datatypes)
    metadata.reflect(bind=source_engine)

    with (
        Session(source_engine) as source,
        Session(target_engine) as target,
        source.begin(),
        target.begin(),
    ):
        table_rows: dict[str, Rows] = {}
        for name, table in metadata.tables.items():
            data = source.execute(select(table)).all()
            rows = [row._asdict() for row in data]  # type: ignore private attribute

            enum_columns, nullable_columns = parse_rows(rows)
            set_enum_columns(table, enum_columns)
            set_nullable_columns(table, nullable_columns)
            remove_pk_index(table)

            table_rows[name] = rows

        metadata.create_all(target_engine)

        for name, rows in table_rows.items():
            if not rows:
                continue

            table = metadata.tables[name]
            target.execute(insert(table), rows)

    return generate_models(metadata, target_engine)
