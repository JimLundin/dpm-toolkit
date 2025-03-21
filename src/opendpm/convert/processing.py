"""Database processing utilities for handling multiple Access databases."""

import logging
import time
from pathlib import Path
from typing import Any

from sqlacodegen.generators import DeclarativeGenerator
from sqlalchemy import Connection, Engine, MetaData, create_engine, event, text

from opendpm.convert.transformations import (
    cast_row_values,
    genericize_datatypes,
    get_enum_columns,
    get_required_columns,
    remove_pk_index,
    set_enum_columns,
    set_required_columns,
)
from opendpm.convert.utils import format_time

logger = logging.getLogger(__name__)


def get_access_engine(db_path: str | Path) -> Engine:
    """Get an engine to an Access database."""
    driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
    conn_str = f"DRIVER={driver};DBQ={db_path}"
    return create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")


def execute_queries(connection: Connection, queries: list[str]) -> None:
    """Execute a list of SQL queries."""
    for query in queries:
        connection.execute(text(query))
    connection.commit()


def process_database(source_path: Path, target_engine: Engine) -> str:
    """Process a single Access database.

    Args:
        source_path: Path to the Access database file
        target_engine: Engine to the target SQLite database

    """
    start = time.time()
    logger.info("%s - Processing database", source_path.name)

    source_engine = get_access_engine(source_path)

    metadata = MetaData()
    event.listen(metadata, "column_reflect", genericize_datatypes)
    metadata.reflect(bind=source_engine)

    with source_engine.connect() as source_conn, target_engine.connect() as target_conn:
        table_rows: dict[str, list[dict[str, Any]]] = {}
        for name, table in metadata.tables.items():
            data = source_conn.execute(table.select()).fetchall()
            rows = [row._asdict() for row in data]  # type: ignore private attribute
            cast_row_values(rows)
            table_rows[name] = rows

            enum_columns = get_enum_columns(rows)
            set_enum_columns(table, enum_columns)

            required_columns = get_required_columns(source_conn, table)
            set_required_columns(table, required_columns)
            remove_pk_index(table)

        metadata.create_all(target_engine)

        target_conn.begin()

        for name, rows in table_rows.items():
            if not rows:
                continue

            target_conn.execute(metadata.tables[name].insert(), rows)

        target_conn.commit()

        # Optimize the database
        execute_queries(
            target_conn,
            [
                "VACUUM",
                "PRAGMA optimize",
            ],
        )

    logger.info(
        "Database: %s, total time: %s",
        source_path.name,
        format_time(time.time() - start),
    )

    generator = DeclarativeGenerator(
        metadata,
        target_engine,
        ["use_inflect", "nojoined", "nobidi"],
        indentation="    ",
        base_class_name="Base",
    )

    return generator.generate()
