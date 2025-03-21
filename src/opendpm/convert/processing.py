"""Database processing utilities for handling multiple Access databases."""

import logging
import time
from pathlib import Path

from sqlalchemy import Connection, Engine, MetaData, create_engine, event

from opendpm.convert.transformations import (
    genericize_datatypes,
    get_required_columns,
    remove_pk_index,
    set_required_columns,
    cast_row_values,
)
from opendpm.convert.utils import format_time

logger = logging.getLogger(__name__)


def get_access_engine(db_path: str | Path) -> Engine:
    """Get an engine to an Access database."""
    driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
    conn_str = f"DRIVER={driver};DBQ={db_path}"
    return create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")


def process_database(
    source_path: Path,
    target_conn: Connection,
) -> None:
    """Process a single Access database.

    Args:
        source_path: Path to the Access database file
        target_conn: Connection to the target SQLite database

    """
    start = time.time()
    logger.info("%s - Processing database", source_path.name)

    source_engine = get_access_engine(source_path)

    metadata = MetaData()
    event.listen(metadata, "column_reflect", genericize_datatypes)
    metadata.reflect(bind=source_engine)

    with source_engine.connect() as source_conn:
        for table in metadata.tables.values():
            required_columns = get_required_columns(source_conn, table)
            set_required_columns(table, required_columns)
            remove_pk_index(table)

        metadata.create_all(target_conn.engine)

        target_conn.begin()

        for table_name, table in metadata.tables.items():
            fetch_start = time.time()
            data = source_conn.execute(table.select()).fetchall()
            if not data:
                logger.info("Table: %s - No data to copy", table_name)
                continue

            rows = [row._asdict() for row in data]  # type: ignore private attribute
            cast_row_values(rows)
            insert_start = time.time()
            target_conn.execute(table.insert(), rows)
            logger.info(
                "Table: %s, rows: %d, columns: %d, fetch: %s, insert: %s",
                table_name,
                len(rows),
                len(rows[0]) if rows else 0,
                format_time(insert_start - fetch_start),
                format_time(time.time() - insert_start),
            )

        target_conn.commit()

    logger.info(
        "Database: %s, total time: %s",
        source_path.name,
        format_time(time.time() - start),
    )
