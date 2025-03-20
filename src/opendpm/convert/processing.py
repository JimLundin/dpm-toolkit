"""Database processing utilities for handling multiple Access databases."""

import logging
import time
from pathlib import Path

from sqlalchemy import Connection, Engine, MetaData, create_engine, event

from opendpm.convert.transformations import (
    ensure_primary_keys_not_null,
    genericize_datatypes,
    identify_non_nullable_columns,
    remove_table_primary_key_index,
    set_columns_not_null,
    transform_row_values,
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
) -> float:
    """Process a single Access database.

    Args:
        source_path: Path to the Access database file
        target_conn: Connection to the target SQLite database

    Returns:
        Processing time in seconds

    """
    db_start_time = time.time()
    logger.info("%s - Processing database", source_path.name)

    source_engine = get_access_engine(source_path)

    metadata = MetaData()
    event.listen(metadata, "column_reflect", genericize_datatypes)
    metadata.reflect(bind=source_engine)

    with source_engine.connect() as source_conn:
        for table in metadata.tables.values():
            not_nullable_columns = identify_non_nullable_columns(
                source_conn,
                table,
            )
            set_columns_not_null(table, not_nullable_columns)
            remove_table_primary_key_index(table)
            ensure_primary_keys_not_null(table)

        metadata.create_all(target_conn.engine)

        target_conn.begin()

        for table_name, table in metadata.tables.items():
            table_start_time = time.time()
            data = source_conn.execute(table.select()).fetchall()
            if not data:
                logger.info("Table: %s - No data to copy", table_name)
                continue

            rows = [row._asdict() for row in data]  # type: ignore private attribute
            transform_row_values(rows)
            insert_start_time = time.time()
            target_conn.execute(table.insert(), rows)
            logger.info(
                "Table: %s, rows: %d, columns: %d, fetch: %s, insert: %s",
                table_name,
                len(rows),
                len(rows[0]) if rows else 0,
                format_time(insert_start_time - table_start_time),
                format_time(time.time() - insert_start_time),
            )

        target_conn.commit()

    elapsed_time = time.time() - db_start_time
    logger.info(
        "Database: %s, total time: %s",
        source_path.name,
        format_time(elapsed_time),
    )

    return elapsed_time
