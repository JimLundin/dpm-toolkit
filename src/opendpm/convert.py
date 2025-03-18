"""Functions for converting Access databases to DuckDB or SQLite format."""

import datetime
import logging
import time
from pathlib import Path
from typing import Any

from sqlalchemy import (
    Boolean,
    Connection,
    Date,
    Inspector,
    MetaData,
    Table,
    Text,
    create_engine,
    event,
)
from sqlalchemy.engine.interfaces import ReflectedColumn

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR


def format_time(seconds: float) -> str:
    """Format time in seconds to a human-readable string."""
    if seconds < MINUTE:
        return f"{seconds:.2f} seconds"
    if seconds < HOUR:
        return f"{seconds / MINUTE:.2f} minutes"
    if seconds < DAY:
        return f"{seconds / HOUR:.2f} hours"
    return f"{seconds / DAY:.2f} days"


def genericize_datatypes(
    _inspector: Inspector,
    _table_name: str,
    column_dict: ReflectedColumn,
) -> None:
    """Convert GUID columns to Text and all other columns to generic types."""
    if column_dict["name"].lower().endswith("guid"):
        # GUIDs are classified as Integer in the Source
        column_dict["type"] = Text()
    elif column_dict["name"].lower().endswith("date"):
        # Dates are classified as String in the Source
        column_dict["type"] = Date()
    elif column_dict["name"].lower().startswith("is"):
        # Boolean columns are classified as Integer in the Source
        column_dict["type"] = Boolean()
    else:
        column_dict["type"] = column_dict["type"].as_generic()


def cast_row_values(rows: list[dict[str, Any]]) -> None:
    """Cast date and boolean columns in a table."""
    for row in rows:
        for col_name, value in row.items():
            try:
                if col_name.lower().endswith("date") and isinstance(value, str):
                    row[col_name] = datetime.date.fromisoformat(value)
                if col_name.lower().startswith("is") and isinstance(value, int):
                    row[col_name] = bool(value)
            except (ValueError, TypeError):
                row[col_name] = None


def remove_table_primary_key_index(table: Table) -> None:
    """Remove the primary key index from a table."""
    indexes_to_drop = [index for index in table.indexes if index.name == "PrimaryKey"]
    for index in indexes_to_drop:
        table.indexes.remove(index)


BATCH_SIZE = 50_000


def insert_data(
    target_conn: Connection,
    table: Table,
    rows: list[dict[str, Any]],
    batch_size: int = BATCH_SIZE,
) -> None:
    """Insert data into a table."""
    total_rows = len(rows)

    if total_rows > batch_size:
        for i, batch_start in enumerate(range(0, total_rows, batch_size)):
            batch = rows[batch_start : batch_start + batch_size]
            target_conn.execute(table.insert(), batch)
            logger.debug(
                "Table: %s, batch %d/%d with %d rows processed",
                table.name,
                i + 1,
                (total_rows + batch_size - 1) // batch_size,
                len(batch),
            )
    else:
        target_conn.execute(table.insert(), rows)


def migrate_database(
    input_dir: str | Path,
    output_dir: str | Path,
) -> None:
    """Convert Access database to SQLite.

    Args:
        input_dir: Path to directory with Access databases.
        output_dir: Path to output directory.

    """
    total_start_time = time.time()

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    target_engine = create_engine(f"sqlite:///{output_dir}/dpm.sqlite")

    with target_engine.connect() as target_conn:
        for source_path in input_dir.glob("**/*.accdb"):
            db_start_time = time.time()
            logger.info("%s - Processing database", source_path.name)
            driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
            conn_str = f"DRIVER={driver};DBQ={source_path}"
            source_engine = create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")

            metadata = MetaData()
            event.listen(metadata, "column_reflect", genericize_datatypes)
            metadata.reflect(bind=source_engine)

            for table in metadata.tables.values():
                remove_table_primary_key_index(table)

            metadata.create_all(target_engine)

            with source_engine.connect() as source_conn:
                target_conn.begin()

                for table_name, table in metadata.tables.items():
                    try:
                        table_start_time = time.time()
                        data = source_conn.execute(table.select()).fetchall()
                        if not data:
                            logger.info("Table: %s - No data to copy", table_name)
                            continue

                        rows = [row._asdict() for row in data]  # type: ignore
                        cast_row_values(rows)
                        insert_start_time = time.time()
                        insert_data(target_conn, table, rows)
                        logger.info(
                            "Table: %s, rows: %d, columns: %d, fetch: %s, insert: %s",
                            table_name,
                            len(rows),
                            len(rows[0]),
                            format_time(insert_start_time - table_start_time),
                            format_time(time.time() - insert_start_time),
                        )

                    except Exception:
                        logger.exception("%s - Failed to copy table", table_name)

                target_conn.commit()

            logger.info(
                "Database: %s, total time: %s",
                input_dir.name,
                format_time(time.time() - db_start_time),
            )

    logger.info("Conversion time: %s", format_time(time.time() - total_start_time))
