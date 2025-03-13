"""Functions for converting Access databases to DuckDB or SQLite format."""

import logging
import time
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import (
    Column,
    Connection,
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
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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
    if column_dict["name"].endswith("GUID"):
        column_dict["type"] = Text()
    else:
        column_dict["type"] = column_dict["type"].as_generic()


def copy_metadata_columns(metadata: MetaData) -> MetaData:
    """Remove foreign keys and constraints from tables in a MetaData object."""
    new_metadata = MetaData()

    for table_name, table in metadata.tables.items():
        columns: list[Column[Any]] = []
        for col in table.columns:
            new_col = col.copy()
            new_col.foreign_keys.clear()
            columns.append(new_col)

        Table(table_name, new_metadata, *columns)

    return new_metadata


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
    db_format: Literal["sqlite", "duckdb"],
) -> None:
    """Convert Access database to SQLite or DuckDB.

    Args:
        input_dir: Path to directory with Access databases.
        output_dir: Path to output directory.
        db_format: The output format, either "sqlite" or "duckdb".

    """
    total_start_time = time.time()

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if db_format not in ["sqlite", "duckdb"]:
        msg = f"Unsupported database format: {db_format}"
        raise ValueError(msg)

    target_engine = create_engine(f"{db_format}:///{output_dir}/dpm.{db_format}")
    logger.info("Using %s as target format", db_format)

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

            if db_format == "duckdb":
                metadata = copy_metadata_columns(metadata)

            for table in metadata.tables.values():
                remove_table_primary_key_index(table)

            metadata.create_all(target_engine)

            with source_engine.connect() as source_conn:
                target_conn.begin()

                for table_name, table in metadata.tables.items():
                    table_start_time = time.time()

                    try:
                        data = source_conn.execute(table.select()).fetchall()

                        if not data:
                            logger.info("Table: %s - No data to copy", table_name)
                            continue

                        rows = [row._asdict() for row in data]
                        total_rows = len(rows)

                        insert_start_time = time.time()
                        insert_data(target_conn, table, rows)
                        logger.info(
                            "Table: %s, rows: %d, fetch time: %s, insert time: %s",
                            table_name,
                            total_rows,
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
