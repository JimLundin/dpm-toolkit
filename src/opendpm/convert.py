"""Functions for converting Access databases to SQLite format."""

import datetime
import logging
import time
from pathlib import Path
from typing import Any, TypedDict

from sqlalchemy import (
    Boolean,
    Date,
    Inspector,
    MetaData,
    Table,
    Text,
    create_engine,
    event,
)
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.types import TypeEngine

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


class ColumnType(TypedDict):
    """Column type mapping."""

    sql_type: TypeEngine[Any]
    python_type: type


COLUMNS_CAST: dict[str, ColumnType] = {
    "ParentFirst": {"sql_type": Boolean(), "python_type": bool},
    "UseIntervalArithmetics": {"sql_type": Boolean(), "python_type": bool},
}


def genericize_datatypes(
    _inspector: Inspector,
    _table_name: str,
    column_dict: ReflectedColumn,
) -> None:
    """Convert columns to generic types."""
    if column_dict["name"].lower().endswith("guid"):
        # GUIDs are classified as Integer in the Source
        column_dict["type"] = Text()
    elif column_dict["name"].lower().endswith("date"):
        # Dates are classified as String in the Source
        column_dict["type"] = Date()
    elif column_dict["name"].lower().startswith(("is", "has")):
        # Boolean columns are classified as Integer in the Source
        column_dict["type"] = Boolean()
    elif column_dict["name"] in COLUMNS_CAST:
        # Cast specific columns to their correct types
        column_dict["type"] = COLUMNS_CAST[column_dict["name"]]["sql_type"]
    else:
        column_dict["type"] = column_dict["type"].as_generic()


def cast_row_values(rows: list[dict[str, Any]]) -> None:
    """Cast columns in a table."""
    for row in rows:
        for col_name, value in row.items():
            try:
                if col_name.lower().endswith("date") and isinstance(value, str):
                    row[col_name] = datetime.date.fromisoformat(value)
                if col_name.lower().startswith(("is", "has")) and isinstance(
                    value,
                    int,
                ):
                    row[col_name] = bool(value)
                if col_name in COLUMNS_CAST:
                    row[col_name] = COLUMNS_CAST[col_name]["python_type"](value)
            except (ValueError, TypeError):
                row[col_name] = None


def remove_table_primary_key_index(table: Table) -> None:
    """Remove the primary key index from a table."""
    indexes_to_drop = [index for index in table.indexes if index.name == "PrimaryKey"]
    for index in indexes_to_drop:
        table.indexes.remove(index)


def ensure_primary_keys_not_null(table: Table) -> None:
    """Ensure all primary key columns are set as NOT NULL."""
    for column in table.primary_key.columns:
        column.nullable = False


def migrate_database(
    input_dir: str | Path,
    output_dir: str | Path,
) -> None:
    """Convert Access database to SQLite.

    Args:
        input_dir: Path to directory with Access databases.
        output_dir: Path to directory where SQLite database will be created.


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
                ensure_primary_keys_not_null(table)

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

                        rows = [row._asdict() for row in data]  # type: ignore[attr-defined]
                        cast_row_values(rows)
                        insert_start_time = time.time()
                        target_conn.execute(table.insert(), rows)
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
