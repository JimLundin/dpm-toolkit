"""Schema transformation utilities for database conversion."""

import datetime
from collections.abc import Callable
from typing import Any, NotRequired, TypedDict

from sqlalchemy import (
    Boolean,
    Connection,
    Date,
    DateTime,
    Inspector,
    Table,
    Text,
    func,
    select,
)
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.types import TypeEngine


class ColumnType(TypedDict):
    """Column type mapping."""

    sql: TypeEngine[Any]
    python: NotRequired[Callable[..., Any]]


# Mapping specific column names to their appropriate types
COLUMNS_CAST: dict[str, ColumnType] = {
    "ParentFirst": {"sql": Boolean(), "python": bool},
    "UseIntervalArithmetics": {"sql": Boolean(), "python": bool},
    "StartDate": {"sql": DateTime()},
    "EndDate": {"sql": DateTime()},
}


def genericize_datatypes(
    _inspector: Inspector,
    _table_name: str,
    column_dict: ReflectedColumn,
) -> None:
    """Convert columns to generic types.

    This function is used as an event listener during metadata reflection
    to convert Access-specific types to more generic SQLAlchemy types.
    """
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
        column_dict["type"] = COLUMNS_CAST[column_dict["name"]]["sql"]
    else:
        column_dict["type"] = column_dict["type"].as_generic()


def transform_row_values(rows: list[dict[str, Any]]) -> None:
    """Transform row values to appropriate Python types."""
    for row in rows:
        for name, value in row.items():
            try:
                if name in COLUMNS_CAST:
                    if caster := COLUMNS_CAST[name].get("python"):
                        row[name] = caster(value)
                elif name.lower().endswith("date"):
                    row[name] = datetime.date.fromisoformat(value)
                elif name.lower().startswith(("is", "has")):
                    row[name] = bool(value)
            except (ValueError, TypeError):
                row[name] = None


def ensure_primary_keys_not_null(table: Table) -> None:
    """Ensure all primary key columns are set as NOT NULL."""
    for column in table.primary_key.columns:
        column.nullable = False


def remove_table_primary_key_index(table: Table) -> None:
    """Remove the primary key index from a table.

    This is due to SQLite maintaining a separate, hidden index for primary keys.
    """
    indexes_to_drop = [index for index in table.indexes if index.name == "PrimaryKey"]
    for index in indexes_to_drop:
        table.indexes.remove(index)


def identify_non_nullable_columns(
    conn: Connection,
    table: Table,
) -> set[str]:
    """Analyze which columns are nullable using SQL COUNT queries."""
    not_nullable_columns: set[str] = set()

    for column in table.columns:
        query = select(func.count()).where(column.is_(None))
        result = conn.execute(query).scalar()
        if not result:
            not_nullable_columns.add(column.name)

    return not_nullable_columns


def set_columns_not_null(table: Table, not_nullable_columns: set[str]) -> None:
    """Set nullable status for columns based on data analysis."""
    for column in table.columns:
        if column.name in not_nullable_columns:
            column.nullable = False
