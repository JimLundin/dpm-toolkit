"""Schema transformation utilities for database conversion."""

import datetime
from collections import defaultdict
from collections.abc import Callable, Sequence
from typing import Any, NotRequired, TypedDict

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Enum,
    Inspector,
    Row,
    Table,
    Text,
)
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.types import TypeEngine

type Rows = list[dict[str, Any]]
type Columns = set[str]
type EnumMap = dict[str, set[str]]
type Data = Sequence[Row[Any]]


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
        column_dict["type"] = COLUMNS_CAST[column_dict["name"]]["sql"]
    else:
        column_dict["type"] = column_dict["type"].as_generic()


def cast_row_value(column: str, value: Any) -> Any:  # noqa: ANN401
    """Transform row values to appropriate Python types."""
    if value is None:
        return None
    if column in COLUMNS_CAST and (caster := COLUMNS_CAST[column].get("python")):
        return caster(value)
    if column.lower().endswith("date") and isinstance(value, str):
        return datetime.date.fromisoformat(value)
    if column.lower().startswith(("is", "has")):
        return bool(value)
    return value


def is_enum(column: str, value: Any) -> bool:  # noqa: ANN401
    """Check if a column is an enum column."""
    return isinstance(value, str) and column.endswith("Type")


def parse_data(data: Data) -> tuple[Rows, EnumMap, Columns]:
    """Transform row values to appropriate Python types."""
    rows: Rows = []
    enum_columns: EnumMap = defaultdict(set)
    nullable_columns: Columns = set()
    for row in data:
        dict_row = row._asdict()  # type: ignore private attribute
        rows.append(dict_row)
        for column in dict_row:
            new_value = cast_row_value(column, dict_row[column])
            dict_row[column] = new_value
            if is_enum(column, new_value):
                enum_columns[column].add(new_value)

            if new_value is None:
                nullable_columns.add(column)

    return rows, enum_columns, nullable_columns


def set_enum_columns(table: Table, enum_columns: EnumMap) -> None:
    """Set enum columns for a table."""
    for column in table.columns:
        if column.name in enum_columns:
            column.type = Enum(*enum_columns[column.name])


def set_null_columns(table: Table, nullable_columns: Columns) -> None:
    """Set nullable status for columns based on data analysis."""
    for column in table.columns:
        if column.name not in nullable_columns:
            column.nullable = False


def remove_pk_index(table: Table) -> None:
    """Remove the primary key index from a table.

    This is due to SQLite maintaining a separate, hidden index for primary keys.
    """
    indexes_to_drop = [index for index in table.indexes if index.name == "PrimaryKey"]
    for index in indexes_to_drop:
        table.indexes.remove(index)
