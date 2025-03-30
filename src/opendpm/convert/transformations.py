"""Schema transformation utilities for database conversion."""

from collections import defaultdict
from collections.abc import Callable, Sequence
from datetime import date, datetime
from typing import Any, NotRequired, TypedDict

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Enum,
    Inspector,
    Integer,
    Row,
    Table,
    Text,
)
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.types import TypeEngine

type Value = str | int | bool | date | datetime | None
type Rows = list[dict[str, Value]]
type Columns = set[str]
type EnumMap = dict[str, set[str]]
type Data = Sequence[Row[Any]]


class ColumnType(TypedDict):
    """Column type mapping."""

    sql: TypeEngine[Any]
    python: NotRequired[Callable[[Value], Value]]


# Mapping specific column names to their appropriate types
COLUMN_CASTS: dict[str, ColumnType] = {
    "ParentFirst": {"sql": Boolean(), "python": bool},
    "UseIntervalArithmetics": {"sql": Boolean(), "python": bool},
    "StartDate": {"sql": DateTime()},
    "EndDate": {"sql": DateTime()},
}


def is_guid(column: str) -> bool:
    """Check if a column is a GUID column."""
    return column.lower().endswith("guid")


def is_bool(column: str) -> bool:
    """Check if a column is a boolean column."""
    return column.lower().startswith(("is", "has"))


def is_date(column: str) -> bool:
    """Check if a column is a date column."""
    return column.lower().endswith("date")


def is_enum(column: str) -> bool:
    """Check if a column is an enum column."""
    return column.lower().endswith(("type", "status", "sign", "optionality"))


def genericize(_i: Inspector, _t: str, column: ReflectedColumn) -> None:
    """Refine column datatypes during database reflection.

    This function enhances SQLAlchemy's type reflection by analyzing column names
    and applying more appropriate data types. The source database often uses generic
    types (like Integer or String) for specialized data such as GUIDs, dates, and
    booleans, which this function corrects based on naming conventions and known
    column characteristics.
    """
    column_name = column["name"]
    column_type = column["type"]
    if column_name in COLUMN_CASTS:
        column_type = COLUMN_CASTS[column_name]["sql"]
    elif is_guid(column_name):
        column_type = Text()
    elif is_date(column_name):
        column_type = Date()
    elif is_bool(column_name):
        column_type = Boolean()
    elif isinstance(column_type, Integer):
        column_type = Integer()
    else:
        column_type = column_type.as_generic()

    column["type"] = column_type


def cast_row_value(column: str, value: Value) -> Value:
    """Transform row values to appropriate Python types."""
    if value is None:
        return None
    if column in COLUMN_CASTS and (caster := COLUMN_CASTS[column].get("python")):
        return caster(value)
    if is_date(column) and isinstance(value, str):
        return date.fromisoformat(value)
    if is_bool(column):
        return bool(value)
    return value


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
            if is_enum(column) and isinstance(new_value, str):
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
