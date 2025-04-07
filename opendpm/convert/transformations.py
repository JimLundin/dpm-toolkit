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
    ForeignKey,
    Inspector,
    Integer,
    Row,
    Table,
    Text,
)
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.types import TypeEngine

type FieldValue = str | int | bool | date | datetime | None
type TableRow = dict[str, FieldValue]
type TableData = list[TableRow]
type ColumnNames = set[str]
type ColumnEnumMap = dict[str, set[str]]
type Rows = Sequence[Row[Any]]


class ColumnType(TypedDict):
    """Column type mapping."""

    sql: TypeEngine[Any]
    python: NotRequired[Callable[[FieldValue], FieldValue]]


# Mapping specific column names to their appropriate types
COLUMN_TYPE_OVERRIDES: dict[str, ColumnType] = {
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
    return column.lower().endswith(
        (
            "type",
            "status",
            "sign",
            "optionality",
            "direction",
            "number",
            "endorsement",
            "source",
            "severity",
            "errorcode",
        ),
    )


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
    if column_name in COLUMN_TYPE_OVERRIDES:
        column_type = COLUMN_TYPE_OVERRIDES[column_name]["sql"]
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


def cast_value(column: str, value: FieldValue) -> FieldValue:
    """Transform row values to appropriate Python types."""
    if value is None:
        return None
    if column in COLUMN_TYPE_OVERRIDES and (
        caster := COLUMN_TYPE_OVERRIDES[column].get("python")
    ):
        return caster(value)
    if is_date(column) and isinstance(value, str):
        return date.fromisoformat(value)
    if is_bool(column):
        return bool(value)
    return value


def parse(table_rows: Rows) -> tuple[TableData, ColumnEnumMap, ColumnNames]:
    """Transform row values to appropriate Python types."""
    rows: TableData = []
    enums: ColumnEnumMap = defaultdict(set)
    nullables: ColumnNames = set()
    for table_row in table_rows:
        row = table_row._asdict()  # pyright: ignore[reportPrivateUsage]
        rows.append(row)
        for column in row:
            new_value = cast_value(column, row[column])
            row[column] = new_value
            if is_enum(column) and isinstance(new_value, str):
                enums[column].add(new_value)
            if new_value is None:
                nullables.add(column)

    return rows, enums, nullables


def apply_enums(table: Table, enums: ColumnEnumMap) -> None:
    """Set enum columns for a table."""
    for column in table.columns:
        if column.name in enums:
            column.type = Enum(*enums[column.name])


def mark_non_nullable(table: Table, nullables: ColumnNames) -> None:
    """Set nullable status for columns based on data analysis."""
    for column in table.columns:
        if column.name not in nullables:
            column.nullable = False


def remove_pk_index(table: Table) -> None:
    """Remove the primary key index from a table.

    This is due to SQLite maintaining a separate, hidden index for primary keys.
    """
    primary_key_indexes = [
        index for index in table.indexes if index.name == "PrimaryKey"
    ]
    for index in primary_key_indexes:
        table.indexes.remove(index)


def add_foreign_key(source_column: str, target_column: str, table: Table) -> None:
    """Set missing foreign keys."""
    column = table.columns.get(source_column)
    if column is None or column.foreign_keys:
        return
    column.append_foreign_key(ForeignKey(target_column))


def add_foreign_keys(table: Table) -> None:
    """Set missing foreign keys."""
    add_foreign_key("RowGUID", "Concept.ConceptGUID", table)
    add_foreign_key("ParentItemID", "Item.ItemID", table)
