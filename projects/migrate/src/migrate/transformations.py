"""Schema transformation utilities for database conversion."""

from collections import defaultdict
from collections.abc import Callable, Iterator
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
    Uuid,
)
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.types import TypeEngine

type FieldValue = str | int | bool | date | datetime | None
type CastedRow = dict[str, FieldValue]
type CastedRows = list[CastedRow]
type ColumnNames = set[str]
type EnumByColumnName = dict[str, set[str]]


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


def is_guid(column_name: str) -> bool:
    """Check if a column is a GUID column."""
    return column_name.lower().endswith("guid")


def is_bool(column_name: str) -> bool:
    """Check if a column is a boolean column."""
    return column_name.lower().startswith(("is", "has"))


def is_date(column_name: str) -> bool:
    """Check if a column is a date column."""
    return column_name.lower().endswith("date")


def is_enum(column_name: str) -> bool:
    """Check if a column is an enum column."""
    return column_name.lower().endswith(
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
        column_type = Uuid()
    elif is_date(column_name):
        column_type = Date()
    elif is_bool(column_name):
        column_type = Boolean()
    elif isinstance(column_type, Integer):
        column_type = Integer()
    else:
        column_type = column_type.as_generic()

    column["type"] = column_type


def cast_value(column_name: str, value: FieldValue) -> FieldValue:
    """Transform row values to appropriate Python types."""
    if value is None:
        return None
    if column_name in COLUMN_TYPE_OVERRIDES and (
        caster := COLUMN_TYPE_OVERRIDES[column_name].get("python")
    ):
        return caster(value)
    if is_date(column_name) and isinstance(value, str):
        return date.fromisoformat(value)
    if is_bool(column_name):
        return bool(value)
    return value


def parse_rows(rows: Iterator[Row]) -> tuple[CastedRows, EnumByColumnName, ColumnNames]:
    """Transform row values to appropriate Python types."""
    casted_rows: CastedRows = []
    enum_by_column_name: EnumByColumnName = defaultdict(set)
    nullable_column_names: ColumnNames = set()
    for row in rows:
        casted_row = row._asdict()  # pyright: ignore[reportPrivateUsage]
        for column_name in casted_row:
            casted_value = cast_value(column_name, casted_row[column_name])
            casted_row[column_name] = casted_value
            if casted_value is None:
                nullable_column_names.add(column_name)
            elif is_enum(column_name):
                enum_by_column_name[column_name].add(casted_value)

        casted_rows.append(casted_row)

    return casted_rows, enum_by_column_name, nullable_column_names


def apply_enums_to_table(table: Table, enum_by_column_name: EnumByColumnName) -> None:
    """Set enum columns for a table."""
    for column in table.columns:
        if column.name in enum_by_column_name:
            column.type = Enum(*enum_by_column_name[column.name])


def mark_nullable_columns_in_table(
    table: Table,
    nullable_column_names: ColumnNames,
) -> None:
    """Set nullable status for columns based on data analysis."""
    for column in table.columns:
        if column.name not in nullable_column_names:
            column.nullable = False


def add_foreign_key_to_table(
    table: Table,
    source_column_name: str,
    target_relationship: str,
) -> None:
    """Set missing foreign keys."""
    column = table.columns.get(source_column_name)
    if column is None or column.foreign_keys:
        return
    column.append_foreign_key(ForeignKey(target_relationship))


def add_foreign_keys_to_table(table: Table) -> None:
    """Set missing foreign keys."""
    add_foreign_key_to_table(table, "RowGUID", "Concept.ConceptGUID")
    add_foreign_key_to_table(table, "ParentItemID", "Item.ItemID")
