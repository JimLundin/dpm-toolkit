"""Schema transformation utilities for database conversion."""

from collections import defaultdict
from collections.abc import Iterator
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Column,
    Enum,
    ForeignKey,
    Inspector,
    Integer,
    Row,
    Table,
)
from sqlalchemy.engine.interfaces import ReflectedColumn

from migrate.type_registry import TypeRegistry

type FieldValue = str | int | bool | date | datetime | None
type CastedRow = dict[str, FieldValue]
type CastedRows = list[CastedRow]
type Columns = set[Column[Any]]
type EnumByColumn = dict[Column[Any], set[str]]


def genericize(
    _inspector: Inspector,
    _table_name: str,
    column: ReflectedColumn,
) -> None:
    """Genericize for SQLAlchemy compatibility only."""
    column_type = column["type"]
    if isinstance(column_type, Integer):
        column["type"] = Integer()
    else:
        column["type"] = column_type.as_generic()


def parse_rows(
    table: Table,
    rows: Iterator[Row[Any]],
    registry: TypeRegistry,
) -> tuple[CastedRows, EnumByColumn, Columns]:
    """Transform row values using TypeRegistry and collect analysis data."""
    casted_rows: CastedRows = []
    enum_by_column: EnumByColumn = defaultdict(set)
    nullable_columns: Columns = set()

    for row in rows:
        casted_row = row._asdict()  # pyright: ignore[reportPrivateUsage]

        for column_name, raw_value in casted_row.items():
            # Get the table column for registry lookup
            table_column = table.columns[column_name]

            if raw_value is None:
                casted_value = None
                nullable_columns.add(table_column)
                continue
            # Get type from registry
            registry_type = registry.get_sql_type(table_column)

            if registry_type is None:
                casted_value = raw_value
                continue
            if isinstance(registry_type, Enum):
                # This is an enum candidate - collect raw values
                if not isinstance(raw_value, str):
                    continue
                enum_by_column[table_column].add(raw_value)
                casted_value = raw_value
            else:
                # Let SQLAlchemy handle the casting based on current column type
                # For now, just pass through - SQLAlchemy will handle conversion
                casted_value = registry_type.python_type(raw_value)

            casted_row[column_name] = casted_value

        casted_rows.append(casted_row)

    return casted_rows, enum_by_column, nullable_columns


def apply_enums_to_table(table: Table, enum_by_column: EnumByColumn) -> None:
    """Set enum columns for a table."""
    for column in table.columns:
        if column.name in enum_by_column:
            column.type = Enum(*enum_by_column[column], create_constraint=True)


def mark_nullable_columns_in_table(
    table: Table,
    nullable_columns: Columns,
) -> None:
    """Set nullable status for columns based on data analysis."""
    for column in table.columns:
        if column.name not in nullable_columns:
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


def apply_types_to_table(table: Table, registry: TypeRegistry) -> None:
    """Apply type corrections from registry to table schema.

    This applies all business logic type transformations after data analysis.
    """
    for column in table.columns:
        registry_type = registry.get_sql_type(column)
        if registry_type is not None:
            # Don't override enum types that have already been set with actual values
            if isinstance(registry_type, Enum):
                # Keep the enum with actual values
                continue
            # Apply the registry type
            column.type = registry_type
