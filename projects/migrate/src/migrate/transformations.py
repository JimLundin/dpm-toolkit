"""Schema transformation utilities for database conversion."""

from collections import defaultdict
from collections.abc import Iterator
from typing import Any

from sqlalchemy import (
    Column,
    Enum,
    ForeignKey,
    Row,
    Table,
)

from migrate.type_registry import TypeRegistry
from migrate.value_casters import Field, cast_value_for_type

type CastedRow = dict[str, Field]
type CastedRows = list[CastedRow]
type Columns = set[Column[Any]]
type EnumByColumn = dict[Column[Any], set[str]]


def parse_rows(
    table: Table,
    rows: Iterator[Row[Any]],
    registry: TypeRegistry,
) -> tuple[CastedRows, EnumByColumn, Columns]:
    """Transform row values using TypeRegistry and collect analysis data."""
    casted_rows: CastedRows = []
    enum_by_column: EnumByColumn = defaultdict(set)
    nullable_columns: Columns = set()

    column_types = {column: registry.column_type(column) for column in table.columns}

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
            registry_type = column_types[table_column]

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
                # Cast using the registry type
                casted_value = cast_value_for_type(registry_type, raw_value)

            casted_row[column_name] = casted_value

        casted_rows.append(casted_row)

    return casted_rows, enum_by_column, nullable_columns


def apply_types_to_table(table: Table, registry: TypeRegistry) -> None:
    """Apply type corrections from registry to table schema.

    This applies all business logic type transformations after data analysis.
    """
    for column in table.columns:
        registry_type = registry.column_type(column)
        if registry_type is not None:
            if isinstance(registry_type, Enum):
                continue
            column.type = registry_type


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
