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

from migrate.value_casters import Field, value_caster

type CastedRow = dict[str, Field]
type CastedRows = list[CastedRow]
type Columns = set[Column[Any]]
type EnumByColumn = dict[Column[Any], set[str]]


def parse_rows(
    table: Table,
    rows: Iterator[Row[Any]],
) -> tuple[CastedRows, EnumByColumn, Columns]:
    """Transform row values using TypeRegistry and collect analysis data."""
    casted_rows: CastedRows = []
    enum_by_column: EnumByColumn = defaultdict(set)
    nullable_columns: Columns = set()

    # Pre-compute column types and casters for performance (don't lookup per row)
    caster_by_column = {column: value_caster(column.type) for column in table.columns}

    for row in rows:
        casted_row = row._asdict()  # pyright: ignore[reportPrivateUsage]

        for column_name, raw_value in casted_row.items():
            table_column = table.columns[column_name]

            if raw_value is None:
                nullable_columns.add(table_column)
                continue

            # Get pre-computed type for this column
            if isinstance(table_column.type, Enum):
                # This is an enum candidate - collect raw values for later analysis
                enum_by_column[table_column].add(raw_value)
                continue

            # Cast using pre-computed caster function
            casted_row[column_name] = caster_by_column[table_column](raw_value)

        casted_rows.append(casted_row)

    return casted_rows, enum_by_column, nullable_columns


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
