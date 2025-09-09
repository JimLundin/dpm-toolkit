"""Schema transformation utilities for database conversion."""

from collections import defaultdict
from collections.abc import Iterator
from typing import Any

from sqlalchemy import (
    Column,
    Engine,
    Enum,
    ForeignKey,
    Inspector,
    MetaData,
    Row,
    String,
    Table,
    event,
)
from sqlalchemy.engine.interfaces import ReflectedColumn

from migrate.type_registry import column_type
from migrate.value_casters import Field, value_caster

type CastedRow = dict[str, Field]
type CastedRows = list[CastedRow]
type Columns = set[Column[Any]]
type EnumByColumn = dict[Column[Any], set[str]]


def genericize(
    _inspector: Inspector,
    _table_name: str,
    column: ReflectedColumn,
) -> None:
    """Genericize for SQLAlchemy compatibility only."""
    registry_type = column_type(column)
    if isinstance(registry_type, Enum):
        column["type"] = column["type"].as_generic()
    column["type"] = registry_type.as_generic()


def reflect_schema(source_database: Engine) -> MetaData:
    """Reflect a database schema with basic SQLAlchemy compatibility.

    No business logic type transformations - those are applied after data analysis.
    """
    schema = MetaData()
    event.listen(schema, "column_reflect", genericize)
    schema.reflect(bind=source_database)
    return schema


def parse_rows(
    table: Table,
    rows: Iterator[Row[Any]],
) -> tuple[CastedRows, EnumByColumn, Columns]:
    """Transform row values using TypeRegistry and collect analysis data."""
    casted_rows: CastedRows = []
    enum_by_column: EnumByColumn = defaultdict(set)
    nullable_columns: Columns = set()

    # Pre-compute column types and casters for performance (don't lookup per row)
    type_by_column = {column: column_type(column) for column in table.columns}
    caster_by_column = {
        column: value_caster(type_by_column[column]) for column in table.columns
    }

    for row in rows:
        casted_row = row._asdict()  # pyright: ignore[reportPrivateUsage]

        for column_name, raw_value in casted_row.items():
            table_column = table.columns[column_name]

            if raw_value is None:
                nullable_columns.add(table_column)
                casted_row[column_name] = None
                continue

            # Get pre-computed type for this column
            registry_type = type_by_column[table_column]

            if isinstance(registry_type, Enum):
                # This is an enum candidate - collect raw values for later analysis
                if isinstance(raw_value, str):
                    enum_by_column[table_column].add(raw_value)
                # Keep raw value for enum candidates
                continue

            # Cast using pre-computed caster function
            casted_row[column_name] = caster_by_column[table_column](raw_value)

        casted_rows.append(casted_row)

    return casted_rows, enum_by_column, nullable_columns


def apply_types_to_table(table: Table) -> None:
    """Apply type corrections from registry to table schema.

    This applies all business logic type transformations after data analysis.
    """
    for column in table.columns:
        registry_type = column_type(column)
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
