"""Database processing utilities for handling multiple Access databases.

Two readers produce the same ``(MetaData, TablesWithRows)`` pair:

* :func:`schema_and_data` reads through ``access+pyodbc``, which needs the
  Microsoft Access ODBC driver and so runs on Windows only.
* :func:`schema_and_data_from_file` reads through GNU mdbtools, which runs
  anywhere mdbtools is packaged.

Everything after the reader — enum detection, data-derived nullability,
naming-convention foreign keys — is shared, in :func:`normalize_table` and
:func:`heal_cross_table_foreign_keys`.
"""

from collections.abc import Iterable
from pathlib import Path

from sqlalchemy import (
    Engine,
    Inspector,
    MetaData,
    Table,
    create_engine,
    event,
    insert,
    select,
)
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.schema import CheckConstraint

from .mdbtools import read_schema_and_rows
from .transformations import (
    Row_,
    Rows,
    add_foreign_keys_to_table,
    heal_cross_table_foreign_keys,
    parse_rows,
)

type TableWithRows = tuple[Table, Rows]
type TablesWithRows = list[TableWithRows]


def access(access_location: Path) -> Engine:
    """Get an engine to an Access database."""
    driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
    connection_string = f"DRIVER={driver};DBQ={access_location}"
    return create_engine(f"access+pyodbc:///?odbc_connect={connection_string}")


def genericize(_inspector: Inspector, _table: Table, column: ReflectedColumn) -> None:
    """Convert Access-specific types to generic SQLAlchemy types."""
    column["type"] = column["type"].as_generic()


def reflect_schema(source_database: Engine) -> MetaData:
    """Reflect a database schema with basic SQLAlchemy compatibility.

    No business logic type transformations - those are applied after data analysis.
    """
    schema = MetaData()
    event.listen(schema, "column_reflect", genericize)
    schema.reflect(bind=source_database)
    return schema


def normalize_table(table: Table, rows: Iterable[Row_]) -> Rows:
    """Analyze rows, then apply the schema transformations they imply.

    Shared by both readers, and unaware of where the rows came from.

    Args:
        table: Table to transform in place.
        rows: Rows belonging to *table*.

    Returns:
        The rows, materialized.

    """
    parsed_rows, enum_by_column, nullable_columns = parse_rows(table, rows)

    # Clear indexes to avoid name collisions and save space
    table.indexes.clear()
    # We are using non-integer primary keys, we disable rowid to save space
    if table.primary_key:
        table.kwargs["sqlite_with_rowid"] = False

    # Apply all transformations after data analysis
    for column, enum in enum_by_column.items():
        table.append_constraint(CheckConstraint(column.in_(enum)))

    # Set columns that never had nulls to non-nullable
    for column in table.columns:
        column.nullable = column in nullable_columns

    add_foreign_keys_to_table(table)

    return parsed_rows


def schema_and_data(access_database: Engine) -> tuple[MetaData, TablesWithRows]:
    """Extract data and schema from a single Access database.

    Args:
        access_database: Engine to the source Access database

    Returns:
        MetaData: Database metadata
        TablesWithRows: Table rows

    """
    schema = reflect_schema(access_database)

    tables_with_rows: TablesWithRows = []
    with access_database.begin() as connection:
        for table in schema.tables.values():
            rows = connection.execute(select(table))
            parsed_rows = normalize_table(
                table,
                (row._asdict() for row in rows),  # pyright: ignore[reportPrivateUsage]
            )

            if parsed_rows:
                tables_with_rows.append((table, parsed_rows))

    heal_cross_table_foreign_keys(schema)

    return schema, tables_with_rows


def schema_and_data_from_file(
    access_location: Path,
) -> tuple[MetaData, TablesWithRows]:
    """Extract data and schema from an Access file using mdbtools.

    The cross-platform counterpart to :func:`schema_and_data`. Produces the
    same pair, so callers downstream cannot tell which reader ran.

    Args:
        access_location: Path to the ``.accdb`` or ``.mdb`` file.

    Returns:
        MetaData: Database metadata
        TablesWithRows: Table rows

    """
    schema, rows_by_table = read_schema_and_rows(access_location)

    tables_with_rows: TablesWithRows = []
    for name, table in schema.tables.items():
        if parsed_rows := normalize_table(table, rows_by_table[name]):
            tables_with_rows.append((table, parsed_rows))

    heal_cross_table_foreign_keys(schema)

    return schema, tables_with_rows


def load_data_to_database(
    target_database: Engine,
    tables_with_rows: TablesWithRows,
) -> None:
    """Populate the target database with data from the source database."""
    with target_database.begin() as connection:
        for table, rows in tables_with_rows:
            connection.execute(insert(table), rows)
