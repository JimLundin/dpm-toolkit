"""Database processing utilities for handling multiple Access databases."""

from pathlib import Path

from sqlalchemy import (
    Engine,
    Enum,
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

from migrate.transformations import (
    CastedRows,
    add_foreign_keys_to_table,
    parse_rows,
)
from migrate.type_registry import column_type

type TableWithRows = tuple[Table, CastedRows]
type TablesWithRows = list[TableWithRows]


def access(access_location: Path) -> Engine:
    """Get an engine to an Access database."""
    driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
    connection_string = f"DRIVER={driver};DBQ={access_location}"
    return create_engine(f"access+pyodbc:///?odbc_connect={connection_string}")


def genericize(_inspector: Inspector, _table: Table, column: ReflectedColumn) -> None:
    """Genericize for SQLAlchemy compatibility only."""
    column["type"] = column_type(column) or column["type"].as_generic()


def reflect_schema(source_database: Engine) -> MetaData:
    """Reflect a database schema with basic SQLAlchemy compatibility.

    No business logic type transformations - those are applied after data analysis.
    """
    schema = MetaData()
    event.listen(schema, "column_reflect", genericize)
    schema.reflect(bind=source_database)
    return schema


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

            # Process rows with registry-based logic
            casted_rows, enum_by_column, nullable_columns = parse_rows(table, rows)

            # Clear indexes to avoid name collisions and save space
            table.indexes.clear()
            # We are using non-integer primary keys, we disable rowid to save space
            if table.primary_key:
                table.kwargs["sqlite_with_rowid"] = False

            # Apply all transformations after data analysis
            for column, enum in enum_by_column.items():
                column.type = Enum(*enum)
                constraint = CheckConstraint(column.in_(enum))
                table.append_constraint(constraint)

            # Set columns that never had nulls to non-nullable
            for column in table.columns:
                column.nullable = column in nullable_columns

            add_foreign_keys_to_table(table)

            if casted_rows:
                tables_with_rows.append((table, casted_rows))

    return schema, tables_with_rows


def load_data_to_database(
    target_database: Engine,
    tables_with_rows: TablesWithRows,
) -> None:
    """Populate the target database with data from the source database."""
    with target_database.begin() as connection:
        for table, rows in tables_with_rows:
            connection.execute(insert(table), rows)
