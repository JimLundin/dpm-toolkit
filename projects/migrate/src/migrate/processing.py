"""Database processing utilities for handling multiple Access databases."""

from pathlib import Path

from sqlalchemy import Engine, MetaData, Table, create_engine, event, insert, select

from migrate.transformations import (
    TransformedRows,
    add_foreign_keys_to_table,
    apply_enums_to_table,
    genericize,
    parse_rows,
    set_table_nullable_column_names,
)

type TableWithRows = tuple[Table, TransformedRows]
type TablesWithRows = list[TableWithRows]


def access_engine(database_location: Path) -> Engine:
    """Get an engine to an Access database."""
    driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
    connection_string = f"DRIVER={driver};DBQ={database_location}"
    return create_engine(f"access+pyodbc:///?odbc_connect={connection_string}")


def reflect_schema(source_database: Engine) -> MetaData:
    """Reflect a database schema."""
    schema = MetaData()
    event.listen(schema, "column_reflect", genericize)
    schema.reflect(bind=source_database)
    return schema


def schema_and_data(source_database: Engine) -> tuple[MetaData, TablesWithRows]:
    """Extract data and schema from a single Access database.

    Args:
        source: Engine to the source Access database

    Returns:
        MetaData: Database metadata
        TableData: Table rows

    """
    schema = reflect_schema(source_database)

    tables_with_rows: TablesWithRows = []
    with source_database.begin() as connection:
        for table in schema.tables.values():
            rows = connection.execute(select(table))
            transformed_rows, enum_by_column_name, nullable_column_names = parse_rows(
                rows
            )

            # Clear indexes to avoid name collisions and save space
            table.indexes.clear()
            # We are using non-integer primary keys, we disable rowid to save space
            if table.primary_key:
                table.kwargs["sqlite_with_rowid"] = False

            apply_enums_to_table(table, enum_by_column_name)
            set_table_nullable_column_names(table, nullable_column_names)
            add_foreign_keys_to_table(table)

            if transformed_rows:
                tables_with_rows.append((table, transformed_rows))

    return schema, tables_with_rows


def load_data_into_database(
    target_database: Engine,
    tables_with_rows: TablesWithRows,
) -> None:
    """Populate the target database with data from the source database."""
    with target_database.begin() as connection:
        for table, rows in tables_with_rows:
            connection.execute(insert(table), rows)
