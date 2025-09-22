"""Main module for unified schema generation."""

from pathlib import Path
from typing import Any
from warnings import catch_warnings, filterwarnings

from sqlalchemy import Engine, Enum, Inspector, MetaData, create_engine, event
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.exc import SAWarning
from sqlalchemy.schema import Column, Table

from schema.enum_detection import detect_enum_for_column
from schema.type_conversion import sql_to_data_type
from schema.types import ColumnSchema, DatabaseSchema, ForeignKeySchema, TableSchema


def detect_enum(inspector: Inspector, table: Table, column: ReflectedColumn) -> None:
    """Event handler to detect enum types during reflection."""
    # Check for enum constraints for this column
    for constraint in inspector.get_check_constraints(table.name):
        if values := detect_enum_for_column(constraint["sqltext"], column["name"]):
            # Replace the column type with our enum type representation
            column["type"] = Enum(*values)
            break


def read_only_sqlite(sqlite_location: Path) -> Engine:
    """Create a read-only SQLAlchemy engine for SQLite database."""
    connection_string = f"sqlite:///{sqlite_location}?mode=ro"
    return create_engine(connection_string, connect_args={"uri": True})


def _column_from_sqla(
    column: Column[Any],
    *,
    foreign_keys: bool = True,
) -> ColumnSchema:
    """Derive ColumnSchema from SQLAlchemy Column object."""
    # Build foreign key targets without recursion to avoid circular references
    return {
        "name": column.name,
        "table_name": column.table.name,
        "type": sql_to_data_type(column.type),
        "nullable": bool(column.nullable),
        "primary_key": column.primary_key,
        "foreign_keys": (
            [
                _column_from_sqla(fk.column, foreign_keys=False)
                for fk in column.foreign_keys
            ]
            if foreign_keys
            else []
        ),
    }


def _foreign_key_from_sqla(
    source_col: Column[Any],
    target_col: Column[Any],
) -> ForeignKeySchema:
    """Derive ForeignKey from SQLAlchemy source and target columns."""
    return {
        "source": _column_from_sqla(source_col),
        "target": _column_from_sqla(target_col),
    }


def _table_from_sqla(table: Table) -> TableSchema:
    """Derive TableSchema from SQLAlchemy Table object."""
    return {
        "name": table.name,
        "primary_keys": table.primary_key.columns.keys(),
        "columns": [_column_from_sqla(col) for col in table.columns],
        "foreign_keys": [
            _foreign_key_from_sqla(column, fk.column)
            for column in table.columns
            for fk in column.foreign_keys
        ],
    }


def reflect_tables(sqlite_database: Engine) -> list[Table]:
    """Reflect database schema from SQLite database using metadata reflection."""
    metadata = MetaData()
    event.listen(metadata, "column_reflect", detect_enum)
    metadata.reflect(bind=sqlite_database)
    with catch_warnings():
        filterwarnings("ignore", category=SAWarning)
        return metadata.sorted_tables


def sqlite_to_schema(sqlite_database: Engine) -> DatabaseSchema:
    """Generate unified schema data from SQLite database using metadata reflection."""
    tables = reflect_tables(sqlite_database)

    return {
        "name": sqlite_database.url.database or "unknown",
        "tables": [_table_from_sqla(table) for table in tables],
    }
