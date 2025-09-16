"""Main module for unified schema generation."""

from pathlib import Path

from sqlalchemy import Engine, Inspector, create_engine, inspect
from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedForeignKeyConstraint

from schema.enum_detection import detect_enum_for_column
from schema.types import (
    ColumnMapping,
    ColumnSchema,
    DatabaseSchema,
    ForeignKeySchema,
    TableSchema,
)


def read_only_sqlite(sqlite_location: Path) -> Engine:
    """Create a read-only SQLAlchemy engine for SQLite database."""
    connection_string = f"sqlite:///{sqlite_location}?mode=ro"
    return create_engine(connection_string, connect_args={"uri": True})


def _build_column(
    col_info: ReflectedColumn,
    table_name: str,
    inspector: Inspector,
) -> ColumnSchema:
    """Build a column schema with enum detection support."""
    # Check for enum values using existing enum detection
    enum_values = None
    check_constraints = inspector.get_check_constraints(table_name)

    for constraint in check_constraints:
        constraint_text = constraint["sqltext"]
        if detected_values := detect_enum_for_column(constraint_text, col_info["name"]):
            enum_values = detected_values
            break

    return {
        "name": col_info["name"],
        "type": str(col_info["type"]),
        "nullable": col_info["nullable"],
        "enum_values": enum_values,
    }


def _build_foreign_key(fk: ReflectedForeignKeyConstraint) -> ForeignKeySchema:
    """Build a foreign key schema from SQLAlchemy foreign key info."""
    column_mappings: list[ColumnMapping] = [
        {"source_column": source_col, "target_column": target_col}
        for source_col, target_col in zip(
            fk["constrained_columns"],
            fk["referred_columns"],
            strict=True,
        )
    ]
    return {
        "target_table": fk["referred_table"],
        "column_mappings": column_mappings,
    }


def _build_table(inspector: Inspector, table_name: str) -> TableSchema:
    """Build a table schema from database introspection."""
    columns_info = inspector.get_columns(table_name)
    pk_constraint = inspector.get_pk_constraint(table_name)
    foreign_keys = inspector.get_foreign_keys(table_name)

    return {
        "name": table_name,
        "columns": [
            _build_column(col_info, table_name, inspector) for col_info in columns_info
        ],
        "primary_keys": pk_constraint["constrained_columns"],
        "foreign_keys": [_build_foreign_key(fk) for fk in foreign_keys],
    }


def sqlite_to_schema(sqlite_database: Engine) -> DatabaseSchema:
    """Generate unified schema data from SQLite database."""
    inspector = inspect(sqlite_database)
    table_names = inspector.get_table_names()

    return {
        "name": str(sqlite_database.url.database),
        "tables": [_build_table(inspector, table_name) for table_name in table_names],
    }
