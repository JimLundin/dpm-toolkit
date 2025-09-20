"""Main module for unified schema generation."""

from pathlib import Path

from sqlalchemy import Engine, Inspector, create_engine, inspect
from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedForeignKeyConstraint

from schema.enum_detection import detect_enum_for_column
from schema.type_conversion import sql_to_data_type
from schema.types import (
    ColumnMapping,
    ColumnSchema,
    DatabaseSchema,
    EnumType,
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
    data_type = sql_to_data_type(col_info["type"])

    for constraint in inspector.get_check_constraints(table_name):
        if values := detect_enum_for_column(constraint["sqltext"], col_info["name"]):
            data_type = EnumType(type="enum", values=values)
            break

    return {
        "name": col_info["name"],
        "type": data_type,
        "nullable": col_info["nullable"],
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
    table_names = (
        table_name
        for table_name, _fkc in inspector.get_sorted_table_and_fkc_names()
        if table_name
    )

    return {
        "name": sqlite_database.url.database or "unknown",
        "tables": [_build_table(inspector, table_name) for table_name in table_names],
    }
