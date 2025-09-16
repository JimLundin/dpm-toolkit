"""Main module for ER diagram generation."""

from pathlib import Path

from sqlalchemy import Engine, Inspector, create_engine, inspect
from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedForeignKeyConstraint

from diagram.schema_types import (
    ColumnSchema,
    DiagramSchema,
    ForeignKeySchema,
    TableSchema,
)


def read_only_sqlite(sqlite_location: Path) -> Engine:
    """Create a read-only SQLAlchemy engine for SQLite database."""
    connection_string = f"sqlite:///{sqlite_location}?mode=ro"
    return create_engine(connection_string, connect_args={"uri": True})


def _build_column(col_info: ReflectedColumn) -> ColumnSchema:
    """Build a column schema from SQLAlchemy column info."""
    return {
        "name": col_info["name"],
        "type": str(col_info["type"]),
        "nullable": col_info["nullable"],
    }


def _build_foreign_key(fk: ReflectedForeignKeyConstraint) -> ForeignKeySchema:
    """Build a foreign key schema from SQLAlchemy foreign key info."""
    return {
        "target_table": fk["referred_table"],
        "column_mappings": [
            {
                "source_column": source_col,
                "target_column": target_col,
            }
            for source_col, target_col in zip(
                fk["constrained_columns"],
                fk["referred_columns"],
                strict=True,
            )
        ],
    }


def _build_table(inspector: Inspector, table_name: str) -> TableSchema:
    """Build a table schema from database introspection."""
    columns_info = inspector.get_columns(table_name)
    pk_constraint = inspector.get_pk_constraint(table_name)
    foreign_keys = inspector.get_foreign_keys(table_name)

    return {
        "name": table_name,
        "columns": [_build_column(col_info) for col_info in columns_info],
        "primary_key": pk_constraint["constrained_columns"],
        "foreign_keys": [_build_foreign_key(fk) for fk in foreign_keys],
    }


def sqlite_to_diagram(sqlite_database: Engine) -> DiagramSchema:
    """Generate ER diagram JSON from SQLite database."""
    inspector = inspect(sqlite_database)
    table_names = inspector.get_table_names()

    # Build complete diagram using functional approach
    return {
        "name": str(sqlite_database.url.database),
        "tables": [_build_table(inspector, table_name) for table_name in table_names],
    }
