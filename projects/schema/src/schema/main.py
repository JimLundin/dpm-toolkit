"""Main module for schema generation."""

from pathlib import Path

from sqlalchemy import Engine, Enum, Inspector, MetaData, Table, create_engine, event
from sqlalchemy.engine.interfaces import ReflectedColumn

from schema.enum_detection import detect_enum_for_column
from schema.generation import Model


def read_only_sqlite(sqlite_location: Path) -> Engine:
    """Create a read-only SQLAlchemy engine for SQLite database."""
    connection_string = f"sqlite:///{sqlite_location}?mode=ro"
    return create_engine(connection_string, connect_args={"uri": True})


def enum_reflection(
    inspector: Inspector,
    table: Table,
    column_info: ReflectedColumn,
) -> None:
    """Event listener to enhance column reflection with enum detection."""
    column_name = column_info["name"]

    # Get check constraints for this table and check if any apply to this column
    check_constraints = inspector.get_check_constraints(table.name)

    for constraint in check_constraints:
        constraint_text = constraint["sqltext"]
        if enum_values := detect_enum_for_column(constraint_text, column_name):
            column_info["type"] = Enum(*enum_values)
            break  # Found enum for this column, no need to check other constraints


def sqlite_to_sqlalchemy_schema(sqlite_database: Engine) -> str:
    """Generate SQLAlchemy schema from migrated SQLite database."""
    schema = MetaData()

    # Set up reflection event listener to detect enums during reflection

    event.listen(schema, "column_reflect", enum_reflection)

    schema.reflect(bind=sqlite_database)

    return Model(schema).render()
