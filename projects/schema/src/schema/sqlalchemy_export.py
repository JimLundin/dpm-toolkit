"""SQLAlchemy code export functionality for database schemas."""

from sqlalchemy import Engine, Enum, Inspector, MetaData, Table, event
from sqlalchemy.engine.interfaces import ReflectedColumn

from schema.enum_detection import detect_enum_for_column
from schema.generation import Model


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


def schema_to_sqlalchemy(sqlite_database: Engine) -> str:
    """Generate SQLAlchemy schema from SQLite database."""
    schema = MetaData()

    # Set up reflection event listener to detect enums during reflection
    event.listen(schema, "column_reflect", enum_reflection)

    schema.reflect(bind=sqlite_database)

    return Model(schema).render()
