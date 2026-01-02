"""Type registry for database migration transformations.

This module provides simple, direct column type mapping for database migration.
Rules are checked in order with early return - no complex priority system needed.
"""

from typing import Any

from sqlalchemy import Boolean, Column, Date, DateTime, String, Uuid
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.types import TypeEngine


def column_type(column: ReflectedColumn) -> TypeEngine[Any] | None:
    """Get SQL type for column using direct rule matching.

    Rules are checked in order from most specific to least specific.
    First match wins - simple and predictable.

    Args:
        column: Column to analyze

    Returns:
        SQLAlchemy type if match found, None otherwise

    """
    name = column["name"]
    name_lower = name.lower()
    data_type: TypeEngine[Any] | None = None

    # Exact column name overrides (most specific)
    if name in ("ParentFirst", "UseIntervalArithmetics"):
        data_type = Boolean()
    elif name in ("StartDate", "EndDate"):
        data_type = DateTime()

    # Prefix patterns
    elif name_lower.startswith(("is", "has")):
        data_type = Boolean()

    # Suffix patterns
    elif name_lower.endswith("guid"):
        data_type = Uuid()
    elif name_lower.endswith("date"):
        data_type = Date()

    return data_type


def enum_type(column: Column[Any]) -> bool:
    """Identify if the column should be cast as an Enum."""
    return isinstance(column.type, String) and column.name.lower().endswith(
        (
            "type",
            "status",
            "sign",
            "optionality",
            "direction",
            "number",
            "endorsement",
            "source",
            "severity",
            "errorcode",
            "operandreference",
        ),
    )
