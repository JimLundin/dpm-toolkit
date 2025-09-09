"""Type registry for database migration transformations.

This module provides simple, direct column type mapping for database migration.
Rules are checked in order with early return - no complex priority system needed.
"""

from typing import Any

from sqlalchemy import Boolean, Column, Date, DateTime, Enum, String, Uuid
from sqlalchemy.types import TypeEngine


def column_type(column: Column[Any]) -> TypeEngine[Any]:
    """Get SQL type for column using direct rule matching.

    Rules are checked in order from most specific to least specific.
    First match wins - simple and predictable.

    Args:
        column: Column to analyze

    Returns:
        SQLAlchemy type if match found, None otherwise

    """
    name = column.name
    name_lower = name.lower()

    data_type = column.type

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
        return Uuid()
    elif name_lower.endswith("date"):
        return Date()

    elif isinstance(data_type, String) and name_lower.endswith(
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
        ),
    ):
        return Enum()

    # No match found
    return data_type
