"""Name-based column type detection for DPM databases.

DPM databases follow naming conventions that encode type information:
- Columns ending with "GUID" contain UUIDs
- Columns ending with "Date" contain dates
- Columns starting with "Is"/"Has" contain booleans
- Specific columns like "StartDate"/"EndDate" are datetimes

These conventions are lost when storing in SQLite (which has no UUID, Date,
or Boolean types), so we recover them by inspecting column names.
"""

from typing import Any

from sqlalchemy import Boolean, Date, DateTime, Uuid
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
