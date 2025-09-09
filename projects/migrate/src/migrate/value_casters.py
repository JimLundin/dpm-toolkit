"""Value casting system for database migration transformations.

This module provides a simple, type-based system for casting raw database values
to their appropriate Python types based on SQLAlchemy type information.
"""

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime
from sqlalchemy.types import TypeEngine

# Type definition - duplicated to avoid circular import
Field = str | float | int | bool | date | datetime | None


def cast_boolean(raw_value: str) -> bool:
    """Cast string to boolean with database-friendly logic."""
    return raw_value.lower() in ("1", "true", "yes", "y")


def cast_date(raw_value: str) -> date | str:
    """Cast string to date with multiple format support, optimized for performance."""
    # Fast path: try ISO format first (most common in databases)
    try:
        return date.fromisoformat(raw_value)
    except ValueError:
        pass

    # Fallback: try other common formats
    fallback_formats = [
        "%d/%m/%Y",  # DD/MM/YYYY: 30/03/2026
        "%m/%d/%Y",  # MM/DD/YYYY: 03/30/2026
        "%d-%m-%Y",  # DD-MM-YYYY: 30-03-2026
        "%Y/%m/%d",  # YYYY/MM/DD: 2026/03/30
    ]

    for fmt in fallback_formats:
        try:
            return datetime.strptime(raw_value, fmt).astimezone().date()
        except ValueError:
            continue

    # Return original string if parsing fails
    return raw_value


def cast_datetime(raw_value: str) -> datetime | str:
    """Cast string to datetime with multiple format support, optimized."""
    # Fast path: try ISO formats first (most common in databases)
    try:
        return datetime.fromisoformat(raw_value)
    except ValueError:
        pass

    # Try ISO with space instead of T
    try:
        return datetime.fromisoformat(raw_value.replace(" ", "T"))
    except ValueError:
        pass

    # Fallback: try other common formats
    fallback_formats = [
        "%d/%m/%Y %H:%M:%S",  # DD/MM/YYYY HH:MM:SS
        "%m/%d/%Y %H:%M:%S",  # MM/DD/YYYY HH:MM:SS
        "%Y-%m-%d",  # Date only, time defaults to 00:00:00
        "%d/%m/%Y",  # DD/MM/YYYY, time defaults to 00:00:00
        "%m/%d/%Y",  # MM/DD/YYYY, time defaults to 00:00:00
    ]

    for fmt in fallback_formats:
        try:
            return datetime.strptime(raw_value, fmt).astimezone()
        except ValueError:
            continue

    # Return original string if parsing fails
    return raw_value


def cast_value_for_type(sql_type: TypeEngine[Field], raw_value: Field) -> Field:
    """Cast a raw value to match the SQLAlchemy type.

    Uses direct type dispatch - no looping or registry creation overhead.

    Args:
        sql_type: SQLAlchemy type to cast to
        raw_value: Raw value to cast

    Returns:
        Casted value or original value if no caster found

    """
    if not isinstance(raw_value, str):
        return raw_value

    # Boolean casting
    if isinstance(sql_type, Boolean):
        return cast_boolean(raw_value)

    # Date casting
    if isinstance(sql_type, Date):
        return cast_date(raw_value)

    # DateTime casting
    if isinstance(sql_type, DateTime):
        return cast_datetime(raw_value)

    return raw_value
