"""Value casting system for database migration transformations.

This module provides a simple, type-based system for casting raw database values
to their appropriate Python types based on SQLAlchemy type information.
"""

from collections.abc import Callable
from datetime import date, datetime
from uuid import UUID

from sqlalchemy import Boolean, Date, DateTime, Uuid
from sqlalchemy.types import TypeEngine

# Type definition - duplicated to avoid circular import
Field = str | float | int | bool | date | datetime | UUID | None


def cast_boolean(raw_value: Field) -> bool:
    """Cast value to boolean with database-friendly logic and fallbacks."""
    if isinstance(raw_value, str):
        return raw_value.lower() in ("1", "true", "yes", "y")
    # Fallback for other types
    return bool(raw_value)


def cast_date(raw_value: Field) -> date:
    """Cast value to date with multiple format support and fallbacks."""
    # Check datetime first since it's a subclass of date
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value
    if isinstance(raw_value, str):
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

        # If all parsing attempts fail, raise error
        msg = f"Cannot convert '{raw_value}' to date"
        raise ValueError(msg)

    # For non-string, non-date types, raise error
    msg = f"Cannot convert {type(raw_value).__name__} to date: {raw_value}"
    raise TypeError(msg)


def cast_datetime(raw_value: Field) -> datetime:
    """Cast value to datetime with multiple format support and fallbacks."""
    if isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, date):
        return datetime.combine(raw_value, datetime.min.time()).astimezone()
    if isinstance(raw_value, str):
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

        # If all parsing attempts fail, raise error
        msg = f"Cannot convert '{raw_value}' to datetime"
        raise ValueError(msg)

    # For non-string, non-date/datetime types, raise error
    msg = f"Cannot convert {type(raw_value).__name__} to datetime: {raw_value}"
    raise TypeError(msg)


def cast_uuid(raw_value: Field) -> UUID:
    """Cast value to UUID with validation."""
    if isinstance(raw_value, UUID):
        return raw_value
    if isinstance(raw_value, str):
        try:
            return UUID(raw_value)
        except ValueError:
            pass
        # Try with common variations
        # Remove hyphens and try again
        try:
            clean_value = raw_value.replace("-", "")
            return UUID(clean_value)
        except ValueError:
            pass

        # If all attempts fail, raise error
        msg = f"Cannot convert '{raw_value}' to UUID"
        raise ValueError(msg)

    # For non-string, non-UUID types, raise error
    msg = f"Cannot convert {type(raw_value).__name__} to UUID: {raw_value}"
    raise TypeError(msg)


def value_caster(sql_type: TypeEngine[Field]) -> Callable[[Field], Field]:
    """Get casting function for a SQLAlchemy type.

    Returns a curried function that can be called with raw_value.
    Enables pre-computation of casters for performance.

    Args:
        sql_type: SQLAlchemy type to get caster for

    Returns:
        Casting function that takes raw_value and returns casted value

    """
    # Direct type dispatch to casting functions
    if isinstance(sql_type, Boolean):
        return cast_boolean
    if isinstance(sql_type, Date):
        return cast_date
    if isinstance(sql_type, DateTime):
        return cast_datetime
    if isinstance(sql_type, Uuid):
        return cast_uuid
    return lambda raw_value: raw_value
