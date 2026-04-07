"""Name-based type inference for DPM databases.

DPM databases follow naming conventions that encode type information:
- Columns ending with "GUID" contain UUIDs
- Columns ending with "Date" contain dates
- Columns starting with "Is"/"Has" contain booleans
- Specific columns like "StartDate"/"EndDate" are datetimes
- String columns ending with "Type"/"Status"/etc. are enum candidates

These conventions are lost when storing in SQLite (which has no UUID, Date,
or Boolean types), so we recover them by inspecting column names.

This module is the single source of truth for all name-based type rules.
It is used by both:
- **schema** during reflection to infer rich types for code generation
- **migrate** during normalization to collect enum data
"""

from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from sqlalchemy import Boolean, Column, String, Uuid
from sqlalchemy.engine.interfaces import Dialect, ReflectedColumn
from sqlalchemy.types import Date, DateTime, TypeEngine


def _process_date(value: date | str) -> date:
    """Parse a date value with ISO fast path and European fallback."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value)
    except ValueError:
        return datetime.strptime(value, "%d/%m/%Y").date()  # noqa: DTZ007


def _process_datetime(value: datetime | date | str) -> datetime:
    """Parse a datetime value with ISO fast path and European fallback."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass
    try:
        return datetime.strptime(value, "%d/%m/%Y %H:%M:%S")  # noqa: DTZ007
    except ValueError:
        return datetime.strptime(value, "%d/%m/%Y")  # noqa: DTZ007


class DPMDate(Date):
    """Date type with European format fallback for DPM databases.

    The source Access databases store dates as strings in ISO format
    (YYYY-MM-DD) or European format (DD/MM/YYYY). This type handles
    both at read time so the migration can store raw values.
    """

    def result_processor(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        dialect: Dialect,  # noqa: ARG002
        coltype: object,  # noqa: ARG002
    ) -> Callable[[date | str], date]:
        """Parse date strings with ISO fast path and European fallback."""
        return _process_date


class DPMDateTime(DateTime):
    """DateTime type with European format fallback for DPM databases."""

    def result_processor(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        dialect: Dialect,  # noqa: ARG002
        coltype: object,  # noqa: ARG002
    ) -> Callable[[datetime | date | str], datetime]:
        """Parse datetime strings with ISO fast path and European fallback."""
        return _process_datetime


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
        data_type = DPMDateTime()

    # Prefix patterns
    elif name_lower.startswith(("is", "has")):
        data_type = Boolean()

    # Suffix patterns
    elif name_lower.endswith("guid"):
        data_type = Uuid()
    elif name_lower.endswith("date"):
        data_type = DPMDate()

    return data_type


def enum_candidate(column: Column[Any]) -> bool:
    """Identify if a reflected column is an enum candidate by name.

    Only String columns are considered — columns already detected as
    UUID, Date, or Boolean by ``column_type`` are excluded.
    """
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
