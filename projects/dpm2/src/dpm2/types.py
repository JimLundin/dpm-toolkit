"""Custom SQLAlchemy types for DPM databases.

DPM databases originated from Microsoft Access and store dates as TEXT values
in a mix of formats: ISO dates (``YYYY-MM-DD``), ISO datetimes (with or without
fractional seconds), and Access-style ``DD/MM/YYYY`` strings. These
``TypeDecorator`` wrappers accept all of those shapes at read time.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.types import TypeDecorator

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect


class DPMDate(TypeDecorator[datetime.date]):
    """Date type accepting ISO date, ISO datetime, and DD/MM/YYYY strings.

    Reflection sometimes types a column as ``Date`` even though the raw
    TEXT values are full datetime strings (``2023-10-15 00:00:00.000000``).
    We try ``date`` / ``datetime`` ISO parsers in turn before falling back
    to the Access DD/MM/YYYY format.
    """

    impl = String
    cache_ok = True

    @property
    def python_type(self) -> type[datetime.date]:
        """Expose ``date`` for ``type_annotation_map`` resolution."""
        return datetime.date

    def process_result_value(
        self,
        value: datetime.date | str | None,
        dialect: Dialect,  # noqa: ARG002
    ) -> datetime.date | None:
        """Parse a raw SQLite value into a ``date`` at read time."""
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value.date()
        if isinstance(value, datetime.date):
            return value
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            pass
        try:
            return datetime.datetime.fromisoformat(value).date()
        except ValueError:
            return datetime.datetime.strptime(value, "%d/%m/%Y").date()  # noqa: DTZ007


class DPMDateTime(TypeDecorator[datetime.datetime]):
    """DateTime type accepting ISO and DD/MM/YYYY[ HH:MM:SS] strings."""

    impl = String
    cache_ok = True

    @property
    def python_type(self) -> type[datetime.datetime]:
        """Expose ``datetime`` for ``type_annotation_map`` resolution."""
        return datetime.datetime

    def process_result_value(
        self,
        value: datetime.datetime | datetime.date | str | None,
        dialect: Dialect,  # noqa: ARG002
    ) -> datetime.datetime | None:
        """Parse a raw SQLite value into a ``datetime`` at read time."""
        if value is None:
            return None
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime.combine(value, datetime.datetime.min.time())
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            pass
        try:
            return datetime.datetime.strptime(  # noqa: DTZ007
                value,
                "%d/%m/%Y %H:%M:%S",
            )
        except ValueError:
            return datetime.datetime.strptime(value, "%d/%m/%Y")  # noqa: DTZ007
