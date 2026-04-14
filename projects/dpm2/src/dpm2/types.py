"""Custom SQLAlchemy types for DPM2 models.

DPM databases originate from Access and store dates as strings in either ISO
(YYYY-MM-DD) or European (DD/MM/YYYY) format. Since migration stores raw
values verbatim in SQLite (which keeps dates as TEXT), we parse these strings
at read time so the generated ORM models can expose real
``datetime.date`` / ``datetime.datetime`` values to callers.

Implementation notes:

* We use ``TypeDecorator`` rather than subclassing ``Date``/``DateTime``
  because the SQLAlchemy dialect ``colspecs`` MRO walk would otherwise swap
  a subclass for the dialect's built-in implementation, bypassing overrides.
* ``impl = String`` (rather than ``Date``/``DateTime``) because SQLite stores
  these values as raw TEXT and we need to bypass the impl's
  ``str_to_date`` / ``str_to_datetime`` result processor — which is what
  rejects the DD/MM/YYYY strings with ``ValueError: Invalid isoformat``.
  ``TypeDecorator`` otherwise composes the impl's processor with
  ``process_result_value``; using ``String`` leaves the raw value
  untouched so we can parse it ourselves.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import String
from sqlalchemy.types import TypeDecorator

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect


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


class DPMDate(TypeDecorator[date]):
    """Date type that accepts both ISO and DD/MM/YYYY strings."""

    impl = String
    cache_ok = True

    @property
    def python_type(self) -> type[date]:
        """Expose ``date`` as the Python type (used by ``type_annotation_map``)."""
        return date

    def process_result_value(
        self,
        value: date | str | None,
        dialect: Dialect,  # noqa: ARG002
    ) -> date | None:
        """Parse a raw SQLite value into a ``date`` at read time."""
        if value is None:
            return None
        return _process_date(value)


class DPMDateTime(TypeDecorator[datetime]):
    """DateTime type that accepts both ISO and DD/MM/YYYY[ HH:MM:SS] strings."""

    impl = String
    cache_ok = True

    @property
    def python_type(self) -> type[datetime]:
        """Expose ``datetime`` as the Python type (used by ``type_annotation_map``)."""
        return datetime

    def process_result_value(
        self,
        value: datetime | date | str | None,
        dialect: Dialect,  # noqa: ARG002
    ) -> datetime | None:
        """Parse a raw SQLite value into a ``datetime`` at read time."""
        if value is None:
            return None
        return _process_datetime(value)
