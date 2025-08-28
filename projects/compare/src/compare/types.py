"""Type definitions for database comparison."""

from collections.abc import Iterable
from sqlite3 import Row
from typing import NamedTuple

ValueType = str | int | float | None


class Header(NamedTuple):
    """Header for a set of changes."""

    new: Iterable[str] | None = None
    old: Iterable[str] | None = None


class Change(NamedTuple):
    """Information about a modified row."""

    new: Row | None = None
    old: Row | None = None


class ChangeSet(NamedTuple):
    """Set of changes for a table."""

    headers: Header
    changes: Iterable[Change]


class TableChange(NamedTuple):
    """Represents the change for a singular table."""

    columns: ChangeSet
    rows: ChangeSet


class Comparison(NamedTuple):
    """Complete comparison result for a table."""

    name: str
    changes: TableChange  # (cols, rows)
