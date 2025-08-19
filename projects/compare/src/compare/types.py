"""Type definitions for database comparison."""

from collections.abc import Iterable
from sqlite3 import Row
from typing import NamedTuple, ReadOnly, TypedDict

ValueType = str | int | float | None


class Header(NamedTuple):
    """Header for a set of changes."""

    new: Iterable[str] | None = None
    old: Iterable[str] | None = None


class Change(NamedTuple):
    """Information about a modified row."""

    new: Row | None = None
    old: Row | None = None


class ChangeSet(TypedDict):
    """Set of changes for a table."""

    headers: ReadOnly[Header]
    changes: ReadOnly[Iterable[Change]]


class Comparison(TypedDict):
    """Complete comparison result for a table."""

    name: ReadOnly[str]
    cols: ReadOnly[ChangeSet]
    rows: ReadOnly[ChangeSet]
