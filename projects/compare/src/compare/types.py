"""Type definitions for database comparison."""

from collections.abc import Iterable
from sqlite3 import Row
from typing import NamedTuple, ReadOnly, TypedDict

ValueType = str | int | float | None


class Mod(NamedTuple):
    """Information about a modified row."""

    new: Row | None = None
    old: Row | None = None


class Comparison(TypedDict):
    """Complete comparison result for a table."""

    name: ReadOnly[str]
    cols: ReadOnly[Iterable[Mod]]
    rows: ReadOnly[Iterable[Mod]]
