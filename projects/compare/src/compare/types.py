"""Type definitions for database comparison."""

from collections.abc import Iterable
from sqlite3 import Row
from typing import NamedTuple, ReadOnly, TypedDict

ValueType = str | int | float | bool | None


class RowMod(NamedTuple):
    """Information about a modified row."""

    new: Row | None = None
    old: Row | None = None


class ColInfo(NamedTuple):
    """Complete column information from database schema."""

    name: str
    type: str
    nullable: bool
    default: ValueType
    primary_key: bool


class ColMod(NamedTuple):
    """Information about a modified column."""

    new: ColInfo | None = None
    old: ColInfo | None = None


class TableComparison(TypedDict):
    """Complete comparison result for a table."""

    name: ReadOnly[str]
    schema: ReadOnly[Iterable[ColMod]]
    data: ReadOnly[Iterable[RowMod]]


type Change = ColMod | RowMod
