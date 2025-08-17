"""Type definitions for database comparison."""

from collections.abc import Iterable
from sqlite3 import Row
from typing import Literal, NamedTuple, ReadOnly, TypedDict

ValueType = str | int | float | bool | None

type ChangeType = Literal["added", "removed", "modified"]


class RowAdd(NamedTuple):
    """Information about a newly added row."""

    new: Row


class RowDel(NamedTuple):
    """Information about a removed row."""

    old: Row


class RowMod(NamedTuple):
    """Information about a modified row."""

    new: Row
    old: Row


type RowChange = RowAdd | RowDel | RowMod


class ColInfo(NamedTuple):
    """Complete column information from database schema."""

    name: str
    type: str
    nullable: bool
    default: ValueType
    primary_key: bool


class ColAdd(NamedTuple):
    """Information about a newly added column."""

    new: ColInfo


class ColDel(NamedTuple):
    """Information about a removed column."""

    old: ColInfo


class ColMod(NamedTuple):
    """Information about a modified column."""

    new: ColInfo
    old: ColInfo


type ColumnChange = ColAdd | ColDel | ColMod


class TableComparison(TypedDict):
    """Complete comparison result for a table."""

    name: ReadOnly[str]
    schema: ReadOnly[Iterable[ColumnChange]]
    data: ReadOnly[Iterable[RowChange]]


class Comparison(TypedDict):
    """Complete database comparison result."""

    source: ReadOnly[str]
    target: ReadOnly[str]
    changes: ReadOnly[Iterable[TableComparison]]


type Change = ColumnChange | RowChange
