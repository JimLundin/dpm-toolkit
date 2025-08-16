"""Type definitions for database comparison."""

from collections.abc import Iterable
from typing import Literal, ReadOnly, TypedDict

ValueType = str | int | float | bool | None

type ChangeType = Literal["added", "removed", "modified"]


class RowAdded(TypedDict):
    """Information about a newly added row."""

    new: ReadOnly[tuple[ValueType, ...]]


class RowRemoved(TypedDict):
    """Information about a removed row."""

    old: ReadOnly[tuple[ValueType, ...]]


class RowModified(RowAdded, RowRemoved):
    """Information about a modified row."""


type RowChange = RowAdded | RowRemoved | RowModified


class ColumnInfo(TypedDict):
    """Complete column information from database schema."""

    name: ReadOnly[str]
    type: ReadOnly[str]
    nullable: ReadOnly[bool]
    default: ReadOnly[ValueType]
    primary_key: ReadOnly[bool]


class ColumnAdded(TypedDict):
    """Information about a newly added column."""

    new: ReadOnly[ColumnInfo]


class ColumnRemoved(TypedDict):
    """Information about a removed column."""

    old: ReadOnly[ColumnInfo]


class ColumnModified(ColumnAdded, ColumnRemoved):
    """Information about a modified column."""


type ColumnChange = ColumnAdded | ColumnRemoved | ColumnModified


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
