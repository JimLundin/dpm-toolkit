"""Type definitions for database comparison."""

from collections.abc import Collection, Mapping
from typing import Literal, ReadOnly, TypedDict

ValueType = str | int | float | bool | None

type ChangeType = Literal["added", "removed", "modified"]


class Row(TypedDict):
    """Base type for row changes in database comparison."""

    key: ReadOnly[Mapping[str, ValueType]]


class RowAdded(Row):
    """Information about a newly added row."""

    new: ReadOnly[Mapping[str, ValueType]]


class RowRemoved(Row):
    """Information about a removed row."""

    old: ReadOnly[Mapping[str, ValueType]]


class RowModified(RowAdded, RowRemoved):
    """Information about a modified row."""


type RowChange = RowAdded | RowRemoved | RowModified


class ColumnInfo(TypedDict):
    """Complete column information from database schema."""

    name: ReadOnly[str]
    type: ReadOnly[str]
    nullable: ReadOnly[bool]
    default: ReadOnly[ValueType]


class Column(TypedDict):
    """Base type for column changes in database comparison."""

    name: ReadOnly[str]


class ColumnAdded(Column):
    """Information about a newly added column."""

    new: ReadOnly[ColumnInfo]


class ColumnRemoved(Column):
    """Information about a removed column."""

    old: ReadOnly[ColumnInfo]


class ColumnModified(ColumnAdded, ColumnRemoved):
    """Information about a modified column."""


type ColumnChange = ColumnAdded | ColumnRemoved | ColumnModified


class TableComparison(TypedDict):
    """Complete comparison result for a table."""

    name: ReadOnly[str]
    schema: ReadOnly[Collection[ColumnChange]]
    data: ReadOnly[Collection[RowChange]]


class Comparison(TypedDict):
    """Complete database comparison result."""

    source: ReadOnly[str]
    target: ReadOnly[str]
    changes: ReadOnly[Collection[TableComparison]]


type Change = ColumnChange | RowChange
