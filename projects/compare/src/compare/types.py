"""Type definitions for database comparison."""

from collections.abc import Mapping, Sequence
from typing import TypedDict

ValueType = str | int | float | bool | None


class Row(TypedDict):
    """Base type for row changes in database comparison."""

    key: Mapping[str, ValueType]


class RowAdded(Row):
    """Information about a newly added row."""

    new: Mapping[str, ValueType]


class RowRemoved(Row):
    """Information about a removed row."""

    old: Mapping[str, ValueType]


class RowModified(RowAdded, RowRemoved):
    """Information about a modified row."""


type RowChange = RowAdded | RowRemoved | RowModified


class ColumnInfo(TypedDict):
    """Complete column information from database schema."""

    name: str
    type: str
    nullable: bool
    default: ValueType


class Column(TypedDict):
    """Base type for column changes in database comparison."""

    name: str


class ColumnAdded(Column):
    """Information about a newly added column."""

    new: ColumnInfo


class ColumnRemoved(Column):
    """Information about a removed column."""

    old: ColumnInfo


class ColumnModified(ColumnAdded, ColumnRemoved):
    """Information about a modified column."""


type ColumnChange = ColumnAdded | ColumnRemoved | ColumnModified


class TableComparison(TypedDict):
    """Complete comparison result for a table."""

    name: str
    schema: Sequence[ColumnChange] | None
    data: Sequence[RowChange] | None


class Comparison(TypedDict):
    """Complete database comparison result."""

    source: str
    target: str
    changes: list[TableComparison]
