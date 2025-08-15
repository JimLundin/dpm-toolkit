"""Type definitions for database comparison."""

from typing import TypedDict

type ValueType = str | int | float | bool | None


class RowAdded(TypedDict):
    """Information about a newly added row."""

    primary_key: dict[str, ValueType]
    new_values: dict[str, ValueType]


class RowRemoved(TypedDict):
    """Information about a removed row."""

    primary_key: dict[str, ValueType]
    old_values: dict[str, ValueType] | None


class RowModified(TypedDict):
    """Information about a modified row."""

    primary_key: dict[str, ValueType]
    old_values: dict[str, ValueType] | None
    new_values: dict[str, ValueType] | None


type RowChange = RowAdded | RowRemoved | RowModified


class ColumnInfo(TypedDict):
    """Complete column information from database schema."""

    name: str
    type: str
    nullable: bool
    default_value: str | None


class ColumnAdded(TypedDict):
    """Information about a newly added column."""

    column_name: str
    new_definition: ColumnInfo


class ColumnRemoved(TypedDict):
    """Information about a removed column."""

    column_name: str
    old_definition: ColumnInfo | None


class ColumnModified(TypedDict):
    """Information about a modified column."""

    column_name: str
    old_definition: ColumnInfo | None
    new_definition: ColumnInfo | None


type ColumnChange = ColumnAdded | ColumnRemoved | ColumnModified


class TableComparison(TypedDict):
    """Complete comparison result for a table."""

    table_name: str
    schema_comparison: list[ColumnChange] | None
    data_comparison: list[RowChange] | None


class Comparison(TypedDict):
    """Complete database comparison result."""

    source_database: str
    target_database: str
    changes: list[TableComparison]
