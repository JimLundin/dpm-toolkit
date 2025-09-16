"""TypedDict schemas for database schema representation."""

from typing import NotRequired, TypedDict


class ColumnSchema(TypedDict):
    """Schema for a database column with enhanced type support."""

    name: str
    type: str
    nullable: bool
    enum_values: NotRequired[list[str] | None]  # For enum detection support


class ColumnMapping(TypedDict):
    """Schema for column mapping in foreign keys."""

    source_column: str  # Column in current table
    target_column: str  # Column in referenced table


class ForeignKeySchema(TypedDict):
    """Schema for foreign key definition."""

    target_table: str
    column_mappings: list[ColumnMapping]


class TableSchema(TypedDict):
    """Schema for a database table."""

    name: str
    columns: list[ColumnSchema]
    primary_keys: list[str]  # Flattened list of column names
    foreign_keys: list[ForeignKeySchema]


class DatabaseSchema(TypedDict):
    """Root schema for the complete database schema."""

    name: str
    tables: list[TableSchema]
