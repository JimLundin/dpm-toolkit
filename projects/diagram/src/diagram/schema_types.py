"""TypedDict schemas for ER diagram JSON structure."""

from typing import TypedDict


class ColumnSchema(TypedDict):
    """Schema for a database column."""

    name: str
    type: str
    nullable: bool


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


class DiagramSchema(TypedDict):
    """Root schema for the complete ER diagram."""

    name: str
    tables: list[TableSchema]
