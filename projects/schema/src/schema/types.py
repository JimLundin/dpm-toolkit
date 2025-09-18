"""TypedDict schemas for database schema representation."""

from typing import Literal, TypedDict

# Base type classes similar to SQLAlchemy's type hierarchy


class IntegerType(TypedDict):
    """Integer column type."""

    type: Literal["integer"]


class TextType(TypedDict):
    """Text column type with optional length constraint."""

    type: Literal["text"]
    length: int | None


class RealType(TypedDict):
    """Real/float column type."""

    type: Literal["real"]


class NumericType(TypedDict):
    """Numeric column type with precision and scale."""

    type: Literal["numeric"]
    precision: int | None
    scale: int | None


class BlobType(TypedDict):
    """Binary large object column type."""

    type: Literal["blob"]


class BooleanType(TypedDict):
    """Boolean column type."""

    type: Literal["boolean"]


class DateType(TypedDict):
    """Date column type."""

    type: Literal["date"]


class DateTimeType(TypedDict):
    """DateTime column type."""

    type: Literal["datetime"]


class EnumType(TypedDict):
    """Enum column type with constrained values."""

    type: Literal["enum"]
    values: list[str]


# Union type for all column types
type DataType = (
    IntegerType
    | TextType
    | RealType
    | NumericType
    | BlobType
    | BooleanType
    | DateType
    | DateTimeType
    | EnumType
)


class ColumnSchema(TypedDict):
    """Schema for a database column."""

    name: str
    type: DataType
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


class DatabaseSchema(TypedDict):
    """Root schema for the complete database schema."""

    name: str
    tables: list[TableSchema]
