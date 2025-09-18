"""Module for parsing SQLAlchemy TypeEngine into structured column types."""

from typing import Any, NamedTuple

from sqlalchemy.types import (
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    Integer,
    LargeBinary,
    Numeric,
    String,
    TypeEngine,
)

from schema.types import (
    BlobType,
    BooleanType,
    DataType,
    DateTimeType,
    DateType,
    EnumType,
    IntegerType,
    NumericType,
    RealType,
    TextType,
)


class TypeInfo(NamedTuple):
    """Holds information about a SQLAlchemy type for code generation."""

    module: str
    name: str
    expression: str


def sql_to_data_type(sql_type: TypeEngine[Any]) -> DataType:
    """Parse a SQLAlchemy TypeEngine into a structured ColumnType.

    This approach is much more robust than string parsing as it leverages
    SQLAlchemy's built-in type system and introspection capabilities.

    Examples:
        VARCHAR(255) -> TextType with length=255
        DECIMAL(10,2) -> NumericType with precision=10, scale=2
        INTEGER -> IntegerType
        TEXT -> TextType

    """
    # Default fallback
    data_type: DataType

    match sql_type:
        case Enum():
            values: list[str] = (
                sql_type.enums
            )  # pyright: ignore [reportUnknownMemberType]
            data_type = EnumType(type="enum", values=values)
        case Integer():
            data_type = IntegerType(type="integer")
        case String():
            data_type = TextType(type="text", length=sql_type.length)
        case Numeric():
            data_type = NumericType(
                type="numeric",
                precision=sql_type.precision,
                scale=sql_type.scale,
            )
        case Float():
            data_type = RealType(type="real")
        case LargeBinary():
            data_type = BlobType(type="blob")
        case Boolean():
            data_type = BooleanType(type="boolean")
        case Date():
            data_type = DateType(type="date")
        case DateTime():
            data_type = DateTimeType(type="datetime")
        case _:
            data_type = TextType(type="text", length=None)

    return data_type


def data_type_to_sql(data_type: DataType) -> TypeEngine[Any]:
    """Convert a ColumnType back to a SQLAlchemy TypeEngine.

    This leverages SQLAlchemy's type system knowledge and validation.
    """
    sql_type: TypeEngine[Any]

    if data_type["type"] == "integer":
        sql_type = Integer()
    elif data_type["type"] == "text":
        sql_type = String(data_type["length"])
    elif data_type["type"] == "real":
        sql_type = Float()
    elif data_type["type"] == "numeric":
        sql_type = Numeric(precision=data_type["precision"], scale=data_type["scale"])
    elif data_type["type"] == "blob":
        sql_type = LargeBinary()
    elif data_type["type"] == "boolean":
        sql_type = Boolean()
    elif data_type["type"] == "date":
        sql_type = Date()
    elif data_type["type"] == "datetime":
        sql_type = DateTime()
    elif data_type["type"] == "enum":
        sql_type = Enum(*data_type["values"])
    else:
        sql_type = String()

    return sql_type


def sql_to_string(sql_type: TypeEngine[Any]) -> str:
    """Convert a SQLAlchemy type to its string representation for code generation."""
    match sql_type:
        case String() if sql_type.length:
            return f"String({sql_type.length})"
        case Numeric() if sql_type.precision and sql_type.scale:
            return f"Numeric({sql_type.precision}, {sql_type.scale})"
        case Numeric() if sql_type.precision:
            return f"Numeric({sql_type.precision})"
        case Numeric():
            return "Numeric"
        case Enum():
            values: list[str] = (
                sql_type.enums
            )  # pyright: ignore [reportUnknownMemberType]
            values_string = ", ".join(f'"{v}"' for v in sorted(values))
            return f"Enum({values_string})"
        case _:
            return sql_type.__class__.__name__


def sql_to_python(sql_type: TypeEngine[Any]) -> TypeInfo:
    """Get the 3 components needed for code generation from SQLAlchemy type.

    Returns module, import_name, and expression.
    For most types these are straightforward, but enums need special handling.
    """
    match sql_type:
        # Special case: Enum types need Literal type hints
        case Enum():
            values: list[str] = (
                sql_type.enums
            )  # pyright: ignore [reportUnknownMemberType]
            values_string = ", ".join(f'"{v}"' for v in sorted(values))
            return TypeInfo(
                module="typing",
                name="Literal",
                expression=f"Literal[{values_string}]",
            )
        case _:
            # Standard case: Use SQLAlchemy's python_type
            py_type = sql_type.python_type
            type_name = py_type.__name__
            module_name = py_type.__module__

            return TypeInfo(
                module=module_name,
                name=type_name,
                expression=type_name,
            )


def data_type_to_python(data_type: DataType) -> str:
    """Get the Python type expression for code generation.

    This returns the full expression (e.g., "Literal["active", "inactive"]").
    """
    sql_type = data_type_to_sql(data_type)
    type_info = sql_to_python(sql_type)
    return type_info.expression
