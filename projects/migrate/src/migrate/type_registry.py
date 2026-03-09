"""Type registry for database migration transformations.

Column type detection is shared from the schema package.
Enum detection remains here as it uses name-based heuristics specific to
the migration step (schema uses constraint-based detection instead).
"""

from typing import Any

from sqlalchemy import Column, String
from schema.type_registry import column_type

__all__ = ["column_type", "enum_type"]


def enum_type(column: Column[Any]) -> bool:
    """Identify if the column should be cast as an Enum."""
    return isinstance(column.type, String) and column.name.lower().endswith(
        (
            "type",
            "status",
            "sign",
            "optionality",
            "direction",
            "number",
            "endorsement",
            "source",
            "severity",
            "errorcode",
            "operandreference",
        ),
    )
