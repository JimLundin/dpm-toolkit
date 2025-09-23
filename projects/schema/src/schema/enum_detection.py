"""Enum detection from SQLite check constraints."""

import re

# Reusable regex components for better readability
IDENTIFIER = r"[\"'`]?(\w+)[\"'`]?"  # Captures identifier inside optional quotes
VALUE = r"'([^']+)'"  # Captures content inside single quotes
WHITESPACE = r"\s*"
OPEN_PAREN = r"\("
CLOSE_PAREN = r"\)"
IN = r"IN"
VALUES = r"([^)]+)"


def detect_enum_for_column(constraint_text: str, column_name: str) -> list[str]:
    """Detect if a specific constraint defines an enum for a given column.

    Handles SQLAlchemy-generated constraints like:
    - column IN ('value1', 'value2', 'value3')
    - "column" IN ('value1', 'value2', 'value3')
    """
    # Build pattern: identifier IN (values)
    in_pattern = re.compile(
        WHITESPACE.join((IDENTIFIER, IN, OPEN_PAREN, VALUES, CLOSE_PAREN)),
        re.IGNORECASE,
    )

    if match := re.search(in_pattern, constraint_text):
        # Check if the captured identifier matches our column name
        return re.findall(VALUE, match[2]) if match[1] == column_name else []
    return []
