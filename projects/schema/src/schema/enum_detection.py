"""Enum detection from SQLite check constraints."""

import re

# Reusable regex components for better readability
IDENTIFIER = r"[\"'`]?\w+[\"'`]?"  # Matches quoted/unquoted identifiers
VALUE = r"'([^']+)'"  # Captures content inside single quotes
WHITESPACE = r"\s*"
OPEN_PAREN = r"\("
CLOSE_PAREN = r"\)"
EQUALS = r"="
IN = r"IN"
VALUES = r"([^)]+)"


def values_from_in_clause(constraint_text: str) -> list[str]:
    """Extract enum values from an IN clause constraint.

    Handles patterns like: column IN ('value1', 'value2', 'value3')
    Also handles quoted column names: "column" IN ('value1', 'value2')
    """
    in_pattern = re.compile(
        WHITESPACE.join((IDENTIFIER, IN, OPEN_PAREN, VALUES, CLOSE_PAREN)),
        re.IGNORECASE,
    )

    # Extract parentheses content and find all quoted values in one expression
    if values := re.search(in_pattern, constraint_text):
        return re.findall(VALUE, values[1])

    return []


def values_from_or_clause(constraint_text: str) -> list[str]:
    """Extract enum values from an OR clause constraint.

    Handles patterns like: column = 'value1' OR column = 'value2' OR column = 'value3'
    Also handles quoted column names: "column" = 'value1' OR "column" = 'value2'
    """
    or_pattern = re.compile(WHITESPACE.join((IDENTIFIER, EQUALS, VALUE)), re.IGNORECASE)
    return or_pattern.findall(constraint_text)


def values_from_constraint(constraint_text: str) -> list[str]:
    """Extract enum values from a check constraint using both detection algorithms."""
    # Try IN clause first
    if values := values_from_in_clause(constraint_text):
        return values

    # Try OR clause
    if values := values_from_or_clause(constraint_text):
        return values

    return []


def detect_enum_for_column(constraint_text: str, column_name: str) -> list[str]:
    """Detect if a specific constraint defines an enum for a given column."""
    # Only process if the constraint references this specific column
    if column_name not in constraint_text:
        return []

    # Extract enum values from the constraint
    return values_from_constraint(constraint_text)
