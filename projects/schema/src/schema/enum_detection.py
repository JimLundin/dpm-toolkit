"""Enum detection from SQLite check constraints."""

import re


def _parse_enum_values_from_in_clause(constraint_text: str) -> list[str] | None:
    """Extract enum values from an IN clause constraint.

    Handles patterns like: column IN ('value1', 'value2', 'value3')
    """
    # Pattern for IN clause: column IN ('val1', 'val2', 'val3')
    in_pattern = r"\w+\s+IN\s*\(\s*([^)]+)\s*\)"
    match = re.search(in_pattern, constraint_text, re.IGNORECASE)

    if match:
        values_text = match.group(1)
        # Extract quoted string values
        value_pattern = r"'([^']+)'"
        values = re.findall(value_pattern, values_text)
        return values if values else None

    return None


def _parse_enum_values_from_or_clause(constraint_text: str) -> list[str] | None:
    """Extract enum values from an OR clause constraint.

    Handles patterns like: column = 'value1' OR column = 'value2' OR column = 'value3'
    """
    # Find all occurrences of column = 'value'
    return re.findall(r"\w+\s*=\s*\'([^\']+)\'", constraint_text, re.IGNORECASE)


def _parse_enum_values_from_constraint(constraint_text: str) -> list[str] | None:
    """Extract enum values from a check constraint using both detection algorithms."""
    # Try IN clause first
    if values := _parse_enum_values_from_in_clause(constraint_text):
        return values

    # Try OR clause
    if values := _parse_enum_values_from_or_clause(constraint_text):
        return values

    return None


def detect_enum_for_column(constraint_text: str, column_name: str) -> list[str] | None:
    """Detect if a specific constraint defines an enum for a given column."""
    # Only process if the constraint references this specific column
    if column_name not in constraint_text:
        return None

    # Extract enum values from the constraint
    return _parse_enum_values_from_constraint(constraint_text)
