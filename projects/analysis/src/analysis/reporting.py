"""Report generation utilities for analysis results."""

from __future__ import annotations

from pathlib import Path

TEMPLATE_DIR = Path(__file__).parent / "templates"


def json_default(obj: object) -> list[str]:
    """Convert non-serializable objects for JSON encoding.

    Handles conversion of set objects to sorted lists of strings.
    Since str() accepts any object, no type narrowing is needed.
    """
    if isinstance(obj, set):
        # Convert each item to string - str() accepts any type
        # Type checkers can't infer the set's item type from isinstance alone,
        # but str() is safe for any object type
        return (
            sorted(str(item) for item in obj)  # pyright: ignore[reportUnknownArgumentType,reportUnknownVariableType]
            if obj
            else []
        )
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)
