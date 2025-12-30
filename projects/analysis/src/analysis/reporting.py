"""Report generation utilities for analysis results."""

from __future__ import annotations

from pathlib import Path

TEMPLATE_DIR = Path(__file__).parent / "templates"


def json_default(obj: object) -> object:
    """Convert non-serializable objects for JSON encoding."""
    if isinstance(obj, set):
        return sorted(obj) if obj else []
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)
