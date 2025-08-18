"""Database comparison module for SQLite databases."""

from compare.main import (
    compare_databases,
    comparisons_to_json,
    render_report,
)

__all__ = [
    "compare_databases",
    "comparisons_to_json",
    "render_report",
]
