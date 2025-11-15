"""Database comparison module for SQLite databases."""

from compare.main import (
    compare_databases,
    comparisons_to_html,
    comparisons_to_json,
    comparisons_to_summary,
)

__all__ = [
    "compare_databases",
    "comparisons_to_html",
    "comparisons_to_json",
    "comparisons_to_summary",
]
