"""Main database comparison functionality."""

import json
from collections.abc import Iterable
from itertools import chain
from pathlib import Path
from sqlite3 import Connection, Row
from typing import Any

from jinja2 import Environment, FileSystemLoader

from compare.comparator import Comparator
from compare.inspector import Inspector
from compare.types import Comparison

TEMPLATE_DIR = Path(__file__).parent / "templates"


def render_report(result: str) -> str:
    """Generate HTML report from comparison result."""
    env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=True,
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("report.html")
    return template.render(comparison_json=result)


def encoder(obj: object) -> dict[str, str] | tuple[Any, ...]:
    """Convert sqlite3.Row to a regular dict for JSON serialization."""
    if isinstance(obj, Row):
        return dict(obj)
    if isinstance(obj, chain):
        return tuple(
            obj,  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
        )
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)


def compare_databases(source: Connection, target: Connection) -> Iterable[Comparison]:
    """Compare two SQLite databases completely and return full results."""
    comparator = Comparator(Inspector(source), Inspector(target))

    return comparator.compare()


def comparisons_to_json(comparisons: Iterable[Comparison], indent: int = 0) -> str:
    """Convert comparison result to JSON string."""
    return json.dumps(comparisons, default=encoder, indent=indent, ensure_ascii=False)
