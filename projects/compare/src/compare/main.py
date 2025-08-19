"""Main database comparison functionality."""

import json
from collections.abc import Iterable, Iterator
from itertools import chain
from pathlib import Path
from sqlite3 import Connection, Row
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja2.environment import TemplateStream

from compare.inspector import Inspector
from compare.types import (
    Change,
    Comparison,
    ValueType,
)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def encoder(obj: object) -> dict[str, str] | tuple[Any, ...]:
    """Convert sqlite3.Row to a regular dict for JSON serialization."""
    if isinstance(obj, Row):
        return dict(obj)
    if isinstance(obj, Iterable):
        return tuple(
            obj,  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
        )
    msg = f"Object of type {type(obj)} is not JSON serializable"
    raise TypeError(msg)


def comparisons_to_json(comparisons: Iterable[Comparison], **_: str) -> str:
    """Convert comparison result to JSON string."""
    return json.dumps(comparisons, default=encoder, ensure_ascii=False)


def render_report(comparisons: Iterable[Comparison]) -> TemplateStream:
    """Generate HTML report from comparison result."""
    env = Environment(
        loader=FileSystemLoader(TEMPLATE_DIR),
        autoescape=select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("report.html")
    env.policies["json.dumps_function"] = comparisons_to_json

    return template.stream(comparisons=comparisons)


def table_diff(
    source: Inspector,
    target: Inspector,
) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    """Return sets of added, removed, and common table names between databases."""
    source_tables = frozenset(source.tables())
    target_tables = frozenset(target.tables())

    added = target_tables - source_tables
    removed = source_tables - target_tables
    common = source_tables & target_tables

    return added, removed, common


def compare_cols(source: Iterable[Row], target: Iterable[Row]) -> Iterator[Change]:
    """Compare column definitions and return modifications, additions, and removals."""
    source_cols = {col["name"]: col for col in source}
    target_cols = {col["name"]: col for col in target}

    return chain(
        (
            Change(target_cols[name], source_cols[name])
            for name in source_cols.keys() & target_cols.keys()
            if source_cols[name] != target_cols[name]
        ),
        (
            Change(new=target_cols[name])
            for name in target_cols.keys() - source_cols.keys()
        ),
        (
            Change(old=source_cols[name])
            for name in source_cols.keys() - target_cols.keys()
        ),
    )


def row_key(row: Row, pk_cols: Iterable[str]) -> tuple[ValueType, ...]:
    """Create a unique key for a row, preferring RowGUID over primary key columns."""
    guid = row["RowGUID"] if "RowGUID" in row.keys() else None  # noqa: SIM118
    return (guid,) if guid else tuple(row[pk] for pk in pk_cols) or tuple(row)


def compare_rows(name: str, source: Inspector, target: Inspector) -> Iterator[Change]:
    """Compare table data and return row modifications, additions, and removals."""
    source_pks = tuple(source.pks(name))
    target_pks = tuple(target.pks(name))

    # This is a memory disaster
    source_map = {row_key(row, source_pks): row for row in source.rows(name)}
    target_map = {row_key(row, target_pks): row for row in target.rows(name)}

    return chain(
        (
            Change(target_map[key], source_map[key])
            for key in source_map.keys() & target_map.keys()
            if source_map[key] != target_map[key]
        ),
        (Change(new=target_map[key]) for key in target_map.keys() - source_map.keys()),
        (Change(old=source_map[key]) for key in source_map.keys() - target_map.keys()),
    )


def common_table(name: str, source: Inspector, target: Inspector) -> Comparison:
    """Compare a table that exists in both databases."""
    return Comparison(
        name=name,
        cols=compare_cols(source.cols(name), target.cols(name)),
        rows=compare_rows(name, source, target),
    )


def added_table(name: str, target: Inspector) -> Comparison:
    """Handle a table that was added to the target database."""
    return Comparison(
        name=name,
        cols=(Change(new=col) for col in target.cols(name)),
        rows=(Change(new=row) for row in target.rows(name)),
    )


def removed_table(name: str, source: Inspector) -> Comparison:
    """Handle a table that was removed from the source database."""
    return Comparison(
        name=name,
        cols=(Change(old=col) for col in source.cols(name)),
        rows=(Change(old=row) for row in source.rows(name)),
    )


def compare_databases(source: Connection, target: Connection) -> Iterator[Comparison]:
    """Compare two SQLite databases and return differences."""
    source_inspector = Inspector(source)
    target_inspector = Inspector(target)

    added, removed, common = table_diff(source_inspector, target_inspector)

    return chain(
        (common_table(name, source_inspector, target_inspector) for name in common),
        (added_table(name, target_inspector) for name in added),
        (removed_table(name, source_inspector) for name in removed),
    )
