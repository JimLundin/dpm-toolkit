"""Main database comparison functionality."""

import json
from collections.abc import Iterable, Iterator
from itertools import chain
from pathlib import Path
from sqlite3 import Connection, Row
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja2.environment import TemplateStream

from compare.inspector import TABLE_INFO_COLS, Inspector
from compare.types import (
    Change,
    ChangeSet,
    Comparison,
    Header,
    ValueType,
)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def encoder(obj: object) -> tuple[Any, ...]:
    """Convert sqlite3.Row to a regular dict for JSON serialization."""
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


def name_diff(
    old: Iterable[str],
    new: Iterable[str],
) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    """Return sets of added, removed, and common names between iterables of strings."""
    old_names = frozenset(old)
    new_names = frozenset(new)

    added = new_names - old_names
    removed = old_names - new_names
    common = old_names & new_names

    return added, removed, common


def compare_cols(old: Iterable[Row], new: Iterable[Row]) -> Iterator[Change]:
    """Compare column definitions and return modifications, additions, and removals."""
    old_cols = {col["name"]: col for col in old}
    new_cols = {col["name"]: col for col in new}

    return chain(
        (
            Change(new_cols[name], old_cols[name])
            for name in old_cols.keys() & new_cols.keys()
            if old_cols[name] != new_cols[name]
        ),
        (Change(new=new_cols[name]) for name in new_cols.keys() - old_cols.keys()),
        (Change(old=old_cols[name]) for name in old_cols.keys() - new_cols.keys()),
    )


def row_key(row: Row, pks: Iterable[str]) -> tuple[ValueType, ...]:
    """Create a unique key for a row, preferring RowGUID over primary key columns."""
    guid = row["RowGUID"] if "RowGUID" in row.keys() else None  # noqa: SIM118
    return (guid,) if guid else tuple(row[pk] for pk in pks) or tuple(row)


def compare_rows(name: str, old: Inspector, new: Inspector) -> Iterator[Change]:
    """Compare table data and return row modifications, additions, and removals."""
    old_pks = tuple(old.pks(name))
    new_pks = tuple(new.pks(name))

    # This is a memory disaster
    old_row_map = {row_key(row, old_pks): row for row in old.rows(name)}
    new_row_map = {row_key(row, new_pks): row for row in new.rows(name)}

    return chain(
        (
            Change(new_row_map[key], old_row_map[key])
            for key in old_row_map.keys() & new_row_map.keys()
            if old_row_map[key] != new_row_map[key]
        ),
        (
            Change(new=new_row_map[key])
            for key in new_row_map.keys() - old_row_map.keys()
        ),
        (
            Change(old=old_row_map[key])
            for key in old_row_map.keys() - new_row_map.keys()
        ),
    )


def common_table(name: str, old: Inspector, new: Inspector) -> Comparison:
    """Compare a table that exists in both databases."""
    return Comparison(
        name=name,
        cols=ChangeSet(
            headers=Header(TABLE_INFO_COLS, TABLE_INFO_COLS),
            changes=(compare_cols(old.cols(name), new.cols(name))),
        ),
        rows=ChangeSet(
            headers=Header(
                (col["name"] for col in new.cols(name)),
                (col["name"] for col in old.cols(name)),
            ),
            changes=compare_rows(name, old, new),
        ),
    )


def added_table(name: str, new: Inspector) -> Comparison:
    """Handle a table that was added to the target database."""
    return Comparison(
        name=name,
        cols=ChangeSet(
            headers=Header(new=TABLE_INFO_COLS),
            changes=(Change(new=col) for col in new.cols(name)),
        ),
        rows=ChangeSet(
            headers=Header(new=(col["name"] for col in new.cols(name))),
            changes=(Change(new=row) for row in new.rows(name)),
        ),
    )


def removed_table(name: str, old: Inspector) -> Comparison:
    """Handle a table that was removed from the source database."""
    return Comparison(
        name=name,
        cols=ChangeSet(
            headers=Header(old=TABLE_INFO_COLS),
            changes=(Change(old=col) for col in old.cols(name)),
        ),
        rows=ChangeSet(
            headers=Header(old=(col["name"] for col in old.cols(name))),
            changes=(Change(old=row) for row in old.rows(name)),
        ),
    )


def compare_databases(old: Connection, new: Connection) -> Iterator[Comparison]:
    """Compare two SQLite databases and return differences."""
    old_inspector = Inspector(old)
    new_inspector = Inspector(new)

    added, removed, common = name_diff(old_inspector.tables(), new_inspector.tables())

    return chain(
        (common_table(name, old_inspector, new_inspector) for name in common),
        (added_table(name, new_inspector) for name in added),
        (removed_table(name, old_inspector) for name in removed),
    )
