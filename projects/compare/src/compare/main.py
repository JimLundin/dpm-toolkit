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


def key(row: Iterable[ValueType]) -> tuple[tuple[bool, ValueType], ...]:
    """Extract a tuple of values for sorting/comparison from a Row."""
    return tuple((val is None, val) for val in row)


def merge_compare_rows(
    old: Iterator[Row],
    new: Iterator[Row],
    keys: Iterable[str],
) -> Iterator[Change]:
    """Memory-efficient merge-based comparison of sorted row iterators."""
    old_row = next(old, None)
    new_row = next(new, None)

    while old_row or new_row:
        if old_row is None:
            yield Change(new=new_row)
            new_row = next(new, None)
        elif new_row is None:
            yield Change(old=old_row)
            old_row = next(old, None)
        else:
            # Compare row keys using the same keys used for sorting
            old_key = key(old_row[col] for col in keys)
            new_key = key(new_row[col] for col in keys)

            if old_key == new_key:
                # Same key - check if content differs
                if old_row != new_row:
                    yield Change(new=new_row, old=old_row)
                old_row = next(old, None)
                new_row = next(new, None)
            elif old_key < new_key:
                # Old key is smaller - row was removed
                yield Change(old=old_row)
                old_row = next(old, None)
            else:
                # New key is smaller - row was added
                yield Change(new=new_row)
                new_row = next(new, None)


def compare_rows(name: str, old: Inspector, new: Inspector) -> Iterator[Change]:
    """Compare table data using consistent sort/comparison key strategy.

    Strategy: Always sort and compare by the same key hierarchy:
    1. RowGUID (if present in both tables)
    2. Common primary keys (if any exist)
    3. All common columns (fallback)
    """
    old_cols = frozenset(col["name"] for col in old.cols(name))
    new_cols = frozenset(col["name"] for col in new.cols(name))
    common_cols = old_cols & new_cols

    old_pks = frozenset(old.pks(name))
    new_pks = frozenset(new.pks(name))
    common_pks = old_pks & new_pks

    # Build unified key hierarchy: RowGUID > common PKs > all common columns
    if "RowGUID" in common_cols:
        # Best case: RowGUID + PKs as tiebreaker for NULL RowGUIDs
        sort_keys = (*common_pks, "RowGUID")
    else:
        # Good case: Common PKs provide stable identity
        sort_keys = tuple(common_pks or common_cols)

    # Use same keys for both sorting and comparison
    old_iter = old.rows(name, sort_keys)
    new_iter = new.rows(name, sort_keys)
    return merge_compare_rows(old_iter, new_iter, sort_keys)


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
