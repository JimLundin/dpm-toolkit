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


def comparisons_to_html(comparisons: Iterable[Comparison]) -> TemplateStream:
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


def compare_columns(
    old_columns: Iterable[Row],
    new_columns: Iterable[Row],
) -> Iterator[Change]:
    """Compare column definitions and return modifications, additions, and removals."""
    old_column_names = {col["name"]: col for col in old_columns}
    new_column_names = {col["name"]: col for col in new_columns}

    return chain(
        (
            Change(new_column_names[name], old_column_names[name])
            for name in old_column_names.keys() & new_column_names.keys()
            if old_column_names[name] != new_column_names[name]
        ),
        (
            Change(new=new_column_names[name])
            for name in new_column_names.keys() - old_column_names.keys()
        ),
        (
            Change(old=old_column_names[name])
            for name in old_column_names.keys() - new_column_names.keys()
        ),
    )


def has_guid(row: Row) -> bool:
    """Check if row has a non-null RowGUID column."""
    return "RowGUID" in row.keys() and row["RowGUID"]  # noqa: SIM118


def row_key(row: Iterable[ValueType]) -> tuple[tuple[bool, ValueType], ...]:
    """Extract a tuple of values for sorting/comparison from a Row."""
    return tuple((val is None, val) for val in row)


def compare_rows(
    old: Iterator[Row],
    new: Iterator[Row],
    primary_keys: Iterable[str],
) -> Iterator[Change]:
    """Memory-efficient merge-based comparison of sorted row iterators."""
    old_row = next(old, None)
    new_row = next(new, None)

    while old_row or new_row:
        if not old_row:
            yield Change(new=new_row)
            new_row = next(new, None)
        elif not new_row:
            yield Change(old=old_row)
            old_row = next(old, None)
        else:
            if has_guid(old_row) and has_guid(new_row):
                old_primary_key = row_key((old_row["RowGUID"],))
                new_primary_key = row_key((new_row["RowGUID"],))
            else:
                # Compare row keys using the same keys used for sorting
                old_primary_key = row_key(old_row[col] for col in primary_keys)
                new_primary_key = row_key(new_row[col] for col in primary_keys)

            if old_primary_key == new_primary_key:
                # Same key - check if content differs
                if old_row != new_row:
                    yield Change(new=new_row, old=old_row)
                old_row = next(old, None)
                new_row = next(new, None)
            elif old_primary_key < new_primary_key:
                # Old key is smaller - row was removed
                yield Change(old=old_row)
                old_row = next(old, None)
            else:
                # New key is smaller - row was added
                yield Change(new=new_row)
                new_row = next(new, None)


def compare_table(table_name: str, old: Inspector, new: Inspector) -> Iterator[Change]:
    """Compare table data using consistent sort/comparison key strategy.

    Strategy: Always sort and compare by the same key hierarchy:
    1. RowGUID (if present in both tables)
    2. Common primary keys (if any exist)
    3. All common columns (fallback)
    """
    old_columns = frozenset(col["name"] for col in old.columns(table_name))
    new_columns = frozenset(col["name"] for col in new.columns(table_name))
    common_columns = old_columns & new_columns

    old_primary_keys = frozenset(old.primary_keys(table_name))
    new_primary_keys = frozenset(new.primary_keys(table_name))
    common_primary_keys = old_primary_keys & new_primary_keys

    # Build unified key hierarchy: RowGUID > common PKs > all common columns
    if "RowGUID" in common_columns:
        # Best case: RowGUID + PKs as tiebreaker for NULL RowGUIDs
        sort_keys = (*common_primary_keys, "RowGUID")
    else:
        # Good case: Common PKs provide stable identity
        sort_keys = tuple(common_primary_keys or common_columns)

    # Use same keys for both sorting and comparison
    old_rows = old.rows(table_name, sort_keys)
    new_rows = new.rows(table_name, sort_keys)
    return compare_rows(old_rows, new_rows, common_primary_keys)


def common_table(table_name: str, old: Inspector, new: Inspector) -> Comparison:
    """Compare a table that exists in both databases."""
    return Comparison(
        name=table_name,
        cols=ChangeSet(
            headers=Header(TABLE_INFO_COLS, TABLE_INFO_COLS),
            changes=(compare_columns(old.columns(table_name), new.columns(table_name))),
        ),
        rows=ChangeSet(
            headers=Header(
                (col["name"] for col in new.columns(table_name)),
                (col["name"] for col in old.columns(table_name)),
            ),
            changes=compare_table(table_name, old, new),
        ),
    )


def added_table(name: str, new: Inspector) -> Comparison:
    """Handle a table that was added to the target database."""
    return Comparison(
        name=name,
        cols=ChangeSet(
            headers=Header(new=TABLE_INFO_COLS),
            changes=(Change(new=col) for col in new.columns(name)),
        ),
        rows=ChangeSet(
            headers=Header(new=(col["name"] for col in new.columns(name))),
            changes=(Change(new=row) for row in new.rows(name)),
        ),
    )


def removed_table(name: str, old: Inspector) -> Comparison:
    """Handle a table that was removed from the source database."""
    return Comparison(
        name=name,
        cols=ChangeSet(
            headers=Header(old=TABLE_INFO_COLS),
            changes=(Change(old=col) for col in old.columns(name)),
        ),
        rows=ChangeSet(
            headers=Header(old=(col["name"] for col in old.columns(name))),
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
