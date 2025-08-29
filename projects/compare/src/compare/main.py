"""Main database comparison functionality."""

import json
from collections.abc import Iterable, Iterator
from itertools import chain
from pathlib import Path
from sqlite3 import Connection, Row
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja2.environment import TemplateStream

from compare.inspector import TABLE_INFO_COLS, DatabaseInspector, TableInspector
from compare.types import (
    Change,
    ChangeSet,
    Comparison,
    Header,
    TableChange,
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


def difference(
    old_names: Iterable[str],
    new_names: Iterable[str],
) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
    """Return sets of added, removed, and common names between iterables of strings."""
    old_names = frozenset(old_names)
    new_names = frozenset(new_names)

    added_names = new_names - old_names
    removed_names = old_names - new_names
    common_names = old_names & new_names

    return added_names, removed_names, common_names


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


def compare_rows(
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


def compare_table_rows(
    old_table: TableInspector,
    new_table: TableInspector,
) -> Iterator[Change]:
    """Compare table data using consistent sort/comparison key strategy.

    Strategy: Always sort and compare by the same key hierarchy:
    1. RowGUID (if present in both tables)
    2. Common primary keys (if any exist)
    3. All common columns (fallback)
    """
    old_columns = frozenset(column["name"] for column in old_table.columns())
    new_columns = frozenset(column["name"] for column in new_table.columns())
    shared_columns = old_columns & new_columns

    old_primary_keys = frozenset(old_table.primary_keys())
    new_primary_keys = frozenset(new_table.primary_keys())
    shared_primary_keys = old_primary_keys & new_primary_keys

    # Build unified key hierarchy: RowGUID > common PKs > all common columns
    if "RowGUID" in shared_columns:
        # Best case: RowGUID + PKs as tiebreaker for NULL RowGUIDs
        sort_keys = (*shared_primary_keys, "RowGUID")
    else:
        # Good case: Common PKs provide stable identity
        sort_keys = tuple(shared_primary_keys or shared_columns)

    # Use same keys for both sorting and comparison
    old_rows = old_table.rows(sort_keys)
    new_rows = new_table.rows(sort_keys)
    return compare_rows(old_rows, new_rows, sort_keys)


def common_table(
    old_table: TableInspector,
    new_table: TableInspector,
) -> TableChange:
    """Compare a table that exists in both databases."""
    return TableChange(
        ChangeSet(
            headers=Header(TABLE_INFO_COLS, TABLE_INFO_COLS),
            changes=compare_cols(old_table.columns(), new_table.columns()),
        ),
        ChangeSet(
            headers=Header(
                (column["name"] for column in new_table.columns()),
                (column["name"] for column in old_table.columns()),
            ),
            changes=compare_table_rows(old_table, new_table),
        ),
    )


def added_table(new_table: TableInspector) -> TableChange:
    """Handle a table that was added, returning (cols_changeset, rows_changeset)."""
    return TableChange(
        ChangeSet(
            headers=Header(new=TABLE_INFO_COLS),
            changes=(Change(new=column) for column in new_table.columns()),
        ),
        ChangeSet(
            headers=Header(new=(column["name"] for column in new_table.columns())),
            changes=(Change(new=row) for row in new_table.rows()),
        ),
    )


def removed_table(old_table: TableInspector) -> TableChange:
    """Handle a table that was removed, returning (cols_changeset, rows_changeset)."""
    return TableChange(
        ChangeSet(
            headers=Header(old=TABLE_INFO_COLS),
            changes=(Change(old=column) for column in old_table.columns()),
        ),
        ChangeSet(
            headers=Header(old=(column["name"] for column in old_table.columns())),
            changes=(Change(old=row) for row in old_table.rows()),
        ),
    )


def compare_databases(
    old_database: Connection,
    new_database: Connection,
) -> Iterator[Comparison]:
    """Compare two SQLite databases and return differences."""
    old = DatabaseInspector(old_database)
    new = DatabaseInspector(new_database)

    added_tables, removed_tables, common_tables = difference(old.tables(), new.tables())

    return chain(
        (
            Comparison(name, common_table(old.table(name), new.table(name)))
            for name in common_tables
        ),
        (Comparison(name, added_table(new.table(name))) for name in added_tables),
        (Comparison(name, removed_table(old.table(name))) for name in removed_tables),
    )
