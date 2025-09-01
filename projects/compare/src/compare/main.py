"""Main database comparison functionality."""

import json
from collections.abc import Iterable, Iterator
from itertools import chain
from pathlib import Path
from sqlite3 import Row
from typing import Any, NamedTuple

from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja2.environment import TemplateStream

from compare.comparison import ComparisonDatabase
from compare.inspector import Schema, Table, TableType

type ValueType = str | int | float | None
type RowValues = Iterable[ValueType]
type ComparisonKey = tuple[tuple[bool, ValueType], ...]


class Header(NamedTuple):
    """Header for a set of changes."""

    new: Iterable[str] | None = None
    old: Iterable[str] | None = None


class Change(NamedTuple):
    """Information about a modified row."""

    new: Row | None = None
    old: Row | None = None


class ChangeSet(NamedTuple):
    """Set of changes for a table."""

    headers: Header
    changes: Iterable[Change]


class TableChange(NamedTuple):
    """Represents the change for a singular table."""

    columns: ChangeSet
    rows: ChangeSet


class Comparison(NamedTuple):
    """Complete comparison result for a table."""

    name: str
    body: TableChange  # (cols, rows)


TEMPLATE_DIR = Path(__file__).parent / "templates"


def intersection[T](main: Iterable[T], other: Iterable[T]) -> frozenset[T]:
    """Compare two iterables and return the intersection of their elements."""
    return frozenset(main) & frozenset(other)


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


def compare_schema(old_schema: Schema, new_schema: Schema) -> Iterator[Change]:
    """Compare column definitions and return modifications, additions, and removals."""
    old_column_by_name = {column["name"]: column for column in old_schema.rows}
    new_column_by_name = {column["name"]: column for column in new_schema.rows}

    return chain(
        (
            Change(new_column_by_name[name], old_column_by_name[name])
            for name in old_column_by_name.keys() & new_column_by_name.keys()
            if old_column_by_name[name] != new_column_by_name[name]
        ),
        (
            Change(new=new_column_by_name[name])
            for name in new_column_by_name.keys() - old_column_by_name.keys()
        ),
        (
            Change(old=old_column_by_name[name])
            for name in old_column_by_name.keys() - new_column_by_name.keys()
        ),
    )


def row_guid(row: Row) -> str | None:
    """Check if row has a non-null RowGUID column."""
    return row["RowGUID"] if "RowGUID" in row.keys() else None  # noqa: SIM118


def row_content(row: Row, primary_keys: Iterable[str]) -> tuple[ValueType, ...]:
    """Extract a tuple of non-key values from a Row for content comparison."""
    return tuple(
        row[column]
        for column in row.keys()  # noqa: SIM118
        if column not in primary_keys
    )


def row_key(row: Iterable[ValueType]) -> ComparisonKey:
    """Extract a tuple of values for sorting/comparison from a Row."""
    return tuple((value is None, value) for value in row)


def get_match_keys(
    old_row: Row,
    new_row: Row,
    primary_keys: Iterable[str],
    columns: Iterable[str],
) -> tuple[RowValues, RowValues]:
    """Get the comparison keys for two rows based on hierarchical matching rules."""
    old_guid = row_guid(old_row)
    new_guid = row_guid(new_row)

    if old_guid and new_guid:
        return (old_guid,), (new_guid,)

    old_content = tuple(old_row[column] for column in columns)
    new_content = tuple(new_row[column] for column in columns)

    if old_content == new_content:
        return old_content, new_content

    if primary_keys:
        return (
            (old_row[column] for column in primary_keys),
            (new_row[column] for column in primary_keys),
        )

    return old_content, new_content


def compare_rows(
    old: Iterator[Row],
    new: Iterator[Row],
    primary_keys: Iterable[str],
    columns: Iterable[str],
) -> Iterator[Change]:
    """Memory-efficient merge-based comparison."""
    old_row = next(old, None)
    new_row = next(new, None)

    # Phase 1: Normal merge algorithm
    while old_row or new_row:
        if not old_row:
            yield Change(new=new_row)
            new_row = next(new, None)
        elif not new_row:
            yield Change(old=old_row)
            old_row = next(old, None)
        else:
            old_key, new_key = get_match_keys(old_row, new_row, primary_keys, columns)
            old_match_key, new_match_key = row_key(old_key), row_key(new_key)

            if old_match_key == new_match_key:
                # Same key - check if content differs
                if old_row != new_row:
                    yield Change(new=new_row, old=old_row)
                old_row = next(old, None)
                new_row = next(new, None)
            elif old_match_key < new_match_key:
                # Old key is smaller - row was removed
                yield Change(old=old_row)
                old_row = next(old, None)
            else:
                # New key is smaller - row was added
                yield Change(new=new_row)
                new_row = next(new, None)


def compare_table_rows(
    old_table: TableType,
    new_table: TableType,
    cmp_db: ComparisonDatabase,
) -> Iterator[Change]:
    """Compare table data using consistent sort/comparison key strategy.

    Strategy: Always sort and compare by the same key hierarchy:
    1. RowGUID (if present in both tables)
    2. Common primary keys (if any exist)
    3. All common columns (fallback)

    Uses SQL EXCEPT to pre-filter to only different rows, then applies Python matching.
    """
    shared_columns = intersection(old_table.columns, new_table.columns)
    shared_primary_keys = intersection(old_table.primary_keys, new_table.primary_keys)
    # Use SQL-filtered rows - only rows that are different
    old_rows = cmp_db.difference(old_table, new_table)
    new_rows = cmp_db.difference(new_table, old_table)
    return compare_rows(old_rows, new_rows, shared_primary_keys, shared_columns)


def compare_tables(
    old_table: Table,
    new_table: Table,
    cmp_db: ComparisonDatabase,
) -> TableChange:
    """Compare a table that exists in both databases."""
    return TableChange(
        ChangeSet(
            headers=Header(old_table.schema.columns, new_table.schema.columns),
            changes=compare_schema(old_table.schema, new_table.schema),
        ),
        ChangeSet(
            headers=Header(new_table.columns, old_table.columns),
            changes=compare_table_rows(old_table, new_table, cmp_db),
        ),
    )


def added_table(new_table: Table) -> TableChange:
    """Handle a table that was added, returning (cols_changeset, rows_changeset)."""
    return TableChange(
        ChangeSet(
            headers=Header(new=new_table.schema.columns),
            changes=(Change(new=column) for column in new_table.schema.rows),
        ),
        ChangeSet(
            headers=Header(new=new_table.columns),
            changes=(Change(new=row) for row in new_table.rows),
        ),
    )


def removed_table(old_table: Table) -> TableChange:
    """Handle a table that was removed, returning (cols_changeset, rows_changeset)."""
    return TableChange(
        ChangeSet(
            headers=Header(old=old_table.schema.columns),
            changes=(Change(old=column) for column in old_table.schema.rows),
        ),
        ChangeSet(
            headers=Header(old=old_table.columns),
            changes=(Change(old=row) for row in old_table.rows),
        ),
    )


def compare_databases(old_location: Path, new_location: Path) -> Iterator[Comparison]:
    """Compare two SQLite databases and return differences."""
    cmp_db = ComparisonDatabase(old_location, new_location)
    return chain(
        # Common tables - use main.py compare_tables with SQL-filtered rows
        (
            Comparison(old_table.name, compare_tables(old_table, new_table, cmp_db))
            for old_table, new_table in cmp_db.common_tables
        ),
        # Added tables - get Table objects directly
        (Comparison(table.name, added_table(table)) for table in cmp_db.added_tables),
        # Removed tables - get Table objects directly
        (
            Comparison(table.name, removed_table(table))
            for table in cmp_db.removed_tables
        ),
    )
