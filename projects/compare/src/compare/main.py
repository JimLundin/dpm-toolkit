"""Main database comparison functionality."""

import json
from collections.abc import Callable, Iterable, Iterator
from itertools import chain
from pathlib import Path
from sqlite3 import Row
from typing import Any, NamedTuple

from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja2.environment import TemplateStream

from compare.comparison import ComparisonDatabase
from compare.inspector import ComparableTable, Table

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


def row_guid(row: Row) -> str | None:
    """Check if row has a non-null RowGUID column."""
    return row["RowGUID"] if "RowGUID" in row.keys() else None  # noqa: SIM118


def row_content(row: Row, content_columns: Iterable[str]) -> tuple[ValueType, ...]:
    """Extract a tuple of non-key values from a Row for content comparison."""
    return tuple(row[column] for column in content_columns)


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

    old_content = row_content(old_row, columns)
    new_content = row_content(new_row, columns)

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
    matcher: Callable[[Row, Row], tuple[RowValues, RowValues]],
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
            old_key, new_key = matcher(old_row, new_row)
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


def compare_tables(
    old_table: ComparableTable,
    new_table: ComparableTable,
) -> Iterator[Change]:
    """Compare table data using consistent sort/comparison key strategy.

    Uses SQL EXCEPT to pre-filter to only different rows, then applies Python matching.
    """
    shared_columns = intersection(old_table.columns, new_table.columns)
    shared_primary_keys = intersection(old_table.primary_keys, new_table.primary_keys)
    # Use ComparableTable.difference() method - only rows that are different
    old_rows = old_table.difference(new_table)
    new_rows = new_table.difference(old_table)

    def matcher(old: Row, new: Row) -> tuple[RowValues, RowValues]:
        return get_match_keys(old, new, shared_primary_keys, shared_columns)

    return compare_rows(old_rows, new_rows, matcher)


def modified_changes(
    old_table: ComparableTable,
    new_table: ComparableTable,
) -> ChangeSet:
    """Return the changeset for a modified table."""
    return ChangeSet(
        headers=Header(old=old_table.columns, new=new_table.columns),
        changes=compare_tables(old_table, new_table),
    )


def new_changes(table: Table) -> ChangeSet:
    """Return the changeset for a new table."""
    return ChangeSet(
        headers=Header(new=table.columns),
        changes=(Change(new=row) for row in table.rows),
    )


def old_changes(table: Table) -> ChangeSet:
    """Return the changest for a removed table."""
    return ChangeSet(
        headers=Header(old=table.columns),
        changes=(Change(old=row) for row in table.rows),
    )


def compare_databases(old_location: Path, new_location: Path) -> Iterator[Comparison]:
    """Compare two SQLite databases and return differences."""
    cmp_db = ComparisonDatabase(old_location, new_location)
    return chain(
        # Common tables - use main.py compare_tables with ComparableTable instances
        (
            Comparison(
                name=old_table.name,
                body=TableChange(
                    columns=modified_changes(old_table.schema, new_table.schema),
                    rows=modified_changes(old_table, new_table),
                ),
            )
            for old_table, new_table in cmp_db.comparable_tables
        ),
        # Added tables - get Table objects directly
        (
            Comparison(
                name=table.name,
                body=TableChange(
                    columns=new_changes(table.schema),
                    rows=new_changes(table),
                ),
            )
            for table in cmp_db.added_tables
        ),
        # Removed tables - get Table objects directly
        (
            Comparison(
                name=table.name,
                body=TableChange(
                    columns=old_changes(table.schema),
                    rows=old_changes(table),
                ),
            )
            for table in cmp_db.removed_tables
        ),
    )
