"""Main database comparison functionality."""

import json
from collections.abc import Iterable, Iterator
from itertools import chain
from pathlib import Path
from sqlite3 import Connection, Row
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja2.environment import TemplateStream

from compare.inspector import TABLE_INFO_COLUMNS, DatabaseInspector, TableInspector
from compare.types import (
    Change,
    ChangeSet,
    Comparison,
    Header,
    TableChange,
    ValueType,
)

type RowValues = Iterable[ValueType]
type ComparisonKey = tuple[tuple[bool, ValueType], ...]

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


def compare_columns(
    old_columns: Iterable[Row],
    new_columns: Iterable[Row],
) -> Iterator[Change]:
    """Compare column definitions and return modifications, additions, and removals."""
    old_column_by_name = {column["name"]: column for column in old_columns}
    new_column_by_name = {column["name"]: column for column in new_columns}

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
    return compare_rows(old_rows, new_rows, shared_primary_keys, shared_columns)


def common_table(
    old_table: TableInspector,
    new_table: TableInspector,
) -> TableChange:
    """Compare a table that exists in both databases."""
    return TableChange(
        ChangeSet(
            headers=Header(TABLE_INFO_COLUMNS, TABLE_INFO_COLUMNS),
            changes=compare_columns(old_table.columns(), new_table.columns()),
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
            headers=Header(new=TABLE_INFO_COLUMNS),
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
            headers=Header(old=TABLE_INFO_COLUMNS),
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
