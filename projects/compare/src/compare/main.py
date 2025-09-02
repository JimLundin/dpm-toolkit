"""Main database comparison functionality."""

import json
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from itertools import chain
from pathlib import Path
from sqlite3 import Row
from typing import Any, NamedTuple

from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja2.environment import TemplateStream

from compare.comparison import DatabaseDifference
from compare.inspection import Table

type ValueType = str | int | float | None
type IndexKey = tuple[ValueType, ...]
type IndexKeys = list[IndexKey]
type Indexer = Callable[[Row], IndexKeys]


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


def create_row_indexer(
    primary_keys: Iterable[str],
    shared_columns: Iterable[str],
) -> Indexer:
    """Create indexer for hierarchical row matching."""
    pk_list = list(primary_keys)
    col_list = list(shared_columns)

    def indexer(row: Row) -> IndexKeys:
        keys: IndexKeys = []

        # Priority 1: RowGUID (if exists)
        guid = row_guid(row)
        if guid:
            keys.append((guid,))

        # Priority 2: Primary keys (if exist)
        if pk_list:
            pk_values = tuple(row[col] for col in pk_list)
            keys.append(pk_values)

        # Priority 3: Full content (if columns exist)
        if col_list:
            content_values = tuple(row[col] for col in col_list)
            keys.append(content_values)

        return keys

    return indexer


def compare_rows(
    old: Iterator[Row],
    new: Iterator[Row],
    indexer: Indexer,
) -> Iterator[Change]:
    """Asymmetric hierarchical matching: build index for new rows, match old rows."""
    new_rows = list(new)

    # Build index: (priority, key) -> row_indices (as deque for O(1) pop)

    index: dict[tuple[int, IndexKey], deque[int]] = {}
    for i, row in enumerate(new_rows):
        for priority, key in enumerate(indexer(row)):
            index.setdefault((priority, key), deque()).append(i)

    # Track consumed new rows
    used: set[int] = set()

    # Match each old row to best available new row
    for old_row in old:
        matched = False

        for priority, key in enumerate(indexer(old_row)):
            candidates = index.get((priority, key))
            if candidates:
                # Find first unused candidate
                while candidates:
                    new_idx = candidates.popleft()
                    if new_idx not in used:
                        used.add(new_idx)
                        new_row = new_rows[new_idx]

                        yield Change(old=old_row, new=new_row)
                        matched = True
                        break

                if matched:
                    break

        if not matched:
            yield Change(old=old_row)

    # Emit remaining new rows as additions
    for i, row in enumerate(new_rows):
        if i not in used:
            yield Change(new=row)


def compare_tables(old_table: Table, new_table: Table) -> Iterator[Change]:
    """Compare table data using hierarchical indexing approach."""
    shared_columns = intersection(old_table.columns, new_table.columns)
    shared_primary_keys = intersection(old_table.primary_keys, new_table.primary_keys)

    old_rows = old_table.difference(new_table)
    new_rows = new_table.difference(old_table)

    # Create hierarchical indexer
    indexer = create_row_indexer(shared_primary_keys, shared_columns)

    return compare_rows(old_rows, new_rows, indexer)


def modified_changes(old_table: Table, new_table: Table) -> ChangeSet:
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
    difference = DatabaseDifference(old_location, new_location)
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
            for old_table, new_table in difference.common_tables
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
            for table in difference.added_tables
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
            for table in difference.removed_tables
        ),
    )
