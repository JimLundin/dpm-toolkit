"""Main database comparison functionality."""

import json
from collections.abc import Iterable, Iterator
from itertools import chain
from pathlib import Path
from sqlite3 import Row
from typing import Any, NamedTuple

from jinja2 import Environment, FileSystemLoader, select_autoescape
from jinja2.environment import TemplateStream

from compare.comparison import DatabaseDifference
from compare.index import HierarchicalIndex, Indexer, IndexKeys
from compare.inspection import Table


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


def comparisons_to_summary(comparisons: Iterable[Comparison]) -> list[dict[str, Any]]:
    """Convert comparisons to summary data with change counts.

    Returns a list of dictionaries with table names and change counts,
    suitable for display in terminal tables or other summary views.

    Each dictionary contains:
        - name: table name
        - change_type: always "table_changes"
        - columns: count of column changes
        - rows: count of row changes
    """
    return [
        {
            "name": comp.name,
            "change_type": "table_changes",
            "columns": len(list(comp.body.columns.changes)),
            "rows": len(list(comp.body.rows.changes)),
        }
        for comp in comparisons
    ]


def create_row_indexer(
    primary_keys: Iterable[str],
    columns: Iterable[str],
) -> Indexer:
    """Create indexer for hierarchical row matching with fixed priority levels."""
    # Store the iterables for re-use across mutiple calls of the indexer
    columns = tuple(columns)
    primary_keys = tuple(primary_keys)

    def indexer(row: Row) -> IndexKeys:
        keys: IndexKeys = []

        # Priority 0: RowGUID - use special key prefix to ensure unique priority level
        if guid := row["RowGUID"] if "RowGUID" in row.keys() else None:  # noqa: SIM118
            keys.append(("GUID", (guid,)))

        # Priority 1: Primary keys - use special key prefix
        if primary_key_values := tuple(row[column] for column in primary_keys):
            keys.append(("PK", primary_key_values))

        # Priority 2: Full content - use special key prefix
        if content_values := tuple(row[column] for column in columns):
            keys.append(("CONTENT", content_values))

        return keys

    return indexer


def schema_indexer(row: Row) -> IndexKeys:
    """Indexer for schema/column comparisons."""
    keys: IndexKeys = []

    # Priority 0: Column name (primary matching criterion)
    column_name = row["name"]
    keys.append(("SCHEMA", (column_name,)))

    return keys


def compare_rows(
    old: Iterator[Row],
    new: Iterator[Row],
    indexer: Indexer,
) -> Iterator[Change]:
    """Hierarchical matching: build index for new rows, match old rows."""
    # Build hierarchical index
    index = HierarchicalIndex(indexer)
    for row in new:
        index.add(row)

    # Match each old row to best available new row
    for old_row in old:
        if new_row := index.pop(old_row):
            yield Change(old=old_row, new=new_row)
        else:
            yield Change(old=old_row)

    # Emit all remaining new rows as additions
    for new_row in index:
        yield Change(new=new_row)


def compare_contents(old_table: Table, new_table: Table) -> ChangeSet:
    """Compare content tables and return complete ChangeSet."""
    shared_columns = intersection(old_table.columns, new_table.columns) - {"RowGUID"}
    shared_primary_keys = intersection(old_table.primary_keys, new_table.primary_keys)
    content_indexer = create_row_indexer(shared_primary_keys, shared_columns)

    old_rows = old_table.difference(new_table)
    new_rows = new_table.difference(old_table)

    return ChangeSet(
        headers=Header(old=old_table.columns, new=new_table.columns),
        changes=compare_rows(old_rows, new_rows, content_indexer),
    )


def compare_schemas(old_table: Table, new_table: Table) -> ChangeSet:
    """Compare schema tables and return complete ChangeSet."""
    old_rows = old_table.difference(new_table)
    new_rows = new_table.difference(old_table)

    return ChangeSet(
        headers=Header(old=old_table.columns, new=new_table.columns),
        changes=compare_rows(old_rows, new_rows, schema_indexer),
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
        # Common tables - use separate comparison functions
        (
            Comparison(
                name=old_table.name,
                body=TableChange(
                    columns=compare_schemas(old_table.schema, new_table.schema),
                    rows=compare_contents(old_table, new_table),
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
