"""Database comparison functionality."""

from collections.abc import Iterator
from pathlib import Path
from sqlite3 import Connection, Row, connect

from compare.inspector import ComparableDatabase, ComparableTable, Table


def attach_schema(
    connection: Connection,
    location: Path,
    schema_name: str,
) -> ComparableDatabase:
    """Attach a database at the given location to the given connection."""
    connection.execute(f"ATTACH ? AS {schema_name}", (f"file:{location}?mode=ro",))
    return ComparableDatabase(connection, schema_name)


class ComparisonDatabase:
    """A comparison context with two databases attached for cross-database actions."""

    def __init__(self, old_location: Path, new_location: Path) -> None:
        """Initialize the comparison context."""
        self._connection = connect(":memory:", uri=True)
        self._connection.row_factory = Row
        self.old = attach_schema(self._connection, old_location, "old")
        self.new = attach_schema(self._connection, new_location, "new")

    @property
    def added_tables(self) -> Iterator[Table]:
        """Tables that exist in new but not old."""
        return (self.new.table(name) for name in self.new.tables - self.old.tables)

    @property
    def removed_tables(self) -> Iterator[Table]:
        """Tables that exist in old but not new."""
        return (self.old.table(name) for name in self.old.tables - self.new.tables)

    @property
    def comparable_tables(self) -> Iterator[tuple[ComparableTable, ComparableTable]]:
        """Comparable table pairs that exist in both databases."""
        return (
            (self.old.comparable_table(name), self.new.comparable_table(name))
            for name in self.old.tables & self.new.tables
        )
