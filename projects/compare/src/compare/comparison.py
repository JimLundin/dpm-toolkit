"""Database comparison functionality."""

from collections.abc import Iterator
from pathlib import Path
from sqlite3 import Row, connect

from compare.inspection import Database, Table


class ComparisonDatabase:
    """A comparison context with two databases attached for cross-database actions."""

    def __init__(self, old_location: Path, new_location: Path) -> None:
        """Initialize the comparison context."""
        self._connection = connect(":memory:", uri=True)
        self._connection.row_factory = Row
        self.old = self._attach_database(old_location, "old")
        self.new = self._attach_database(new_location, "new")

    def _attach_database(self, location: Path, database_name: str) -> Database:
        self._connection.execute(
            f"ATTACH ? AS {database_name}",
            (f"file:{location}?mode=ro",),
        )
        return Database(self._connection, database_name)

    @property
    def added_tables(self) -> Iterator[Table]:
        """Tables that exist in new but not old."""
        return (self.new.table(name) for name in self.new.tables - self.old.tables)

    @property
    def removed_tables(self) -> Iterator[Table]:
        """Tables that exist in old but not new."""
        return (self.old.table(name) for name in self.old.tables - self.new.tables)

    @property
    def common_tables(self) -> Iterator[tuple[Table, Table]]:
        """Tables that exist in both old and new."""
        return (
            (self.old.table(name), self.new.table(name))
            for name in self.old.tables & self.new.tables
        )
