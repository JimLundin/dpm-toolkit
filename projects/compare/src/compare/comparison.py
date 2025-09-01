"""Database comparison functionality."""

from collections.abc import Iterator
from pathlib import Path
from sqlite3 import Connection, Row, connect

from compare.inspector import ComparableTable, Database, Table


def attach_database(
    connection: Connection,
    location: Path,
    database_name: str,
) -> Database:
    """Attach a database at the given location to the given connection."""
    connection.execute(f"ATTACH ? AS {database_name}", (f"file:{location}?mode=ro",))
    return Database(connection, database_name)


class ComparisonDatabase:
    """A comparison context with two databases attached for cross-database actions."""

    def __init__(self, old_location: Path, new_location: Path) -> None:
        """Initialize the comparison context."""
        self._connection = connect(":memory:", uri=True)
        self._connection.row_factory = Row
        self.old = attach_database(self._connection, old_location, "old")
        self.new = attach_database(self._connection, new_location, "new")

    def comparable_table(self, table_name: str, database_name: str) -> ComparableTable:
        """Return a ComparableTable which allows for table comparisons."""
        return ComparableTable(self._connection, table_name, database_name)

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
        """Create fresh ComparableTable instances for comparison."""
        return (
            (self.comparable_table(name, "old"), self.comparable_table(name, "new"))
            for name in self.old.tables & self.new.tables
        )
