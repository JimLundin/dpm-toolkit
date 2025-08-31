"""Database comparison functionality."""

from collections.abc import Iterator
from pathlib import Path
from sqlite3 import Row, connect

from compare.inspector import Database, Table, TableType


class ComparisonDatabase:
    """A comparison context with two databases attached for cross-database actions."""

    def __init__(self, old_location: Path, new_location: Path) -> None:
        """Initialize the comparison context."""
        self._connection = connect(":memory:", uri=True)
        self._connection.execute("ATTACH ? AS old", (f"file:{old_location}?mode=ro",))
        self._connection.execute("ATTACH ? AS new", (f"file:{new_location}?mode=ro",))
        # Create schema-aware Database instances for the attached databases
        self.old = Database(self._connection, "old")
        self.new = Database(self._connection, "new")

    def added_tables(self) -> Iterator[Table]:
        """Tables that exist in new but not old."""
        old_names = frozenset(self.old.tables())
        new_names = frozenset(self.new.tables())
        added_names = new_names - old_names
        return (self.new.table(name) for name in added_names)

    def removed_tables(self) -> Iterator[Table]:
        """Tables that exist in old but not new."""
        old_names = frozenset(self.old.tables())
        new_names = frozenset(self.new.tables())
        removed_names = old_names - new_names
        return (self.old.table(name) for name in removed_names)

    def common_table_pairs(self) -> Iterator[tuple[Table, Table]]:
        """Table pairs that exist in both databases (old_table, new_table)."""
        old_names = frozenset(self.old.tables())
        new_names = frozenset(self.new.tables())
        common_names = old_names & new_names
        return ((self.old.table(name), self.new.table(name)) for name in common_names)

    def difference(self, main: TableType, other: TableType) -> Iterator[Row]:
        """Compare table rows using SQL EXCEPT operations."""
        return self._connection.execute(
            f"""
        SELECT * FROM {main.qualified_name}
        EXCEPT
        SELECT * FROM {other.qualified_name}
        """,  # noqa: S608
        )
