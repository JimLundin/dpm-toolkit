"""Database comparison functionality."""

from collections.abc import Iterator
from itertools import chain
from pathlib import Path
from sqlite3 import Row, connect

from compare.inspector import TABLE_INFO_COLUMNS, Database, Table, TableType
from compare.types import Change, ChangeSet, Header, TableChange


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

    def compare_table(self, old_table: Table, new_table: Table) -> TableChange:
        """Compare a table comprehensively using SQL operations."""
        # Schema comparison using the generalized column_difference method
        old_schema_diff = self.difference(old_table.schema(), new_table.schema())
        new_schema_diff = self.difference(new_table.schema(), old_table.schema())

        # Row comparison using the simplified table_difference method
        old_rows_diff = self.difference(old_table, new_table)
        new_rows_diff = self.difference(new_table, old_table)

        return TableChange(
            ChangeSet(
                headers=Header(TABLE_INFO_COLUMNS, TABLE_INFO_COLUMNS),
                changes=chain(
                    (Change(old=row) for row in old_schema_diff),
                    (Change(new=row) for row in new_schema_diff),
                ),
            ),
            ChangeSet(
                headers=Header(
                    old=old_table.columns(),
                    new=new_table.columns(),
                ),
                changes=chain(
                    (Change(old=row) for row in old_rows_diff),
                    (Change(new=row) for row in new_rows_diff),
                ),
            ),
        )
