"""Database comparison functionality."""

from collections.abc import Iterator
from pathlib import Path
from sqlite3 import connect
from types import TracebackType
from typing import Self

from .inspection import Database, Table
from .query import attach


class DatabaseDifference:
    """A comparison context with two databases attached for cross-database actions."""

    def __init__(self, old_location: Path, new_location: Path) -> None:
        """Initialize the comparison context."""
        self._connection = connect(":memory:", uri=True)
        self.old = self._attach_database("old", old_location)
        self.new = self._attach_database("new", new_location)

    def _attach_database(self, database_name: str, database_location: Path) -> Database:
        self._connection.execute(
            attach(f"file:{database_location}?mode=ro", database_name),
        )
        return Database(self._connection, database_name)

    def close(self) -> None:
        """Close the connection, releasing both attached database files.

        Windows keeps a lock on an open file, so leaving this connection open
        blocks callers from moving or deleting the databases afterwards.
        """
        self._connection.close()

    def __enter__(self) -> Self:
        """Enter the comparison context."""
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Close the connection on leaving the context."""
        self.close()

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
