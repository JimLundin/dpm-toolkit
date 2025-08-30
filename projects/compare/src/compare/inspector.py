"""Database inspection functionality using sqlite3."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from sqlite3 import Connection, Row
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator

TABLE_INFO_COLUMNS = ("cid", "name", "type", "notnull", "dflt_value", "pk")


class Table:
    """Represents a specific table within a SQLite database."""

    def __init__(self, database: Connection, name: str) -> None:
        """Initialize table with a database connection and table name."""
        self._connection = database
        self.name = name
        self._connection.row_factory = Row

    def rows(self, sort_keys: Iterable[str] | None = None) -> Iterator[Row]:
        """Return all rows from this table, sorted by given sort keys."""
        order = ", ".join(
            sort_keys or (column["name"] for column in self.columns()),
        )
        return self._connection.execute(
            f"SELECT * FROM `{self.name}` ORDER BY {order}",  # noqa: S608
        )

    def columns(self) -> Iterator[Row]:
        """Return column metadata for this table."""
        return self._connection.execute(
            "SELECT * FROM pragma_table_info(?)",
            (self.name,),
        )

    def primary_keys(self) -> Iterator[str]:
        """Return primary key column names for this table."""
        return (column["name"] for column in self.columns() if column["pk"])


class Database:
    """Inspects SQLite databases to extract schema information."""

    def __init__(self, database: Connection) -> None:
        """Initialize inspector with a database connection."""
        self._connection = database
        self._connection.row_factory = Row

    def path(self) -> Path:
        """Path of the main SQLite database on the connection."""
        database = self._connection.execute(
            "SELECT file FROM pragma_database_list WHERE name='main'",
        ).fetchone()
        return Path(database["file"])

    def tables(self) -> Iterator[str]:
        """Return all user table names, excluding system tables."""
        cursor = self._connection.execute(
            "SELECT name "
            "FROM sqlite_schema "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name",
        )
        return (row["name"] for row in cursor)

    def table(self, table_name: str) -> Table:
        """Return a Table instance for the given table name."""
        return Table(self._connection, table_name)

    @contextmanager
    def attach(self, target: Database) -> Generator[None]:
        """Attach the target SQLite database to the current connection."""
        target_location = target.path()
        target_identifier = f"file:{target_location}?mode=ro"
        try:
            self._connection.execute("ATTACH ? AS other", (target_identifier,))
            yield
        finally:
            self._connection.execute("DETACH other")
