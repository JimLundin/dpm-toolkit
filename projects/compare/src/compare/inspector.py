"""Database inspection functionality using sqlite3."""

from collections.abc import Iterable, Iterator
from sqlite3 import Connection, Row

TABLE_INFO_COLS = ("cid", "name", "type", "notnull", "dflt_value", "pk")


class TableInspector:
    """Represents a specific table within a SQLite database."""

    def __init__(self, database: Connection, name: str) -> None:
        """Initialize table with a database connection and table name."""
        self._connection = database
        self.name = name
        self._connection.row_factory = Row

    def rows(self, sort_keys: Iterable[str] | None = None) -> Iterator[Row]:
        """Return all rows from this table, sorted by given sort keys."""
        with self._connection as conn:
            order = ", ".join(
                sort_keys or (column["name"] for column in self.columns()),
            )
            return conn.execute(
                f"SELECT * FROM `{self.name}` ORDER BY {order}",  # noqa: S608
            )

    def columns(self) -> Iterator[Row]:
        """Return column metadata for this table."""
        with self._connection as conn:
            return conn.execute("SELECT * FROM pragma_table_info(?)", (self.name,))

    def primary_keys(self) -> Iterator[str]:
        """Return primary key column names for this table."""
        return (column["name"] for column in self.columns() if column["pk"])


class DatabaseInspector:
    """Inspects SQLite databases to extract schema information."""

    def __init__(self, database: Connection) -> None:
        """Initialize inspector with a database connection."""
        self._connection = database
        self._connection.row_factory = Row

    def tables(self) -> Iterator[str]:
        """Return all user table names, excluding system tables."""
        with self._connection as conn:
            cursor = conn.execute(
                "SELECT name "
                "FROM sqlite_schema "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "ORDER BY name",
            )
            return (row["name"] for row in cursor)

    def table(self, table_name: str) -> TableInspector:
        """Return a Table instance for the given table name."""
        return TableInspector(self._connection, table_name)
