"""Database inspection functionality using sqlite3."""

from collections.abc import Iterator
from sqlite3 import Connection, Row

TABLE_INFO_COLS = ("cid", "name", "type", "notnull", "dflt_value", "pk")


class Inspector:
    """Inspects SQLite databases to extract complete schema and data information."""

    def __init__(self, db: Connection) -> None:
        """Initialize inspector with a database connection."""
        self.conn = db
        self.conn.row_factory = Row

    def tables(self) -> Iterator[str]:
        """Return all user table names, excluding system tables."""
        with self.conn as conn:
            cursor = conn.execute(
                "SELECT name "
                "FROM sqlite_schema "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' ",
            )
            return (row["name"] for row in cursor)

    def rows(self, name: str) -> Iterator[Row]:
        """Return all rows from the specified table, ordered by primary key."""
        with self.conn as conn:
            return conn.execute(f"SELECT * FROM `{name}`")  # noqa: S608

    def cols(self, name: str) -> Iterator[Row]:
        """Return column metadata for the specified table."""
        with self.conn as conn:
            return conn.execute("SELECT * FROM pragma_table_info(?)", (name,))

    def pks(self, name: str) -> Iterator[str]:
        """Return primary key column names for the specified table."""
        return (col["name"] for col in self.cols(name) if col["pk"])
