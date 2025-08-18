"""Database inspection functionality using sqlite3."""

from collections.abc import Iterator
from sqlite3 import Connection, Row

from compare.types import ColInfo


class Inspector:
    """Inspects SQLite databases to extract complete schema and data information."""

    def __init__(self, db: Connection) -> None:
        """Initialize inspector for a database file."""
        self.conn = db
        self.conn.row_factory = Row  # Enable named access to columns

    def tables(self) -> Iterator[str]:
        """Get all table names in the database."""
        with self.conn as conn:
            cursor = conn.execute(
                "SELECT name "
                "FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "ORDER BY name",
            )
            return (row["name"] for row in cursor)

    def cols(self, name: str) -> Iterator[ColInfo]:
        """Get complete column information for a table."""
        with self.conn as conn:
            cursor = conn.execute("SELECT * FROM pragma_table_info(?)", (name,))

            return (
                ColInfo(
                    name=row["name"],
                    type=row["type"],
                    nullable=not bool(row["notnull"]),
                    default=row["dflt_value"],
                    primary_key=bool(row["pk"]),
                )
                for row in cursor
            )

    def pks(self, name: str) -> Iterator[str]:
        """Get primary key column names for a table."""
        with self.conn as conn:
            cursor = conn.execute(
                "SELECT name FROM pragma_table_info(?) WHERE pk",
                (name,),
            )

            return (row["name"] for row in cursor)

    def rows(self, name: str) -> Iterator[Row]:
        """Get all data from a table as list of dictionaries."""
        with self.conn as conn:
            pk_cols = self.pks(name)
            order = ", ".join(f"`{col}`" for col in pk_cols) or "rowid"

            return conn.execute(
                f"SELECT * FROM `{name}` ORDER BY {order}",  # noqa: S608
            )
