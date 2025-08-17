"""Database inspection functionality using sqlite3."""

from collections.abc import Generator, Iterator
from pathlib import Path
from sqlite3 import Row, connect

from compare.types import ColumnInfo, ValueType


class Inspector:
    """Inspects SQLite databases to extract complete schema and data information."""

    def __init__(self, db: Path) -> None:
        """Initialize inspector for a database file."""
        self.conn = connect(db)
        self.conn.row_factory = Row  # Enable named access to columns

    def tables(self) -> Generator[str]:
        """Get all table names in the database."""
        with self.conn as conn:
            cursor = conn.execute(
                "SELECT name "
                "FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            )
            return (row[0] for row in cursor)

    def columns(self, name: str) -> Generator[ColumnInfo]:
        """Get complete column information for a table."""
        with self.conn as conn:
            cursor = conn.execute("SELECT * FROM pragma_table_info(?)", (name,))

            return (
                ColumnInfo(
                    name=row["name"],
                    type=row["type"],
                    nullable=not bool(row["notnull"]),
                    default=row["dflt_value"],
                    primary_key=bool(row["pk"]),
                )
                for row in cursor
            )

    def primary_key(self, name: str) -> Generator[str]:
        """Get primary key column names for a table."""
        with self.conn as conn:
            cursor = conn.execute(
                "SELECT name FROM pragma_table_info(?) WHERE pk",
                (name,),
            )

            return (row[0] for row in cursor)

    def data(self, name: str) -> Iterator[dict[str, ValueType]]:
        """Get all data from a table as list of dictionaries."""
        with self.conn as conn:
            # Order by primary key columns for consistent ordering
            pk_cols = self.primary_key(name)

            order = ", ".join(f"`{col}`" for col in pk_cols) or "rowid"

            return conn.execute(
                f"SELECT * FROM `{name}` ORDER BY {order}",  # noqa: S608
            )
