"""Database inspection functionality using sqlite3."""

from pathlib import Path
from sqlite3 import Connection, Row, connect

from compare.types import ColumnInfo, ValueType


class DatabaseInspector:
    """Inspects SQLite databases to extract complete schema and data information."""

    def __init__(self, db: Path) -> None:
        """Initialize inspector for a database file."""
        self.conn: Connection | None = None
        self.path = db
        if not self.path.exists():
            msg = f"Database file not found: {db}"
            raise FileNotFoundError(msg)

    def get_connection(self) -> Connection:
        """Get a connection to the database."""
        if self.conn is not None:
            return self.conn
        # Create a new connection and set row factory to Row for named access
        conn = connect(self.path)
        conn.row_factory = Row  # Enable column access by name
        self.conn = conn
        return conn

    def get_tables(self) -> list[str]:
        """Get all table names in the database."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT name "
                "FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "ORDER BY name",
            )
            return [row[0] for row in cursor]

    def get_table_columns(self, name: str) -> list[ColumnInfo]:
        """Get complete column information for a table."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM pragma_table_info(?)", (name,))

            return [
                ColumnInfo(
                    name=row["name"],
                    type=row["type"],
                    nullable=not bool(row["notnull"]),
                    default=row["dflt_value"],
                )
                for row in cursor
            ]

    def get_primary_key_columns(self, name: str) -> list[str]:
        """Get primary key column names for a table."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM pragma_table_info(?)", (name,))

            return [row["name"] for row in cursor if row["pk"]]

    def get_all_table_data(self, name: str) -> list[dict[str, ValueType]]:
        """Get all data from a table as list of dictionaries."""
        with self.get_connection() as conn:
            # Order by primary key columns for consistent ordering
            pk_cols = self.get_primary_key_columns(name)

            order = ", ".join(f"`{col}`" for col in pk_cols) if pk_cols else "rowid"

            cursor = conn.execute(
                f"SELECT * FROM `{name}` ORDER BY {order}",  # noqa: S608
            )
            return list(cursor)
