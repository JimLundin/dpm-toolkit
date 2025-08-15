"""Database inspection functionality using sqlite3."""

import sqlite3
from pathlib import Path

from .types import ColumnInfo, ValueType


class DatabaseInspector:
    """Inspects SQLite databases to extract complete schema and data information."""

    def __init__(self, db: Path) -> None:
        """Initialize inspector for a database file."""
        self.path = db
        if not self.path.exists():
            msg = f"Database file not found: {db}"
            raise FileNotFoundError(msg)

    def get_connection(self) -> sqlite3.Connection:
        """Get a connection to the database."""
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
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
            return [row[0] for row in cursor.fetchall()]

    def get_table_columns(self, name: str) -> list[ColumnInfo]:
        """Get complete column information for a table."""
        with self.get_connection() as conn:
            cursor = conn.execute(f"PRAGMA table_info({name})")

            return [
                ColumnInfo(
                    name=row["name"],
                    type=row["type"],
                    nullable=not bool(row["notnull"]),
                    default=row["dflt_value"],
                )
                for row in cursor.fetchall()
            ]

    def get_primary_key_columns(self, name: str) -> list[str]:
        """Get primary key column names for a table."""
        with self.get_connection() as conn:
            cursor = conn.execute(f"PRAGMA table_info({name})")

            return [row["name"] for row in cursor.fetchall() if row["pk"]]

    def get_all_table_data(self, name: str) -> list[dict[str, ValueType]]:
        """Get all data from a table as list of dictionaries."""
        with self.get_connection() as conn:
            # Order by primary key columns for consistent ordering
            pk_columns = self.get_primary_key_columns(name)

            if pk_columns:
                order = f"ORDER BY {', '.join(f'`{col}`' for col in pk_columns)}"
            else:
                order = "ORDER BY rowid"

            cursor = conn.execute(f"SELECT * FROM `{name}` {order}")  # noqa: S608
            return [dict(row) for row in cursor.fetchall()]

    def get_table_row_count(self, name: str) -> int:
        """Get the number of rows in a table."""
        with self.get_connection() as conn:
            cursor = conn.execute(f"SELECT COUNT(*) FROM `{name}`")  # noqa: S608
            result = cursor.fetchone()
            return int(result[0]) or 0
