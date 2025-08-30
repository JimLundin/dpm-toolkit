"""Database inspection functionality using sqlite3."""

from collections.abc import Iterator
from pathlib import Path
from sqlite3 import Connection, Row

TABLE_INFO_COLUMNS = ("cid", "name", "type", "notnull", "dflt_value", "pk")


class Schema:
    """Represents the schema metadata for a table (simplified table)."""

    def __init__(self, connection: Connection, qualified_table_name: str) -> None:
        """Initialize the schema table."""
        self._connection = connection
        self._connection.row_factory = Row
        self._qualified_table_name = qualified_table_name
        self.qualified_name = f"pragma_table_info('{self._qualified_table_name}')"

    def rows(self) -> Iterator[Row]:
        """Return schema metadata rows for this table."""
        return self._connection.execute(
            "SELECT * FROM pragma_table_info(?)",
            (self._qualified_table_name,),
        )

    def columns(self) -> Iterator[str]:
        """Return the column names of the schema metadata (TABLE_INFO_COLUMNS)."""
        return iter(TABLE_INFO_COLUMNS)


class Table:
    """Represents a specific table within a SQLite database."""

    def __init__(self, connection: Connection, name: str, schema: str = "main") -> None:
        """Initialize table with a database connection, table name, and schema."""
        self._connection = connection
        self._connection.row_factory = Row
        self._name = name
        self._schema = schema
        # Use square brackets to properly escape identifiers (handles reserved keywords)
        if self._schema != "main":
            self.qualified_name = f'{self._schema}."{self._name}"'
        else:
            self.qualified_name = f'"{self._name}]"'

    def rows(self) -> Iterator[Row]:
        """Return all rows from this table."""
        return self._connection.execute(
            f"SELECT * FROM {self.qualified_name}",  # noqa: S608
        )

    def columns(self) -> Iterator[str]:
        """Return column names for this table."""
        return (row["name"] for row in self.schema().rows())

    def schema(self) -> Schema:
        """Return a SchemaTable representing this table's metadata."""
        return Schema(self._connection, self.qualified_name)

    def primary_keys(self) -> Iterator[str]:
        """Return primary key column names for this table."""
        return (row["name"] for row in self.schema().rows() if row["pk"])


class Database:
    """Inspects SQLite databases to extract schema information."""

    def __init__(self, connection: Connection, schema: str = "main") -> None:
        """Initialize inspector with a database connection and schema name."""
        self._connection = connection
        self.schema = schema
        self._connection.row_factory = Row

    def path(self) -> Path:
        """Path of the SQLite database for this schema."""
        database = self._connection.execute(
            "SELECT file FROM pragma_database_list WHERE name=?",
            (self.schema,),
        ).fetchone()
        return Path(database["file"])

    def tables(self) -> Iterator[str]:
        """Return all user table names for this schema, excluding system tables."""
        schema_ref = (
            f"{self.schema}.sqlite_schema" if self.schema != "main" else "sqlite_schema"
        )

        cursor = self._connection.execute(
            f"""
            SELECT name FROM {schema_ref}
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """,  # noqa: S608
        )
        return (row["name"] for row in cursor)

    def table(self, table_name: str) -> Table:
        """Return a Table instance for the given table name in this schema."""
        return Table(self._connection, table_name, self.schema)


type TableType = Table | Schema
