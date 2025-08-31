"""Database inspection functionality using sqlite3."""

from collections.abc import Iterator
from functools import cached_property
from sqlite3 import Connection, Row


class Schema:
    """Represents the schema metadata for a table (simplified table)."""

    def __init__(
        self,
        connection: Connection,
        table_name: str,
        schema: str = "main",
    ) -> None:
        """Initialize the schema table."""
        self._connection = connection
        self._connection.row_factory = Row
        self._table_name = table_name
        self._schema = schema
        self.qualified_name = f"pragma_table_info({self._table_name}, {self._schema})"

    @cached_property
    def rows(self) -> tuple[Row, ...]:
        """Return schema metadata rows for this table."""
        return tuple(
            self._connection.execute(
                "SELECT * FROM pragma_table_info(?, ?)",
                (self._table_name, self._schema),
            ),
        )

    @cached_property
    def columns(self) -> tuple[str, ...]:
        """Return the column names of the schema metadata."""
        return ("cid", "name", "type", "notnull", "dflt_value", "pk")

    @cached_property
    def primary_keys(self) -> tuple[str, ...]:
        """Return the names that represent the unique key of the metadata table."""
        return ("name",)


class Table:
    """Represents a specific table within a SQLite database."""

    def __init__(
        self,
        connection: Connection,
        table_name: str,
        schema_name: str = "main",
    ) -> None:
        """Initialize table with a database connection, table name, and schema."""
        self._connection = connection
        self._connection.row_factory = Row
        self.name = table_name
        self.schema = Schema(connection, table_name, schema_name)
        self.qualified_name = f'{schema_name}."{table_name}"'

    @property
    def rows(self) -> Iterator[Row]:
        """Return all rows from this table."""
        return self._connection.execute(
            f"SELECT * FROM {self.qualified_name}",  # noqa: S608
        )

    @cached_property
    def columns(self) -> tuple[str, ...]:
        """Return column names for this table."""
        return tuple(row["name"] for row in self.schema.rows)

    @cached_property
    def primary_keys(self) -> tuple[str, ...]:
        """Return primary key column names for this table."""
        return tuple(row["name"] for row in self.schema.rows if row["pk"])


class Database:
    """Inspects SQLite databases to extract schema information."""

    def __init__(self, connection: Connection, schema_name: str = "main") -> None:
        """Initialize inspector with a database connection and schema name."""
        self._connection = connection
        self._connection.row_factory = Row
        self._schema_name = schema_name
        self._table_cache: dict[str, Table] = {}

    @cached_property
    def tables(self) -> frozenset[str]:
        """Return all user table names for this schema, excluding system tables."""
        schema_ref = (
            f"{self._schema_name}.sqlite_schema"
            if self._schema_name != "main"
            else "sqlite_schema"
        )

        cursor = self._connection.execute(
            f"""
            SELECT name FROM {schema_ref}
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """,  # noqa: S608
        )
        return frozenset(row["name"] for row in cursor)

    def table(self, table_name: str) -> Table:
        """Return a Table instance for the given table name in this schema."""
        if table_name not in self._table_cache:
            self._table_cache[table_name] = Table(
                self._connection,
                table_name,
                self._schema_name,
            )
        return self._table_cache[table_name]


type TableType = Table | Schema
