"""Database inspection functionality using sqlite3."""

from __future__ import annotations

from functools import cached_property
from sqlite3 import Connection, Row
from typing import TYPE_CHECKING, Literal, Self

if TYPE_CHECKING:
    from collections.abc import Iterator


class Table:
    """Unified table representation that handles both data tables and schema tables."""

    def __init__(
        self,
        connection: Connection,
        table_name: str,
        schema_name: str = "main",
        table_type: Literal["data", "schema"] = "data",
    ) -> None:
        """Initialize table with a database connection, table name, and schema."""
        self._connection = connection
        self._connection.row_factory = Row
        self._schema_name = schema_name
        self.name = "pragma_table_info" if table_type == "schema" else table_name
        self.table_type = table_type

        if table_type == "schema":
            self.qualified_name = f"pragma_table_info('{table_name}', '{schema_name}')"
        else:
            self.qualified_name = f'{schema_name}."{table_name}"'

    @property
    def schema(self) -> Self:
        """Return table representing the schema of the table."""
        return type(self)(self._connection, self.name, self._schema_name, "schema")

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


class ComparableTable(Table):
    """A table with comparison capabilities."""

    def difference(self, other: ComparableTable) -> Iterator[Row]:
        """Compare this table with another using SQL EXCEPT operations."""
        select = f"SELECT * FROM {self.qualified_name}"  # noqa: S608
        except_clause = f" EXCEPT SELECT * FROM {other.qualified_name}"  # noqa: S608

        # Add EXCEPT clause only when columns match exactly
        query = select + (except_clause if self.columns == other.columns else "")

        return self._connection.execute(query)


class ComparableDatabase(Database):
    """Database that can create comparable tables when needed."""

    def comparable_table(self, table_name: str) -> ComparableTable:
        """Return a ComparableTable instance for comparison operations."""
        return ComparableTable(self._connection, table_name, self._schema_name)
