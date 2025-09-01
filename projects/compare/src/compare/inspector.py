"""Database inspection functionality using sqlite3."""

from __future__ import annotations

from functools import cached_property
from sqlite3 import Connection, Row
from typing import TYPE_CHECKING, Literal, Self

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


def select_from(table_name: str, columns: Iterable[str] = ("*",)) -> str:
    """Select given columns from the table with the given table name."""
    return f"SELECT {", ".join(columns)} FROM {table_name}"  # noqa: S608


class Table:
    """Unified table representation that handles both data tables and schema tables."""

    def __init__(
        self,
        connection: Connection,
        table_name: str,
        database_name: str = "main",
        table_type: Literal["data", "schema"] = "data",
    ) -> None:
        """Initialize table with a database connection, table name, and schema."""
        self._connection = connection
        self._connection.row_factory = Row
        self._database_name = database_name
        self.table_type = table_type

        if table_type == "schema":
            self.name = "pragma_table_info"
            self.qualified_name = (
                f"pragma_table_info('{table_name}', '{database_name}')"
            )
        else:
            self.name = table_name
            self.qualified_name = f'{database_name}."{table_name}"'

    @property
    def schema(self) -> Self:
        """Return table representing the schema of the table."""
        return type(self)(self._connection, self.name, self._database_name, "schema")

    @property
    def rows(self) -> Iterator[Row]:
        """Return all rows from this table."""
        return self._connection.execute(select_from(self.qualified_name))

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

    def __init__(self, connection: Connection, database_name: str = "main") -> None:
        """Initialize inspector with a database connection and schema name."""
        self._connection = connection
        self._connection.row_factory = Row
        self._database_name = database_name
        self._table_cache: dict[str, Table] = {}

    @cached_property
    def tables(self) -> frozenset[str]:
        """Return all user table names for this schema, excluding system tables."""
        cursor = self._connection.execute(
            f"""
            {select_from(self._database_name, ("name",))}.sqlite_schema
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """,
        )
        return frozenset(row["name"] for row in cursor)

    def table(self, table_name: str) -> Table:
        """Return a Table instance for the given table name in this schema."""
        if table_name not in self._table_cache:
            self._table_cache[table_name] = Table(
                self._connection,
                table_name,
                self._database_name,
            )
        return self._table_cache[table_name]


class ComparableTable(Table):
    """A table with comparison capabilities."""

    def difference(self, other: ComparableTable) -> Iterator[Row]:
        """Compare this table with another using SQL EXCEPT operations."""
        select = select_from(self.qualified_name)
        except_clause = f" EXCEPT {select_from(other.qualified_name)}"

        # Add EXCEPT clause only when columns match exactly
        query = select + (except_clause if self.columns == other.columns else "")

        return self._connection.execute(query)
