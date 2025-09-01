"""Database inspection functionality using sqlite3."""

from __future__ import annotations

from functools import cached_property
from sqlite3 import Connection, Row
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


def select_from(table_name: str, columns: Iterable[str] = ("*",)) -> str:
    """Select given columns from the table with the given table name."""
    return f"SELECT {", ".join(columns)} FROM {table_name}"  # noqa: S608


class Database:
    """Inspects SQLite databases to extract schema information."""

    def __init__(self, connection: Connection, database_name: str = "main") -> None:
        """Initialize inspector with a database connection and schema name."""
        self._connection = connection
        self._connection.row_factory = Row
        self._table_cache: dict[str, Table] = {}
        self.name = database_name

    def execute(self, query: str) -> Iterator[Row]:
        """Proxies a query to the underlying connection."""
        return self._connection.execute(query)

    @cached_property
    def tables(self) -> frozenset[str]:
        """Return all user table names for this schema, excluding system tables."""
        cursor = self.execute(
            f"""
            {select_from(f"{self.name}.sqlite_schema", ("name",))}
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """,
        )
        return frozenset(row["name"] for row in cursor)

    def table(self, table_name: str) -> Table:
        """Return a Table instance for the given table name in this schema."""
        if table_name not in self._table_cache:
            self._table_cache[table_name] = Table(self, table_name)
        return self._table_cache[table_name]


class Table:
    """Unified table representation that handles both data tables and schema tables."""

    def __init__(
        self,
        database: Database,
        table_name: str,
        table_type: Literal["data", "schema"] = "data",
    ) -> None:
        """Initialize table with a database connection, table name, and schema."""
        self._database = database
        self.table_type = table_type

        if table_type == "schema":
            self.name = "pragma_table_info"
            self.qualified_name = (
                f"pragma_table_info('{table_name}', '{database.name}')"
            )
        else:
            self.name = table_name
            self.qualified_name = f'{database.name}."{table_name}"'

    @property
    def schema(self) -> Table:
        """Return table representing the schema of the table."""
        return Table(self._database, self.name, "schema")

    @property
    def rows(self) -> Iterator[Row]:
        """Return all rows from this table."""
        return self._database.execute(select_from(self.qualified_name))

    @cached_property
    def columns(self) -> tuple[str, ...]:
        """Return column names for this table."""
        return tuple(row["name"] for row in self.schema.rows)

    @cached_property
    def primary_keys(self) -> tuple[str, ...]:
        """Return primary key column names for this table."""
        return tuple(row["name"] for row in self.schema.rows if row["pk"])

    def difference(self, other: Table) -> Iterator[Row]:
        """Compare this table with another using SQL EXCEPT operations."""
        select = select_from(self.qualified_name)
        except_clause = f" EXCEPT {select_from(other.qualified_name)}"

        # Add EXCEPT clause only when columns match exactly
        query = select + (except_clause if self.columns == other.columns else "")

        return self._database.execute(query)
