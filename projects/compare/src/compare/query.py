"""Simple query builder for database operations."""

from __future__ import annotations


def select(*columns: str) -> Query:
    """Start a SELECT query with the given columns."""
    return Query(columns or ("*",))


def attach(database_uri: str, alias: str) -> str:
    """Generate an ATTACH DATABASE statement."""
    return f"ATTACH '{database_uri}' AS {alias}"


def pragma_table_info(table: str, schema: str = "main") -> str:
    """Generate a pragma_table_info function call."""
    return f"pragma_table_info('{table}', '{schema}')"


def qualified_table(schema: str, table: str) -> str:
    """Generate a qualified table name with proper escaping."""
    return f'{schema}."{table}"'


class Query:
    """A fluent SELECT query builder."""

    def __init__(self, columns: tuple[str, ...]) -> None:
        """Initialize with column names."""
        self._columns = columns
        self._table: str | None = None
        self._where: list[str] = []
        self._except: Query | None = None

    def from_(self, table: str) -> Query:
        """Set the FROM clause."""
        self._table = table
        return self

    def where(self, *conditions: str) -> Query:
        """Add a WHERE condition."""
        self._where.extend(conditions)
        return self

    def except_(self, other: Query) -> Query:
        """Add an EXCEPT clause with another SelectQuery."""
        self._except = other
        return self

    def __str__(self) -> str:
        """Convert the query to SQL string."""
        if not self._table:
            msg = "FROM clause is required"
            raise ValueError(msg)

        query = f"SELECT {', '.join(self._columns)} FROM {self._table}"  # noqa: S608
        if self._where:
            query += f" WHERE {' AND '.join(self._where)}"
        if self._except:
            query += f" EXCEPT {self._except}"

        return query
