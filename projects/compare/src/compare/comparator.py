"""Database comparison engine."""

from collections.abc import Generator, Iterable
from sqlite3 import Row

from compare.inspector import Inspector
from compare.types import (
    ColInfo,
    ColMod,
    RowMod,
    TableComparison,
    ValueType,
)


class Comparator:
    """Compares two databases and generates complete difference information."""

    def __init__(self, source: Inspector, target: Inspector) -> None:
        """Initialize comparator with source and target database inspectors."""
        self.source = source
        self.target = target

    def tables(self) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
        """Get table lists: added, removed, common."""
        source = frozenset(self.source.tables())
        target = frozenset(self.target.tables())

        added = target - source
        removed = source - target
        common = source & target

        return added, removed, common

    def compare_columns(
        self,
        source: Iterable[ColInfo],
        target: Iterable[ColInfo],
    ) -> list[ColMod]:
        """Compare column definitions between two tables."""
        # Create lookup dictionaries
        source_cols = {col.name: col for col in source}
        target_cols = {col.name: col for col in target}

        # Find added columns
        changes: list[ColMod] = [
            ColMod(new=target_cols[col_name])
            for col_name in target_cols.keys() - source_cols.keys()
        ]

        # Find removed columns
        changes.extend(
            ColMod(old=source_cols[col_name])
            for col_name in source_cols.keys() - target_cols.keys()
        )

        # Find modified columns
        changes.extend(
            ColMod(target_cols[name], source_cols[name])
            for name in source_cols.keys() & target_cols.keys()
            if source_cols[name] != target_cols[name]
        )

        return changes

    def _row_key(
        self,
        row: Row,
        pk_columns: Iterable[str],
    ) -> Generator[ValueType]:
        """Create a unique key for a row based on primary key columns."""
        if pk_columns:
            return (row[pk] for pk in pk_columns)
        # Use all columns if no primary key
        return (f"{k}:{row[k]}" for k in sorted(row.keys()))

    def compare_data(self, name: str) -> list[RowMod]:
        """Compare all data in a table between source and target databases."""
        # Get all data from both tables
        source_data = self.source.data(name)
        target_data = self.target.data(name)

        # Get primary key columns
        source_pk = tuple(self.source.primary_keys(name))
        target_pk = tuple(self.target.primary_keys(name))

        # Create row lookup dictionaries
        source_rows = {tuple(self._row_key(row, source_pk)): row for row in source_data}
        target_rows = {tuple(self._row_key(row, target_pk)): row for row in target_data}

        # Find modified rows
        changes: list[RowMod] = [
            RowMod(target_rows[key], source_rows[key])
            for key in source_rows.keys() & target_rows.keys()
            if source_rows[key] != target_rows[key]
        ]

        # Find added rows
        changes.extend(
            RowMod(new=target_rows[key])
            for key in target_rows.keys() - source_rows.keys()
        )

        # Find removed rows
        changes.extend(
            RowMod(old=source_rows[key])
            for key in source_rows.keys() - target_rows.keys()
        )

        return changes

    def compare_table(self, name: str) -> TableComparison:
        """Compare complete table (schema and data) between databases."""
        return TableComparison(
            name=name,
            schema=self.compare_columns(
                self.source.columns(name),
                self.target.columns(name),
            ),
            data=self.compare_data(name),
        )
