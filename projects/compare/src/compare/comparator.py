"""Database comparison engine."""

from collections.abc import Iterable
from itertools import chain
from sqlite3 import Row

from compare.inspector import Inspector
from compare.types import (
    ColInfo,
    ColMod,
    Comparison,
    RowMod,
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

    def compare_cols(
        self,
        source: Iterable[ColInfo],
        target: Iterable[ColInfo],
    ) -> Iterable[ColMod]:
        """Compare column definitions between two tables."""
        # Create lookup dictionaries
        source_cols = {col.name: col for col in source}
        target_cols = {col.name: col for col in target}

        # Find added columns
        return chain(
            (
                ColMod(target_cols[name], source_cols[name])
                for name in source_cols.keys() & target_cols.keys()
                if source_cols[name] != target_cols[name]
            ),
            (
                ColMod(new=target_cols[col_name])
                for col_name in target_cols.keys() - source_cols.keys()
            ),
            (
                ColMod(old=source_cols[col_name])
                for col_name in source_cols.keys() - target_cols.keys()
            ),
        )

    def _row_key(self, row: Row, pk_columns: Iterable[str]) -> tuple[ValueType, ...]:
        """Create a unique key for a row based on primary key columns."""
        return tuple(row[pk] for pk in pk_columns) or tuple(row)

    def compare_rows(self, name: str) -> Iterable[RowMod]:
        """Compare all data in a table between source and target databases."""
        # Get all data from both tables
        source_data = self.source.rows(name)
        target_data = self.target.rows(name)

        # Get primary key columns
        source_pk = tuple(self.source.primary_keys(name))
        target_pk = tuple(self.target.primary_keys(name))

        # Create row lookup dictionaries
        source_map = {self._row_key(row, source_pk): row for row in source_data}
        target_map = {self._row_key(row, target_pk): row for row in target_data}

        return chain(
            (
                RowMod(target_map[key], source_map[key])
                for key in source_map.keys() & target_map.keys()
                if source_map[key] != target_map[key]
            ),
            (
                RowMod(new=target_map[key])
                for key in target_map.keys() - source_map.keys()
            ),
            (
                RowMod(old=source_map[key])
                for key in source_map.keys() - target_map.keys()
            ),
        )

    def compare(self) -> Iterable[Comparison]:
        """Compare all tables in both databases and return full results."""
        added, removed, common = self.tables()

        return chain(
            (
                Comparison(
                    name=name,
                    cols=self.compare_cols(
                        self.source.cols(name),
                        self.target.cols(name),
                    ),
                    rows=self.compare_rows(name),
                )
                for name in common
            ),
            (
                Comparison(
                    name=name,
                    cols=(ColMod(new=col) for col in self.target.cols(name)),
                    rows=(RowMod(new=row) for row in self.target.rows(name)),
                )
                for name in added
            ),
            (
                Comparison(
                    name=name,
                    cols=(ColMod(old=col) for col in self.source.cols(name)),
                    rows=(RowMod(old=row) for row in self.source.rows(name)),
                )
                for name in removed
            ),
        )
