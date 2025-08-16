"""Database comparison engine."""

from collections.abc import Iterable, Mapping

from compare.inspector import Inspector
from compare.types import (
    ColumnAdded,
    ColumnChange,
    ColumnInfo,
    ColumnModified,
    ColumnRemoved,
    RowAdded,
    RowChange,
    RowModified,
    RowRemoved,
    TableComparison,
    ValueType,
)


class Comparator:
    """Compares two databases and generates complete difference information."""

    def __init__(self, source: Inspector, target: Inspector) -> None:
        """Initialize comparator with source and target database inspectors."""
        self.source = source
        self.target = target

    def tables(self) -> tuple[set[str], set[str], set[str]]:
        """Get table lists: added, removed, common."""
        source = set(self.source.tables())
        target = set(self.target.tables())

        added = target - source
        removed = source - target
        common = source & target

        return added, removed, common

    def compare_columns(
        self,
        source: Iterable[ColumnInfo],
        target: Iterable[ColumnInfo],
    ) -> list[ColumnChange]:
        """Compare column definitions between two tables."""
        # Create lookup dictionaries
        source_cols = {col["name"]: col for col in source}
        target_cols = {col["name"]: col for col in target}

        # Find added columns
        changes: list[ColumnChange] = []
        changes.extend(
            ColumnAdded(new=target_cols[col_name])
            for col_name in target_cols.keys() - source_cols.keys()
        )

        # Find removed columns
        changes.extend(
            ColumnRemoved(old=source_cols[col_name])
            for col_name in source_cols.keys() - target_cols.keys()
        )

        # Find modified columns
        changes.extend(
            ColumnModified(old=source_cols[name], new=target_cols[name])
            for name in source_cols.keys() & target_cols.keys()
            if source_cols[name] != target_cols[name]
        )

        return changes

    def _create_row_key(
        self,
        row: Mapping[str, ValueType],
        pk_columns: Iterable[str],
    ) -> str:
        """Create a unique key for a row based on primary key columns."""
        if pk_columns:
            key_parts = [str(row[pk]) for pk in pk_columns]
        else:
            # Use all columns if no primary key
            key_parts = [f"{k}:{row[k]}" for k in sorted(row.keys())]
        return "|".join(key_parts)

    def compare_data(self, table_name: str) -> list[RowChange]:
        """Compare all data in a table between source and target databases."""
        # Get all data from both tables
        source_data = self.source.data(table_name)
        target_data = self.target.data(table_name)

        # Get primary key columns
        source_pk = tuple(self.source.get_primary_key_columns(table_name))
        target_pk = tuple(self.target.get_primary_key_columns(table_name))

        # Create row lookup dictionaries
        source_rows = {self._create_row_key(row, source_pk): row for row in source_data}
        target_rows = {self._create_row_key(row, target_pk): row for row in target_data}

        changes: list[RowChange] = []

        # Find modified rows
        changes.extend(
            RowModified(old=source_rows[key], new=target_rows[key])
            for key in source_rows.keys() & target_rows.keys()
            if source_rows[key] != target_rows[key]
        )

        # Find added rows
        changes.extend(
            RowAdded(new=target_rows[key])
            for key in target_rows.keys() - source_rows.keys()
        )

        # Find removed rows
        changes.extend(
            RowRemoved(old=source_rows[key])
            for key in source_rows.keys() - target_rows.keys()
        )

        return changes

    def compare_table(self, name: str) -> TableComparison:
        """Compare complete table (schema and data) between databases."""
        # Compare schema
        source_columns = self.source.columns(name)
        target_columns = self.target.columns(name)
        schema_changes = self.compare_columns(source_columns, target_columns)

        # Compare data
        data_changes = self.compare_data(name)

        return TableComparison(name=name, schema=schema_changes, data=data_changes)
