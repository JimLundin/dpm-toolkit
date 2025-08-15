"""Database comparison engine."""

from .inspector import DatabaseInspector
from .types import (
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


class DatabaseComparator:
    """Compares two databases and generates complete difference information."""

    def __init__(
        self,
        source: DatabaseInspector,
        target: DatabaseInspector,
    ) -> None:
        """Initialize comparator with source and target database inspectors."""
        self.source = source
        self.target = target

    def get_all_tables(self) -> tuple[list[str], list[str], list[str]]:
        """Get table lists: added, removed, common."""
        source_tables = set(self.source.get_tables())
        target_tables = set(self.target.get_tables())

        added = sorted(target_tables - source_tables)
        removed = sorted(source_tables - target_tables)
        common = sorted(source_tables & target_tables)

        return added, removed, common

    def compare_columns(
        self,
        source: list[ColumnInfo],
        target: list[ColumnInfo],
    ) -> list[ColumnChange]:
        """Compare column definitions between two tables."""
        # Create lookup dictionaries
        source_cols = {col["name"]: col for col in source}
        target_cols = {col["name"]: col for col in target}

        # Find added columns
        changes: list[ColumnChange] = []
        changes.extend(
            ColumnAdded(name=col_name, new=target_cols[col_name])
            for col_name in target_cols.keys() - source_cols.keys()
        )

        # Find removed columns
        changes.extend(
            ColumnRemoved(name=col_name, old=source_cols[col_name])
            for col_name in source_cols.keys() - target_cols.keys()
        )

        # Find modified columns
        changes.extend(
            ColumnModified(name=name, old=source_cols[name], new=target_cols[name])
            for name in source_cols.keys() & target_cols.keys()
            if source_cols[name] != target_cols[name]
        )

        return changes

    def _create_row_key(self, row: dict[str, ValueType], pk_columns: list[str]) -> str:
        """Create a unique key for a row based on primary key columns."""
        if pk_columns:
            key_parts = [str(row.get(pk, "NULL")) for pk in pk_columns]
        else:
            # Use all columns if no primary key
            key_parts = [f"{k}:{v}" for k, v in sorted(row.items())]
        return "|".join(key_parts)

    def _create_primary_key_dict(
        self,
        row: dict[str, ValueType],
        pk_columns: list[str],
    ) -> dict[str, ValueType]:
        """Create primary key dictionary from row data."""
        return {col: row.get(col) for col in pk_columns}

    def compare_data(self, table_name: str) -> list[RowChange]:
        """Compare all data in a table between source and target databases."""
        # Get all data from both tables
        source_data = self.source.get_all_table_data(table_name)
        target_data = self.target.get_all_table_data(table_name)

        # Get primary key columns
        source_pk = self.source.get_primary_key_columns(table_name)
        target_pk = self.target.get_primary_key_columns(table_name)

        # Create row lookup dictionaries
        source_rows = {self._create_row_key(row, source_pk): row for row in source_data}
        target_rows = {self._create_row_key(row, target_pk): row for row in target_data}

        changes: list[RowChange] = []

        # Find added rows
        for key in target_rows.keys() - source_rows.keys():
            row = target_rows[key]
            pk_dict = self._create_primary_key_dict(row, target_pk)

            changes.append(RowAdded(key=pk_dict, new=row))

        # Find removed rows
        for key in source_rows.keys() - target_rows.keys():
            row = source_rows[key]
            pk_dict = self._create_primary_key_dict(row, source_pk)

            changes.append(RowRemoved(key=pk_dict, old=row))

        # Use source primary key columns for comparison
        pk_columns = source_pk or target_pk

        # Find modified rows
        for key in source_rows.keys() & target_rows.keys():
            source_row = source_rows[key]
            target_row = target_rows[key]

            if source_row != target_row:
                pk_dict = self._create_primary_key_dict(source_row, pk_columns)

                changes.append(
                    RowModified(key=pk_dict, old=source_row, new=target_row),
                )

        return changes

    def compare_table(self, name: str) -> TableComparison:
        """Compare complete table (schema and data) between databases."""
        # Compare schema
        source_columns = self.source.get_table_columns(name)
        target_columns = self.target.get_table_columns(name)
        schema_changes = self.compare_columns(source_columns, target_columns)

        # Compare data
        data_changes = self.compare_data(name)

        return TableComparison(
            name=name,
            schema=schema_changes or None,
            data=data_changes or None,
        )
