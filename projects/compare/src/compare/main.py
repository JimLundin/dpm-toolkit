"""Main database comparison functionality."""

import json
from pathlib import Path

from .comparator import DatabaseComparator
from .inspector import DatabaseInspector
from .types import (
    ColumnAdded,
    ColumnRemoved,
    Comparison,
    RowAdded,
    RowRemoved,
    TableComparison,
)


def compare_databases(source_path: str | Path, target_path: str | Path) -> Comparison:
    """Compare two SQLite databases completely and return full results."""
    source_path = Path(source_path)
    target_path = Path(target_path)

    # Validate database files exist
    if not source_path.exists():
        msg = f"Source database not found: {source_path}"
        raise FileNotFoundError(msg)
    if not target_path.exists():
        msg = f"Target database not found: {target_path}"
        raise FileNotFoundError(msg)

    # Create inspectors
    source_inspector = DatabaseInspector(source_path)
    target_inspector = DatabaseInspector(target_path)

    # Create comparator
    comparator = DatabaseComparator(source_inspector, target_inspector)

    # Get all tables
    tables_added, tables_removed, tables_common = comparator.get_all_tables()

    # Compare all tables (common, added, removed)
    all_changes: list[TableComparison] = []

    # Compare common tables (schema and data)
    for table_name in tables_common:
        table_comparison = comparator.compare_table(table_name)
        all_changes.append(table_comparison)

    # Add tables that were added (only exist in target)
    for table_name in tables_added:
        target_columns = target_inspector.get_table_columns(table_name)
        target_data = target_inspector.get_all_table_data(table_name)
        pk_columns = target_inspector.get_primary_key_columns(table_name)

        # Create row additions for all data in the new table

        schema_added = [
            ColumnAdded(name=col["name"], new=col) for col in target_columns
        ]

        data_added = [
            RowAdded(key={col: row.get(col) for col in pk_columns}, new=row)
            for row in target_data
        ]

        all_changes.append(
            TableComparison(name=table_name, schema=schema_added, data=data_added),
        )

    # Add tables that were removed (only exist in source)
    for table_name in tables_removed:
        source_columns = source_inspector.get_table_columns(table_name)
        source_data = source_inspector.get_all_table_data(table_name)
        pk_columns = source_inspector.get_primary_key_columns(table_name)

        # Create row removals for all data in the removed table

        schema_removed = [
            ColumnRemoved(name=col["name"], old=col) for col in source_columns
        ]

        data_removed = [
            RowRemoved(key={col: row.get(col) for col in pk_columns}, old=row)
            for row in source_data
        ]

        all_changes.append(
            TableComparison(name=table_name, schema=schema_removed, data=data_removed),
        )

    # Build complete result
    return Comparison(
        source=str(source_path),
        target=str(target_path),
        changes=all_changes,
    )


def comparison_to_json(result: Comparison, indent: int = 2) -> str:
    """Convert comparison result to JSON string."""
    return json.dumps(result, indent=indent, ensure_ascii=False)


def save_comparison_json(result: Comparison, output_path: str | Path) -> None:
    """Save comparison result as JSON file."""
    output_path = Path(output_path)
    json_content = comparison_to_json(result)

    output_path.write_text(json_content, encoding="utf-8")


def load_comparison_json(json_path: str | Path) -> Comparison:
    """Load comparison result from JSON file."""
    json_path = Path(json_path)
    if not json_path.exists():
        msg = f"JSON file not found: {json_path}"
        raise FileNotFoundError(msg)

    content = json_path.read_text(encoding="utf-8")
    result: Comparison = json.loads(content)
    return result
