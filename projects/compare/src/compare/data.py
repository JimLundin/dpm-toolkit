"""Data comparison functionality."""

from dataclasses import dataclass
from typing import Any

from sqlalchemy import Engine, MetaData, Table, func, select, text

from compare.diff import ChangeType, Diff, format_diffs


@dataclass
class TableData:
    """Complete data representation for a table."""

    count: int
    rows: list[dict[str, Any]]
    primary_keys: list[str]


def _get_table_primary_keys(engine: Engine, table_name: str) -> list[str]:
    """Get primary key columns for a table."""
    meta = MetaData()
    meta.reflect(bind=engine)
    table = meta.tables[table_name]
    return [col.name for col in table.primary_key.columns]


def _get_tables(engine: Engine) -> dict[str, Table]:
    """Get all table names in the database."""
    meta = MetaData()
    meta.reflect(bind=engine)
    return meta.tables


def _get_all_table_data(engine: Engine, table_name: str) -> TableData:
    """Get all data from a table."""
    tables = _get_tables(engine)
    if table_name not in tables:
        msg = f"Table '{table_name}' does not exist in the database."
        raise ValueError(msg)
    table = tables[table_name]

    with engine.connect() as conn:
        # Get count
        count = conn.execute(select(func.count()).select_from(table)).scalar() or 0

        # Get all rows (ordered by primary key if available for consistency)
        primary_keys = _get_table_primary_keys(engine, table_name)
        if primary_keys:
            order_by = ", ".join(f"`{pk}`" for pk in primary_keys)
            query = select(table).order_by(text(order_by))
        else:
            query = select(table).order_by(text("rowid"))

        result = conn.execute(query).fetchall()
        rows = [row._asdict() for row in result]

        return TableData(count=count, rows=rows, primary_keys=primary_keys)


def _create_row_key(row: dict[str, Any], primary_keys: list[str]) -> str:
    """Create a unique key for a row based on primary keys or all columns."""
    if primary_keys:
        key_parts = [str(row.get(pk, "NULL")) for pk in primary_keys]
    else:
        # Use all columns as key if no primary key
        key_parts = [f"{k}:{v}" for k, v in sorted(row.items())]
    return "|".join(key_parts)


def _compare_table_data(
    source_data: TableData,
    target_data: TableData,
    table_name: str,
) -> list[Diff]:
    """Compare two table datasets and return detailed differences."""
    diffs: list[Diff] = []

    # Count differences
    if source_data.count != target_data.count:
        diffs.append(
            Diff(
                ChangeType.MODIFIED,
                f"{table_name}.count",
                f"{source_data.count} rows",
                f"{target_data.count} rows",
            ),
        )

    # Create row mappings for comparison
    source_rows = {
        _create_row_key(row, source_data.primary_keys): row for row in source_data.rows
    }
    target_rows = {
        _create_row_key(row, target_data.primary_keys): row for row in target_data.rows
    }

    # Find added rows
    diffs.extend(
        Diff(
            ChangeType.ADDED,
            f"{table_name}.row[{key}]",
            new_value=target_rows[key],
        )
        for key in target_rows.keys() - source_rows.keys()
    )

    # Find removed rows
    diffs.extend(
        Diff(
            ChangeType.REMOVED,
            f"{table_name}.row[{key}]",
            old_value=source_rows[key],
        )
        for key in source_rows.keys() - target_rows.keys()
    )

    # Find modified rows
    for key in source_rows.keys() & target_rows.keys():
        source_row = source_rows[key]
        target_row = target_rows[key]

        if source_row != target_row:
            # Find specific field changes within the row
            for col_name in source_row.keys() | target_row.keys():
                old_val = source_row.get(col_name)
                new_val = target_row.get(col_name)

                if old_val != new_val:
                    diffs.append(
                        Diff(
                            ChangeType.MODIFIED,
                            f"{table_name}.row[{key}].{col_name}",
                            old_val,
                            new_val,
                        ),
                    )

    return diffs


def compare_data(source_engine: Engine, target_engine: Engine) -> list[str]:
    """Compare database data with comprehensive row-by-row analysis for ALL tables."""
    # Connect to databases
    source_meta = MetaData()
    target_meta = MetaData()
    source_meta.reflect(bind=source_engine)
    target_meta.reflect(bind=target_engine)

    common_tables = set(source_meta.tables.keys()) & set(target_meta.tables.keys())

    # Collect all differences across all tables
    all_diffs: list[Diff] = []

    for table_name in sorted(common_tables):
        # Get complete table data for ALL tables - no limits
        source_data = _get_all_table_data(source_engine, table_name)
        target_data = _get_all_table_data(target_engine, table_name)

        # Compare the table data comprehensively for every table
        table_diffs = _compare_table_data(source_data, target_data, table_name)
        all_diffs.extend(table_diffs)

    # Format and return results
    if not all_diffs:
        return ["Data Changes: None"]

    return format_diffs(all_diffs, "Data Changes")
