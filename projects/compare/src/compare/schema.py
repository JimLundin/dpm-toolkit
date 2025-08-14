"""Schema comparison functionality."""

from sqlalchemy import Engine, MetaData

from compare.diff import diff_dicts, format_diffs


def _extract_table_schema(metadata: MetaData) -> dict[str, dict[str, str]]:
    """Extract detailed schema information from metadata."""
    schema: dict[str, dict[str, str]] = {}

    for table_name, table in metadata.tables.items():
        columns: dict[str, dict[str, bool | str | None]] = {}
        for col_name, column in table.columns.items():
            columns[col_name] = {
                "type": str(column.type),
                "nullable": column.nullable,
                "primary_key": column.primary_key,
                "default": str(column.default) if column.default else None,
            }

        # Get indexes
        indexes: dict[str | None, dict[str, list[str] | bool]] = {}
        for index in table.indexes:
            indexes[index.name] = {
                "columns": [col.name for col in index.columns],
                "unique": index.unique,
            }

        # Get foreign keys
        foreign_keys: dict[str, dict[str, str]] = {}
        for fk in table.foreign_keys:
            fk_name = f"{fk.parent.name}->{fk.column}"
            foreign_keys[fk_name] = {
                "column": fk.parent.name,
                "references": str(fk.column),
            }

        schema[table_name] = {
            "columns": columns,
            "indexes": indexes,
            "foreign_keys": foreign_keys,
        }

    return schema


def compare_schema(source_engine: Engine, target_engine: Engine) -> list[str]:
    """Compare database schemas and return detailed list of changes."""
    # Connect to databases
    source_meta = MetaData()
    target_meta = MetaData()
    source_meta.reflect(bind=source_engine)
    target_meta.reflect(bind=target_engine)

    # Extract detailed schema information
    source_schema = _extract_table_schema(source_meta)
    target_schema = _extract_table_schema(target_meta)

    # Use generic differ
    diffs = diff_dicts(source_schema, target_schema)

    # Convert to formatted strings
    if not diffs:
        return ["Schema Changes: None"]

    return format_diffs(diffs, "Schema Changes")
