"""Schema comparison functionality."""

from sqlalchemy import Engine, MetaData

from compare.diff import diff_dicts, format_diffs

# Type definitions for schema structure
ColumnSchema = dict[str, str | bool | None]
ForeignKeySchema = dict[str, str]
TableSchema = dict[str, dict[str, ColumnSchema] | dict[str, ForeignKeySchema]]


def _extract_table_schema(metadata: MetaData) -> dict[str, TableSchema]:
    """Extract comprehensive schema information from metadata."""
    schema: dict[str, TableSchema] = {}

    for table_name, table in metadata.tables.items():
        # Extract column details
        columns: dict[str, ColumnSchema] = {}
        for col_name, column in table.columns.items():
            columns[col_name] = {
                "type": str(column.type),
                "nullable": column.nullable,
                "primary_key": column.primary_key,
                "default": str(column.default) if column.default else None,
            }

        # Extract foreign keys
        foreign_keys: dict[str, ForeignKeySchema] = {}
        for fk in table.foreign_keys:
            fk_name = f"{fk.parent.name}->{fk.column}"
            foreign_keys[fk_name] = {
                "column": fk.parent.name,
                "references": str(fk.column),
            }

        schema[table_name] = {
            "columns": columns,
            "foreign_keys": foreign_keys,
        }

    return schema


def compare_schema(source_engine: Engine, target_engine: Engine) -> list[str]:
    """Compare database schemas comprehensively."""
    # Reflect both databases
    source_meta = MetaData()
    target_meta = MetaData()
    source_meta.reflect(bind=source_engine)
    target_meta.reflect(bind=target_engine)

    # Extract complete schema information
    source_schema = _extract_table_schema(source_meta)
    target_schema = _extract_table_schema(target_meta)

    # Compare using generic diff logic
    diffs = diff_dicts(source_schema, target_schema)

    # Format results
    if not diffs:
        return ["Schema Changes: None"]

    return format_diffs(diffs, "Schema Changes")
