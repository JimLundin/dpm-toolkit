"""Schema comparison functionality."""

from sqlalchemy import Engine, MetaData


def compare_schema(source_engine: Engine, target_engine: Engine) -> list[str]:
    """Compare database schemas and return list of changes."""
    # Connect to databases
    source_meta = MetaData()
    target_meta = MetaData()
    source_meta.reflect(bind=source_engine)
    target_meta.reflect(bind=target_engine)

    source_tables = set(source_meta.tables.keys())
    target_tables = set(target_meta.tables.keys())

    changes: list[str] = []

    # Added/removed tables
    changes.extend(f"Added table: {table}" for table in target_tables - source_tables)
    changes.extend(f"Removed table: {table}" for table in source_tables - target_tables)

    # Compare columns in common tables
    for name in source_tables & target_tables:
        src_cols = set(source_meta.tables[name].columns.keys())
        tgt_cols = set(target_meta.tables[name].columns.keys())

        changes.extend(f"Added column: {name}.{col}" for col in tgt_cols - src_cols)
        changes.extend(f"Removed column: {name}.{col}" for col in src_cols - tgt_cols)

    return changes
