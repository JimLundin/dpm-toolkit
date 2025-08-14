"""Data comparison functionality."""

from sqlalchemy import MetaData, Engine, text


def compare_data(source_engine: Engine, target_engine: Engine) -> list[str]:
    """Compare database data and return list of changes."""
    # Connect to databases
    source_meta = MetaData()
    target_meta = MetaData()
    source_meta.reflect(bind=source_engine)
    target_meta.reflect(bind=target_engine)

    changes: list[str] = []
    common_tables = set(source_meta.tables.keys()) & set(target_meta.tables.keys())

    for table_name in common_tables:
        with source_engine.connect() as src_conn, target_engine.connect() as tgt_conn:
            src_count = src_conn.execute(
                text(f"SELECT COUNT(*) FROM `{table_name}`")
            ).scalar()
            tgt_count = tgt_conn.execute(
                text(f"SELECT COUNT(*) FROM `{table_name}`")
            ).scalar()

            if src_count != tgt_count:
                changes.append(f"{table_name}: {src_count} -> {tgt_count} rows")

    return changes
