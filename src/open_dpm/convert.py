"""Functions for converting Access databases to DuckDB and SQLite formats."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import MetaData, create_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def migrate_databases(source_dir: Path, target_engines: list[Engine]) -> None:
    """Migrate databases from source directory to target databases.

    Args:
        source_dir: Directory containing Access databases
        target_engines: List of SQLAlchemy engines to migrate data to

    """
    if not target_engines:
        logger.error("No target engines provided")
        return

    # Process each source database
    for source_path in source_dir.glob("**/*.accdb"):
        logger.info("Processing database: %s", source_path.name)
        driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
        conn_str = f"DRIVER={driver};DBQ={source_path}"
        source_engine = create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")

        # Get all tables
        metadata = MetaData()
        metadata.reflect(bind=source_engine)

        # Copy each table to all targets
        with source_engine.connect() as conn:
            for table_name, table in metadata.tables.items():
                logger.info("Copying table: %s", table_name)

                try:
                    # Read all data
                    data = conn.execute(table.select()).fetchall()

                    # Copy to each target
                    for engine in target_engines:
                        table.metadata = MetaData()
                        table.create(engine, checkfirst=True)

                        if data:
                            with engine.begin() as target_conn:
                                target_conn.execute(table.insert().values(data))
                        else:
                            logger.warning("Table %s is empty", table_name)
                except Exception:
                    logger.exception("Failed to process table %s", table_name)
                    continue


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[2]
    input_dir = project_root / ".scratch" / "db_input"
    output_dir = project_root / ".scratch" / "db_output"
    output_dir.mkdir(parents=True, exist_ok=True)

    target_engines = [
        create_engine(f"sqlite:///{output_dir / 'dpm.sqlite'}"),
        create_engine(f"duckdb:///{output_dir / 'dpm.duckdb'}"),
    ]

    migrate_databases(input_dir, target_engines)
