"""Functions for converting Access databases to DuckDB and SQLite formats."""

import logging
from pathlib import Path

from sqlalchemy import MetaData, create_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def migrate_databases(source_dir: Path, target_urls: list[str]) -> None:
    """Migrate databases from source directory to target databases.

    Args:
        source_dir: Directory containing Access databases
        target_urls: List of SQLAlchemy database URLs to migrate data to

    """
    if not target_urls:
        logger.error("No target URLs provided")
        return

    target_engines = [create_engine(url) for url in target_urls]

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

                except Exception:
                    logger.exception("Failed to copy table: %s", table_name)
