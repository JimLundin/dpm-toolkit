"""Functions for converting Access databases to DuckDB format."""

import logging
from pathlib import Path

from sqlalchemy import Inspector, MetaData, create_engine, event
from sqlalchemy.engine.interfaces import ReflectedColumn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def migrate_database(source_dir: Path, target_dir: Path) -> None:
    """Migrate databases from source directory to DuckDB.

    Args:
        source_dir: Directory containing Access databases
        target_dir: Directory to save converted DuckDB database

    """
    target_dir.mkdir(parents=True, exist_ok=True)

    # Create the DuckDB engine
    target_engine = create_engine(f"duckdb:///{target_dir}/dpm.duckdb")

    with target_engine.connect() as target_conn:
        # Process each source database
        for source_path in source_dir.glob("**/*.accdb"):
            logger.info("Processing database: %s", source_path.name)
            driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
            conn_str = f"DRIVER={driver};DBQ={source_path}"
            source_engine = create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")

            # Get all tables
            metadata = MetaData()

            @event.listens_for(metadata, "column_reflect")
            def genericize_datatypes( # type: ignore
                _inspector: Inspector,
                _tablename: str,
                column_dict: ReflectedColumn,
            ) -> None:
                column_dict["type"] = column_dict["type"].as_generic()

            metadata.reflect(bind=source_engine)

            # Copy each table
            with source_engine.connect() as source_conn:
                for table_name, table in metadata.tables.items():
                    logger.info("Copying table: %s", table_name)

                    try:
                        # Read all data
                        data = source_conn.execute(table.select()).fetchall()

                        # Create table and copy data
                        table.metadata = MetaData()
                        table.create(target_engine, checkfirst=True)

                        if data:
                            target_conn.execute(table.insert().values(data))
                        else:
                            logger.warning("No data found for table: %s", table_name)

                    except Exception:
                        logger.exception("Failed to copy table: %s", table_name)
