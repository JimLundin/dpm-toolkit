"""Functions for converting Access databases to SQLite format."""

import logging
import time
from pathlib import Path

from sqlalchemy import Inspector, MetaData, Text, create_engine, event
from sqlalchemy.engine.interfaces import ReflectedColumn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


def genericize_datatypes(
    _inspector: Inspector,
    _table_name: str,
    column_dict: ReflectedColumn,
) -> None:
    """Convert GUID columns to Text and all other columns to generic types."""
    if column_dict["name"].endswith("GUID"):
        column_dict["type"] = Text()
    else:
        column_dict["type"] = column_dict["type"].as_generic()


def migrate_database(source_dir: Path, target_dir: Path) -> None:
    """Migrate databases from source directory to target directory in SQLite.

    Args:
        source_dir: Directory containing Access databases
        target_dir: Directory to save converted SQLite database

    """
    total_start_time = time.time()
    target_dir.mkdir(parents=True, exist_ok=True)

    # Create the SQLite engine
    target_engine = create_engine(f"sqlite:///{target_dir}/dpm.sqlite")

    with target_engine.connect() as target_conn:
        # Process each source database
        for source_path in source_dir.glob("**/*.accdb"):
            db_start_time = time.time()
            logger.info("%s - Processing database", source_path.name)
            driver = "{Microsoft Access Driver (*.mdb, *.accdb)}"
            conn_str = f"DRIVER={driver};DBQ={source_path}"
            source_engine = create_engine(f"access+pyodbc:///?odbc_connect={conn_str}")

            # Get all tables
            metadata = MetaData()

            event.listens_for(metadata, "column_reflect")(genericize_datatypes)

            metadata.reflect(bind=source_engine)

            for table in metadata.tables.values():
                indexes_to_modify = [
                    index for index in table.indexes if index.name == "PrimaryKey"
                ]
                for index in indexes_to_modify:
                    table.indexes.remove(index)

            schema_start_time = time.time()
            metadata.create_all(target_engine)
            logger.info("Schema recreation time: %.2f", time.time() - schema_start_time)

            # Copy each table
            with source_engine.connect() as source_conn:
                for table_name, table in metadata.tables.items():
                    table_start_time = time.time()

                    try:
                        # Read all data
                        data = source_conn.execute(table.select()).fetchall()

                        insert_start_time = time.time()
                        target_conn.execute(
                            table.insert(),
                            [row._asdict() for row in data],  # type: ignore
                        )
                        target_conn.commit()
                        logger.info(
                            "Table: %s, rows: %d, fetch time: %.2f, insert time: %.2f",
                            table_name,
                            len(data),
                            insert_start_time - table_start_time,
                            time.time() - insert_start_time,
                        )

                    except Exception:
                        logger.exception("%s - Failed to copy table", table_name)

            logger.info(
                "Database: %s, total time: %.2f",
                source_path.name,
                time.time() - db_start_time,
            )

    logger.info("Conversion time: %.2f", time.time() - total_start_time)
