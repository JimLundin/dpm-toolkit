"""Main module for database conversion."""

import logging
from pathlib import Path
from time import time

from sqlalchemy import create_engine, text

from opendpm.convert.generation import Model
from opendpm.convert.processing import (
    create_access_engine,
    extract_schema_and_data,
    get_database,
    load_data,
)
from opendpm.convert.utils import format_duration

logger = logging.getLogger(__name__)


def convert_access_to_sqlite(
    source: Path,
    output: Path,
    *,
    overwrite: bool = False,
) -> None:
    """Migrate Access databases to SQLite.

    Args:
        source: Directory containing Access databases
        output: Directory to store SQLite database
        overwrite: Whether to overwrite existing database

    """
    start_time = time()

    sqlite_path = output / "dpm.sqlite"
    if sqlite_path.exists():
        if not overwrite:
            logger.warning("Target database already exists: %s", sqlite_path)
            return

        sqlite_path.unlink(missing_ok=True)

    output.mkdir(parents=True, exist_ok=True)

    database = source if source.is_file() else get_database(source)
    if not database:
        logger.warning("No Access database files found in %s", source)
        return

    logger.info("Processing: %s", database.stem)

    access = create_access_engine(database)
    metadata, tables = extract_schema_and_data(access)

    sqlite = create_engine("sqlite:///:memory:")
    metadata.create_all(sqlite)
    load_data(sqlite, tables)

    with (output / "dpm.py").open("w") as model_file:
        model_file.write(Model(metadata).render())

    with sqlite.connect() as connection:
        connection.execute(text(f"VACUUM INTO '{sqlite_path}'"))
        logger.info("Saved: %s", sqlite_path)

    stop_time = time()
    logger.info("Migrated database in %s", format_duration(stop_time - start_time))
