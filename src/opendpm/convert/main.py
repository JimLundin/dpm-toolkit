"""Main module for database conversion."""

import logging
import time
from pathlib import Path

from sqlalchemy import MetaData, create_engine

from opendpm.convert.generation import ModelGenerator
from opendpm.convert.processing import (
    create_access_engine,
    execute_queries,
    fetch_database,
    populate_database,
)
from opendpm.convert.utils import format_time

logger = logging.getLogger(__name__)


def migrate_database(input_dir: str | Path, output_dir: str | Path) -> None:
    """Migrate Access databases to SQLite.

    Args:
        input_dir: Directory containing Access databases
        output_dir: Directory to store SQLite databases

    """
    start = time.time()

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    access_files = list(input_dir.glob("**/*.accdb"))
    if not access_files:
        logger.warning("No Access database files found in %s", input_dir)
        return

    logger.info("Found %d Access database files", len(access_files))

    target_engine = create_engine("sqlite:///:memory:")

    models: dict[str, MetaData] = {}
    for access_file in access_files:
        start_fetch = time.time()
        logger.info("Processing: %s", access_file.name)
        source_engine = create_access_engine(access_file)
        metadata, table_rows = fetch_database(source_engine)
        logger.info(
            "Fetched data from %s in %s",
            access_file.name,
            format_time(time.time() - start_fetch),
        )
        models[access_file.stem] = metadata
        start_populate = time.time()
        metadata.create_all(target_engine)
        populate_database(target_engine, table_rows)
        logger.info(
            "Processed %s in %s",
            access_file.stem,
            format_time(time.time() - start_populate),
        )

    with target_engine.connect() as connection:
        database_path = output_dir / "dpm.sqlite"
        execute_queries(
            connection,
            ["PRAGMA optimize", f"VACUUM INTO '{database_path}'"],
        )
        logger.info("Database saved to %s", database_path)

    # Write the models to disk

    for name, metadata in models.items():
        with (output_dir / f"{name}_model.py").open("w") as f:
            model = ModelGenerator(metadata).generate()
            f.write(model)

    stop = time.time()
    logger.info("Migrated databases in %s", format_time(stop - start))
