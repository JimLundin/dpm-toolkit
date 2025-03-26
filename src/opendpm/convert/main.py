"""Main module for database conversion."""

import logging
from pathlib import Path
from time import time

from sqlalchemy import MetaData, create_engine, text

from opendpm.convert.generation import Model
from opendpm.convert.processing import (
    create_access_engine,
    fetch_database,
    populate_database,
)
from opendpm.convert.utils import format_time

logger = logging.getLogger(__name__)


def migrate_database(input_dir: Path, output_dir: Path) -> None:
    """Migrate Access databases to SQLite.

    Args:
        input_dir: Directory containing Access databases
        output_dir: Directory to store SQLite database

    """
    start = time()

    output_dir.mkdir(parents=True, exist_ok=True)

    access_files = list(input_dir.glob("**/*.accdb"))
    if not access_files:
        logger.warning("No Access database files found in %s", input_dir)
        return

    logger.info("Found %d Access database files", len(access_files))

    target_engine = create_engine("sqlite:///:memory:")

    models: list[tuple[str, MetaData]] = []
    for access_file in access_files:
        file_name = access_file.stem
        logger.info("Processing: %s", file_name)

        start_fetch = time()
        source_engine = create_access_engine(access_file)
        metadata, table_rows = fetch_database(source_engine)
        models.append((file_name, metadata))
        end_fetch = time()
        logger.info("Fetched in %s", format_time(end_fetch - start_fetch))

        start_populate = time()
        metadata.create_all(target_engine)
        populate_database(target_engine, table_rows)
        end_populate = time()
        logger.info("Processed in %s", format_time(end_populate - start_populate))

    for name, metadata in models:
        output_path = output_dir / f"{name}_model.py"
        with output_path.open("w") as f:
            model = Model(metadata)
            f.write(model.render())

    with target_engine.connect() as connection:
        start_save = time()
        target_path = output_dir / "dpm.sqlite"
        connection.execute(text(f"VACUUM INTO '{target_path}'"))
        end_save = time()
        logger.info("Saved: %s in %s", target_path, format_time(end_save - start_save))

    stop = time()
    logger.info("Migrated databases in %s", format_time(stop - start))
