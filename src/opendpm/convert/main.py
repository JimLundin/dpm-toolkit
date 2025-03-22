"""Main module for database conversion."""

import logging
import time
from pathlib import Path

from sqlalchemy import create_engine

from opendpm.convert.processing import (
    create_access_engine,
    execute_queries,
    process_database,
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

    models: dict[str, str] = {}

    target_engine = create_engine(f"sqlite:///{output_dir}/dpm.sqlite")
    for access_file in access_files:
        start_convert = time.time()
        logger.info("Processing database: %s", access_file.name)
        source_engine = create_access_engine(access_file)
        models[access_file.stem] = process_database(source_engine, target_engine)
        stop_convert = time.time()
        logger.info(
            "Database %s converted in %s",
            access_file.stem,
            format_time(stop_convert - start_convert),
        )

    with target_engine.connect() as connection:
        execute_queries(
            connection,
            ["VACUUM", "PRAGMA optimize"],
        )

    for name, model in models.items():
        with (output_dir / f"{name}_model.py").open("w") as f:
            f.write(model)

    stop = time.time()
    logger.info("Migrated databases in %s", format_time(stop - start))
