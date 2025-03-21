"""Main module for database conversion."""

import logging
import time
from pathlib import Path

from sqlalchemy import create_engine

from opendpm.convert.processing import process_database
from opendpm.convert.utils import format_time

logger = logging.getLogger(__name__)


def migrate_database(input_dir: str | Path, output_dir: str | Path) -> None:
    """Migrate Access databases to SQLite.

    Args:
        input_dir: Directory containing Access databases
        output_dir: Directory to store SQLite databases

    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    target_engine = create_engine(f"sqlite:///{output_dir}/dpm.sqlite")

    access_files = list(input_dir.glob("**/*.accdb"))
    if not access_files:
        logger.warning("No Access database files found in %s", input_dir)
        return

    logger.info("Found %d Access database files", len(access_files))

    start_time = time.time()

    models = {
        i: process_database(access_file, target_engine)
        for i, access_file in enumerate(access_files)
    }

    for i, model in models.items():
        with (output_dir / f"{i}_dpm_model.py").open("w") as f:
            f.write(model)

    logger.info("Migrated databases in %s", format_time(time.time() - start_time))
