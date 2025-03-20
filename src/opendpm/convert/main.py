"""Main module for database conversion."""

import logging
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

    total_elapsed = 0.0

    for access_file in access_files:
        relative_path = access_file.relative_to(input_dir)
        output_file = output_dir / relative_path.with_suffix(".sqlite")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with target_engine.connect() as target_conn:
            total_elapsed += process_database(access_file, target_conn)

    logger.info(
        "Total migration time: %s for %d databases",
        format_time(total_elapsed),
        len(access_files),
    )
