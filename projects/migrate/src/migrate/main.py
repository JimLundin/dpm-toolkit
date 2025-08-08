"""Main module for database conversion."""

from datetime import UTC, datetime
from logging import getLogger
from pathlib import Path

from sqlalchemy import create_engine, text

from migrate.processing import (
    create_access_engine,
    extract_schema_and_data,
    load_data,
)

logger = getLogger(__name__)


def migrate_to_sqlite(source: Path, target: Path) -> None:
    """Migrate Access database to SQLite.

    Args:
        source: Directory or path to Access database
        target: Directory to store SQLite database

    """
    start_time = datetime.now(UTC)

    if not source.exists():
        logger.error("Source %s does not exist", source)
        return
    if source.suffix not in (".mdb", ".accdb"):
        logger.error(
            "Source must be an Access database file (.mdb or .accdb), found %s",
            source.suffix,
        )
        return

    if target.exists():
        logger.warning("Target database already exists, overwriting.")
        target.unlink(missing_ok=True)
    if target.suffix not in (".sqlite", ".db"):
        logger.error(
            "Target must be a SQLite database file (.sqlite or .db), found %s",
            target.suffix,
        )
        return

    logger.info("Processing: %s", source.stem)

    access = create_access_engine(source)
    metadata, tables = extract_schema_and_data(access)

    target.parent.mkdir(parents=True, exist_ok=True)

    sqlite = create_engine("sqlite:///:memory:")
    metadata.create_all(sqlite)
    load_data(sqlite, tables)

    with sqlite.connect() as connection:
        connection.execute(text(f"VACUUM INTO '{target}'"))
        logger.info("Saved: %s", target)

    stop_time = datetime.now(UTC)
    logger.info("Migrated database in %s", stop_time - start_time)
