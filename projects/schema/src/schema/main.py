"""Main module for schema generation."""

from datetime import UTC, datetime
from logging import getLogger
from pathlib import Path

from sqlalchemy import MetaData, create_engine

from schema.generation import Model

logger = getLogger(__name__)


def generate_schema(source: Path, target: Path = Path(), name: str = "dpm") -> None:
    """Generate SQLAlchemy schema from migrated SQLite database.

    Args:
        source: Path to SQLite database
        target: Directory to save SQLAlchemy schema file
        name: Desired name of the output schema file

    """
    start_time = datetime.now(UTC)

    if not source.is_file():
        logger.warning("%s is not an SQLite file", source)
        return

    database = create_engine(f"sqlite:///{source}?mode=ro", connect_args={"uri": True})
    metadata = MetaData()
    metadata.reflect(bind=database)

    logger.info("Processing: %s", source.stem)

    target.mkdir(parents=True, exist_ok=True)
    file_path = target / f"{name}.py"
    with file_path.open("w") as model_file:
        schema = Model(metadata).render()
        model_file.write(schema)

    stop_time = datetime.now(UTC)
    logger.info("Generated schema in %s", stop_time - start_time)
