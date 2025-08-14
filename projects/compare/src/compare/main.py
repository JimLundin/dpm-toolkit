"""Database comparison functionality."""

from pathlib import Path

from sqlalchemy import create_engine

from compare.data import compare_data
from compare.schema import compare_schema


def compare_databases(
    source: Path,
    target: Path,
    comp_type: str = "both",
) -> dict[str, list[str]]:
    """Compare two SQLite databases and return differences."""
    if not source.exists() or not target.exists():
        msg = "Database file not found"
        raise FileNotFoundError(msg)

    source_engine = create_engine(
        f"sqlite:///{source}?mode=ro",
        connect_args={"uri": True},
    )
    target_engine = create_engine(
        f"sqlite:///{target}?mode=ro",
        connect_args={"uri": True},
    )

    result: dict[str, list[str]] = {}

    # Schema comparison
    if comp_type in ("schema", "both"):
        result["schema_changes"] = compare_schema(source_engine, target_engine)

    # Data comparison
    if comp_type in ("data", "both"):
        result["data_changes"] = compare_data(source_engine, target_engine)

    return result
