"""Build the dpmlite database from dpm2.

This script imports dpm2, queries the full DPM database for the tables
and data relevant to report building, table structure, and validation
rules, and writes a new simplified SQLite database.

Usage::

    python projects/dpmlite/build_db.py OUTPUT_PATH

The resulting database is a standalone artifact — dpmlite does not
depend on dpm2 at runtime.

To add or change what's included, edit the ``build_tables`` function.
Each table is defined by a CREATE TABLE statement and a SELECT query
against the dpm2 source.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def get_source() -> Engine:
    """Get an in-memory engine to the full dpm2 database."""
    from dpm2 import get_db  # noqa: PLC0415

    return get_db()


def build_tables(source: Engine, dest: sqlite3.Connection) -> None:
    """Define and populate the dpmlite tables from dpm2.

    Each block below creates one table in the destination database by
    querying the source.  Add, remove, or modify blocks here to change
    what dpmlite contains.

    Pattern for adding a table::

        with source.connect() as conn:
            dest.execute("CREATE TABLE ...")
            rows = conn.execute(text("SELECT ... FROM ..."))
            dest.executemany("INSERT INTO ... VALUES (...)", rows.fetchall())
    """
    # Report structure
    # ---------------------------------------------------------------
    # Populate once dpm2 models.py is committed and table names known

    # Table layout
    # ---------------------------------------------------------------

    # Validation rules
    # ---------------------------------------------------------------

    _ = source, dest  # placeholder until real queries are added


def build_database(output: Path) -> None:
    """Build the dpmlite SQLite database at *output*."""
    source = get_source()

    dest = sqlite3.connect(output)
    try:
        build_tables(source, dest)
        dest.execute("VACUUM")
        dest.commit()

        cursor = dest.execute(
            "SELECT name FROM sqlite_master"
            " WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            " ORDER BY name",
        )
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Built dpmlite database with {len(tables)} tables: {tables}")
    finally:
        dest.close()


def main() -> None:
    """Entry point for the build script."""
    if len(sys.argv) != 2:  # noqa: PLR2004
        print(f"Usage: {sys.argv[0]} OUTPUT_PATH", file=sys.stderr)
        sys.exit(1)

    output = Path(sys.argv[1])
    build_database(output)


if __name__ == "__main__":
    main()
