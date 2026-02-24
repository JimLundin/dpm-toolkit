"""Filter a DPM SQLite database to only include tables in the dpmlite allowlist.

Usage::

    python projects/dpmlite/filter_db.py SOURCE DEST

Copies SOURCE to DEST, then drops all tables not in INCLUDED_TABLES and
vacuums the result.  Exits with a non-zero status if any table in the
allowlist is missing from the source database (guards against stale config).
"""

from __future__ import annotations

import shutil
import sqlite3
import sys
from pathlib import Path

from dpmlite.tables import INCLUDED_TABLES


def get_all_tables(conn: sqlite3.Connection) -> set[str]:
    """Return the set of user table names in the database."""
    query = (
        "SELECT name FROM sqlite_master"
        " WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    cursor = conn.execute(query)
    return {row[0] for row in cursor.fetchall()}


def validate_allowlist(
    all_tables: set[str],
    included: frozenset[str],
) -> None:
    """Verify every table in the allowlist exists in the source database."""
    missing = included - all_tables
    if missing:
        print(
            f"ERROR: allowlist references tables not in the source database: "
            f"{sorted(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)


def filter_database(source: Path, dest: Path) -> None:
    """Copy *source* to *dest* and drop tables not in the allowlist."""
    if not INCLUDED_TABLES:
        print("ERROR: INCLUDED_TABLES is empty — nothing to keep.", file=sys.stderr)
        sys.exit(1)

    shutil.copy2(source, dest)

    conn = sqlite3.connect(dest)
    try:
        all_tables = get_all_tables(conn)
        validate_allowlist(all_tables, INCLUDED_TABLES)

        to_drop = all_tables - INCLUDED_TABLES
        for table in sorted(to_drop):
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            print(f"  dropped {table}")

        conn.execute("VACUUM")
        conn.commit()

        kept = get_all_tables(conn)
        print(
            f"Filtered: {len(all_tables)} -> {len(kept)} tables "
            f"({len(to_drop)} dropped)",
        )
    finally:
        conn.close()


def main() -> None:
    """Entry point for the filter script."""
    if len(sys.argv) != 3:  # noqa: PLR2004
        print(f"Usage: {sys.argv[0]} SOURCE DEST", file=sys.stderr)
        sys.exit(1)

    source = Path(sys.argv[1])
    dest = Path(sys.argv[2])

    if not source.exists():
        print(f"ERROR: source database not found: {source}", file=sys.stderr)
        sys.exit(1)

    filter_database(source, dest)


if __name__ == "__main__":
    main()
