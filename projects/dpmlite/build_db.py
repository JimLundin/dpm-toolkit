"""Build the dpmlite database from dpm2.

This script uses dpm2's SQLAlchemy models to read from the full DPM
database and dpmlite's own models to write into a new simplified
database.  Both sides are fully typed — if dpm2's schema changes in
a way that breaks a read, you get a real import or query error.

Usage::

    python projects/dpmlite/build_db.py OUTPUT_PATH

The resulting database is a standalone artifact — dpmlite does not
depend on dpm2 at runtime.

To add or change what's included, define new models in
``dpmlite.models`` and add the corresponding query here.
"""

from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from dpmlite.models import DPMLite


def build_database(output: Path) -> None:
    """Build the dpmlite SQLite database at *output*."""
    from dpm2 import get_db  # noqa: PLC0415

    source_engine = get_db()
    dest_engine = create_engine(f"sqlite:///{output}")

    # Create all tables defined in dpmlite.models
    DPMLite.metadata.create_all(dest_engine)

    with Session(source_engine) as source, Session(dest_engine) as dest:
        populate(source, dest)
        dest.commit()

    tables = list(DPMLite.metadata.tables)
    print(f"Built dpmlite database with {len(tables)} tables: {tables}")


def populate(source: Session, dest: Session) -> None:
    """Query dpm2 and insert into dpmlite.

    Each block reads from dpm2's typed models and writes into dpmlite's
    typed models.  Add new blocks here when adding tables to dpmlite.

    Example once models are defined::

        from dpm2.models import Template as DpmTemplate
        from dpmlite.models import Template

        for row in source.execute(select(DpmTemplate)):
            dest.add(Template(
                template_id=row.template_id,
                name=row.name,
            ))
    """
    _ = source, dest  # placeholder until real models are added
    _ = select  # used by populate blocks


def main() -> None:
    """Entry point for the build script."""
    if len(sys.argv) != 2:  # noqa: PLR2004
        print(f"Usage: {sys.argv[0]} OUTPUT_PATH", file=sys.stderr)
        sys.exit(1)

    build_database(Path(sys.argv[1]))


if __name__ == "__main__":
    main()
