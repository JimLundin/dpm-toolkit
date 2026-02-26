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
from dpmlite.models import Module as LiteModule


def build_database(output: Path) -> None:
    """Build the dpmlite SQLite database at *output*."""
    from dpm2 import get_db  # noqa: PLC0415

    source_engine = get_db()
    dest_engine = create_engine(f"sqlite:///{output}")

    DPMLite.metadata.create_all(dest_engine)

    with Session(source_engine) as source, Session(dest_engine) as dest:
        populate(source, dest)
        dest.commit()

    tables = list(DPMLite.metadata.tables)
    print(f"Built dpmlite database with {len(tables)} tables: {tables}")


def populate(source: Session, dest: Session) -> None:
    """Query dpm2 and insert into dpmlite.

    Each block reads from dpm2's typed models and writes into dpmlite's
    typed models.
    """
    populate_modules(source, dest)


def populate_modules(source: Session, dest: Session) -> None:
    """Flatten ModuleVersion/Module/Framework/Concept/Organisation."""
    from dpm2.models import (  # noqa: PLC0415
        Concept,
        Framework,
        ModuleVersion,
        Organisation,
    )
    from dpm2.models import Module as DpmModule  # noqa: PLC0415

    rows = source.execute(
        select(
            ModuleVersion.module_vid,
            ModuleVersion.code,
            ModuleVersion.name,
            ModuleVersion.description,
            ModuleVersion.version_number,
            Framework.code.label("framework_code"),
            Framework.name.label("framework_name"),
            Organisation.acronym.label("organisation"),
        )
        .join(DpmModule, ModuleVersion.module_id == DpmModule.module_id)
        .join(Framework, DpmModule.framework_id == Framework.framework_id)
        .join(Concept, Framework.row_guid == Concept.concept_guid)
        .join(Organisation, Concept.owner_id == Organisation.org_id),
    )

    for row in rows:
        dest.add(
            LiteModule(
                id=row.module_vid,
                code=row.code,
                name=row.name,
                description=row.description,
                version_number=row.version_number,
                framework_code=row.framework_code,
                framework_name=row.framework_name,
                organisation=row.organisation,
            ),
        )


def main() -> None:
    """Entry point for the build script."""
    if len(sys.argv) != 2:  # noqa: PLR2004
        print(f"Usage: {sys.argv[0]} OUTPUT_PATH", file=sys.stderr)
        sys.exit(1)

    build_database(Path(sys.argv[1]))


if __name__ == "__main__":
    main()
