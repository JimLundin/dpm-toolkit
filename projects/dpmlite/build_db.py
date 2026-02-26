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
from dpmlite.models import ModuleGroupMembership as LiteModuleGroup
from dpmlite.models import Table as LiteTable
from dpmlite.models import TableGroup as LiteTableGroup
from dpmlite.models import Template as LiteTemplate
from dpmlite.models import TemplateGroupMembership as LiteTemplateMembership


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
    populate_tables(source, dest)


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
            Organisation.acronym.label("organisation_code"),
            Organisation.name.label("organisation_name"),
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
                organisation_code=row.organisation_code,
                organisation_name=row.organisation_name,
            ),
        )


def populate_tables(source: Session, dest: Session) -> None:
    """Build the Module -> TableGroup -> Template -> Table hierarchy.

    The hierarchy is reconstructed from several DPM2 sources:

    * **TableGroup** - ``TableGroup`` rows, linked to a module by finding
      which groups contain that module's concrete tables (via
      ``TableGroupComposition`` and ``ModuleVersionComposition``).
    * **Template** - abstract ``Table``/``TableVersion`` rows that appear
      in ``ModuleVersionComposition``, plus synthetic one-to-one
      templates for standalone concrete tables (no abstract parent).
    * **TemplateGroupMembership** - many-to-many junction linking each
      template to the group(s) its concrete tables belong to.
    * **Table** - concrete ``TableVersion`` rows from
      ``ModuleVersionComposition``, each belonging to a template.
    """
    from dpm2.models import (  # noqa: PLC0415
        ModuleVersionComposition,
        Table,
        TableGroup,
        TableGroupComposition,
        TableVersion,
    )

    module_vids = [
        row[0] for row in dest.execute(select(LiteModule.id)).all()
    ]

    next_id = {"group": 1, "template": 1, "table": 1}
    # dpm2 key -> lite id (global, shared across modules)
    group_map: dict[tuple[str, str], int] = {}
    template_map: dict[int, int] = {}
    table_map: dict[int, int] = {}
    # dedup sets for junction rows
    seen_module_groups: set[tuple[int, int]] = set()
    seen_template_groups: set[tuple[int, int]] = set()

    for mvid in module_vids:
        mvc_rows = _fetch_module_tables(
            source, ModuleVersionComposition, TableVersion, Table, mvid,
        )
        if not mvc_rows:
            continue

        group_info = _resolve_groups(
            source, mvc_rows, TableGroup, TableGroupComposition,
        )
        _insert_groups(dest, group_info, group_map, next_id)
        _insert_module_group_memberships(
            dest, mvid, group_info, group_map, seen_module_groups,
        )
        _insert_templates(dest, mvc_rows, template_map, next_id)
        _link_templates_to_groups(
            dest, mvc_rows, group_info, group_map,
            template_map, seen_template_groups,
        )
        _insert_concrete_tables(
            dest, mvc_rows, template_map, table_map, next_id,
        )


def _fetch_module_tables(
    source: Session,
    MVC: type,  # noqa: N803
    TV: type,  # noqa: N803
    T: type,  # noqa: N803
    mvid: int,
) -> list:
    """Fetch all table entries for a single module version."""
    return source.execute(
        select(
            MVC.table_id, MVC.table_vid, MVC.order,
            TV.code.label("tv_code"),
            TV.name.label("tv_name"),
            TV.abstract_table_id,
            T.is_abstract,
        )
        .select_from(MVC)
        .join(TV, MVC.table_vid == TV.table_vid)
        .join(T, MVC.table_id == T.table_id)
        .where(MVC.module_vid == mvid)
        .order_by(MVC.order),
    ).all()


def _resolve_groups(
    source: Session,
    mvc_rows: list,
    TG: type,  # noqa: N803
    TGC: type,  # noqa: N803
) -> dict[int, tuple[str, str]]:
    """Map each concrete table_id to its (group_code, group_name)."""
    concrete_ids = [r.table_id for r in mvc_rows if not r.is_abstract]
    if not concrete_ids:
        return {}

    result: dict[int, tuple[str, str]] = {}
    for row in source.execute(
        select(TGC.table_id, TG.code, TG.name)
        .join(TG, TGC.table_group_id == TG.table_group_id)
        .where(TGC.table_id.in_(concrete_ids)),
    ):
        result[row.table_id] = (row.code, row.name)
    return result


def _insert_groups(
    dest: Session,
    group_info: dict[int, tuple[str, str]],
    group_map: dict[tuple[str, str], int],
    next_id: dict[str, int],
) -> None:
    """Insert new TableGroup rows into *group_map* (shared across modules)."""
    for code, name in group_info.values():
        key = (code, name)
        if key not in group_map:
            group_map[key] = next_id["group"]
            dest.add(
                LiteTableGroup(
                    id=next_id["group"], code=code, name=name,
                ),
            )
            next_id["group"] += 1


def _insert_module_group_memberships(
    dest: Session,
    mvid: int,
    group_info: dict[int, tuple[str, str]],
    group_map: dict[tuple[str, str], int],
    seen: set[tuple[int, int]],
) -> None:
    """Insert ModuleGroupMembership rows for this module version."""
    for group_key in set(group_info.values()):
        lite_group = group_map[group_key]
        pair = (mvid, lite_group)
        if pair not in seen:
            seen.add(pair)
            dest.add(LiteModuleGroup(module_id=mvid, group_id=lite_group))


def _insert_templates(
    dest: Session,
    mvc_rows: list,
    template_map: dict[int, int],
    next_id: dict[str, int],
) -> None:
    """Insert new Template rows into *template_map* (shared across modules).

    Creates a template for every abstract table (real templates) and
    for every standalone concrete table that has no abstract parent
    (synthetic one-to-one templates).  Already-seen templates (by dpm2
    table_id) are skipped, so the same template is reused across modules.
    """
    for r in mvc_rows:
        # Determine if this row produces a template key
        if r.is_abstract:
            key = r.table_id
        elif r.abstract_table_id is None:
            key = r.table_id  # standalone -> synthetic template
        else:
            continue  # concrete with abstract parent, no new template

        if key not in template_map:
            template_map[key] = next_id["template"]
            dest.add(
                LiteTemplate(
                    id=next_id["template"], code=r.tv_code,
                    name=r.tv_name,
                ),
            )
            next_id["template"] += 1


def _link_templates_to_groups(  # noqa: PLR0913
    dest: Session,
    mvc_rows: list,
    group_info: dict[int, tuple[str, str]],
    group_map: dict[tuple[str, str], int],
    template_map: dict[int, int],
    seen: set[tuple[int, int]],
) -> None:
    """Insert TemplateGroupMembership rows linking templates to groups.

    The link is inferred from each concrete table's group membership:
    if a table belongs to group G and template T, then T belongs to G.
    """
    for r in mvc_rows:
        if r.is_abstract:
            continue
        group_key = group_info.get(r.table_id)
        if group_key is None:
            continue
        tmpl_key = r.abstract_table_id or r.table_id
        lite_group = group_map[group_key]
        lite_template = template_map[tmpl_key]
        pair = (lite_group, lite_template)
        if pair not in seen:
            seen.add(pair)
            dest.add(
                LiteTemplateMembership(
                    group_id=lite_group,
                    template_id=lite_template,
                ),
            )


def _insert_concrete_tables(
    dest: Session,
    mvc_rows: list,
    template_map: dict[int, int],
    table_map: dict[int, int],
    next_id: dict[str, int],
) -> None:
    """Insert concrete Table rows (deduplicated globally by table_id)."""
    for r in mvc_rows:
        if r.is_abstract or r.table_id in table_map:
            continue
        tmpl_key = r.abstract_table_id or r.table_id
        if tmpl_key not in template_map:
            continue
        table_map[r.table_id] = next_id["table"]
        dest.add(
            LiteTable(
                id=next_id["table"], code=r.tv_code, name=r.tv_name,
                template_id=template_map[tmpl_key],
                order=r.order,
            ),
        )
        next_id["table"] += 1


def main() -> None:
    """Entry point for the build script."""
    if len(sys.argv) != 2:  # noqa: PLR2004
        print(f"Usage: {sys.argv[0]} OUTPUT_PATH", file=sys.stderr)
        sys.exit(1)

    build_database(Path(sys.argv[1]))


if __name__ == "__main__":
    main()
