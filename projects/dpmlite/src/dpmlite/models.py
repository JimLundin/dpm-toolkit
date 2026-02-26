"""SQLAlchemy models for the DPM Lite database.

These models define the simplified schema that dpmlite ships.  They are
hand-written (not generated) and represent an intentional subset of the
full DPM 2.0 schema focused on report structure, table layout, and
validation rules.

The build script (``build_db.py``) reads from ``dpm2.models`` and writes
into these models, so both sides are fully type-checked.  If dpm2's
schema changes in a way that breaks a query, you get a real error.

To add a new table:
  1. Define a new class below inheriting from ``DPMLite``.
  2. Add the corresponding query in ``build_db.py``.
"""

from __future__ import annotations

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class DPMLite(DeclarativeBase):
    """Base class for all DPM Lite models."""


# ---------------------------------------------------------------------------
# Module — top-level object, flattened from ModuleVersion + Module + Framework
# ---------------------------------------------------------------------------


class Module(DPMLite):
    """A reporting module (flattened from dpm2 ModuleVersion/Module/Framework).

    Each row represents a specific version of a module.  The framework and
    organisation are denormalised as plain strings so that downstream
    consumers don't need to join through the full DPM hierarchy.

    Source tables: ``ModuleVersion`` → ``Module`` → ``Framework``
                   → ``Concept`` → ``Organisation``
    """

    __tablename__ = "Module"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    name: Mapped[str]
    description: Mapped[str | None]
    version_number: Mapped[str]
    framework_code: Mapped[str]
    framework_name: Mapped[str]
    organisation_code: Mapped[str]
    organisation_name: Mapped[str]


# ---------------------------------------------------------------------------
# TableGroup — grouping of tables within a module
# ---------------------------------------------------------------------------


class TableGroup(DPMLite):
    """A template group.

    Groups organise templates (and therefore tables) into logical sections
    (e.g. "Credit Risk", "Capital Adequacy").  A group may be shared
    across multiple modules and is linked to them via
    ``ModuleGroupMembership``.  It links to templates via
    ``TemplateGroupMembership``.

    Source tables: ``TableGroup`` + ``TableGroupComposition``
                   (linked to modules via shared ``Table`` rows)
    """

    __tablename__ = "TableGroup"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    name: Mapped[str]


# ---------------------------------------------------------------------------
# ModuleGroupMembership — many-to-many between Module and TableGroup
# ---------------------------------------------------------------------------


class ModuleGroupMembership(DPMLite):
    """Junction linking template groups to the modules that include them.

    A group may appear in more than one module (e.g. "CoRep - General
    Information" is shared by COREP_ALM, COREP_LE, COREP_OF, etc.).
    """

    __tablename__ = "ModuleGroupMembership"

    module_id: Mapped[int] = mapped_column(
        ForeignKey("Module.id"), primary_key=True,
    )
    group_id: Mapped[int] = mapped_column(
        ForeignKey("TableGroup.id"), primary_key=True,
    )


# ---------------------------------------------------------------------------
# TemplateGroupMembership — many-to-many between TableGroup and Template
# ---------------------------------------------------------------------------


class TemplateGroupMembership(DPMLite):
    """Junction linking templates to the groups they belong to.

    A template may appear in more than one group within the same module
    (roughly 8 templates across the full DPM dataset have children that
    span two groups).
    """

    __tablename__ = "TemplateGroupMembership"

    group_id: Mapped[int] = mapped_column(
        ForeignKey("TableGroup.id"), primary_key=True,
    )
    template_id: Mapped[int] = mapped_column(
        ForeignKey("Template.id"), primary_key=True,
    )


# ---------------------------------------------------------------------------
# Template — abstract table that defines a shared shape for concrete tables
# ---------------------------------------------------------------------------


class Template(DPMLite):
    """A template that defines the shared shape for one or more concrete tables.

    In DPM, an *abstract* table (``is_abstract = True``) groups several
    concrete tables that share the same row/column structure but differ
    by reporting population or sheet variant (e.g. ``F_32.02`` spawns
    ``F_32.02.a``, ``F_32.02.b``).

    To keep the hierarchy strict (every table belongs to exactly one
    template), standalone concrete tables that have no abstract parent
    in DPM are given a synthetic template with the same code and name.

    A template does not belong to a module directly.  Its module is
    reachable via ``TemplateGroupMembership`` -> ``TableGroup`` ->
    ``ModuleGroupMembership`` -> ``Module``.

    Source tables: ``TableVersion`` / ``Table`` where ``is_abstract = True``,
                   plus synthetic entries for standalone concrete tables.
    """

    __tablename__ = "Template"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    name: Mapped[str]


# ---------------------------------------------------------------------------
# Table — concrete reporting table
# ---------------------------------------------------------------------------


class Table(DPMLite):
    """A concrete reporting table.

    Each row is a specific version of a table assigned to a module version.
    Tables always belong to a ``Template`` (either a real abstract table
    or a synthetic one-to-one template for standalone tables).  The module
    and group are reachable through the template's membership links.

    Source tables: ``ModuleVersionComposition`` → ``TableVersion`` → ``Table``
    """

    __tablename__ = "Table"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    name: Mapped[str]
    template_id: Mapped[int] = mapped_column(ForeignKey("Template.id"))
    order: Mapped[int | None]
