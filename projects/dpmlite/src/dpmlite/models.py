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

from typing import Literal

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


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

    group_memberships: Mapped[list[ModuleGroupMembership]] = relationship(
        back_populates="module", order_by="ModuleGroupMembership.order",
    )
    groups: Mapped[list[TableGroup]] = relationship(
        secondary="ModuleGroupMembership",
        order_by="ModuleGroupMembership.order",
        viewonly=True,
    )


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

    module_memberships: Mapped[list[ModuleGroupMembership]] = relationship(
        back_populates="group",
    )
    modules: Mapped[list[Module]] = relationship(
        secondary="ModuleGroupMembership", viewonly=True,
    )
    template_memberships: Mapped[list[TemplateGroupMembership]] = relationship(
        back_populates="group", order_by="TemplateGroupMembership.order",
    )
    templates: Mapped[list[Template]] = relationship(
        secondary="TemplateGroupMembership",
        order_by="TemplateGroupMembership.order",
        viewonly=True,
    )


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
    order: Mapped[int]

    module: Mapped[Module] = relationship(back_populates="group_memberships")
    group: Mapped[TableGroup] = relationship(
        back_populates="module_memberships",
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
    order: Mapped[int]

    group: Mapped[TableGroup] = relationship(
        back_populates="template_memberships",
    )
    template: Mapped[Template] = relationship(
        back_populates="group_memberships",
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

    group_memberships: Mapped[list[TemplateGroupMembership]] = relationship(
        back_populates="template",
    )
    groups: Mapped[list[TableGroup]] = relationship(
        secondary="TemplateGroupMembership", viewonly=True,
    )
    tables: Mapped[list[Table]] = relationship(back_populates="template")


# ---------------------------------------------------------------------------
# Table — concrete reporting table
# ---------------------------------------------------------------------------


class Table(DPMLite):
    """A concrete reporting table.

    Each row is a specific version of a table assigned to a module version.
    Tables always belong to a ``Template`` (either a real abstract table
    or a synthetic one-to-one template for standalone tables).  The module
    and group are reachable through the template's membership links.

    The ``has_open_*`` flags indicate whether the table allows dynamic
    extension along the column, row, or sheet axis.

    Source tables: ``ModuleVersionComposition`` - ``TableVersion`` - ``Table``
    """

    __tablename__ = "Table"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    name: Mapped[str]
    description: Mapped[str | None]
    template_id: Mapped[int] = mapped_column(ForeignKey("Template.id"))
    has_open_rows: Mapped[bool]
    has_open_sheets: Mapped[bool]
    has_open_columns: Mapped[bool]

    template: Mapped[Template] = relationship(back_populates="tables")
    cells: Mapped[list[Cell]] = relationship(back_populates="table")


# ---------------------------------------------------------------------------
# Cell - data point within a table
# ---------------------------------------------------------------------------


class Cell(DPMLite):
    """A cell within a concrete reporting table.

    Each cell represents a data point at a specific position in a table,
    defined by its column, row, and optional sheet coordinates (header
    codes from the X, Y, and Z axes respectively).

    The ``code`` field is the cell's display name (e.g.
    ``{C_01.00, r0010, c0010}``), taken from ``cell_code`` in the
    source ``TableVersionCell`` assignment.

    Source tables: ``TableVersionCell`` - ``Cell`` - ``Header``
                   - ``HeaderVersion`` (via ``TableVersionHeader``)
    """

    __tablename__ = "Cell"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    table_id: Mapped[int] = mapped_column(ForeignKey("Table.id"))
    column: Mapped[str]
    row: Mapped[str | None]
    sheet: Mapped[str | None]
    is_excluded: Mapped[bool]
    is_nullable: Mapped[bool]
    sign: Mapped[Literal["positive", "negative"] | None]
    variable_vid: Mapped[int | None]

    table: Mapped[Table] = relationship(back_populates="cells")
