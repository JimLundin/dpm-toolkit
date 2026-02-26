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
    """A group of tables within a module.

    Groups organise the concrete tables of a module into logical sections
    (e.g. "Credit Risk", "Capital Adequacy").  The relationship to module
    is inferred during the build by checking which module's tables appear
    in each group.

    Source tables: ``TableGroup`` + ``TableGroupComposition``
                   (linked to modules via shared ``Table`` rows)
    """

    __tablename__ = "TableGroup"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    name: Mapped[str]
    module_id: Mapped[int] = mapped_column(ForeignKey("Module.id"))


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

    Source tables: ``TableVersion`` / ``Table`` where ``is_abstract = True``,
                   plus synthetic entries for standalone concrete tables.
    """

    __tablename__ = "Template"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    name: Mapped[str]
    module_id: Mapped[int] = mapped_column(ForeignKey("Module.id"))


# ---------------------------------------------------------------------------
# Table — concrete reporting table
# ---------------------------------------------------------------------------


class Table(DPMLite):
    """A concrete reporting table within a module.

    Each row is a specific version of a table assigned to a module version.
    Tables belong to a ``TableGroup`` and always belong to a ``Template``
    (either a real abstract table or a synthetic one-to-one template for
    standalone tables).

    Source tables: ``ModuleVersionComposition`` → ``TableVersion`` → ``Table``
                   + ``TableGroupComposition`` for group membership
    """

    __tablename__ = "Table"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str]
    name: Mapped[str]
    module_id: Mapped[int] = mapped_column(ForeignKey("Module.id"))
    group_id: Mapped[int] = mapped_column(ForeignKey("TableGroup.id"))
    template_id: Mapped[int] = mapped_column(ForeignKey("Template.id"))
    order: Mapped[int | None]
