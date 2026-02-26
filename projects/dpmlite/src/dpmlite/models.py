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
    organisation: Mapped[str]
