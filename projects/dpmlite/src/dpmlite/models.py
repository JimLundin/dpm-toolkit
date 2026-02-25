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

from sqlalchemy.orm import DeclarativeBase


class DPMLite(DeclarativeBase):
    """Base class for all DPM Lite models."""


# Report structure — define models once dpm2 models.py is committed

# Table layout

# Validation rules
