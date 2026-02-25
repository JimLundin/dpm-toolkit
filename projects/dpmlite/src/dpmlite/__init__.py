"""DPM Lite Data Package Module.

Lightweight subset of DPM 2.0 focused on report structure, table layout,
and validation rules.  Built from the full dpm2 package using typed
SQLAlchemy queries — both the source reads and destination writes are
fully type-checked.

Example usage::

    from dpmlite import get_db, models

    engine = get_db()
"""

from __future__ import annotations

from dpmlite import models
from dpmlite.utils import disk_engine, get_db, in_memory_engine

__all__ = [
    "disk_engine",
    "get_db",
    "in_memory_engine",
    "models",
]
