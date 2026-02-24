"""DPM Lite Data Package Module.

Lightweight subset of DPM 2.0 focused on report structure, table layout,
and validation rules.  Derived from the full dpm2 package by filtering
the database to only the tables needed for building and validating reports.

Example usage::

    from dpmlite import get_db, models

    # Get an in-memory database engine (default)
    engine = get_db()

    # Access generated models via namespace
    from dpmlite.models import TableVersion, Cell
"""

from __future__ import annotations

import importlib
import types

from .utils import disk_engine, get_db, in_memory_engine

models: types.ModuleType
try:
    models = importlib.import_module(".models", __package__)
except ImportError:
    models = types.ModuleType("dpmlite.models")
    models.__doc__ = "Stub: models are generated during the release process."

__all__ = [
    "disk_engine",
    "get_db",
    "in_memory_engine",
    "models",
]
