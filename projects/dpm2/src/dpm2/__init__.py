"""DPM2 Data Package Module.

This package contains data artifacts for DPM databases that are generated
during the release process using conversion logic from the DPM Toolkit project.

Example usage::

    from dpm2 import get_db, models

    # Get an in-memory database engine (default)
    engine = get_db()

    # Access generated models via namespace
    from dpm2.models import ConceptClass, EntityClass
"""

from __future__ import annotations

import importlib
import types

from .utils import disk_engine, get_db, in_memory_engine

models: types.ModuleType
try:
    models = importlib.import_module(".models", __package__)
except ImportError:
    models = types.ModuleType("dpm2.models")
    models.__doc__ = "Stub: models are generated during the release process."

__all__ = [
    "disk_engine",
    "get_db",
    "in_memory_engine",
    "models",
]
