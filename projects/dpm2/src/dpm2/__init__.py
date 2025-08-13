"""DPM2 Data Package Module.

This package contains data artifacts for DPM databases that are generated
during the release process using conversion logic from the OpenDPM project.

Example usage:
    from dpm2 import get_db()

    # Get database connection
    engine = get_db()

    # Access generated models via namespace
    from dpm2.models import ConceptClass, EntityClass
    # Or access via attribute
    concept = dpm2.models.ConceptClass(...)
"""

# The version is set by the build process
__version__ = None

# Import utilities at top level
# Import models module as a namespace (if it exists)
from . import models
from .utils import disk_engine, get_db, in_memory_engine

__all__ = [
    "disk_engine",
    "get_db",
    "in_memory_engine",
    "models",
]
