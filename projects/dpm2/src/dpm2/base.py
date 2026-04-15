"""SQLAlchemy declarative base and registry for DPM models.

The registry installs a ``type_annotation_map`` that routes
``Mapped[datetime.date]`` and ``Mapped[datetime.datetime]`` annotations
through our custom :class:`DPMDate` / :class:`DPMDateTime` ``TypeDecorator``
wrappers, so generated models can keep plain stdlib type hints while still
accepting the mix of date formats seen in DPM databases.
"""

from __future__ import annotations

import datetime

from sqlalchemy.orm import DeclarativeMeta, registry

from dpm2.types import DPMDate, DPMDateTime

dpm_registry = registry(
    type_annotation_map={
        datetime.date: DPMDate,
        datetime.datetime: DPMDateTime,
    },
)


class DPM(metaclass=DeclarativeMeta):
    """Declarative base class for all generated DPM models."""

    __abstract__ = True
    registry = dpm_registry
    metadata = dpm_registry.metadata
