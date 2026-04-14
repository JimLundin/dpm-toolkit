"""Tests that verify real data can be queried through the SQLAlchemy models.

These tests require the bundled ``dpm.sqlite`` database and are automatically
skipped when it is not available (it is generated during the release process).

They verify that the generated artifact (models.py + dpm.sqlite) actually
works end-to-end: ORM selects, relationship traversal, joins, and basic
data integrity.  All model references are resolved dynamically so the tests
work with any DPM schema version.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

import pytest
from dpm2 import models
from dpm2.models import DPM
from sqlalchemy import Table as AlchemyTable
from sqlalchemy import func, select
from sqlalchemy import inspect as sa_inspect

from .conftest import requires_db

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = requires_db


# ---------------------------------------------------------------------------
# Dynamic model discovery
# ---------------------------------------------------------------------------


def _orm_model_classes() -> list[type[DPM]]:
    """Return every concrete ORM model class defined in ``dpm2.models``."""
    return [
        obj
        for obj in vars(models).values()
        if inspect.isclass(obj) and issubclass(obj, DPM) and obj is not DPM
    ]


def _alchemy_tables() -> list[AlchemyTable]:
    """Return the raw ``AlchemyTable`` objects (non-ORM tables)."""
    return [
        obj
        for obj in vars(models).values()
        if isinstance(obj, AlchemyTable)
    ]


def _models_with_relationships() -> list[tuple[type[DPM], str]]:
    """Return (model, relationship_name) pairs for all models with relationships."""
    pairs = []
    for cls in _orm_model_classes():
        try:
            mapper = sa_inspect(cls)
        except Exception:  # noqa: BLE001, S112
            continue
        pairs.extend((cls, rel.key) for rel in mapper.relationships)
    return pairs


_ORM_MODELS = _orm_model_classes()
_ALCHEMY_TABLES = _alchemy_tables()
_REL_PAIRS = _models_with_relationships()


# ---------------------------------------------------------------------------
# Database connectivity
# ---------------------------------------------------------------------------


class TestDatabaseConnectivity:
    def test_tables_present_in_database(self, db_engine: Session) -> None:
        """The real database should contain all tables defined in the metadata."""
        inspector = sa_inspect(db_engine)
        db_tables = set(inspector.get_table_names())
        metadata_tables = set(DPM.metadata.tables.keys())
        missing = metadata_tables - db_tables
        assert not missing, f"Tables in metadata but not in database: {missing}"


# ---------------------------------------------------------------------------
# ORM queryability — every model can be selected
# ---------------------------------------------------------------------------


class TestORMQueryability:
    """Each ORM model should be queryable against the real database."""

    @pytest.mark.parametrize(
        "model_cls",
        _ORM_MODELS,
        ids=[cls.__name__ for cls in _ORM_MODELS],
    )
    def test_select_model(self, db_session: Session, model_cls: type[DPM]) -> None:
        """``select(Model).limit(1)`` must succeed without mapping errors."""
        db_session.execute(select(model_cls).limit(1)).first()


# ---------------------------------------------------------------------------
# Raw AlchemyTable objects are queryable too
# ---------------------------------------------------------------------------


class TestAlchemyTables:
    @pytest.mark.parametrize(
        "table",
        _ALCHEMY_TABLES,
        ids=[t.name for t in _ALCHEMY_TABLES],
    )
    def test_select_alchemy_table(
        self, db_session: Session, table: AlchemyTable,
    ) -> None:
        """Raw AlchemyTable objects (non-ORM) should also be queryable."""
        db_session.execute(select(table).limit(1)).first()


# ---------------------------------------------------------------------------
# Row counts — database isn't empty
# ---------------------------------------------------------------------------


class TestRowCounts:
    """At least some tables should contain data."""

    def test_database_has_data(self, db_session: Session) -> None:
        """At least one ORM table should contain rows."""
        total = 0
        for cls in _ORM_MODELS:
            count = db_session.scalar(
                select(func.count()).select_from(cls.__table__),
            )
            total += count or 0
        assert total > 0, "Database has no data in any ORM table"

    @pytest.mark.parametrize(
        "model_cls",
        _ORM_MODELS,
        ids=[cls.__name__ for cls in _ORM_MODELS],
    )
    def test_row_count(self, db_session: Session, model_cls: type[DPM]) -> None:
        """Report the row count for each table (never fails, informational)."""
        count = db_session.scalar(
            select(func.count()).select_from(model_cls.__table__),
        )
        # This test exists to surface row counts in CI output.
        # An empty table is not necessarily a bug.
        assert count is not None


# ---------------------------------------------------------------------------
# Relationship traversal (fully dynamic)
# ---------------------------------------------------------------------------


class TestRelationships:
    """Verify that ORM relationships can be traversed without errors.

    Discovers all relationships from the mapper and tests each one.
    """

    @pytest.mark.parametrize(
        ("model_cls", "rel_name"),
        _REL_PAIRS,
        ids=[f"{cls.__name__}.{name}" for cls, name in _REL_PAIRS],
    )
    def test_relationship_loads(
        self,
        db_session: Session,
        model_cls: type[DPM],
        rel_name: str,
    ) -> None:
        """Loading a relationship attribute must not raise."""
        row = db_session.execute(select(model_cls).limit(1)).first()
        if row is None:
            pytest.skip(f"{model_cls.__name__} table is empty")
        obj = row[0]
        # Access the relationship — triggers the lazy load.
        getattr(obj, rel_name)


# ---------------------------------------------------------------------------
# Non-nullable columns have data
# ---------------------------------------------------------------------------


class TestDataIntegrity:
    """Verify that non-nullable mapped columns contain non-null values."""

    @pytest.mark.parametrize(
        "model_cls",
        _ORM_MODELS,
        ids=[cls.__name__ for cls in _ORM_MODELS],
    )
    def test_non_nullable_columns_populated(
        self, db_session: Session, model_cls: type[DPM],
    ) -> None:
        row = db_session.execute(select(model_cls).limit(1)).first()
        if row is None:
            pytest.skip(f"{model_cls.__name__} table is empty")
        obj = row[0]
        try:
            mapper = sa_inspect(type(obj))
        except Exception:  # noqa: BLE001
            pytest.skip(f"Cannot inspect mapper for {model_cls.__name__}")
        for attr in mapper.column_attrs:
            col = attr.columns[0]
            if not col.nullable:
                val = getattr(obj, attr.key)
                assert val is not None, (
                    f"{model_cls.__name__}.{attr.key} is NULL "
                    f"but column is NOT NULL"
                )
