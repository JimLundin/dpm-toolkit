"""Tests that verify real data can be queried through the SQLAlchemy models.

These tests require the bundled ``dpm.sqlite`` database and are automatically
skipped when it is not available (it is generated during the release process).

All model references are resolved dynamically so the tests work with any
DPM schema version (the generated models.py varies across database versions).
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

import pytest
from dpm2 import models
from dpm2.models import DPM
from sqlalchemy import Engine, func, select
from sqlalchemy import Table as AlchemyTable
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


def _get_model(name: str) -> type[DPM] | None:
    """Get a model class by name, or None if it doesn't exist in this schema."""
    cls = getattr(models, name, None)
    if cls is not None and inspect.isclass(cls) and issubclass(cls, DPM):
        return cls
    return None


def _models_with_relationships() -> list[tuple[type[DPM], str]]:
    """Return (model, relationship_name) pairs for all models with relationships."""
    pairs = []
    for cls in _orm_model_classes():
        mapper = sa_inspect(cls)
        pairs.extend((cls, rel.key) for rel in mapper.relationships)
    return pairs


# Names of tables that should be populated in any valid DPM database.
_CORE_TABLE_NAMES = [
    "Concept",
    "DPMClass",
    "DataType",
    "Organisation",
    "Language",
    "Role",
    "Operator",
    "Framework",
    "Release",
    "Table",
    "Variable",
    "Header",
    "Item",
    "Property",
    "Category",
    "Context",
    "Module",
    "Cell",
    "TableVersion",
    "Translation",
    "Operation",
    "DPMAttribute",
]


def _available_core_tables() -> list[type[DPM]]:
    return [cls for name in _CORE_TABLE_NAMES if (cls := _get_model(name)) is not None]


# ---------------------------------------------------------------------------
# Database connectivity
# ---------------------------------------------------------------------------


class TestDatabaseConnectivity:
    def test_engine_connects(self, db_engine: Engine) -> None:
        with db_engine.connect() as conn:
            first = _orm_model_classes()[0]
            result = conn.execute(
                select(func.count()).select_from(first.__table__),
            )
            assert result.scalar() is not None

    def test_tables_present_in_database(self, db_engine: Engine) -> None:
        """The real database should contain all tables defined in the metadata."""
        inspector = sa_inspect(db_engine)
        db_tables = set(inspector.get_table_names())
        metadata_tables = set(DPM.metadata.tables.keys())
        missing = metadata_tables - db_tables
        assert not missing, f"Tables in metadata but not in database: {missing}"


# ---------------------------------------------------------------------------
# ORM queryability — every model returns rows
# ---------------------------------------------------------------------------


class TestORMQueryability:
    """Each ORM model should be queryable and the statement should not error."""

    @pytest.mark.parametrize(
        "model_cls",
        _orm_model_classes(),
        ids=[cls.__name__ for cls in _orm_model_classes()],
    )
    def test_select_model(self, db_session: Session, model_cls: type[DPM]) -> None:
        """``select(Model).limit(1)`` must succeed without mapping errors."""
        db_session.execute(select(model_cls).limit(1)).first()


# ---------------------------------------------------------------------------
# Core tables should have data
# ---------------------------------------------------------------------------


class TestCoreTablesPopulated:
    """Core reference tables must contain at least one row."""

    @pytest.mark.parametrize(
        "model_cls",
        _available_core_tables(),
        ids=[cls.__name__ for cls in _available_core_tables()],
    )
    def test_table_not_empty(
        self, db_session: Session, model_cls: type[DPM],
    ) -> None:
        count = db_session.scalar(
            select(func.count()).select_from(model_cls.__table__),
        )
        assert count is not None
        assert count > 0, f"{model_cls.__name__} table is empty"


# ---------------------------------------------------------------------------
# Raw AlchemyTable objects are queryable too
# ---------------------------------------------------------------------------


class TestAlchemyTables:
    @pytest.mark.parametrize(
        "table",
        _alchemy_tables(),
        ids=[t.name for t in _alchemy_tables()],
    )
    def test_select_alchemy_table(
        self, db_session: Session, table: AlchemyTable,
    ) -> None:
        """Raw AlchemyTable objects (non-ORM) should also be queryable."""
        db_session.execute(select(table).limit(1)).first()


# ---------------------------------------------------------------------------
# Relationship traversal (fully dynamic)
# ---------------------------------------------------------------------------

_REL_PAIRS = _models_with_relationships()


class TestRelationships:
    """Verify that ORM relationships can be traversed without errors.

    Discovers all relationships from the mapper and tests each one.
    """

    @pytest.mark.parametrize(
        ("model_cls", "rel_name"),
        _REL_PAIRS,
        ids=[f"{cls.__name__}.{name}" for cls, name in _REL_PAIRS],
    )
    def test_relationship_traversal(
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
        _available_core_tables(),
        ids=[cls.__name__ for cls in _available_core_tables()],
    )
    def test_non_nullable_columns_populated(
        self, db_session: Session, model_cls: type[DPM],
    ) -> None:
        row = db_session.execute(select(model_cls).limit(1)).first()
        if row is None:
            pytest.skip(f"{model_cls.__name__} table is empty")
        obj = row[0]
        mapper = sa_inspect(type(obj))
        for attr in mapper.column_attrs:
            col = attr.columns[0]
            if not col.nullable:
                val = getattr(obj, attr.key)
                assert val is not None, (
                    f"{model_cls.__name__}.{attr.key} is NULL but column is NOT NULL"
                )
