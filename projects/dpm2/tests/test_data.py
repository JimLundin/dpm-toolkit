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


def _require_model(name: str) -> type[DPM]:
    """Get a model class by name, skip the test if not in this schema version."""
    cls = _get_model(name)
    if cls is None:
        pytest.skip(f"{name} not present in this schema version")
    return cls


# Names of tables that should be populated in any valid DPM database.
# Looked up dynamically so the tests still collect when a schema version
# doesn't contain one of these.
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
            # Pick the first ORM table to do a basic count query.
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
# Relationship traversal
# ---------------------------------------------------------------------------


class TestRelationships:
    """Verify that foreign-key relationships can be traversed.

    Each test dynamically looks up the models it needs and skips if
    the current schema version doesn't include them.
    """

    def test_concept_to_class(self, db_session: Session) -> None:
        concept_cls = _require_model("Concept")
        row = db_session.execute(select(concept_cls).limit(1)).scalar_one()
        assert row.class_ is not None
        assert row.class_.name is not None

    def test_concept_to_organisation(self, db_session: Session) -> None:
        concept_cls = _require_model("Concept")
        row = db_session.execute(select(concept_cls).limit(1)).scalar_one()
        assert row.owner is not None

    def test_table_version_to_table(self, db_session: Session) -> None:
        tv_cls = _require_model("TableVersion")
        row = db_session.execute(select(tv_cls).limit(1)).scalar_one()
        assert row.table is not None

    def test_table_version_to_release(self, db_session: Session) -> None:
        tv_cls = _require_model("TableVersion")
        row = db_session.execute(select(tv_cls).limit(1)).scalar_one()
        assert row.start_release is not None

    def test_cell_to_table(self, db_session: Session) -> None:
        cell_cls = _require_model("Cell")
        row = db_session.execute(select(cell_cls).limit(1)).scalar_one()
        assert row.table is not None

    def test_cell_to_header(self, db_session: Session) -> None:
        cell_cls = _require_model("Cell")
        row = db_session.execute(select(cell_cls).limit(1)).scalar_one()
        assert row.column is not None

    def test_item_to_organisation(self, db_session: Session) -> None:
        item_cls = _require_model("Item")
        row = db_session.execute(select(item_cls).limit(1)).scalar_one()
        assert row.owner is not None

    def test_operator_argument_to_operator(self, db_session: Session) -> None:
        arg_cls = _require_model("OperatorArgument")
        row = db_session.execute(select(arg_cls).limit(1)).scalar_one()
        assert row.operator is not None


# ---------------------------------------------------------------------------
# Joined queries
# ---------------------------------------------------------------------------


class TestJoinedQueries:
    """Verify that common joins work correctly."""

    def test_join_concept_with_class(self, db_session: Session) -> None:
        concept_cls = _require_model("Concept")
        class_cls = _require_model("DPMClass")
        stmt = (
            select(concept_cls, class_cls)
            .join(class_cls, concept_cls.class_id == class_cls.class_id)
            .limit(5)
        )
        rows = db_session.execute(stmt).all()
        assert len(rows) > 0
        for concept, dpm_class in rows:
            assert concept.class_id == dpm_class.class_id

    def test_join_table_version_with_table(self, db_session: Session) -> None:
        tv_cls = _require_model("TableVersion")
        table_cls = _require_model("Table")
        stmt = (
            select(tv_cls, table_cls)
            .join(table_cls, tv_cls.table_id == table_cls.table_id)
            .limit(5)
        )
        rows = db_session.execute(stmt).all()
        assert len(rows) > 0
        for tv, table in rows:
            assert tv.table_id == table.table_id

    def test_join_cell_with_table_and_header(self, db_session: Session) -> None:
        cell_cls = _require_model("Cell")
        table_cls = _require_model("Table")
        header_cls = _require_model("Header")
        stmt = (
            select(cell_cls, table_cls, header_cls)
            .join(table_cls, cell_cls.table_id == table_cls.table_id)
            .join(header_cls, cell_cls.column_id == header_cls.header_id)
            .limit(5)
        )
        rows = db_session.execute(stmt).all()
        assert len(rows) > 0
        for cell, table, header in rows:
            assert cell.table_id == table.table_id
            assert cell.column_id == header.header_id


# ---------------------------------------------------------------------------
# Data integrity
# ---------------------------------------------------------------------------


class TestDataIntegrity:
    """Spot-check data types and constraints on queried data."""

    def test_organisation_fields(self, db_session: Session) -> None:
        org_cls = _require_model("Organisation")
        orgs = db_session.execute(select(org_cls).limit(10)).scalars().all()
        for org in orgs:
            assert org.name is not None
            assert org.acronym is not None

    def test_language_fields(self, db_session: Session) -> None:
        lang_cls = _require_model("Language")
        langs = db_session.execute(select(lang_cls)).scalars().all()
        assert len(langs) > 0
        for lang in langs:
            assert lang.name is not None

    def test_release_has_code(self, db_session: Session) -> None:
        release_cls = _require_model("Release")
        releases = db_session.execute(select(release_cls)).scalars().all()
        assert len(releases) > 0
        for release in releases:
            assert release.code is not None

    def test_data_type_has_code(self, db_session: Session) -> None:
        dt_cls = _require_model("DataType")
        data_types = db_session.execute(select(dt_cls)).scalars().all()
        assert len(data_types) > 0
        for dt in data_types:
            assert dt.code is not None
            assert dt.name is not None
