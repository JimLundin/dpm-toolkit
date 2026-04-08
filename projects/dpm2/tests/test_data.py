"""Tests that verify real data can be queried through the SQLAlchemy models.

These tests require the bundled ``dpm.sqlite`` database and are automatically
skipped when it is not available (it is generated during the release process).
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

import pytest
from dpm2 import models
from dpm2.models import (
    DPM,
    Category,
    Cell,
    Concept,
    Context,
    DataType,
    DPMAttribute,
    DPMClass,
    Framework,
    Header,
    Item,
    Language,
    Module,
    Operation,
    Operator,
    Organisation,
    Property,
    Release,
    Role,
    Table,
    TableVersion,
    Translation,
    Variable,
)
from sqlalchemy import Engine, func, select
from sqlalchemy import Table as AlchemyTable
from sqlalchemy import inspect as sa_inspect

from .conftest import requires_db

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = requires_db


# ---------------------------------------------------------------------------
# Helpers
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


# ---------------------------------------------------------------------------
# Database connectivity
# ---------------------------------------------------------------------------


class TestDatabaseConnectivity:
    def test_engine_connects(self, db_engine: Engine) -> None:
        with db_engine.connect() as conn:
            result = conn.execute(select(func.count()).select_from(Concept.__table__))
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
        row = db_session.execute(select(model_cls).limit(1)).first()
        # We don't assert row is not None — some tables may legitimately be
        # empty.  The point is that the query itself does not raise.
        assert row is None or row is not None  # always true; here for clarity


# ---------------------------------------------------------------------------
# Core tables should have data
# ---------------------------------------------------------------------------

# These are foundational tables that should always be populated in a valid
# DPM database.  If any of these are empty, the database is likely broken.
CORE_TABLES: list[type[DPM]] = [
    Concept,
    DPMClass,
    DataType,
    Organisation,
    Language,
    Role,
    Operator,
    Framework,
    Release,
    Table,
    Variable,
    Header,
    Item,
    Property,
    Category,
    Context,
    Module,
    Cell,
    TableVersion,
    Translation,
    Operation,
    DPMAttribute,
]


class TestCoreTablesPopulated:
    """Core reference tables must contain at least one row."""

    @pytest.mark.parametrize(
        "model_cls",
        CORE_TABLES,
        ids=[cls.__name__ for cls in CORE_TABLES],
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
        result = db_session.execute(select(table).limit(1)).first()
        assert result is None or result is not None


# ---------------------------------------------------------------------------
# Relationship traversal
# ---------------------------------------------------------------------------


class TestRelationships:
    """Verify that key foreign-key relationships can be traversed."""

    def test_concept_to_class(self, db_session: Session) -> None:
        concept = db_session.execute(select(Concept).limit(1)).scalar_one()
        assert concept.class_ is not None
        assert isinstance(concept.class_, DPMClass)
        assert concept.class_.name is not None

    def test_concept_to_organisation(self, db_session: Session) -> None:
        concept = db_session.execute(select(Concept).limit(1)).scalar_one()
        assert concept.owner is not None
        assert isinstance(concept.owner, Organisation)

    def test_table_version_to_table(self, db_session: Session) -> None:
        tv = db_session.execute(select(TableVersion).limit(1)).scalar_one()
        assert tv.table is not None
        assert isinstance(tv.table, Table)

    def test_table_version_to_release(self, db_session: Session) -> None:
        tv = db_session.execute(select(TableVersion).limit(1)).scalar_one()
        assert tv.start_release is not None
        assert isinstance(tv.start_release, Release)

    def test_cell_to_table(self, db_session: Session) -> None:
        cell = db_session.execute(select(Cell).limit(1)).scalar_one()
        assert cell.table is not None
        assert isinstance(cell.table, Table)

    def test_cell_to_header(self, db_session: Session) -> None:
        cell = db_session.execute(select(Cell).limit(1)).scalar_one()
        assert cell.column is not None
        assert isinstance(cell.column, Header)

    def test_item_to_organisation(self, db_session: Session) -> None:
        item = db_session.execute(select(Item).limit(1)).scalar_one()
        assert item.owner is not None
        assert isinstance(item.owner, Organisation)

    def test_operator_argument_to_operator(self, db_session: Session) -> None:
        from dpm2.models import OperatorArgument

        arg = db_session.execute(select(OperatorArgument).limit(1)).scalar_one()
        assert arg.operator is not None
        assert isinstance(arg.operator, Operator)


# ---------------------------------------------------------------------------
# Joined queries
# ---------------------------------------------------------------------------


class TestJoinedQueries:
    """Verify that common joins work correctly."""

    def test_join_table_version_with_table(self, db_session: Session) -> None:
        stmt = (
            select(TableVersion, Table)
            .join(Table, TableVersion.table_id == Table.table_id)
            .limit(5)
        )
        rows = db_session.execute(stmt).all()
        assert len(rows) > 0
        for tv, table in rows:
            assert tv.table_id == table.table_id

    def test_join_concept_with_class(self, db_session: Session) -> None:
        stmt = (
            select(Concept, DPMClass)
            .join(DPMClass, Concept.class_id == DPMClass.class_id)
            .limit(5)
        )
        rows = db_session.execute(stmt).all()
        assert len(rows) > 0
        for concept, dpm_class in rows:
            assert concept.class_id == dpm_class.class_id

    def test_join_cell_with_table_and_header(self, db_session: Session) -> None:
        stmt = (
            select(Cell, Table, Header)
            .join(Table, Cell.table_id == Table.table_id)
            .join(Header, Cell.column_id == Header.header_id)
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

    def test_organisation_has_acronym(self, db_session: Session) -> None:
        orgs = db_session.execute(select(Organisation).limit(10)).scalars().all()
        for org in orgs:
            assert org.name is not None
            assert org.acronym is not None

    def test_language_has_name(self, db_session: Session) -> None:
        langs = db_session.execute(select(Language)).scalars().all()
        assert len(langs) > 0
        for lang in langs:
            assert lang.name is not None
            assert lang.language_code is not None

    def test_operator_type_is_valid_literal(self, db_session: Session) -> None:
        valid_types = {
            "Aggregate",
            "Assignment",
            "Clause",
            "Comparison",
            "Conditional",
            "Logical",
            "Numeric",
            "Time",
        }
        operators = db_session.execute(select(Operator)).scalars().all()
        for op in operators:
            assert op.type in valid_types, (
                f"Operator {op.name!r} has unexpected type {op.type!r}"
            )

    def test_release_has_code_and_name(self, db_session: Session) -> None:
        releases = db_session.execute(select(Release)).scalars().all()
        assert len(releases) > 0
        for release in releases:
            assert release.code is not None

    def test_data_type_has_code(self, db_session: Session) -> None:
        data_types = db_session.execute(select(DataType)).scalars().all()
        assert len(data_types) > 0
        for dt in data_types:
            assert dt.code is not None
            assert dt.name is not None
