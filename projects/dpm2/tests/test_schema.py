"""Tests that the generated SQLAlchemy schema is structurally valid.

These tests always run — they do not require the bundled dpm.sqlite
database.  They create the full schema in an in-memory SQLite database
and verify that all ORM models are consistent and well-formed.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from dpm2 import models
from dpm2.models import DPM
from sqlalchemy import Engine, func, select
from sqlalchemy import inspect as sa_inspect

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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


def _orm_model_ids() -> list[str]:
    return [cls.__name__ for cls in _orm_model_classes()]


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


class TestSchemaCreation:
    """Verify that DPM.metadata.create_all() produces a valid SQLite schema."""

    def test_create_all_succeeds(self, empty_engine: Engine) -> None:
        """The metadata should create all tables without errors."""
        inspector = sa_inspect(empty_engine)
        table_names = inspector.get_table_names()
        assert len(table_names) > 0

    def test_expected_table_count(self, empty_engine: Engine) -> None:
        """Every table in the metadata should be created in the database."""
        inspector = sa_inspect(empty_engine)
        db_tables = set(inspector.get_table_names())
        metadata_tables = set(DPM.metadata.tables.keys())
        missing = metadata_tables - db_tables
        assert not missing, f"Tables in metadata but not in database: {missing}"

    def test_tables_have_columns(self, empty_engine: Engine) -> None:
        """Every created table should have at least one column."""
        inspector = sa_inspect(empty_engine)
        for table_name in inspector.get_table_names():
            columns = inspector.get_columns(table_name)
            assert len(columns) > 0, f"{table_name} has no columns"


# ---------------------------------------------------------------------------
# ORM model consistency
# ---------------------------------------------------------------------------


class TestORMModels:
    """Verify that each ORM model class is well-formed."""

    def test_all_models_have_tablename(self) -> None:
        for cls in _orm_model_classes():
            assert hasattr(cls, "__tablename__"), (
                f"{cls.__name__} is missing __tablename__"
            )

    def test_tablenames_are_unique(self) -> None:
        tablenames = [cls.__tablename__ for cls in _orm_model_classes()]
        assert len(tablenames) == len(set(tablenames)), (
            "Duplicate __tablename__ found"
        )

    def test_orm_tables_exist_in_metadata(self) -> None:
        """Every ORM model's table should be registered in DPM.metadata."""
        metadata_tables = set(DPM.metadata.tables.keys())
        for cls in _orm_model_classes():
            assert cls.__tablename__ in metadata_tables, (
                f"{cls.__name__}.__tablename__ = {cls.__tablename__!r} "
                f"not in metadata"
            )

    def test_every_orm_table_has_primary_key(self, empty_engine: Engine) -> None:
        """Each ORM table must define a primary key (column-level or mapper-level)."""
        inspector = sa_inspect(empty_engine)
        for cls in _orm_model_classes():
            pk = inspector.get_pk_constraint(cls.__tablename__)
            has_ddl_pk = bool(pk["constrained_columns"])
            # Some tables define PKs via __mapper_args__ instead of
            # mapped_column(primary_key=True), so DDL won't show them.
            has_mapper_pk = bool(
                getattr(cls, "__mapper_args__", {}).get("primary_key"),
            )
            assert has_ddl_pk or has_mapper_pk, (
                f"{cls.__name__} ({cls.__tablename__}) has no primary key"
            )


# ---------------------------------------------------------------------------
# Foreign key integrity
# ---------------------------------------------------------------------------


class TestForeignKeys:
    """Verify that foreign keys reference tables that actually exist."""

    def test_foreign_key_targets_exist(self, empty_engine: Engine) -> None:
        inspector = sa_inspect(empty_engine)
        all_tables = set(inspector.get_table_names())
        for table_name in all_tables:
            for fk in inspector.get_foreign_keys(table_name):
                assert fk["referred_table"] in all_tables, (
                    f"{table_name} has FK to non-existent "
                    f"table {fk['referred_table']}"
                )


# ---------------------------------------------------------------------------
# Queryability against the empty schema
# ---------------------------------------------------------------------------


class TestEmptySchemaQueries:
    """Verify that SELECT statements work for every ORM model on an empty db."""

    def test_select_all_orm_models(self, empty_session: Session) -> None:
        """A simple ``select(Model)`` must not raise for any model."""
        for cls in _orm_model_classes():
            result = empty_session.execute(select(cls)).all()
            assert result == [], f"Expected empty result for {cls.__name__}"

    def test_count_query(self, empty_session: Session) -> None:
        """``SELECT count(*) FROM <table>`` must work for all tables."""
        for table in DPM.metadata.sorted_tables:
            row = empty_session.execute(
                select(func.count()).select_from(table),
            ).scalar()
            assert row == 0
