"""Tests that the generated SQLAlchemy schema is structurally valid.

These tests inspect the model metadata directly — no database is created.
They verify that the ORM models are well-formed and internally consistent.
"""

from __future__ import annotations

import inspect

from dpm2 import models
from dpm2.models import DPM

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


# ---------------------------------------------------------------------------
# ORM model consistency
# ---------------------------------------------------------------------------


class TestORMModels:
    """Verify that each ORM model class is well-formed."""

    def test_models_discovered(self) -> None:
        """There should be at least one ORM model in the module."""
        assert len(_orm_model_classes()) > 0

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

    def test_every_model_has_primary_key(self) -> None:
        """Each ORM model must define a primary key (column-level or mapper-level)."""
        for cls in _orm_model_classes():
            table = DPM.metadata.tables[cls.__tablename__]
            has_ddl_pk = len(table.primary_key.columns) > 0
            has_mapper_pk = bool(
                getattr(cls, "__mapper_args__", {}).get("primary_key"),
            )
            assert has_ddl_pk or has_mapper_pk, (
                f"{cls.__name__} ({cls.__tablename__}) has no primary key"
            )

    def test_every_table_has_columns(self) -> None:
        """Every table in the metadata should have at least one column."""
        for table in DPM.metadata.tables.values():
            assert len(table.columns) > 0, f"{table.name} has no columns"


# ---------------------------------------------------------------------------
# Foreign key consistency (metadata-level)
# ---------------------------------------------------------------------------


class TestForeignKeys:
    """Verify that foreign keys reference tables that exist in the metadata."""

    def test_foreign_key_targets_exist(self) -> None:
        all_tables = set(DPM.metadata.tables.keys())
        for table in DPM.metadata.tables.values():
            for fk in table.foreign_keys:
                try:
                    target_table = fk.column.table.name
                except Exception:  # noqa: BLE001
                    # FK couldn't be resolved — extract from the target spec.
                    target_table = str(fk.target_fullname).split(".")[0]
                assert target_table in all_tables, (
                    f"{table.name} has FK to non-existent table {target_table}"
                )
